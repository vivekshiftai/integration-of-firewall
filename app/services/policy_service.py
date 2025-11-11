"""
Policy Service.
Orchestrates the retrieval and storage of firewall policies.
"""

import logging
from typing import Dict, List, Optional

from app.clients.fortigate_client import FortiGateClient
from app.database.clickhouse_handler import ClickHouseHandler
from app.utils.data_processor import DataProcessor
from app.utils.sample_data_loader import SampleDataLoader
from app.core.exceptions import FortiGateAPIError, DatabaseError


logger = logging.getLogger("fortigate_policy_retriever")


class PolicyService:
    """Service for managing firewall policy operations."""
    
    def __init__(
        self,
        fortigate_client: Optional[FortiGateClient] = None,
        clickhouse_handler: Optional[ClickHouseHandler] = None,
        sample_data_loader: Optional[SampleDataLoader] = None
    ):
        """
        Initialize policy service.
        
        Args:
            fortigate_client: Optional FortiGate API client
            clickhouse_handler: Optional ClickHouse database handler
            sample_data_loader: Optional sample data loader for fallback
        """
        self.fortigate_client = fortigate_client
        self.clickhouse_handler = clickhouse_handler
        self.sample_data_loader = sample_data_loader or SampleDataLoader()
        logger.info("Initialized PolicyService")
    
    def fetch_and_store_policies(
        self,
        store_in_db: bool = True,
        use_sample_data: bool = False,
        firewall_config: Optional[Dict] = None,
        vendor_type: Optional[str] = None,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None
    ) -> Dict:
        """
        Fetch policies from FortiGate (or sample data) and store them.
        
        Args:
            store_in_db: Whether to store policies in database
            use_sample_data: Force use of sample data instead of API
            firewall_config: Optional firewall configuration dict (overrides .env config)
            vendor_type: Optional vendor type (overrides default)
            device_id: Optional device ID (overrides default)
            device_name: Optional device name (overrides default)
            
        Returns:
            Dict: Result dictionary with status and summary
            
        Raises:
            FortiGateAPIError: If fetching policies fails (when not using sample data)
            DatabaseError: If database operations fail
        """
        result = {
            "success": False,
            "policies_count": 0,
            "db_stored": False,
            "db_count": 0,
            "summary": None,
            "error": None,
            "data_source": "api"  # "api" or "sample"
        }
        
        policies = []
        use_fallback = False
        temp_client = None
        
        # Determine which client to use (endpoint config takes priority)
        client_to_use = None
        if firewall_config:
            # Create temporary client from endpoint config
            from app.config.settings import FortiGateConfig
            fgt_config = FortiGateConfig.from_dict(firewall_config)
            temp_client = FortiGateClient(fgt_config)
            client_to_use = temp_client
            logger.info(f"Using firewall configuration from endpoint: {firewall_config.get('ip_address')}")
        elif self.fortigate_client:
            # Use default client from .env
            client_to_use = self.fortigate_client
            logger.info("Using firewall configuration from environment variables")
        
        # Determine vendor and device info (endpoint params take priority)
        final_vendor_type = vendor_type or "fortigate"
        final_device_id = device_id
        final_device_name = device_name
        
        if firewall_config:
            final_device_id = device_id or firewall_config.get("device_id") or firewall_config.get("ip_address")
            final_device_name = device_name or firewall_config.get("device_name") or f"{final_vendor_type}-{final_device_id}"
        elif client_to_use:
            final_device_id = device_id or client_to_use.config.ip_address
            final_device_name = device_name or f"{final_vendor_type}-{final_device_id}"
        else:
            final_device_id = device_id or "unknown"
            final_device_name = device_name or f"{final_vendor_type}-{final_device_id}"
        
        try:
            # Try to fetch from API first (unless explicitly using sample data)
            if not use_sample_data and client_to_use:
                try:
                    logger.info("Attempting to fetch policies from firewall API")
                    policies = client_to_use.fetch_policies()
                    result["data_source"] = "api"
                    logger.info(f"Successfully fetched {len(policies)} policies from API")
                except (FortiGateAPIError, Exception) as e:
                    logger.warning(f"Failed to fetch from API: {e}")
                    logger.info("Falling back to sample data")
                    use_fallback = True
            
            # Use sample data if API failed or not configured
            if use_sample_data or use_fallback or not client_to_use:
                try:
                    logger.info("Loading policies from sample data")
                    policies = None
                    
                    # Try fortinet-config.json first (full config format), then fall back to sample_policies.json
                    sample_files = ["fortinet-config.json", "sample_policies.json"]
                    for filename in sample_files:
                        if self.sample_data_loader.is_sample_data_available(filename):
                            logger.info(f"Found {filename}, attempting to load policies")
                            try:
                                policies = self.sample_data_loader.load_sample_policies(filename)
                                logger.info(f"Successfully loaded {len(policies)} policies from {filename}")
                                break
                            except Exception as e:
                                logger.warning(f"Failed to load {filename}: {e}, trying next file")
                                continue
                    
                    if policies is None:
                        error_msg = (
                            "No API configured and no valid sample data found. "
                            "Please configure FGT_API_TOKEN or add sample data to "
                            "sampledata/fortinet-config.json or sampledata/sample_policies.json"
                        )
                        logger.error(error_msg)
                        result["error"] = error_msg
                        result["success"] = False
                        return result
                    
                    result["data_source"] = "sample"
                    logger.info(f"Successfully loaded {len(policies)} policies from sample data")
                except Exception as e:
                    error_msg = f"Failed to load sample data: {e}"
                    logger.error(error_msg)
                    result["error"] = error_msg
                    result["success"] = False
                    return result
            
            result["policies_count"] = len(policies)
            
            if not policies:
                logger.warning("No policies retrieved")
                result["success"] = True
                return result
            
            # Prepare metadata
            metadata = {
                "data_source": result["data_source"],
                "total_policies": len(policies)
            }
            
            # Store in database
            if store_in_db and self.clickhouse_handler:
                try:
                    self.clickhouse_handler.connect()
                    self.clickhouse_handler.ensure_database_exists()
                    self.clickhouse_handler.create_table()
                    
                    inserted_count = self.clickhouse_handler.insert_configs(
                        configs=policies,
                        vendor_type=final_vendor_type,
                        device_id=final_device_id,
                        device_name=final_device_name,
                        config_type="policy",
                        metadata=metadata
                    )
                    result["db_stored"] = True
                    result["db_count"] = inserted_count
                    
                    total_count = self.clickhouse_handler.get_policy_count()
                    logger.info(f"Total policies in database: {total_count}")
                    
                except DatabaseError as e:
                    logger.error(f"Database operation failed: {e}")
                    result["error"] = f"Database operation failed: {e}"
                    # Continue execution even if database fails
            
            # Generate summary
            result["summary"] = DataProcessor.format_summary(policies)
            result["success"] = True
            
            logger.info("Policy fetch and store operation completed successfully")
            return result
            
        except FortiGateAPIError as e:
            logger.error(f"FortiGate API error: {e}")
            result["error"] = str(e)
            raise
        except Exception as e:
            logger.exception(f"Unexpected error in fetch_and_store_policies: {e}")
            result["error"] = str(e)
            raise

