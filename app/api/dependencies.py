"""
Dependency injection for FastAPI routes.
"""

import logging
from functools import lru_cache
from typing import Optional

from app.config.settings import AppConfig
from app.clients.fortigate_client import FortiGateClient
from app.database.clickhouse_handler import ClickHouseHandler
from app.services.policy_service import PolicyService

logger = logging.getLogger("fortigate_policy_retriever")


@lru_cache()
def get_config() -> AppConfig:
    """
    Get application configuration (cached).
    
    Returns:
        AppConfig: Application configuration
    """
    try:
        return AppConfig.from_env()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        raise


def get_fortigate_client(config: AppConfig = None) -> Optional[FortiGateClient]:
    """
    Get FortiGate client instance (if configured).
    
    Args:
        config: Application configuration (optional, will fetch if not provided)
        
    Returns:
        FortiGateClient or None: FortiGate API client if configured, None otherwise
    """
    if config is None:
        config = get_config()
    
    # Only create client if API is properly configured
    if config.fortigate.is_configured:
        return FortiGateClient(config.fortigate)
    else:
        logger.info("FortiGate API not configured, will use sample data fallback")
        return None


def get_clickhouse_handler(config: AppConfig = None) -> ClickHouseHandler:
    """
    Get ClickHouse handler instance.
    
    Args:
        config: Application configuration (optional, will fetch if not provided)
        
    Returns:
        ClickHouseHandler: ClickHouse database handler
    """
    if config is None:
        config = get_config()
    return ClickHouseHandler(config.clickhouse)


def get_policy_service() -> PolicyService:
    """
    Get policy service instance.
    
    This function handles all internal dependencies and doesn't expose
    Optional types to FastAPI, avoiding dependency injection issues.
    
    Returns:
        PolicyService: Policy service instance
    """
    config = get_config()
    
    # Get FortiGate client (may be None if not configured)
    fortigate_client = get_fortigate_client(config)
    
    # Get ClickHouse handler
    clickhouse_handler = get_clickhouse_handler(config)
    
    # Create sample data loader
    from app.utils.sample_data_loader import SampleDataLoader
    sample_data_loader = SampleDataLoader(config.sample_data_dir)
    
    return PolicyService(
        fortigate_client=fortigate_client,
        clickhouse_handler=clickhouse_handler,
        sample_data_loader=sample_data_loader
    )

