"""
ClickHouse Database Handler.
Manages database connections, schema creation, and data insertion.
Supports multiple firewall vendors with a vendor-agnostic schema.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import json

try:
    from clickhouse_driver import Client
    from clickhouse_driver.errors import Error as ClickHouseError
except ImportError:
    raise ImportError(
        "clickhouse-driver is required. Install it with: pip install clickhouse-driver"
    )

from app.config.settings import ClickHouseConfig
from app.core.exceptions import DatabaseError


logger = logging.getLogger("fortigate_policy_retriever")


class ClickHouseHandler:
    """
    Handler for ClickHouse database operations.
    
    Manages connection, schema creation, and data insertion for firewall configurations.
    Uses a vendor-agnostic schema that supports multiple firewall vendors.
    """
    
    # Vendor-agnostic ClickHouse table schema for firewall configurations
    TABLE_SCHEMA = """
    CREATE TABLE IF NOT EXISTS firewall_configs (
        id UUID DEFAULT generateUUIDv4(),
        vendor_type String,
        device_id String,
        device_name String,
        config_type String,
        config_json String,
        metadata String,
        version String,
        created_at DateTime DEFAULT now(),
        updated_at DateTime DEFAULT now(),
        retrieved_at DateTime,
        INDEX idx_vendor_type vendor_type TYPE bloom_filter GRANULARITY 1,
        INDEX idx_device_id device_id TYPE bloom_filter GRANULARITY 1,
        INDEX idx_config_type config_type TYPE bloom_filter GRANULARITY 1
    ) ENGINE = MergeTree()
    ORDER BY (vendor_type, device_id, retrieved_at)
    PARTITION BY toYYYYMM(retrieved_at)
    """
    
    def __init__(self, config: ClickHouseConfig):
        """
        Initialize ClickHouse handler.
        
        Args:
            config: ClickHouse configuration object
        """
        self.config = config
        self.client: Optional[Client] = None
        logger.info(f"Initialized ClickHouse handler for {config.host}:{config.port}")
    
    def connect(self, database: Optional[str] = None) -> None:
        """
        Establish connection to ClickHouse database.
        
        Args:
            database: Optional database name. If None, uses self.config.database.
                     If empty string, connects without specifying a database.
        
        Raises:
            DatabaseError: If connection fails
        """
        try:
            db_name = database if database is not None else self.config.database
            logger.info(f"Connecting to ClickHouse at {self.config.host}:{self.config.port}" + 
                       (f" (database: {db_name})" if db_name else " (no database specified)"))
            
            self.client = Client(
                host=self.config.host,
                port=self.config.port,
                database=db_name if db_name else None,
                user=self.config.username,
                password=self.config.password,
                secure=self.config.secure,
                verify=self.config.verify
            )
            
            # Test connection
            self.client.execute("SELECT 1")
            logger.info("Successfully connected to ClickHouse")
            
        except ClickHouseError as e:
            error_msg = f"Failed to connect to ClickHouse: {e}"
            logger.error(error_msg)
            raise DatabaseError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error connecting to ClickHouse: {e}"
            logger.error(error_msg)
            raise DatabaseError(error_msg) from e
    
    def ensure_database_exists(self) -> None:
        """
        Ensure the database exists, create if it doesn't.
        
        This method connects to the default database first, creates the target database
        if it doesn't exist, then reconnects to the target database.
        
        Raises:
            DatabaseError: If database creation fails
        """
        try:
            logger.info(f"Ensuring database '{self.config.database}' exists")
            
            # First, connect without specifying a database (or to 'default' database)
            # to check if target database exists
            temp_client = None
            try:
                # Try to connect to the target database first
                temp_client = Client(
                    host=self.config.host,
                    port=self.config.port,
                    database=self.config.database,
                    user=self.config.username,
                    password=self.config.password,
                    secure=self.config.secure,
                    verify=self.config.verify
                )
                temp_client.execute("SELECT 1")
                # Database exists, we can use it
                logger.info(f"Database '{self.config.database}' already exists")
                if self.client:
                    self.client.disconnect()
                self.client = temp_client
                return
            except ClickHouseError:
                # Database doesn't exist, need to create it
                logger.info(f"Database '{self.config.database}' does not exist, creating it...")
                if temp_client:
                    temp_client.disconnect()
            
            # Connect to default database to create the target database
            temp_client = Client(
                host=self.config.host,
                port=self.config.port,
                database="default",
                user=self.config.username,
                password=self.config.password,
                secure=self.config.secure,
                verify=self.config.verify
            )
            
            # Create the database
            temp_client.execute(f"CREATE DATABASE IF NOT EXISTS {self.config.database}")
            logger.info(f"Database '{self.config.database}' created successfully")
            
            # Disconnect and reconnect to the target database
            temp_client.disconnect()
            self.connect(database=self.config.database)
            
        except ClickHouseError as e:
            error_msg = f"Failed to create database: {e}"
            logger.error(error_msg)
            raise DatabaseError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error ensuring database exists: {e}"
            logger.error(error_msg)
            raise DatabaseError(error_msg) from e
    
    def create_table(self) -> None:
        """
        Create the firewall_configs table if it doesn't exist.
        
        Raises:
            DatabaseError: If table creation fails
        """
        try:
            logger.info("Creating firewall_configs table if not exists")
            self.client.execute(self.TABLE_SCHEMA)
            logger.info("Table 'firewall_configs' is ready")
        except ClickHouseError as e:
            error_msg = f"Failed to create table: {e}"
            logger.error(error_msg)
            raise DatabaseError(error_msg) from e
    
    def insert_configs(
        self,
        configs: List[Dict],
        vendor_type: str,
        device_id: str,
        device_name: Optional[str] = None,
        config_type: str = "policy",
        metadata: Optional[Dict[str, Any]] = None,
        version: Optional[str] = None
    ) -> int:
        """
        Insert firewall configurations into ClickHouse.
        
        Args:
            configs: List of configuration dictionaries (policies, rules, etc.)
            vendor_type: Vendor type (e.g., 'fortigate', 'zscaler', 'paloalto')
            device_id: Device identifier (IP address, hostname, etc.)
            device_name: Human-readable device name (optional)
            config_type: Type of configuration (e.g., 'policy', 'rule', 'configuration')
            metadata: Additional metadata dictionary (optional)
            version: Configuration version/revision (optional)
            
        Returns:
            int: Number of configurations inserted
            
        Raises:
            DatabaseError: If insertion fails
        """
        if not configs:
            logger.warning("No configurations to insert")
            return 0
        
        try:
            logger.info(
                f"Inserting {len(configs)} {config_type}s for vendor '{vendor_type}' "
                f"device '{device_id}' into ClickHouse"
            )
            
            # Prepare data for insertion
            rows = []
            retrieved_at = datetime.now()
            device_name = device_name or device_id
            metadata_json = json.dumps(metadata or {}, ensure_ascii=False)
            
            for config in configs:
                row = self._normalize_config(
                    config=config,
                    vendor_type=vendor_type,
                    device_id=device_id,
                    device_name=device_name,
                    config_type=config_type,
                    metadata_json=metadata_json,
                    version=version,
                    retrieved_at=retrieved_at
                )
                rows.append(row)
            
            # Batch insert
            self.client.execute(
                """
                INSERT INTO firewall_configs (
                    vendor_type, device_id, device_name, config_type,
                    config_json, metadata, version, retrieved_at
                ) VALUES
                """,
                rows
            )
            
            logger.info(f"Successfully inserted {len(rows)} configurations")
            return len(rows)
            
        except ClickHouseError as e:
            error_msg = f"Failed to insert configurations: {e}"
            logger.error(error_msg)
            raise DatabaseError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error inserting configurations: {e}"
            logger.error(error_msg)
            raise DatabaseError(error_msg) from e
    
    def insert_policies(
        self,
        policies: List[Dict],
        vendor_type: str = "fortigate",
        device_id: Optional[str] = None,
        device_name: Optional[str] = None
    ) -> int:
        """
        Insert firewall policies into ClickHouse (backward compatibility method).
        
        Args:
            policies: List of firewall policy dictionaries
            vendor_type: Vendor type (default: 'fortigate')
            device_id: Device identifier (optional, will use 'unknown' if not provided)
            device_name: Human-readable device name (optional)
            
        Returns:
            int: Number of policies inserted
            
        Raises:
            DatabaseError: If insertion fails
        """
        device_id = device_id or "unknown"
        return self.insert_configs(
            configs=policies,
            vendor_type=vendor_type,
            device_id=device_id,
            device_name=device_name,
            config_type="policy"
        )
    
    def _normalize_config(
        self,
        config: Dict,
        vendor_type: str,
        device_id: str,
        device_name: str,
        config_type: str,
        metadata_json: str,
        version: Optional[str],
        retrieved_at: datetime
    ) -> tuple:
        """
        Normalize configuration data for database insertion.
        
        Args:
            config: Configuration dictionary (policy, rule, etc.)
            vendor_type: Vendor type
            device_id: Device identifier
            device_name: Device name
            config_type: Type of configuration
            metadata_json: Metadata as JSON string
            version: Configuration version
            retrieved_at: Timestamp when configuration was retrieved
            
        Returns:
            tuple: Normalized configuration data as tuple
        """
        # Store full config as JSON string
        config_json = json.dumps(config, ensure_ascii=False)
        
        return (
            vendor_type,
            device_id,
            device_name,
            config_type,
            config_json,
            metadata_json,
            version or "",
            retrieved_at
        )
    
    def get_config_count(
        self,
        vendor_type: Optional[str] = None,
        device_id: Optional[str] = None,
        config_type: Optional[str] = None
    ) -> int:
        """
        Get total count of configurations in database.
        
        Args:
            vendor_type: Filter by vendor type (optional)
            device_id: Filter by device ID (optional)
            config_type: Filter by config type (optional)
            
        Returns:
            int: Number of configurations
        """
        try:
            query = "SELECT COUNT(*) FROM firewall_configs"
            conditions = []
            
            # Safely escape and quote string values for ClickHouse
            def escape_string(value: str) -> str:
                """Escape single quotes for ClickHouse string literals."""
                return value.replace("'", "''")
            
            if vendor_type:
                escaped_value = escape_string(vendor_type)
                conditions.append(f"vendor_type = '{escaped_value}'")
            if device_id:
                escaped_value = escape_string(device_id)
                conditions.append(f"device_id = '{escaped_value}'")
            if config_type:
                escaped_value = escape_string(config_type)
                conditions.append(f"config_type = '{escaped_value}'")
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            result = self.client.execute(query)
            return result[0][0] if result else 0
        except ClickHouseError as e:
            logger.warning(f"Failed to get config count: {e}")
            return 0
    
    def get_policy_count(self) -> int:
        """
        Get total count of policies in database (backward compatibility method).
        
        Returns:
            int: Number of policies
        """
        return self.get_config_count(config_type="policy")
    
    def close(self) -> None:
        """Close database connection."""
        if self.client:
            self.client.disconnect()
            logger.debug("ClickHouse connection closed")

