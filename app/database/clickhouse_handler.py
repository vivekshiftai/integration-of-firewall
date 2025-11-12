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
    import clickhouse_connect
    from clickhouse_connect.driver.exceptions import ClickHouseError
except ImportError:
    raise ImportError(
        "clickhouse-connect is required. Install it with: pip install clickhouse-connect"
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
        self.client = None
        logger.info(
            f"Initialized ClickHouse handler - Host: {config.host}, Port: {config.port}, "
            f"Database: {config.database}, User: {config.username or 'default'}"
        )
    
    def _get_interface(self) -> str:
        """
        Determine the correct ClickHouse interface based on port and secure settings.
        
        Returns:
            str: Interface name ('http', 'https', or 'native')
        """
        if self.config.port == 8123:
            return 'https' if self.config.secure else 'http'
        elif self.config.port == 9000:
            return 'native'
        else:
            # For other ports, default to HTTP if secure, otherwise HTTP
            return 'https' if self.config.secure else 'http'
    
    def _create_client(self, database: Optional[str] = None) -> Any:
        """
        Create a ClickHouse client with proper interface configuration.
        
        Args:
            database: Optional database name
            
        Returns:
            ClickHouse client instance
        """
        interface = self._get_interface()
        logger.debug(f"Creating ClickHouse client with interface: {interface} for port {self.config.port}")
        
        try:
            # Try with interface parameter (for newer versions of clickhouse-connect)
            return clickhouse_connect.get_client(
                host=self.config.host,
                port=self.config.port,
                database=database if database else None,
                username=self.config.username,
                password=self.config.password,
                secure=self.config.secure,
                verify=self.config.verify,
                interface=interface
            )
        except TypeError:
            # If interface parameter is not supported (older version), try without it
            # clickhouse-connect should auto-detect based on port
            logger.warning("Interface parameter not supported, using auto-detection")
            return clickhouse_connect.get_client(
                host=self.config.host,
                port=self.config.port,
                database=database if database else None,
                username=self.config.username,
                password=self.config.password,
                secure=self.config.secure,
                verify=self.config.verify
            )
    
    def connect(self, database: Optional[str] = None) -> None:
        """
        Establish connection to ClickHouse database using HTTP interface.
        
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
            
            # Create client with proper interface configuration
            self.client = self._create_client(database=db_name)
            
            # Test connection
            self.client.command("SELECT 1")
            logger.info("Successfully connected to ClickHouse via HTTP interface")
            
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
            DatabaseError: If database creation fails or ClickHouse server is not accessible
        """
        try:
            logger.info(
                f"Ensuring database '{self.config.database}' exists on "
                f"ClickHouse server at {self.config.host}:{self.config.port}"
            )
            
            # First, try to connect to the target database to check if it exists
            temp_client = None
            try:
                logger.debug(f"Attempting to connect to database '{self.config.database}'")
                temp_client = self._create_client(database=self.config.database)
                temp_client.command("SELECT 1")
                # Database exists, we can use it
                logger.info(f"Database '{self.config.database}' already exists and is accessible")
                if self.client:
                    self.client.close()
                self.client = temp_client
                return
            except ClickHouseError as e:
                error_code = str(e)
                # Check if it's a connection error (server not running) vs database not found
                if "connection" in error_code.lower() or "refused" in error_code.lower() or "10061" in error_code or "81" in error_code:
                    # Error code 81 means database doesn't exist, which is expected
                    if "81" in error_code or "database" in error_code.lower() and "does not exist" in error_code.lower():
                        logger.info(f"Database '{self.config.database}' does not exist, creating it...")
                    else:
                        # This is a connection error - ClickHouse server is not accessible
                        error_msg = (
                            f"Failed to connect to ClickHouse server at {self.config.host}:{self.config.port}. "
                            f"Please ensure ClickHouse is running and accessible. Error: {e}"
                        )
                        logger.error(error_msg)
                        raise DatabaseError(error_msg) from e
                # Otherwise, assume database doesn't exist
                logger.info(f"Database '{self.config.database}' does not exist (error: {e}), creating it...")
                if temp_client:
                    try:
                        temp_client.close()
                    except:
                        pass
            
            # Connect to default database to create the target database
            logger.info(f"Connecting to 'default' database to create '{self.config.database}'")
            try:
                temp_client = self._create_client(database="default")
                temp_client.command("SELECT 1")  # Test connection
            except ClickHouseError as e:
                error_code = str(e)
                if "connection" in error_code.lower() or "refused" in error_code.lower() or "10061" in error_code:
                    error_msg = (
                        f"Failed to connect to ClickHouse server at {self.config.host}:{self.config.port}. "
                        f"ClickHouse server is not running or not accessible. "
                        f"Please check: 1) ClickHouse is running, 2) Host and port are correct, "
                        f"3) Network connectivity. Error: {e}"
                    )
                    logger.error(error_msg)
                    raise DatabaseError(error_msg) from e
                raise
            
            # Create the database
            logger.info(f"Creating database '{self.config.database}'")
            temp_client.command(f"CREATE DATABASE IF NOT EXISTS {self.config.database}")
            logger.info(f"Database '{self.config.database}' created successfully")
            
            # Close and reconnect to the target database
            temp_client.close()
            self.connect(database=self.config.database)
            
        except DatabaseError:
            # Re-raise DatabaseError as-is
            raise
        except ClickHouseError as e:
            error_msg = f"ClickHouse error while ensuring database exists: {e}"
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
            self.client.command(self.TABLE_SCHEMA)
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
    ) -> tuple[int, Optional[str]]:
        """
        Insert firewall configurations into ClickHouse.
        Stores the entire JSON array as a single row.
        
        Args:
            configs: List of configuration dictionaries (policies, rules, etc.)
            vendor_type: Vendor type (e.g., 'fortigate', 'zscaler', 'paloalto')
            device_id: Device identifier (IP address, hostname, etc.)
            device_name: Human-readable device name (optional)
            config_type: Type of configuration (e.g., 'policy', 'rule', 'configuration')
            metadata: Additional metadata dictionary (optional)
            version: Configuration version/revision (optional)
            
        Returns:
            tuple[int, Optional[str]]: Tuple of (number of configurations inserted, config_id UUID)
                                       Returns (0, None) if no configs to insert
                                       Returns (1, config_id) on successful insertion
            
        Raises:
            DatabaseError: If insertion fails
        """
        if not configs:
            logger.warning("No configurations to insert")
            return (0, None)
        
        try:
            logger.info(
                f"Inserting {len(configs)} {config_type}s as a single JSON object for vendor '{vendor_type}' "
                f"device '{device_id}' into ClickHouse"
            )
            
            # Store the entire configs as a single JSON object
            retrieved_at = datetime.now()
            device_name = device_name or device_id
            metadata_json = json.dumps(metadata or {}, ensure_ascii=False)
            
            # Convert entire configs to JSON string
            # If configs is a list with one item, use that item; otherwise use the entire list
            if len(configs) == 1:
                # Single JSON object/array - store it directly
                configs_json = json.dumps(configs[0], ensure_ascii=False)
            else:
                # Multiple items - store as array
                configs_json = json.dumps(configs, ensure_ascii=False)
            
            # Prepare single row with entire JSON array
            data_to_insert = [[
                vendor_type,
                device_id,
                device_name,
                config_type,
                configs_json,  # Entire JSON array as string
                metadata_json,
                version or "",
                retrieved_at
            ]]
            
            # Insert data - clickhouse-connect insert method
            table_name = 'firewall_configs'
            logger.debug(f"Inserting into table: {self.config.database}.{table_name}")
            logger.debug(f"Storing {len(configs)} configurations as a single JSON object")
            
            # clickhouse-connect insert method signature:
            # insert(table, data, database=None, column_names=None, column_types=None, settings=None)
            # data should be list of lists when using column_names
            self.client.insert(
                table_name,
                data_to_insert,
                database=self.config.database,
                column_names=['vendor_type', 'device_id', 'device_name', 'config_type',
                             'config_json', 'metadata', 'version', 'retrieved_at']
            )
            logger.info(f"Successfully inserted {len(configs)} configurations as a single JSON object")
            
            # Retrieve the inserted row's ID by querying the most recent insertion
            # We use vendor_type, device_id, and config_type to identify the row
            # and order by created_at DESC to get the most recent one
            try:
                # Safely escape string values for ClickHouse
                def escape_string(value: str) -> str:
                    """Escape single quotes for ClickHouse string literals."""
                    return value.replace("'", "''")
                
                escaped_vendor = escape_string(vendor_type)
                escaped_device = escape_string(device_id)
                escaped_config_type = escape_string(config_type)
                
                # Query to get the ID of the most recently inserted row for this combination
                # We match on vendor_type, device_id, and config_type, then order by created_at DESC
                # This ensures we get the row we just inserted (most recent)
                query = f"""
                    SELECT id
                    FROM firewall_configs
                    WHERE vendor_type = '{escaped_vendor}'
                      AND device_id = '{escaped_device}'
                      AND config_type = '{escaped_config_type}'
                    ORDER BY created_at DESC, retrieved_at DESC
                    LIMIT 1
                """
                
                result = self.client.query(query)
                if result.result_rows:
                    config_id = str(result.result_rows[0][0])
                    logger.info(f"Retrieved config_id: {config_id} for inserted configuration")
                    return (1, config_id)
                else:
                    logger.warning("Could not retrieve config_id after insertion, but insertion succeeded")
                    return (1, None)
                    
            except Exception as e:
                logger.warning(f"Failed to retrieve config_id after insertion: {e}, but insertion succeeded")
                # Insertion succeeded, but we couldn't get the ID
                return (1, None)
            
        except ClickHouseError as e:
            error_msg = f"Failed to insert configurations: {e}"
            logger.error(error_msg)
            raise DatabaseError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error inserting configurations: {e}"
            logger.error(error_msg)
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise DatabaseError(error_msg) from e
    
    def insert_policies(
        self,
        policies: List[Dict],
        vendor_type: str = "fortigate",
        device_id: Optional[str] = None,
        device_name: Optional[str] = None
    ) -> tuple[int, Optional[str]]:
        """
        Insert firewall policies into ClickHouse (backward compatibility method).
        
        Args:
            policies: List of firewall policy dictionaries
            vendor_type: Vendor type (default: 'fortigate')
            device_id: Device identifier (optional, will use 'unknown' if not provided)
            device_name: Human-readable device name (optional)
            
        Returns:
            tuple[int, Optional[str]]: Tuple of (number of policies inserted, config_id UUID)
            
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
            
            result = self.client.query(query)
            # clickhouse-connect returns result as a QueryResult object
            # Access the first row, first column for count
            if result.result_rows:
                return result.result_rows[0][0]
            return 0
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
    
    def get_config_by_id(self, config_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a configuration by its UUID ID.
        
        Args:
            config_id: UUID string of the configuration to retrieve
            
        Returns:
            Dict[str, Any] or None: Configuration dictionary with all fields if found, None otherwise
            
        Raises:
            DatabaseError: If database query fails
            ValueError: If config_id is not a valid UUID format
        """
        import uuid
        
        # Validate UUID format
        try:
            uuid.UUID(config_id)
        except ValueError:
            error_msg = f"Invalid UUID format: {config_id}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        try:
            logger.info(f"Retrieving configuration with ID: {config_id}")
            
            # Query to fetch configuration by ID
            # Using UUID type casting for proper comparison
            # ClickHouse UUID type requires proper format, so we validate and use it directly
            query = f"""
                SELECT 
                    id,
                    vendor_type,
                    device_id,
                    device_name,
                    config_type,
                    config_json,
                    metadata,
                    version,
                    created_at,
                    updated_at,
                    retrieved_at
                FROM firewall_configs
                WHERE id = toUUID('{config_id}')
                LIMIT 1
            """
            
            # Execute query
            result = self.client.query(query)
            
            if not result.result_rows:
                logger.warning(f"Configuration with ID {config_id} not found")
                return None
            
            # Extract row data
            row = result.result_rows[0]
            column_names = result.column_names
            
            # Build dictionary from row data
            config_dict = dict(zip(column_names, row))
            
            # Parse JSON strings back to dictionaries
            try:
                if config_dict.get("config_json"):
                    config_dict["config_json"] = json.loads(config_dict["config_json"])
                    
                    # Log information about the retrieved rules/policies
                    config_data = config_dict["config_json"]
                    if isinstance(config_data, dict):
                        # Check for common policy/rule keys
                        if "policies" in config_data:
                            policies = config_data["policies"]
                            if isinstance(policies, list):
                                logger.info(f"Retrieved configuration with {len(policies)} policies/rules")
                            else:
                                logger.info("Retrieved configuration with policies/rules (non-list format)")
                        elif "policy" in config_data:
                            policy_data = config_data["policy"]
                            if isinstance(policy_data, list):
                                logger.info(f"Retrieved configuration with {len(policy_data)} policies/rules")
                            else:
                                logger.info("Retrieved configuration with policy/rule (single object)")
                        else:
                            # Check if it's a list of policies directly
                            if isinstance(config_data, list):
                                logger.info(f"Retrieved configuration with {len(config_data)} policies/rules")
                            else:
                                logger.info("Retrieved configuration data (structure may vary by vendor)")
                    elif isinstance(config_data, list):
                        logger.info(f"Retrieved configuration with {len(config_data)} policies/rules")
                    
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"Failed to parse config_json for ID {config_id}: {e}")
                # Keep as string if parsing fails
            
            try:
                if config_dict.get("metadata"):
                    config_dict["metadata"] = json.loads(config_dict["metadata"])
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"Failed to parse metadata for ID {config_id}: {e}")
                # Keep as string if parsing fails
            
            logger.info(f"Successfully retrieved configuration with ID: {config_id}")
            return config_dict
            
        except ClickHouseError as e:
            error_msg = f"Failed to retrieve configuration by ID {config_id}: {e}"
            logger.error(error_msg)
            raise DatabaseError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error retrieving configuration by ID {config_id}: {e}"
            logger.error(error_msg)
            raise DatabaseError(error_msg) from e
    
    def close(self) -> None:
        """Close database connection."""
        if self.client:
            self.client.close()
            logger.debug("ClickHouse connection closed")

