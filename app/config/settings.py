"""
Configuration management for FortiGate Policy Retriever.
Handles environment variables and application settings.
Loads configuration from .env file if available.
"""

import os
import logging
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

logger = logging.getLogger("fortigate_policy_retriever")

# Load environment variables from .env file if available
if DOTENV_AVAILABLE:
    # Try to find .env file in project root (where main.py is located)
    # First try current directory, then try parent directories up to 3 levels
    env_path = None
    current_dir = Path(__file__).parent.parent.parent  # Go up from app/config/settings.py to project root
    possible_paths = [
        Path(".env"),  # Current working directory
        current_dir / ".env",  # Project root
        Path(__file__).parent.parent / ".env",  # app/.env
    ]
    
    for path in possible_paths:
        if path.exists() and path.is_file():
            env_path = path
            break
    
    if env_path:
        load_dotenv(env_path, override=True)  # override=True ensures .env values take precedence
        logger.info(f"✅ Loaded environment variables from {env_path.absolute()}")
    else:
        logger.warning(
            f"No .env file found. Searched in: {[str(p) for p in possible_paths]}. "
            f"Using system environment variables only."
        )
else:
    logger.warning("python-dotenv not installed. Install it with: pip install python-dotenv")


@dataclass
class FortiGateConfig:
    """FortiGate firewall configuration."""
    ip_address: str
    api_token: Optional[str] = None
    verify_ssl: bool = False
    timeout: int = 30
    api_version: str = "v2"
    use_sample_data: bool = False
    
    @property
    def api_endpoint(self) -> str:
        """Construct the full API endpoint URL."""
        return f"https://{self.ip_address}/api/{self.api_version}/cmdb/firewall/policy"
    
    @property
    def is_configured(self) -> bool:
        """Check if FortiGate API is properly configured."""
        return self.api_token is not None and not self.use_sample_data
    
    @classmethod
    def from_env(cls) -> "FortiGateConfig":
        """
        Create configuration from environment variables.
        
        Returns:
            FortiGateConfig: Configured instance
        """
        ip_address = os.getenv("FORTIGATE_IP", "192.168.1.99")
        api_token = os.getenv("FGT_API_TOKEN")
        use_sample_data = os.getenv("USE_SAMPLE_DATA", "false").lower() == "true"
        
        # If no API token and not explicitly using sample data, check if sample data exists
        if not api_token and not use_sample_data:
            from pathlib import Path
            sample_file = Path("sampledata/sample_policies.json")
            if sample_file.exists():
                logger.warning(
                    "FGT_API_TOKEN not set and sample data found. "
                    "Will use sample data as fallback. Set USE_SAMPLE_DATA=true to suppress this warning."
                )
                use_sample_data = True
        
        verify_ssl = os.getenv("FORTIGATE_VERIFY_SSL", "false").lower() == "true"
        timeout = int(os.getenv("FORTIGATE_TIMEOUT", "30"))
        api_version = os.getenv("FORTIGATE_API_VERSION", "v2")
        
        return cls(
            ip_address=ip_address,
            api_token=api_token,
            verify_ssl=verify_ssl,
            timeout=timeout,
            api_version=api_version,
            use_sample_data=use_sample_data
        )
    
    @classmethod
    def from_dict(cls, config_dict: dict) -> "FortiGateConfig":
        """
        Create configuration from dictionary (e.g., from API request).
        
        Args:
            config_dict: Dictionary with configuration values
            
        Returns:
            FortiGateConfig: Configured instance
        """
        return cls(
            ip_address=config_dict.get("ip_address"),
            api_token=config_dict.get("api_token"),
            verify_ssl=config_dict.get("verify_ssl", False),
            timeout=config_dict.get("timeout", 30),
            api_version=config_dict.get("api_version", "v2"),
            use_sample_data=False
        )


@dataclass
class ClickHouseConfig:
    """ClickHouse database configuration."""
    host: str
    port: int
    database: str
    username: Optional[str] = None
    password: Optional[str] = None
    secure: bool = False
    verify: bool = False
    
    @classmethod
    def from_env(cls) -> "ClickHouseConfig":
        """
        Create ClickHouse configuration from environment variables.
        
        Returns:
            ClickHouseConfig: Configured instance
        """
        # Read environment variables with detailed logging
        host = os.getenv("CLICKHOUSE_HOST")
        if not host:
            host = "localhost"
            logger.warning("CLICKHOUSE_HOST not set in environment, using default 'localhost'")
        else:
            logger.debug(f"CLICKHOUSE_HOST from env: {host}")
        
        # Read port from environment
        port_env = os.getenv("CLICKHOUSE_PORT")
        if not port_env:
            port = 8123
            logger.warning("CLICKHOUSE_PORT not set in environment, using default 8123 (HTTP interface)")
        else:
            logger.debug(f"CLICKHOUSE_PORT from env: {port_env}")
            try:
                port = int(port_env)
            except ValueError:
                logger.error(f"Invalid CLICKHOUSE_PORT value '{port_env}', using default 8123")
                port = 8123
        
        # Log port information
        if port == 8123:
            logger.info("Using ClickHouse HTTP interface (port 8123)")
        elif port == 9000:
            logger.info("Using ClickHouse native protocol (port 9000)")
        else:
            logger.info(f"Using ClickHouse port {port}")
        
        # Read database name
        database = os.getenv("CLICKHOUSE_DATABASE")
        if not database:
            database = "firewall_configuration"
            logger.warning("CLICKHOUSE_DATABASE not set in environment, using default 'firewall_configuration'")
        else:
            database = database.strip()
            if database == "":
                logger.warning("CLICKHOUSE_DATABASE is empty, using default 'firewall_configuration'")
                database = "firewall_configuration"
            else:
                logger.debug(f"CLICKHOUSE_DATABASE from env: {database}")
        
        username = os.getenv("CLICKHOUSE_USER")
        password = os.getenv("CLICKHOUSE_PASSWORD")
        secure = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"
        verify = os.getenv("CLICKHOUSE_VERIFY", "false").lower() == "true"
        
        # Log final configuration (without password)
        logger.info(
            f"ClickHouse configuration - Host: {host}, Port: {port}, Database: {database}, "
            f"User: {username or 'default'}"
        )
        
        return cls(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            secure=secure,
            verify=verify
        )


@dataclass
class AppConfig:
    """Application-wide configuration."""
    fortigate: FortiGateConfig
    clickhouse: ClickHouseConfig
    output_file: str = "fortigate_policies.json"
    log_level: str = "INFO"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    sample_data_dir: str = "sampledata"
    
    @classmethod
    def from_env(cls) -> "AppConfig":
        """
        Create application configuration from environment variables.
        
        Returns:
            AppConfig: Configured instance
        """
        output_file = os.getenv("OUTPUT_FILE", "fortigate_policies.json")
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        api_host = os.getenv("API_HOST", "0.0.0.0")
        api_port = int(os.getenv("API_PORT", "8000"))
        sample_data_dir = os.getenv("SAMPLE_DATA_DIR", "sampledata")
        
        return cls(
            fortigate=FortiGateConfig.from_env(),
            clickhouse=ClickHouseConfig.from_env(),
            output_file=output_file,
            log_level=log_level,
            api_host=api_host,
            api_port=api_port,
            sample_data_dir=sample_data_dir
        )

