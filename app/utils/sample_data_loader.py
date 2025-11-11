"""
Sample Data Loader.
Loads sample firewall policies from JSON files when API is unavailable.
"""

import json
import logging
from pathlib import Path
from typing import List, Dict, Optional

logger = logging.getLogger("fortigate_policy_retriever")


class SampleDataLoader:
    """Handles loading sample firewall policies from JSON files."""
    
    def __init__(self, sample_data_dir: str = "sampledata"):
        """
        Initialize sample data loader.
        
        Args:
            sample_data_dir: Directory containing sample data files
        """
        self.sample_data_dir = Path(sample_data_dir)
        logger.info(f"Initialized SampleDataLoader with directory: {sample_data_dir}")
    
    def load_sample_policies(self, filename: str = "sample_policies.json") -> List[Dict]:
        """
        Load sample policies from JSON file.
        Supports both formats:
        1. Array of policies: [{"policyid": 1, ...}, ...]
        2. Full configuration: {"policies": [...], "system_configuration": {...}, ...}
        
        Args:
            filename: Name of the sample data file
            
        Returns:
            List[Dict]: List of firewall policy dictionaries
            
        Raises:
            FileNotFoundError: If sample data file doesn't exist
            json.JSONDecodeError: If file contains invalid JSON
        """
        file_path = self.sample_data_dir / filename
        
        if not file_path.exists():
            error_msg = f"Sample data file not found: {file_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        try:
            logger.info(f"Loading sample policies from {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Handle full configuration format (like fortinet-config.json)
            if isinstance(data, dict):
                if "policies" in data:
                    policies = data["policies"]
                    logger.info(
                        f"Loaded full configuration format. Found {len(policies)} policies "
                        f"in configuration file"
                    )
                elif "policy" in data:
                    # Handle singular "policy" key
                    policy_data = data["policy"]
                    policies = policy_data if isinstance(policy_data, list) else [policy_data]
                    logger.info(f"Loaded {len(policies)} policies from configuration")
                else:
                    # If it's a dict but no policies key, treat as single policy
                    logger.warning("Configuration file is a dict but no 'policies' key found. Treating as single policy.")
                    policies = [data]
            # Handle array format (list of policies)
            elif isinstance(data, list):
                policies = data
                logger.info(f"Loaded array format with {len(policies)} policies")
            else:
                logger.warning(f"Unexpected data type: {type(data)}. Converting to list.")
                policies = []
            
            # Ensure policies is a list
            if not isinstance(policies, list):
                policies = [policies] if policies else []
            
            logger.info(f"Successfully loaded {len(policies)} sample policies")
            return policies
            
        except json.JSONDecodeError as e:
            error_msg = f"Failed to parse sample data JSON: {e}"
            logger.error(f"{error_msg} File: {file_path}")
            raise json.JSONDecodeError(error_msg, e.doc, e.pos) from e
        except Exception as e:
            error_msg = f"Unexpected error loading sample data: {e}"
            logger.error(f"{error_msg} File: {file_path}")
            raise
    
    def load_full_config(self, filename: str = "fortinet-config.json") -> Optional[Dict]:
        """
        Load full configuration from JSON file.
        
        Args:
            filename: Name of the configuration file
            
        Returns:
            Dict: Full configuration dictionary, or None if file doesn't exist
            
        Raises:
            json.JSONDecodeError: If file contains invalid JSON
        """
        file_path = self.sample_data_dir / filename
        
        if not file_path.exists():
            logger.warning(f"Configuration file not found: {file_path}")
            return None
        
        try:
            logger.info(f"Loading full configuration from {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            if not isinstance(config, dict):
                logger.warning("Configuration file is not a dictionary")
                return None
            
            logger.info("Successfully loaded full configuration")
            return config
            
        except json.JSONDecodeError as e:
            error_msg = f"Failed to parse configuration JSON: {e}"
            logger.error(f"{error_msg} File: {file_path}")
            raise json.JSONDecodeError(error_msg, e.doc, e.pos) from e
        except Exception as e:
            error_msg = f"Unexpected error loading configuration: {e}"
            logger.error(f"{error_msg} File: {file_path}")
            raise
    
    def get_available_samples(self) -> List[str]:
        """
        Get list of available sample data files.
        
        Returns:
            List[str]: List of available sample file names
        """
        if not self.sample_data_dir.exists():
            logger.warning(f"Sample data directory does not exist: {self.sample_data_dir}")
            return []
        
        json_files = list(self.sample_data_dir.glob("*.json"))
        return [f.name for f in json_files]
    
    def is_sample_data_available(self, filename: str = "sample_policies.json") -> bool:
        """
        Check if sample data file is available.
        
        Args:
            filename: Name of the sample data file
            
        Returns:
            bool: True if file exists, False otherwise
        """
        file_path = self.sample_data_dir / filename
        exists = file_path.exists()
        if not exists:
            logger.debug(f"Sample data file not found: {file_path} (resolved from {self.sample_data_dir} / {filename})")
        return exists

