"""
Data processing utilities for firewall policies.
Handles data transformation and file operations.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger("fortigate_policy_retriever")


class DataProcessor:
    """Handles data processing and file operations for firewall policies."""
    
    @staticmethod
    def save_to_json(
        configs: List[Dict],
        filepath: str,
        vendor_type: str = "fortigate",
        device_id: str = "unknown",
        device_name: Optional[str] = None,
        config_type: str = "policy",
        metadata: Optional[Dict[str, Any]] = None,
        version: Optional[str] = None
    ) -> None:
        """
        Save configurations to a JSON file in the same format as database storage.
        
        Args:
            configs: List of configuration dictionaries (policies, rules, etc.)
            filepath: Path to output file
            vendor_type: Vendor type (e.g., 'fortigate', 'zscaler', 'paloalto')
            device_id: Device identifier (IP address, hostname, etc.)
            device_name: Human-readable device name (optional)
            config_type: Type of configuration (e.g., 'policy', 'rule', 'configuration')
            metadata: Additional metadata dictionary (optional)
            version: Configuration version/revision (optional)
            
        Raises:
            IOError: If file write fails
        """
        try:
            output_path = Path(filepath)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            logger.info(
                f"Saving {len(configs)} {config_type}s for vendor '{vendor_type}' "
                f"device '{device_id}' to {filepath}"
            )
            
            # Format data to match database structure exactly
            # Each config becomes a separate entry matching a database row
            retrieved_at = datetime.now().isoformat()
            device_name = device_name or device_id
            metadata_dict = metadata or {}
            
            # Create array where each element represents one database row
            output_data = []
            
            # Format each config to match database row structure exactly
            # Both config_json and metadata are stored as JSON strings in the database
            for config in configs:
                db_row = {
                    "vendor_type": vendor_type,
                    "device_id": device_id,
                    "device_name": device_name,
                    "config_type": config_type,
                    "config_json": json.dumps(config, ensure_ascii=False),  # Store as JSON string (like in DB)
                    "metadata": json.dumps(metadata_dict, ensure_ascii=False),  # Metadata as JSON string (like in DB)
                    "version": version or "",
                    "retrieved_at": retrieved_at
                }
                output_data.append(db_row)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Successfully saved {len(configs)} configurations to {filepath}")
            
        except IOError as e:
            error_msg = f"Failed to write to file {filepath}: {e}"
            logger.error(error_msg)
            raise IOError(error_msg) from e
    
    @staticmethod
    def format_summary(policies: List[Dict]) -> Dict:
        """
        Format summary of retrieved policies.
        
        Args:
            policies: List of firewall policy dictionaries
            
        Returns:
            Dict: Summary dictionary
        """
        total_policies = len(policies)
        
        summary = {
            "total_policies": total_policies,
            "sample_policies": []
        }
        
        if total_policies > 0:
            for policy in policies[:5]:
                policy_name = policy.get("name", "Unnamed")
                policy_id = policy.get("policyid", "N/A")
                src_intf = policy.get("srcintf", [])
                dst_intf = policy.get("dstintf", [])
                action = policy.get("action", "N/A")
                
                src_interfaces = DataProcessor._format_interfaces(src_intf)
                dst_interfaces = DataProcessor._format_interfaces(dst_intf)
                
                summary["sample_policies"].append({
                    "name": policy_name,
                    "policy_id": policy_id,
                    "source_interface": src_interfaces,
                    "destination_interface": dst_interfaces,
                    "action": action
                })
        
        return summary
    
    @staticmethod
    def _format_interfaces(interfaces: any) -> str:
        """
        Format interface field for display.
        
        Args:
            interfaces: Interface field (can be list, dict, or string)
            
        Returns:
            str: Formatted interface string
        """
        if isinstance(interfaces, list):
            names = [item.get("name", str(item)) if isinstance(item, dict) else str(item) for item in interfaces]
            return ", ".join(names) if names else "N/A"
        elif isinstance(interfaces, dict):
            return interfaces.get("name", "N/A")
        elif interfaces:
            return str(interfaces)
        else:
            return "N/A"

