"""
FortiGate API Client.
Handles authentication and communication with FortiGate firewall REST API.
"""

import json
import logging
from typing import Dict, List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from app.config.settings import FortiGateConfig
from app.core.exceptions import FortiGateAPIError


logger = logging.getLogger("fortigate_policy_retriever")


class FortiGateClient:
    """
    Client for interacting with FortiGate REST API.
    
    Provides methods to authenticate and retrieve firewall policies.
    """
    
    def __init__(self, config: FortiGateConfig):
        """
        Initialize FortiGate client.
        
        Args:
            config: FortiGate configuration object
        """
        self.config = config
        self.session = self._create_session()
        logger.info(f"Initialized FortiGate client for {config.ip_address}")
    
    def _create_session(self) -> requests.Session:
        """
        Create a requests session with authentication and retry strategy.
        
        Returns:
            requests.Session: Configured session object
        """
        session = requests.Session()
        
        # Set authentication headers
        session.headers.update({
            "Authorization": f"Bearer {self.config.api_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        })
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        return session
    
    def fetch_policies(self) -> List[Dict]:
        """
        Fetch all firewall policies from FortiGate.
        
        Returns:
            List[Dict]: List of firewall policy dictionaries
            
        Raises:
            FortiGateAPIError: If API request fails, connection fails, or timeout occurs
        """
        try:
            logger.info(f"Fetching firewall policies from {self.config.ip_address}")
            logger.debug(f"API endpoint: {self.config.api_endpoint}")
            
            response = self.session.get(
                self.config.api_endpoint,
                verify=self.config.verify_ssl,
                timeout=self.config.timeout
            )
            
            # Handle HTTP errors
            self._validate_response(response)
            
            # Parse JSON response
            data = self._parse_response(response)
            
            # Extract policies
            policies = self._extract_policies(data)
            
            logger.info(f"Successfully retrieved {len(policies)} firewall policies")
            return policies
            
        except requests.exceptions.ConnectionError as e:
            error_msg = (
                f"Failed to connect to FortiGate at {self.config.ip_address}. "
                f"Check network connectivity and IP address."
            )
            logger.error(f"{error_msg} Error: {e}")
            raise FortiGateAPIError(error_msg) from e
            
        except requests.exceptions.Timeout as e:
            error_msg = f"Connection timeout while connecting to FortiGate (timeout: {self.config.timeout}s)"
            logger.error(f"{error_msg} Error: {e}")
            raise FortiGateAPIError(error_msg) from e
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Request error occurred: {e}"
            logger.error(error_msg)
            raise FortiGateAPIError(error_msg) from e
    
    def _validate_response(self, response: requests.Response) -> None:
        """
        Validate HTTP response and raise appropriate errors.
        
        Args:
            response: HTTP response object
            
        Raises:
            FortiGateAPIError: If response indicates an error
        """
        status_code = response.status_code
        
        if status_code == 401:
            error_msg = "Authentication failed. Invalid or expired API token."
            logger.error(error_msg)
            raise FortiGateAPIError(error_msg)
            
        elif status_code == 403:
            error_msg = "Access forbidden. Check API token permissions."
            logger.error(error_msg)
            raise FortiGateAPIError(error_msg)
            
        elif status_code == 404:
            error_msg = (
                "API endpoint not found. Check FortiGate version and API availability. "
                f"Endpoint: {self.config.api_endpoint}"
            )
            logger.error(error_msg)
            raise FortiGateAPIError(error_msg)
            
        elif not response.ok:
            error_msg = (
                f"API request failed with status {status_code}: {response.text[:500]}"
            )
            logger.error(error_msg)
            raise FortiGateAPIError(error_msg)
    
    def _parse_response(self, response: requests.Response) -> Dict:
        """
        Parse JSON response from API.
        
        Args:
            response: HTTP response object
            
        Returns:
            Dict: Parsed JSON data
            
        Raises:
            FortiGateAPIError: If JSON parsing fails
        """
        try:
            return response.json()
        except json.JSONDecodeError as e:
            error_msg = f"Failed to parse JSON response: {e}"
            logger.error(f"{error_msg} Response text: {response.text[:500]}")
            raise FortiGateAPIError(error_msg) from e
    
    def _extract_policies(self, data: Dict) -> List[Dict]:
        """
        Extract policies from API response.
        
        Handles different response formats from various FortiGate versions.
        
        Args:
            data: Parsed JSON response
            
        Returns:
            List[Dict]: List of policy dictionaries
        """
        if isinstance(data, list):
            policies = data
        elif isinstance(data, dict):
            if "results" in data:
                policies = data["results"]
            elif "data" in data:
                policies = data["data"] if isinstance(data["data"], list) else [data["data"]]
            else:
                # Single policy object
                policies = [data]
        else:
            logger.warning(f"Unexpected response format: {type(data)}")
            policies = []
        
        logger.debug(f"Extracted {len(policies)} policies from response")
        return policies
    
    def close(self) -> None:
        """Close the session and cleanup resources."""
        if self.session:
            self.session.close()
            logger.debug("FortiGate client session closed")

