"""
Pydantic models for API request/response validation.
"""

from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime


class PolicySummaryResponse(BaseModel):
    """Response model for policy summary."""
    success: bool
    policies_count: int
    db_stored: bool
    db_count: int
    data_source: str = "api"  # "api" or "sample"
    summary: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.now)


class HealthResponse(BaseModel):
    """Response model for health check."""
    status: str
    version: str
    timestamp: datetime = Field(default_factory=datetime.now)


class PolicySample(BaseModel):
    """Sample policy model for summary."""
    name: str
    policy_id: Any
    source_interface: str
    destination_interface: str
    action: str


class FirewallConfigRequest(BaseModel):
    """Request model for dynamic firewall configuration."""
    ip_address: str = Field(..., description="Firewall IP address or hostname")
    api_token: str = Field(..., description="API token for authentication")
    vendor_type: str = Field(default="fortigate", description="Vendor type (e.g., 'fortigate', 'zscaler', 'paloalto')")
    device_id: Optional[str] = Field(None, description="Device identifier (defaults to IP address if not provided)")
    device_name: Optional[str] = Field(None, description="Human-readable device name")
    verify_ssl: bool = Field(default=False, description="Verify SSL certificates")
    timeout: int = Field(default=30, description="Request timeout in seconds")
    api_version: str = Field(default="v2", description="API version")

