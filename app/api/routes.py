"""
API routes for FortiGate Policy Retriever.
"""

import logging
from typing import Annotated, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends, Body

from app.models.schemas import PolicySummaryResponse, HealthResponse, FirewallConfigRequest
from app.services.policy_service import PolicyService
from app.core.exceptions import FortiGateAPIError, DatabaseError
from app.api.dependencies import get_policy_service

logger = logging.getLogger("fortigate_policy_retriever")

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint.
    
    Returns:
        HealthResponse: Service health status
    """
    return HealthResponse(
        status="healthy",
        version="1.0.0"
    )


@router.post("/policies/fetch", response_model=PolicySummaryResponse)
async def fetch_policies(
    background_tasks: BackgroundTasks,
    policy_service: Annotated[PolicyService, Depends(get_policy_service)],
    store_in_db: bool = True,
    config: Optional[FirewallConfigRequest] = Body(None, description="Optional firewall configuration (overrides .env)")
):
    """
    Fetch firewall policies from firewall and store them in database.
    
    This endpoint triggers the policy fetching process and stores them
    in ClickHouse database. Can accept firewall configuration via request body
    (takes priority over .env configuration).
    
    Args:
        background_tasks: FastAPI background tasks
        policy_service: Policy service instance (dependency injection)
        store_in_db: Whether to store policies in database
        config: Optional firewall configuration (IP, API token, vendor type, etc.)
                If provided, overrides .env configuration
        
    Returns:
        PolicySummaryResponse: Result of the fetch operation
        
    Raises:
        HTTPException: If the operation fails
    """
    try:
        logger.info("Policy fetch endpoint triggered")
        
        # Prepare configuration dict if provided
        firewall_config = None
        vendor_type = None
        device_id = None
        device_name = None
        
        if config:
            firewall_config = {
                "ip_address": config.ip_address,
                "api_token": config.api_token,
                "verify_ssl": config.verify_ssl,
                "timeout": config.timeout,
                "api_version": config.api_version
            }
            vendor_type = config.vendor_type
            device_id = config.device_id
            device_name = config.device_name
            logger.info(f"Using configuration from request: vendor={vendor_type}, ip={config.ip_address}")
        
        result = policy_service.fetch_and_store_policies(
            store_in_db=store_in_db,
            firewall_config=firewall_config,
            vendor_type=vendor_type,
            device_id=device_id,
            device_name=device_name
        )
        
        if not result["success"]:
            raise HTTPException(
                status_code=500,
                detail=result.get("error", "Failed to fetch policies")
            )
        
        return PolicySummaryResponse(**result)
        
    except FortiGateAPIError as e:
        logger.error(f"FortiGate API error: {e}")
        raise HTTPException(
            status_code=503,
            detail=f"FortiGate API error: {str(e)}"
        )
    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(
            status_code=503,
            detail=f"Database error: {str(e)}"
        )
    except Exception as e:
        logger.exception(f"Unexpected error in fetch_policies endpoint: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/policies/status")
async def get_status(policy_service: Annotated[PolicyService, Depends(get_policy_service)]):
    """
    Get status of the policy service.
    
    Args:
        policy_service: Policy service instance (dependency injection)
        
    Returns:
        Dict: Service status information
    """
    try:
        db_count = 0
        if policy_service.clickhouse_handler:
            try:
                policy_service.clickhouse_handler.connect()
                db_count = policy_service.clickhouse_handler.get_policy_count()
            except Exception as e:
                logger.warning(f"Failed to get database count: {e}")
        
        return {
            "status": "operational",
            "fortigate_configured": policy_service.fortigate_client is not None,
            "database_configured": policy_service.clickhouse_handler is not None,
            "total_policies_in_db": db_count
        }
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get status: {str(e)}"
        )

