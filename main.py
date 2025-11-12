#!/usr/bin/env python3
"""
FortiGate Policy Retriever - Main Entry Point
FastAPI application for retrieving and storing FortiGate firewall policies.
"""

import logging
import sys
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from app.config.settings import AppConfig
from app.core.logger import setup_logging
from app.api import routes
from app.api.dependencies import get_config, get_policy_service, get_fortigate_client, get_clickhouse_handler
from app.core.exceptions import ConfigurationError


# Global logger
logger: logging.Logger = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.
    Handles startup and shutdown events.
    """
    # Startup
    global logger
    try:
        config = get_config()
        logger = setup_logging(config.log_level)
        
        logger.info("="*60)
        logger.info("FortiGate Policy Retriever API - Starting")
        logger.info("="*60)
        logger.info(f"API will run on {config.api_host}:{config.api_port}")
        logger.info(f"FortiGate IP: {config.fortigate.ip_address}")
        logger.info(f"ClickHouse: {config.clickhouse.host}:{config.clickhouse.port}")
        
        yield
        
    except Exception as e:
        logger.error(f"Startup error: {e}")
        raise
    
    # Shutdown
    logger.info("Shutting down application")
    logger.info("="*60)


# Create FastAPI application
app = FastAPI(
    title="FortiGate Policy Retriever API",
    description="REST API for retrieving and storing FortiGate firewall policies",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(routes.router, prefix="/api/v1", tags=["policies"])


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": "FortiGate Policy Retriever API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "health": "/api/v1/health",
            "fetch_policies": "/api/v1/policies/fetch",
            "get_config_by_id": "/api/v1/policies/{config_id}",
            "status": "/api/v1/policies/status",
            "docs": "/docs",
            "redoc": "/redoc"
        }
    }


# Dependencies are injected via Depends() in routes


def main():
    """Main entry point."""
    try:
        config = get_config()
        
        # Run the application
        uvicorn.run(
            "main:app",
            host=config.api_host,
            port=config.api_port,
            log_level=config.log_level.lower(),
            reload=False  # Set to True for development
        )
        
    except ConfigurationError as e:
        print(f"ERROR: Configuration error - {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nApplication interrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"ERROR: Failed to start application: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

