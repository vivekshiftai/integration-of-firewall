"""
Airflow DAG for fetching and storing firewall policies.
This DAG triggers the firewall integration service to fetch policies from FortiGate
and store them in ClickHouse database.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Get service URL from Airflow Variables (or use default)
# You can set this in Airflow UI: Admin -> Variables -> Set "firewall_service_url"
SERVICE_URL = Variable.get("firewall_service_url", default_var="http://localhost:8000")


def fetch_policies():
    """
    Fetch firewall policies from the firewall integration service.
    
    This function calls the /api/v1/policies/fetch endpoint which:
    - Fetches policies from FortiGate firewall (or uses sample data)
    - Stores them in ClickHouse database
    """
    url = f"{SERVICE_URL}/api/v1/policies/fetch"
    payload = {
        "store_in_db": True
    }
    headers = {"Content-Type": "application/json"}
    
    try:
        logging.info(f"Triggering policy fetch from: {url}")
        logging.info(f"Payload: {payload}")
        
        response = requests.post(url, json=payload, headers=headers, timeout=300)
        
        if response.status_code == 200:
            result = response.json()
            logging.info(f"✅ Successfully fetched policies: {result}")
            
            # Log summary information
            if "policies_count" in result:
                logging.info(f"Policies fetched: {result['policies_count']}")
            if "db_count" in result:
                logging.info(f"Policies stored in DB: {result['db_count']}")
            if "data_source" in result:
                logging.info(f"Data source: {result['data_source']}")
            
            return result
        else:
            error_msg = f"❌ Failed to fetch policies. Status code: {response.status_code}, Response: {response.text}"
            logging.error(error_msg)
            raise Exception(error_msg)
            
    except requests.exceptions.Timeout:
        error_msg = f"❌ Request timeout after 300 seconds when calling {url}"
        logging.error(error_msg)
        raise Exception(error_msg)
    except requests.exceptions.ConnectionError as e:
        error_msg = f"❌ Connection error when calling {url}: {e}"
        logging.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        logging.error(f"Exception during policy fetch POST request: {e}")
        raise


def check_status():
    """
    Check the status of the firewall policy service.
    
    This function calls the /api/v1/policies/status endpoint to verify:
    - Service is operational
    - Database connection status
    - Total policies in database
    """
    url = f"{SERVICE_URL}/api/v1/policies/status"
    headers = {"Content-Type": "application/json"}
    
    try:
        logging.info(f"Checking service status from: {url}")
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            status = response.json()
            logging.info(f"✅ Service status: {status}")
            
            # Log key status information
            if "status" in status:
                logging.info(f"Service status: {status['status']}")
            if "fortigate_configured" in status:
                logging.info(f"FortiGate configured: {status['fortigate_configured']}")
            if "database_configured" in status:
                logging.info(f"Database configured: {status['database_configured']}")
            if "total_policies_in_db" in status:
                logging.info(f"Total policies in database: {status['total_policies_in_db']}")
            
            return status
        else:
            error_msg = f"❌ Failed to get status. Status code: {response.status_code}, Response: {response.text}"
            logging.error(error_msg)
            raise Exception(error_msg)
            
    except requests.exceptions.Timeout:
        error_msg = f"❌ Request timeout when checking status from {url}"
        logging.error(error_msg)
        raise Exception(error_msg)
    except requests.exceptions.ConnectionError as e:
        error_msg = f"❌ Connection error when calling {url}: {e}"
        logging.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        logging.error(f"Exception during status check GET request: {e}")
        raise


def health_check():
    """
    Perform a health check on the firewall integration service.
    
    This function calls the /api/v1/health endpoint to verify the service is running.
    """
    url = f"{SERVICE_URL}/api/v1/health"
    headers = {"Content-Type": "application/json"}
    
    try:
        logging.info(f"Performing health check on: {url}")
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            health = response.json()
            logging.info(f"✅ Service health check passed: {health}")
            return health
        else:
            error_msg = f"❌ Health check failed. Status code: {response.status_code}, Response: {response.text}"
            logging.error(error_msg)
            raise Exception(error_msg)
            
    except requests.exceptions.Timeout:
        error_msg = f"❌ Health check timeout when calling {url}"
        logging.error(error_msg)
        raise Exception(error_msg)
    except requests.exceptions.ConnectionError as e:
        error_msg = f"❌ Connection error during health check: {e}"
        logging.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        logging.error(f"Exception during health check: {e}")
        raise


# Define the DAG
with DAG(
    dag_id="firewall_policy_fetch_dag",
    default_args=default_args,
    description="Automatically fetch firewall policies from FortiGate and store in ClickHouse database",
    schedule="*/30 * * * *",  # Run every 30 minutes (adjust as needed)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["firewall", "policies", "fortigate", "clickhouse"]
) as dag:

    # Task 1: Health check - verify service is running
    health_check_task = PythonOperator(
        task_id="health_check",
        python_callable=health_check,
        execution_timeout=timedelta(minutes=1),
        doc_md="""
        Performs a health check on the firewall integration service.
        This ensures the service is running before attempting to fetch policies.
        """
    )

    # Task 2: Fetch policies from firewall and store in database
    fetch_policies_task = PythonOperator(
        task_id="fetch_policies",
        python_callable=fetch_policies,
        execution_timeout=timedelta(minutes=5),
        doc_md="""
        Fetches firewall policies from FortiGate (or uses sample data if API is not configured)
        and stores them in ClickHouse database.
        """
    )

    # Task 3: Check status - verify the operation completed successfully
    check_status_task = PythonOperator(
        task_id="check_status",
        python_callable=check_status,
        execution_timeout=timedelta(minutes=1),
        doc_md="""
        Checks the status of the firewall policy service to verify:
        - Service is operational
        - Database connection status
        - Total policies stored in database
        """
    )

    # Define task dependencies
    health_check_task >> fetch_policies_task >> check_status_task

