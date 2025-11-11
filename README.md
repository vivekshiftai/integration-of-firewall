# FortiGate Policy Retriever API

A production-ready REST API service for retrieving and storing FortiGate firewall policies. This application connects to FortiGate firewalls via REST API, fetches all configured firewall policies, and stores them in ClickHouse database for analysis and monitoring.

## Features

- 🔐 **Secure API Authentication** - Token-based authentication with FortiGate
- 📊 **ClickHouse Integration** - Automatic schema creation and batch data insertion
- 🚀 **REST API** - FastAPI-based endpoints for triggering operations
- 📝 **Comprehensive Logging** - Structured logging throughout the application
- 🔄 **Error Handling** - Robust error handling with retry mechanisms
- 📦 **Modular Architecture** - Clean, maintainable code structure
- 📄 **JSON Export** - Optional JSON file export for policies
- 🏥 **Health Checks** - Built-in health monitoring endpoints
- 🔄 **Sample Data Fallback** - Automatic fallback to sample data when API is unavailable

## Project Structure

```
firewallintegration/
├── main.py                    # FastAPI application entry point
├── requirements.txt          # Python dependencies
├── README.md                 # This file
└── app/                      # Application package
    ├── __init__.py
    ├── api/                   # REST API endpoints
    │   ├── routes.py         # API route definitions
    │   └── dependencies.py   # Dependency injection
    ├── clients/              # External API clients
    │   └── fortigate_client.py
    ├── config/               # Configuration management
    │   └── settings.py
    ├── core/                 # Core utilities
    │   ├── exceptions.py
    │   └── logger.py
    ├── database/             # Database handlers
    │   └── clickhouse_handler.py
    ├── models/               # Data models/schemas
    │   └── schemas.py
    ├── services/             # Business logic
    │   └── policy_service.py
    └── utils/                # Utility functions
        └── data_processor.py
```

## Prerequisites

- Python 3.8 or higher
- FortiGate firewall with REST API enabled (optional - can use sample data)
- ClickHouse database (optional, for data storage)
- FortiGate API token with read permissions (optional - will use sample data if not provided)

## Installation

1. **Clone or download the repository**

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure the application:**
   
   **Option 1: Use .env file (Recommended)**
   
   Create a `.env` file in the project root directory with the following configuration:
   ```env
   # FortiGate Configuration
   FORTIGATE_IP=192.168.1.99
   FGT_API_TOKEN=your_api_token_here
   FORTIGATE_VERIFY_SSL=false
   FORTIGATE_TIMEOUT=30
   FORTIGATE_API_VERSION=v2
   USE_SAMPLE_DATA=false
   
   # ClickHouse Database Configuration
   CLICKHOUSE_HOST=localhost
   CLICKHOUSE_PORT=8123
   CLICKHOUSE_DATABASE=fortigate
   CLICKHOUSE_USER=default
   CLICKHOUSE_PASSWORD=
   CLICKHOUSE_SECURE=false
   CLICKHOUSE_VERIFY=false
   
   # Application Configuration
   API_HOST=0.0.0.0
   API_PORT=8000
   LOG_LEVEL=INFO
   SAMPLE_DATA_DIR=sampledata
   ```
   
   **Note:** The API token is optional. If not provided, the application will automatically use sample data from the `sampledata` folder.
   
   **Option 2: Set environment variables manually:**
   ```bash
   # Windows PowerShell
   $env:FGT_API_TOKEN="your_fortigate_api_token"
   $env:CLICKHOUSE_HOST="localhost"
   $env:CLICKHOUSE_PORT="8123"
   # ... etc
   
   # Windows CMD
   set FGT_API_TOKEN=your_fortigate_api_token
   set CLICKHOUSE_HOST=localhost
   # ... etc
   
   # Linux/Mac
   export FGT_API_TOKEN="your_fortigate_api_token"
   export CLICKHOUSE_HOST="localhost"
   # ... etc
   ```

## Usage

### Starting the API Server

Run the FastAPI application:

```bash
python main.py
```

The API server will start on `http://localhost:8000` (or the configured host/port).

### API Documentation

Once the server is running, access the interactive API documentation:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## API Endpoints

### 1. Root Endpoint

**GET** `/`

Returns API information and available endpoints.

**Response:**
```json
{
  "name": "FortiGate Policy Retriever API",
  "version": "1.0.0",
  "status": "running",
  "endpoints": {
    "health": "/api/v1/health",
    "fetch_policies": "/api/v1/policies/fetch",
    "status": "/api/v1/policies/status",
    "docs": "/docs",
    "redoc": "/redoc"
  }
}
```

### 2. Health Check

**GET** `/api/v1/health`

Check if the service is healthy and running.

**Response:**
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "timestamp": "2024-01-15T10:30:00"
}
```

### 3. Fetch Policies

**POST** `/api/v1/policies/fetch`

Trigger the policy fetching process from FortiGate. This endpoint:
- Connects to FortiGate firewall
- Retrieves all firewall policies
- Optionally saves to JSON file
- Optionally stores in ClickHouse database

**Query Parameters:**
- `save_to_file` (boolean, default: `true`) - Save policies to JSON file
- `store_in_db` (boolean, default: `true`) - Store policies in ClickHouse

**Example Request:**
```bash
curl -X POST "http://localhost:8000/api/v1/policies/fetch?save_to_file=true&store_in_db=true"
```

**Response:**
```json
{
  "success": true,
  "policies_count": 150,
  "file_saved": true,
  "db_stored": true,
  "db_count": 150,
  "summary": {
    "total_policies": 150,
    "sample_policies": [
      {
        "name": "Allow_HTTP",
        "policy_id": 1,
        "source_interface": "port1",
        "destination_interface": "port2",
        "action": "accept"
      }
    ]
  },
  "error": null,
  "timestamp": "2024-01-15T10:30:00"
}
```

### 4. Get Status

**GET** `/api/v1/policies/status`

Get the current status of the policy service.

**Response:**
```json
{
  "status": "operational",
  "fortigate_configured": true,
  "database_configured": true,
  "total_policies_in_db": 150
}
```

## ClickHouse Database Schema

The application automatically creates the following table structure:

```sql
CREATE TABLE firewall_policies (
    policy_id UInt32,
    name String,
    action String,
    status String,
    srcintf Array(String),
    dstintf Array(String),
    srcaddr Array(String),
    dstaddr Array(String),
    service Array(String),
    schedule String,
    logtraffic String,
    comments String,
    policy_type String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    raw_data String,
    retrieved_at DateTime
) ENGINE = MergeTree()
ORDER BY (policy_id, retrieved_at)
PARTITION BY toYYYYMM(retrieved_at)
```

The table is partitioned by month for efficient querying and storage.

## Error Handling

The application includes comprehensive error handling for:

- **Connection Errors** - Network connectivity issues
- **Authentication Failures** - Invalid or expired API tokens
- **Database Errors** - ClickHouse connection or insertion failures
- **Timeout Errors** - Request timeouts
- **Configuration Errors** - Missing or invalid configuration

All errors are logged with detailed information and returned as appropriate HTTP status codes.

## Logging

The application uses structured logging with the following levels:

- **DEBUG** - Detailed information for debugging
- **INFO** - General informational messages
- **WARNING** - Warning messages
- **ERROR** - Error messages
- **CRITICAL** - Critical errors

Logs include:
- Timestamp
- Log level
- Function name and line number
- Detailed error messages

## Development

### Running in Development Mode

For development with auto-reload:

```python
# In main.py, change:
uvicorn.run(
    "main:app",
    host=config.api_host,
    port=config.api_port,
    log_level=config.log_level.lower(),
    reload=True  # Enable auto-reload
)
```

### Code Structure

The application follows clean architecture principles:

- **API Layer** (`app/api/`) - HTTP endpoints and request/response handling
- **Service Layer** (`app/services/`) - Business logic orchestration
- **Client Layer** (`app/clients/`) - External API integrations
- **Database Layer** (`app/database/`) - Database operations
- **Core Layer** (`app/core/`) - Shared utilities (logging, exceptions)
- **Config Layer** (`app/config/`) - Configuration management

## Security Considerations

1. **API Token**: Store the FortiGate API token securely. Never commit it to version control.

2. **SSL Verification**: In production, set `FORTIGATE_VERIFY_SSL=true` and ensure proper certificates.

3. **CORS**: Configure CORS middleware appropriately for production (currently allows all origins).

4. **Environment Variables**: Use a secrets management system in production.

5. **Network Security**: Ensure the API server is only accessible from trusted networks.

## Sample Data Fallback

The application includes a fallback mechanism that automatically uses sample data when:

1. **API token is not configured** - If `FGT_API_TOKEN` is not set, the app will use sample data
2. **API connection fails** - If the FortiGate API is unreachable or returns an error, sample data is used
3. **Explicitly enabled** - Set `USE_SAMPLE_DATA=true` to force sample data usage

### Sample Data Location

Sample data files are located in the `sampledata/` directory. The default file is `sample_policies.json`.

The application will:
- Automatically detect if sample data exists
- Use sample data when API is unavailable
- Store sample data in the database just like API data
- Indicate the data source (`api` or `sample`) in the response

### Adding Custom Sample Data

1. Create JSON files in the `sampledata/` directory
2. Follow the FortiGate API response format
3. The application will automatically detect and use available sample files

See `sampledata/README.md` for more details on the sample data format.

## Troubleshooting

### Common Issues

1. **"FGT_API_TOKEN environment variable is required"**
   - Solution: Set the `FGT_API_TOKEN` environment variable, OR
   - Solution: Add sample data to `sampledata/sample_policies.json` (the app will use it automatically)

2. **"Failed to connect to FortiGate"**
   - Check network connectivity
   - Verify FortiGate IP address
   - Ensure FortiGate REST API is enabled

3. **"Authentication failed"**
   - Verify API token is valid and not expired
   - Check token permissions

4. **"Failed to connect to ClickHouse"**
   - Verify ClickHouse is running
   - Check connection settings (host, port)
   - Ensure database credentials are correct

5. **Import errors**
   - Ensure all dependencies are installed: `pip install -r requirements.txt`

## License

This project is provided as-is for network automation and firewall management purposes.

## Support

For issues or questions, please check the logs for detailed error messages. The application provides comprehensive logging to help diagnose problems.

## Version

Current Version: **1.0.0**

---

**Note**: This application is designed for network automation and firewall policy management. Ensure you have proper authorization before accessing firewall configurations.

