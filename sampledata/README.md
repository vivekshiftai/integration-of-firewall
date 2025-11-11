# Sample Data Directory

This directory contains sample firewall policy data that will be used as a fallback when the FortiGate API is not configured or unavailable.

## Usage

When the `FGT_API_TOKEN` environment variable is not set, or when the FortiGate API connection fails, the application will automatically fall back to using sample data from this directory.

## Sample Data Format

The sample data files should be JSON files containing an array of firewall policy objects. Each policy should follow the FortiGate API response format.

### Example Structure

```json
[
  {
    "policyid": 1,
    "name": "Policy_Name",
    "action": "accept",
    "status": "enable",
    "srcintf": [{"name": "port1"}],
    "dstintf": [{"name": "port2"}],
    "srcaddr": [{"name": "all"}],
    "dstaddr": [{"name": "all"}],
    "service": [{"name": "HTTP"}],
    "schedule": "always",
    "logtraffic": "all",
    "comments": "Policy description",
    "policy_type": "firewall"
  }
]
```

## Default File

The default sample data file is `sample_policies.json`. You can add additional sample files and reference them by name.

## Configuration

You can configure the sample data directory using the `SAMPLE_DATA_DIR` environment variable:

```bash
$env:SAMPLE_DATA_DIR="sampledata"
```

To force the use of sample data even when the API is configured, set:

```bash
$env:USE_SAMPLE_DATA="true"
```

