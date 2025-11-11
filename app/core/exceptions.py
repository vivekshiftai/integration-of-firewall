"""
Custom exceptions for the FortiGate Policy Retriever application.
"""


class FortiGateAPIError(Exception):
    """Raised when FortiGate API returns an error."""
    pass


class DatabaseError(Exception):
    """Raised when database operations fail."""
    pass


class ConfigurationError(Exception):
    """Raised when configuration is invalid or missing."""
    pass

