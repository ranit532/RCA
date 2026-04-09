"""Snowflake connection configuration for RCA PoC"""
from dataclasses import dataclass
from typing import Optional
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

@dataclass
class SnowflakeConfig:
    """Snowflake connection configuration"""
    account: str = None
    user: str = None
    password: str = None
    authenticator: str = None
    role: str = None
    warehouse: str = None
    database: str = None
    schema_raw: str = "RAW"
    schema_curated: str = "CURATED"
    schema_analytics: str = "ANALYTICS"
    schema_controls: str = "CONTROLS"
    
    def __post_init__(self):
        """Initialize from environment variables if not provided"""
        self.account = self.account or os.getenv("SNOWFLAKE_ACCOUNT", "TQWSLTQ-TW60698")
        self.user = self.user or os.getenv("SNOWFLAKE_USER", "RANIT532")
        self.password = self.password or os.getenv("SNOWFLAKE_PASSWORD")
        self.authenticator = self.authenticator or os.getenv("SNOWFLAKE_AUTHENTICATOR", "externalbrowser")
        self.role = self.role or os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
        self.warehouse = self.warehouse or os.getenv("SNOWFLAKE_WAREHOUSE", "RCA_ANALYTICS_WH")
        self.database = self.database or os.getenv("SNOWFLAKE_DATABASE", "RCA_POC_DB")
    
    def get_connection_params(self) -> dict:
        """Get connection parameters for Snowflake connector"""
        return {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "authenticator": self.authenticator,
            "role": self.role,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema_raw,
        }

# Default configuration instance
DEFAULT_CONFIG = SnowflakeConfig()
