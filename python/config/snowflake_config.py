"""Snowflake connection configuration for RCA PoC"""
from dataclasses import dataclass
from typing import Optional

@dataclass
class SnowflakeConfig:
    """Snowflake connection configuration"""
    account: str = "TQWSLTQ-TW60698"
    user: str = "RANIT532"
    authenticator: str = "externalbrowser"
    role: str = "ACCOUNTADMIN"
    warehouse: str = "RCA_ANALYTICS_WH"
    database: str = "RCA_POC_DB"
    schema_raw: str = "RAW"
    schema_curated: str = "CURATED"
    schema_analytics: str = "ANALYTICS"
    schema_controls: str = "CONTROLS"
    
    def get_connection_params(self) -> dict:
        """Get connection parameters for Snowpark Session"""
        return {
            "account": self.account,
            "user": self.user,
            "authenticator": self.authenticator,
            "role": self.role,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema_raw,
        }

# Default configuration instance
DEFAULT_CONFIG = SnowflakeConfig()
