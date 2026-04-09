"""Snowflake connector-based session management for RCA PoC"""
import logging
from typing import Optional, Any
import snowflake.connector
from config.snowflake_config import SnowflakeConfig

logger = logging.getLogger(__name__)

class SnowparkSessionManager:
    """Manages Snowflake connector session lifecycle (Snowpark replacement for Python 3.14 compatibility)"""
    
    _session: Optional[Any] = None
    _config: Optional[SnowflakeConfig] = None
    
    @classmethod
    def initialize(cls, config: SnowflakeConfig = None) -> Any:
        """Initialize and return Snowflake connector session"""
        if cls._session is not None:
            logger.info("Reusing existing Snowflake session")
            return cls._session
        
        cfg = config or SnowflakeConfig()
        cls._config = cfg
        
        try:
            logger.info(f"Creating Snowflake connector session for account: {cfg.account}")
            params = cfg.get_connection_params()
            
            # Build connection kwargs
            connection_kwargs = {
                'user': params.get('user'),
                'account': params.get('account'),
                'warehouse': params.get('warehouse'),
                'database': params.get('database'),
                'schema': params.get('schema'),
                'authenticator': params.get('authenticator'),
                'role': params.get('role'),
            }
            
            # Add password if available (for password-based auth)
            if params.get('password'):
                connection_kwargs['password'] = params.get('password')
            
            cls._session = snowflake.connector.connect(**connection_kwargs)
            logger.info("Snowflake connector session created successfully")
            return cls._session
        except Exception as e:
            logger.error(f"Failed to create Snowflake connector session: {str(e)}")
            raise
    
    @classmethod
    def get_session(cls) -> Any:
        """Get active Snowflake connector session"""
        if cls._session is None:
            raise RuntimeError("Session not initialized. Call initialize() first")
        return cls._session
    
    @classmethod
    def close(cls):
        """Close active Snowflake connector session"""
        if cls._session:
            cls._session.close()
            cls._session = None
            logger.info("Snowflake connector session closed")
    
    @classmethod
    def switch_schema(cls, schema_name: str):
        """Switch to specified schema"""
        if cls._session is None:
            raise RuntimeError("Session not initialized")
        cursor = cls._session.cursor()
        cursor.execute(f"USE SCHEMA {schema_name}")
        cursor.close()
        logger.info(f"Switched to schema: {schema_name}")
    
    @classmethod
    def get_config(cls) -> SnowflakeConfig:
        """Get current configuration"""
        return cls._config or SnowflakeConfig()
