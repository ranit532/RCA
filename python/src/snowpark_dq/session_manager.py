"""Snowpark session management for RCA PoC"""
import logging
from typing import Optional
from snowpark.session import Session
from config.snowflake_config import SnowflakeConfig

logger = logging.getLogger(__name__)

class SnowparkSessionManager:
    """Manages Snowpark session lifecycle"""
    
    _session: Optional[Session] = None
    _config: Optional[SnowflakeConfig] = None
    
    @classmethod
    def initialize(cls, config: SnowflakeConfig = None) -> Session:
        """Initialize and return Snowpark session"""
        if cls._session is not None:
            logger.info("Reusing existing Snowpark session")
            return cls._session
        
        cfg = config or SnowflakeConfig()
        cls._config = cfg
        
        try:
            logger.info(f"Creating Snowpark session for account: {cfg.account}")
            cls._session = Session.builder.configs(cfg.get_connection_params()).create()
            cls._session.sql_simplifier_enabled = True
            logger.info("Snowpark session created successfully")
            return cls._session
        except Exception as e:
            logger.error(f"Failed to create Snowpark session: {str(e)}")
            raise
    
    @classmethod
    def get_session(cls) -> Session:
        """Get active Snowpark session"""
        if cls._session is None:
            raise RuntimeError("Session not initialized. Call initialize() first")
        return cls._session
    
    @classmethod
    def close(cls):
        """Close active Snowpark session"""
        if cls._session:
            cls._session.close()
            cls._session = None
            logger.info("Snowpark session closed")
    
    @classmethod
    def switch_schema(cls, schema_name: str):
        """Switch to specified schema"""
        if cls._session is None:
            raise RuntimeError("Session not initialized")
        cls._session.use_schema(schema_name)
        logger.info(f"Switched to schema: {schema_name}")
    
    @classmethod
    def get_config(cls) -> SnowflakeConfig:
        """Get current configuration"""
        return cls._config or SnowflakeConfig()
