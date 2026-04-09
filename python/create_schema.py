import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from config.snowflake_config import SnowflakeConfig, DEFAULT_CONFIG
from src.snowpark_dq.session_manager import SnowparkSessionManager

config = DEFAULT_CONFIG
session = SnowparkSessionManager.initialize(config)
cursor = session.cursor()

# Create CURATED schema
cursor.execute("CREATE SCHEMA IF NOT EXISTS CURATED")
print("✅ CURATED schema created/verified")

cursor.close()
session.close()