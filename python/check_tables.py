import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from config.snowflake_config import SnowflakeConfig, DEFAULT_CONFIG
from src.snowpark_dq.session_manager import SnowparkSessionManager

config = DEFAULT_CONFIG
session = SnowparkSessionManager.initialize(config)
cursor = session.cursor()

# Check if CURATED tables exist
cursor.execute("SELECT TABLE_NAME FROM RCA_POC_DB.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'CURATED'")
tables = cursor.fetchall()
print('CURATED tables:')
for table in tables:
    print(f'  - {table[0]}')

cursor.close()
session.close()