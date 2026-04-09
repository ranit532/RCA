import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from config.snowflake_config import DEFAULT_CONFIG
from src.snowpark_dq.session_manager import SnowparkSessionManager

session = SnowparkSessionManager.initialize(DEFAULT_CONFIG)
cursor = session.cursor()

# Check results tables
tables = ['DQ_EXECUTION_RESULTS', 'RECONCILIATION_RESULTS', 'DYD_MAPPINGS', 'DYD_METADATA']
for table in tables:
    try:
        cursor.execute(f'SELECT COUNT(*) as cnt FROM CONTROLS.{table}')
        count = cursor.fetchone()[0]
        print(f'{table}: {count} records')
    except Exception as e:
        print(f'{table}: Error - {e}')

cursor.close()
session.close()