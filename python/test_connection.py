import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from config.snowflake_config import DEFAULT_CONFIG
from src.snowpark_dq.session_manager import SnowparkSessionManager

print('🔍 Testing Snowflake Connection...')
session = SnowparkSessionManager.initialize(DEFAULT_CONFIG)
cursor = session.cursor()

# Test basic connectivity
cursor.execute('SELECT CURRENT_ACCOUNT() as account, CURRENT_USER() as user')
result = cursor.fetchone()
print(f'✅ Connected to: {result[0]} as {result[1]}')

# Check data
cursor.execute('SELECT COUNT(*) as cnt FROM RAW.DIM_PARTY_RAW')
party_count = cursor.fetchone()[0]
cursor.execute('SELECT COUNT(*) as cnt FROM RAW.DIM_INSTRUMENT_RAW')
instrument_count = cursor.fetchone()[0]

print(f'📊 Data Status: {party_count} parties, {instrument_count} instruments')

cursor.close()
session.close()
print('✅ Connection test completed!')