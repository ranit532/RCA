import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from config.snowflake_config import SnowflakeConfig, DEFAULT_CONFIG
from src.snowpark_dq.session_manager import SnowparkSessionManager

config = DEFAULT_CONFIG
session = SnowparkSessionManager.initialize(config)
cursor = session.cursor()

# Check ISIN uniqueness
cursor.execute("""
SELECT
    COUNT(DISTINCT ISIN) as unique_isin,
    COUNT(*) as total_records,
    COUNT(*) - COUNT(DISTINCT ISIN) as duplicates
FROM RAW.DIM_INSTRUMENT_RAW
""")
result = cursor.fetchone()
print(f"ISIN Analysis: {result[0]} unique ISINs, {result[1]} total records, {result[2]} duplicates")

# Check a few ISINs
cursor.execute("""
SELECT ISIN, COUNT(*) as count
FROM RAW.DIM_INSTRUMENT_RAW
GROUP BY ISIN
ORDER BY count DESC
LIMIT 5
""")
rows = cursor.fetchall()
print("Top ISIN frequencies:")
for row in rows:
    print(f"  ISIN {row[0]}: {row[1]} times")

cursor.close()
session.close()