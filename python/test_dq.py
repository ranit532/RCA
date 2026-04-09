import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from config.snowflake_config import DEFAULT_CONFIG
from src.snowpark_dq.session_manager import SnowparkSessionManager
from src.snowpark_dq.quality_engine import DataQualityEngine
from src.snowpark_dq.quality_rules import ASSET_DATA_QUALITY_RULES

print('🔍 Testing Data Quality Engine...')
session = SnowparkSessionManager.initialize(DEFAULT_CONFIG)
dq_engine = DataQualityEngine(session)
results = dq_engine.execute_rules(ASSET_DATA_QUALITY_RULES)

passed = sum(1 for r in results if r.passed)
print(f'✅ DQ Results: {passed}/{len(results)} rules passed')

session.close()
print('✅ Test completed successfully!')