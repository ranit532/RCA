import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from config.snowflake_config import SnowflakeConfig, DEFAULT_CONFIG
from src.snowpark_dq.session_manager import SnowparkSessionManager

config = DEFAULT_CONFIG
session = SnowparkSessionManager.initialize(config)
cursor = session.cursor()

# Read and execute the dynamic tables SQL
sql_file = Path(__file__).parent.parent / "sql" / "06_dynamic_tables.sql"
with open(sql_file, 'r') as f:
    sql_content = f.read()

# Split by semicolon and execute each statement
statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip() and not stmt.strip().startswith('--')]

for i, stmt in enumerate(statements):
    if stmt:
        try:
            print(f"Executing statement {i+1}/{len(statements)}...")
            cursor.execute(stmt)
            print("✅ Success")
        except Exception as e:
            print(f"❌ Error in statement {i+1}: {e}")
            # Continue with other statements

print("Dynamic tables creation completed")

cursor.close()
session.close()