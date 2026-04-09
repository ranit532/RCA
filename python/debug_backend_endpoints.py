import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.streamlit_backend import (
    initialize_snowflake_session,
    get_dq_results,
    get_recon_results,
    get_audit_trail,
    get_dyd_status,
)

session = initialize_snowflake_session()
print('session', session is not None)

for fn in [get_dq_results, get_recon_results, get_audit_trail, get_dyd_status]:
    print('\n===', fn.__name__, '===')
    try:
        result = fn(session)
        print('type', type(result))
        if hasattr(result, 'shape'):
            print('shape', result.shape)
            print(result.head(5))
        else:
            print(result)
    except Exception as exc:
        import traceback
        traceback.print_exc()
