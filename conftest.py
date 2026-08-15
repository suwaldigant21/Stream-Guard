import os

# Deterministic test environment: set a test API key before mock_vendor_api is
# imported. python-dotenv's load_dotenv() does not override already-set env
# vars, so tests don't depend on a local .env (and never use a real key).
os.environ.setdefault("STREAMGUARD_API_KEY", "test_api_key")
os.environ.setdefault("STREAMGUARD_DATASET_PARQUET", "PS_20174392719_1491204439457_log.parquet")
