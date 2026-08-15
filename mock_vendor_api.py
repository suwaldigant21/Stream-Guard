import os

import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Security
from fastapi.security import APIKeyHeader

load_dotenv()

app = FastAPI(title="PaySim Mock Vendor API", version="1.0.0")

# Security setup (value comes from .env - no fallback here on purpose, so the
# key is never committed to the repo).
API_KEY = os.getenv("STREAMGUARD_API_KEY")
if not API_KEY:
    print("WARNING: STREAMGUARD_API_KEY is not set - all requests will be rejected.")
    print("Create a .env file from .env.example to enable the feed.")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

# Dataset: CSV on first run, Parquet cache afterwards for fast --reload restarts.
DATASET_PATH = os.getenv(
    "STREAMGUARD_DATASET_PATH", "PS_20174392719_1491204439457_log.csv"
)
PARQUET_PATH = os.getenv(
    "STREAMGUARD_DATASET_PARQUET", "PS_20174392719_1491204439457_log.parquet"
)


def verify_api_key(header_key: str = Security(api_key_header)):
    if header_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API Key")
    return header_key


def load_dataset() -> pd.DataFrame:
    # Prefer the fast Parquet cache so restarts (incl. --reload) are cheap.
    if os.path.exists(PARQUET_PATH):
        print(f"Loading PaySim dataset from Parquet cache ({PARQUET_PATH})...")
        return pd.read_parquet(PARQUET_PATH)

    print(f"Loading PaySim dataset (CSV): {DATASET_PATH} ...")
    df = pd.read_csv(DATASET_PATH)

    # Build the cache for next time (best-effort; failure only means slower restarts).
    try:
        df.to_parquet(PARQUET_PATH, index=False)
        print(f"Parquet cache written to {PARQUET_PATH}")
    except (OSError, ValueError) as e:
        print(f"Could not write Parquet cache: {e}")

    return df


try:
    df = load_dataset()
    print(f"Loaded {len(df)} transactions into memory.")
except (FileNotFoundError, OSError, ValueError) as e:
    print(f"Error loading dataset: {e}")
    df = pd.DataFrame()


@app.get("/health")
def health_check():
    return {"status": "online", "total_records": len(df)}


@app.get("/v1/transactions")
def get_transactions(
    offset: int = Query(0, ge=0),
    limit: int = Query(100, le=1000),
    api_key: str = Security(verify_api_key),
):
    """
    Paginated endpoint returning simulated transaction feeds.
    """
    if df.empty:
        raise HTTPException(status_code=500, detail="Dataset not loaded.")

    records = df.iloc[offset : offset + limit].to_dict(orient="records")
    return {
        "offset": offset,
        "limit": limit,
        "count": len(records),
        "data": records,
    }
