import hashlib
import hmac
import json
import os
import sys
from datetime import UTC, datetime

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

PEPPER = os.getenv("STREAMGUARD_GDPR_PEPPER", "default-dev-pepper-change-in-prod").encode("utf-8")
AUDIT_LOG_PATH = "data/gdpr_audit_log.json"
TARGET_FILES = ["data/gold_training.parquet", "data/gold_train.parquet", "data/gold_test.parquet"]

def hash_account_id(account_id: str) -> str:
    """Compute deterministic salted HMAC-SHA256 alias."""
    return "ANON_" + hmac.new(PEPPER, account_id.encode("utf-8"), hashlib.sha256).hexdigest()[:16]

def append_audit_entry(entry: dict) -> None:
    """Append a non-PII audit record (alias, never the raw account id)."""
    audit_logs = []
    if os.path.exists(AUDIT_LOG_PATH):
        with open(AUDIT_LOG_PATH, "r") as f:
            audit_logs = json.load(f)
    audit_logs.append(entry)
    with open(AUDIT_LOG_PATH, "w") as f:
        json.dump(audit_logs, f, indent=2)

def execute_erasure(
    account_id: str, request_id: str = "manual", write_audit: bool = True
) -> tuple[str, int]:
    anonymized_id = hash_account_id(account_id)
    total_rows_updated = 0

    for file_path in TARGET_FILES:
        if not os.path.exists(file_path):
            continue

        df = pd.read_parquet(file_path)

        # Check if columns exist in frame (Gold frames omit raw name cols, but if present, hash them)
        modified = False
        for col in ["name_orig", "name_dest"]:
            if col in df.columns:
                mask = df[col] == account_id
                count = mask.sum()
                if count > 0:
                    df.loc[mask, col] = anonymized_id
                    total_rows_updated += int(count)  # np.int64 is not JSON-serializable
                    modified = True

        if modified:
            df.to_parquet(file_path, index=False)
            print(f"[GDPR Local] Rewrote {file_path} for {account_id} -> {anonymized_id}")

    if write_audit:
        # Log non-PII audit record
        audit_entry = {
            "timestamp": datetime.now(UTC).isoformat(),
            "request_id": request_id,
            "anonymized_alias": anonymized_id,
            "rows_anonymized": total_rows_updated,
        }
        append_audit_entry(audit_entry)
        print(f"[GDPR Complete] Request {request_id}: Alias {anonymized_id} logged.")

    return anonymized_id, total_rows_updated

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: uv run python -m scripts.anonymize_batch_gdpr <account_id> [request_id]")
        sys.exit(1)

    acct = sys.argv[1]
    req = sys.argv[2] if len(sys.argv) > 2 else "manual-cli"
    execute_erasure(acct, req)
