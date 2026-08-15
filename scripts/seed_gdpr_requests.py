"""P1-2 — seed the append-only GDPR erasure registry on the lakehouse.

Each erasure request writes ONE immutable parquet file under
``s3://<lakehouse>/gdpr_requests/<request_id>.parquet`` (append-only — an
existing file is never rewritten, so a dbt view over the folder is fully
reproducible from the request trail). The registry maps a RAW account id to its
deterministic salted-HMAC alias (``hash_account_id`` — the same function used
at erasure time), which lets ``stg_transactions`` LEFT JOIN it and mask erased
accounts at query time with no Bronze rewrite and no DELETE.

PII note: the raw account id IS stored here — that is inherent to an erasure
registry (the request is the lawful basis, and the join needs the raw key). It
is only ever projected to its alias on every downstream read; the sole
consumers are the ``stg_transactions`` masking view and the Gold mart.

Usage:
  uv run python -m scripts.seed_gdpr_requests \\
      --account C0000000001 --request gdpr-suggestionfix-001
  uv run python -m scripts.seed_gdpr_requests --input requests.json
      # requests.json = [{"account_id": "...", "request_id": "..."}, ...]
"""

import argparse
import io
import json
import os
import sys
import time
from datetime import UTC, datetime

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.anonymize_batch_gdpr import hash_account_id

REGION = "us-east-1"
GLUE_DATABASE = "streamguard_db"
WORKGROUP = os.getenv("STREAMGUARD_ATHENA_WORKGROUP", "streamguard-dev")
LAKEHOUSE_BUCKET = os.getenv(
    "STREAMGUARD_LAKEHOUSE_BUCKET", "streamguard-lakehouse-83db02e0"
)
GDPR_FOLDER = "gdpr_requests"
RESULTS_BUCKET = os.getenv(
    "STREAMGUARD_ATHENA_RESULTS_BUCKET", "streamguard-athena-results-83db02e0"
)


def build_request_row(account_id: str, request_id: str) -> dict:
    """One append-only registry row: raw id + deterministic salted-HMAC alias."""
    return {
        "account_id": account_id,
        "account_alias": hash_account_id(account_id),
        "request_id": request_id,
        "requested_at": datetime.now(UTC).isoformat(),
    }


def _ddl() -> str:
    return (
        f"CREATE EXTERNAL TABLE IF NOT EXISTS {GLUE_DATABASE}.gdpr_requests (\n"
        "  account_id STRING,\n"
        "  account_alias STRING,\n"
        "  request_id STRING,\n"
        "  requested_at STRING\n"
        ")\n"
        "STORED AS PARQUET\n"
        f"LOCATION 's3://{LAKEHOUSE_BUCKET}/{GDPR_FOLDER}/'\n"
        "TBLPROPERTIES ('external' = 'true')"
    )


def _run_athena(query: str, wait: bool = False) -> None:
    athena = boto3.client("athena", region_name=REGION)
    exec_id = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": GLUE_DATABASE},
        WorkGroup=WORKGROUP,
        ResultConfiguration={"OutputLocation": f"s3://{RESULTS_BUCKET}/query-results/"},
    )["QueryExecutionId"]
    if not wait:
        return
    deadline = time.time() + 120
    while time.time() < deadline:
        status = athena.get_query_execution(QueryExecutionId=exec_id)["QueryExecution"]["Status"]["State"]
        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if status != "SUCCEEDED":
                raise RuntimeError(f"Athena query {status}: {exec_id}")
            return
        time.sleep(3)
    raise RuntimeError(f"Athena query timed out: {exec_id}")


def ensure_registry_table() -> None:
    """Register the external table in Glue/Athena if it does not exist yet."""
    _run_athena(_ddl(), wait=True)


def seed_requests(requests: list[dict]) -> list[str]:
    """Append one immutable parquet per request id; returns written keys."""
    s3 = boto3.client("s3", region_name=REGION)
    written = []
    for req in requests:
        account_id = req["account_id"]
        request_id = req["request_id"]
        key = f"{GDPR_FOLDER}/{request_id}.parquet"
        exists = False
        try:
            s3.head_object(Bucket=LAKEHOUSE_BUCKET, Key=key)
            exists = True
        except Exception:  # noqa: BLE001 - head_object raises NoSuchKey/ClientError
            exists = False
        if exists:
            print(f"[GDPR Registry] {key} already present — append-only, skipping")
            continue
        row = build_request_row(account_id, request_id)
        table = pa.table(
            {
                "account_id": pa.array([row["account_id"]], pa.string()),
                "account_alias": pa.array([row["account_alias"]], pa.string()),
                "request_id": pa.array([row["request_id"]], pa.string()),
                "requested_at": pa.array([row["requested_at"]], pa.string()),
            }
        )
        buf = io.BytesIO()
        pq.write_table(table, buf)
        s3.put_object(
            Bucket=LAKEHOUSE_BUCKET, Key=key, Body=buf.getvalue(), ContentType="application/octet-stream"
        )
        written.append(key)
        print(
            f"[GDPR Registry] Seeded {request_id}: {account_id} -> "
            f"{row['account_alias']} ({key})"
        )
    return written


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", help="account_id to register")
    parser.add_argument("--request", help="request_id (used as the immutable file name)")
    parser.add_argument("--input", help="path to a JSON list of {account_id, request_id}")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    requests: list[dict] = []
    if args.input:
        with open(args.input) as f:
            requests = json.load(f)
    elif args.account and args.request:
        requests = [{"account_id": args.account, "request_id": args.request}]
    else:
        print("Provide either --account + --request, or --input <requests.json>")
        return 2
    ensure_registry_table()
    seed_requests(requests)
    _run_athena(
        f"SELECT request_id, account_alias FROM {GLUE_DATABASE}.gdpr_requests ORDER BY requested_at",
        wait=True,
    )
    print("[GDPR Registry] Registry seeded. dbt now masks these accounts at query time.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
