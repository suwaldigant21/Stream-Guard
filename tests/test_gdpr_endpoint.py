import json
import os
import re

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from scripts import anonymize_batch_gdpr as batch
from src.api import gdpr
from src.api.main import METADATA_PATH, MODEL_PATH, app

pytestmark = pytest.mark.skipif(
    not os.path.exists(MODEL_PATH) or not os.path.exists(METADATA_PATH),
    reason="frozen Phase 5b model artifacts missing (run scripts/export_phase5b.py)",
)

ACCOUNT = "C123456789"
ALIAS_RE = re.compile(r"ANON_[0-9a-f]{16}")


@pytest.fixture
def patched(monkeypatch, tmp_path):
    # Point the batch anonymizer at a tiny PII-bearing frame + a fresh audit log
    # so the real rewrite path runs deterministically (never touches Gold).
    df = pd.DataFrame(
        {
            "name_orig": ["C123456789", "C111111111", "C123456789"],
            "name_dest": ["M000000000", "C123456789", "M000000000"],
            "amount": [10.0, 20.0, 30.0],
        }
    )
    target = tmp_path / "pii.parquet"
    df.to_parquet(target)
    audit = tmp_path / "audit.json"
    monkeypatch.setattr(batch, "TARGET_FILES", [str(target)])
    monkeypatch.setattr(batch, "AUDIT_LOG_PATH", str(audit))
    monkeypatch.setattr(gdpr, "publish_erasure", lambda payload: True)
    return {"target": str(target), "audit": str(audit)}


def test_erasure_request_202_batches_and_audits(patched):
    with TestClient(app) as client:
        resp = client.post("/v1/gdpr/erasure", json={"account_id": ACCOUNT})
    assert resp.status_code == 202
    body = resp.json()
    assert body["request_id"].startswith("req-")
    assert ALIAS_RE.fullmatch(body["account_alias"])
    assert body["status"] == "erasure_requested"
    assert body["streaming_purge_published"] is True
    # 3 matches: name_orig x2 + name_dest x1
    assert body["rows_anonymized"] == 3

    with open(patched["audit"]) as f:
        audit = json.load(f)
    assert len(audit) == 1
    assert audit[0]["request_id"] == body["request_id"]
    assert audit[0]["account_alias"] == body["account_alias"]
    assert ACCOUNT not in json.dumps(audit)

    df = pd.read_parquet(patched["target"])
    assert (df.values == ACCOUNT).sum() == 0


def test_erasure_uses_supplied_request_id(patched):
    with TestClient(app) as client:
        resp = client.post(
            "/v1/gdpr/erasure",
            json={"account_id": ACCOUNT, "request_id": "req-fixed-001"},
        )
    assert resp.status_code == 202
    assert resp.json()["request_id"] == "req-fixed-001"


def test_erasure_publish_failure_still_batches(patched, monkeypatch):
    monkeypatch.setattr(gdpr, "publish_erasure", lambda payload: False)
    with TestClient(app) as client:
        resp = client.post("/v1/gdpr/erasure", json={"account_id": ACCOUNT})
    assert resp.status_code == 202
    body = resp.json()
    assert body["streaming_purge_published"] is False
    assert body["rows_anonymized"] == 3


def test_erasure_unknown_account_is_noop(patched):
    with TestClient(app) as client:
        resp = client.post("/v1/gdpr/erasure", json={"account_id": "M2044282225"})
    assert resp.status_code == 202
    assert resp.json()["rows_anonymized"] == 0


def test_erasure_invalid_account_id_rejected():
    with TestClient(app) as client:
        for bad in ["", "123456789", "C12$3", "  C123456789", "C"]:
            resp = client.post("/v1/gdpr/erasure", json={"account_id": bad})
            assert resp.status_code == 422, bad
