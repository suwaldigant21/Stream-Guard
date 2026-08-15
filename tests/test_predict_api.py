import json
import os

import pandas as pd
import pytest
import xgboost as xgb
from fastapi.testclient import TestClient

from src.api.main import (
    METADATA_PATH,
    MODEL_PATH,
    TransactionPayload,
    app,
    build_feature_vector,
)

pytestmark = pytest.mark.skipif(
    not os.path.exists(MODEL_PATH) or not os.path.exists(METADATA_PATH),
    reason="frozen Phase 5b model artifacts missing (run scripts/export_phase5b.py)",
)

DATASET_PATH = os.getenv(
    "STREAMGUARD_DATASET_PARQUET", "PS_20174392719_1491204439457_log.parquet"
)


def _payload(**overrides):
    base = {
        "step": 1,
        "type": "CASH_OUT",
        "amount": 181.0,
        "nameOrig": "C0000000001",
        "oldbalanceOrg": 0.0,
        "newbalanceOrig": 0.0,
        "nameDest": "C0000000002",
        "oldbalanceDest": 0.0,
        "newbalanceDest": 181.0,
        "isFraud": 1,
        "isFlaggedFraud": 0,
    }
    base.update(overrides)
    return base


def _metadata():
    with open(METADATA_PATH) as f:
        return json.load(f)


def _reference_proba(payload):
    metadata = _metadata()
    booster = xgb.Booster()
    booster.load_model(MODEL_PATH)
    fv = build_feature_vector(TransactionPayload(**payload))
    ordered = [fv[name] for name in metadata["features"]]
    raw = booster.predict(xgb.DMatrix([ordered], feature_names=metadata["features"]))
    return float(raw[0, 1] if raw.ndim == 2 else raw[0])


def test_predict_contract():
    with TestClient(app) as client:
        resp = client.post("/v1/predict", json=_payload())
    assert resp.status_code == 200
    body = resp.json()
    assert set(body) == {
        "is_fraud",
        "fraud_probability",
        "threshold",
        "model_version",
        "feature_names",
        "features",
    }
    metadata = _metadata()
    assert body["feature_names"] == metadata["features"]
    assert body["threshold"] == metadata["decision_threshold"]
    assert body["model_version"] == f'{metadata["phase"]}-t{metadata["num_trees"]}'
    assert 0.0 <= body["fraud_probability"] <= 1.0
    assert isinstance(body["is_fraud"], bool)


def test_probability_matches_direct_booster():
    payload = _payload()
    with TestClient(app) as client:
        body = client.post("/v1/predict", json=payload).json()
    assert body["fraud_probability"] == pytest.approx(
        _reference_proba(payload), rel=1e-6
    )


def client_post(payload):
    with TestClient(app) as client:
        return client.post("/v1/predict", json=payload)


def test_fan_in_defaults_to_zero_and_overrides():
    base = _payload()
    default_body = client_post(base).json()
    override_body = client_post({**base, "fan_in_dest_count_24h": 90}).json()
    assert default_body["features"]["fan_in_dest_count_24h"] == 0.0
    assert override_body["features"]["fan_in_dest_count_24h"] == 90.0
    assert default_body["fraud_probability"] != override_body["fraud_probability"]


def test_out_of_distribution_type_rejected():
    resp = client_post(_payload(type="PAYMENT"))
    assert resp.status_code == 400
    assert "out of distribution" in resp.json()["detail"]


@pytest.mark.skipif(
    not os.path.exists(DATASET_PATH),
    reason="PaySim dataset not present locally",
)
def test_real_in_distribution_rows_separate():
    # Label assertions use REAL dataset rows only: crafted payloads cannot proxy
    # fraud/legit — real PaySim legit rows carry large balance errors too, so the
    # model's learned boundary is not a simple "error != 0" threshold.
    df = pd.read_parquet(
        DATASET_PATH,
        columns=[
            "step", "type", "amount", "nameOrig", "oldbalanceOrg",
            "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest",
            "isFraud", "isFlaggedFraud",
        ],
    )
    in_dist = df[df["type"].isin(["CASH_OUT", "TRANSFER"])]
    fraud = in_dist[in_dist["isFraud"] == 1].iloc[0].to_dict()
    legit = in_dist[in_dist["isFraud"] == 0].iloc[0].to_dict()

    fraud_body = client_post(fraud).json()
    legit_body = client_post(legit).json()
    assert fraud_body["fraud_probability"] >= 0.90
    assert legit_body["fraud_probability"] <= 0.05
    assert fraud_body["is_fraud"] is True
    assert legit_body["is_fraud"] is False
