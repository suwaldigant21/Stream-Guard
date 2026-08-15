"""Phase 6 — fraud scoring service.

Loads the frozen Phase 5b XGBoost model + metadata once at startup and serves
low-latency inference at POST /v1/predict. Feature derivation replicates the
Gold-layer engineering (see dbt/models/staging/stg_transactions.sql) in the
exact column order frozen in metadata.json["features"].

Run:  uv run uvicorn src.api.main:app --port 8001
"""

import asyncio
import json
import os
from collections.abc import Iterator
from contextlib import asynccontextmanager

import xgboost as xgb
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from src.api.gdpr import router as gdpr_router

MODEL_DIR = os.getenv("STREAMGUARD_MODEL_DIR", "data/model_artifacts")
MODEL_PATH = os.path.join(MODEL_DIR, "model.json")
METADATA_PATH = os.path.join(MODEL_DIR, "metadata.json")


class TransactionPayload(BaseModel):
    """Raw PaySim-style transaction (mirrors the streaming schema)."""

    step: int
    type: str
    amount: float
    nameOrig: str
    oldbalanceOrg: float
    newbalanceOrig: float
    nameDest: str
    oldbalanceDest: float
    newbalanceDest: float
    isFraud: int | None = None
    isFlaggedFraud: int | None = None
    fan_in_dest_count_24h: float | None = Field(
        default=None,
        description=(
            "Optional pre-computed upstream 24h destination fan-in. If omitted, "
            "the honest prior 0.0 is used (no observed fan-in)."
        ),
    )


class PredictResponse(BaseModel):
    is_fraud: bool
    fraud_probability: float
    threshold: float
    model_version: str
    feature_names: list[str]
    features: dict[str, float]


def build_feature_vector(payload: TransactionPayload) -> dict[str, float]:
    """Replicate the Gold-layer derived features (stg_transactions.sql)."""
    balance_delta_orig = payload.oldbalanceOrg - payload.newbalanceOrig
    balance_delta_dest = payload.newbalanceDest - payload.oldbalanceDest
    return {
        "amount": payload.amount,
        "has_balance_orig": float(payload.oldbalanceOrg > 0),
        "balance_delta_orig": balance_delta_orig,
        "error_balance_orig": balance_delta_orig - payload.amount,
        "has_balance_dest": float(payload.oldbalanceDest > 0),
        "balance_delta_dest": balance_delta_dest,
        "error_balance_dest": (
            payload.oldbalanceDest + payload.amount - payload.newbalanceDest
        ),
        "is_flagged_fraud": float(payload.isFlaggedFraud or 0),
        "fan_in_dest_count_24h": float(payload.fan_in_dest_count_24h or 0),
    }


@asynccontextmanager
async def lifespan(app: FastAPI) -> Iterator[None]:
    model, metadata = await asyncio.to_thread(_load_artifacts)
    app.state.model = model
    app.state.metadata = metadata
    yield


def _load_artifacts() -> tuple:
    if not os.path.exists(MODEL_PATH) or not os.path.exists(METADATA_PATH):
        raise RuntimeError(
            f"Frozen model artifacts not found in {MODEL_DIR} — run "
            "scripts/export_phase5b.py first."
        )
    model = xgb.Booster()
    model.load_model(MODEL_PATH)
    with open(METADATA_PATH) as f:
        return model, json.load(f)


app = FastAPI(
    title="StreamGuard Fraud Scoring",
    version="1.0.0",
    lifespan=lifespan,
)
app.include_router(gdpr_router)


@app.post("/v1/predict", response_model=PredictResponse)
def predict(payload: TransactionPayload) -> PredictResponse:
    # The frozen model was trained ONLY on CASH_OUT/TRANSFER (the fraud-bearing
    # classes; see train/extract_gold.py). Other types are out-of-distribution —
    # probing real PAYMENT rows scores ~0.99 both ways, so never serve them.
    if payload.type not in ("CASH_OUT", "TRANSFER"):
        raise HTTPException(
            status_code=400,
            detail=(
                f"type '{payload.type}' is out of distribution — the frozen model "
                "was trained only on CASH_OUT/TRANSFER (the fraud-bearing classes)."
            ),
        )

    metadata = app.state.metadata
    feature_values = build_feature_vector(payload)
    ordered = [feature_values[name] for name in metadata["features"]]

    pred = app.state.model.predict(
        xgb.DMatrix([ordered], feature_names=metadata["features"])
    )
    proba = float(pred[0, 1] if pred.ndim == 2 else pred[0])

    threshold = float(metadata["decision_threshold"])
    version = f'{metadata["phase"]}-t{metadata["num_trees"]}'

    return PredictResponse(
        is_fraud=proba >= threshold,
        fraud_probability=proba,
        threshold=threshold,
        model_version=version,
        feature_names=metadata["features"],
        features=feature_values,
    )
