"""Phase 10 (P1-1) — GDPR Article 17 request-submission endpoint.

``POST /v1/gdpr/erasure`` turns the two-layer purge mechanics into a verifiable
request surface: it validates and records the erasure request (non-PII audit),
publishes it to ``gdpr-deletion-requests`` so the streaming consumer purges the
account's fan-in state, and runs the batch anonymizer over the cached training
parquet files. Included in the Phase 6 scoring service via ``src/api/main.py``.

Article 17 language per the plan: "implements GDPR Article 17 (Right to
Erasure) via a verifiable cascading-delete endpoint" — never "GDPR-compliant".
"""

import os
import re
import uuid
from datetime import UTC, datetime

from dotenv import load_dotenv
from fastapi import APIRouter
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pydantic import BaseModel, field_validator

from scripts.anonymize_batch_gdpr import append_audit_entry, execute_erasure
from streamguard_serializers import JSONSerializer

load_dotenv()

BROKER = os.getenv("STREAMGUARD_REDPANDA_BROKER", "localhost:19092")
GDPR_TOPIC = os.getenv("STREAMGUARD_GDPR_TOPIC", "gdpr-deletion-requests")

# PaySim account ids are a single letter ("C" customer / "M" merchant) followed
# by digits — 100 % of a 50k-row sample, lengths 6-11.
_ACCOUNT_ID_RE = re.compile(r"^[A-Za-z][0-9]{2,31}$")

_producer = None


class GdprErasureRequest(BaseModel):
    account_id: str
    request_id: str | None = None

    @field_validator("account_id")
    @classmethod
    def _validate_account_id(cls, value: str) -> str:
        if not _ACCOUNT_ID_RE.match(value):
            raise ValueError("account_id must be a letter followed by digits (e.g. C123456789)")
        return value


class GdprErasureResponse(BaseModel):
    request_id: str
    account_alias: str
    status: str
    streaming_purge_published: bool
    rows_anonymized: int
    timestamp: str


router = APIRouter(prefix="/v1/gdpr", tags=["gdpr"])


def _get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=[BROKER],
            value_serializer=JSONSerializer(),
            acks="all",
            max_block_ms=2000,
        )
    return _producer


def publish_erasure(payload: dict) -> bool:
    """Best-effort publish to ``gdpr-deletion-requests``.

    Returns False (and logs a WARNING) on any Kafka failure so the batch half
    still runs and the request is never lost to the requester silently.
    """
    try:
        _get_producer().send(GDPR_TOPIC, value=payload)
        _get_producer().flush()
        return True
    except KafkaError as e:
        print(f"WARNING: GDPR erasure publish failed ({e})")
        return False


@router.post("/erasure", response_model=GdprErasureResponse, status_code=202)
def submit_erasure(req: GdprErasureRequest) -> GdprErasureResponse:
    request_id = req.request_id or f"req-{uuid.uuid4().hex[:8]}"
    payload = {"account_id": req.account_id, "request_id": request_id}

    streaming_published = publish_erasure(payload)
    alias, rows = execute_erasure(req.account_id, request_id, write_audit=False)

    timestamp = datetime.now(UTC).isoformat()
    append_audit_entry(
        {
            "timestamp": timestamp,
            "request_id": request_id,
            "account_alias": alias,
            "streaming_purge_published": streaming_published,
            "rows_anonymized": rows,
        }
    )

    return GdprErasureResponse(
        request_id=request_id,
        account_alias=alias,
        status="erasure_requested",
        streaming_purge_published=streaming_published,
        rows_anonymized=rows,
        timestamp=timestamp,
    )
