"""Phase 7/8 — streaming consumer: real-time fan-in state + fraud scoring + GDPR.

Reads raw PaySim transactions off Redpanda, maintains a rolling windowed fan-in
count per destination account (reproducing the Gold-layer
``fan_in_dest_count_24h`` feature), scores every CASH_OUT/TRANSFER transaction
through the Phase 6 scoring service, and publishes a fraud-alert record for
every scoring hit.

Phase 8: the same consumer also subscribes to the ``gdpr-deletion-requests``
topic and purges erased accounts from the in-memory fan-in state so a forgotten
data subject stops being tracked in real time.

Run (after the scorer on :8001):
    uv run python -m src.consumer.main
"""

import os
import time
from collections import defaultdict, deque

import requests
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

try:
    import boto3
except ImportError:  # pragma: no cover - optional dependency
    boto3 = None

from src.dq import dq_reject_reasons
from streamguard_serializers import JSONDeserializer, JSONSerializer

load_dotenv()

# --- Configuration (from .env; only non-secrets get fallbacks) --------------
BROKER = os.getenv("STREAMGUARD_REDPANDA_BROKER", "localhost:19092")
TOPIC = os.getenv("STREAMGUARD_TOPIC", "transactions-raw")
ALERTS_TOPIC = os.getenv("STREAMGUARD_ALERTS_TOPIC", "fraud-alerts")
GDPR_TOPIC = os.getenv("STREAMGUARD_GDPR_TOPIC", "gdpr-deletion-requests")
CONSUMER_GROUP = os.getenv("STREAMGUARD_CONSUMER_GROUP", "streamguard-scorer")
# The Phase 6 scoring service (mock vendor feed owns :8000).
SCORER_URL = os.getenv("STREAMGUARD_SCORER_URL", "http://127.0.0.1:8001/v1/predict")
SCORER_TIMEOUT = float(os.getenv("STREAMGUARD_SCORER_TIMEOUT", "2.0"))
POLL_TIMEOUT_MS = int(os.getenv("STREAMGUARD_POLL_TIMEOUT_MS", "1000"))
HEARTBEAT_INTERVAL = int(os.getenv("STREAMGUARD_HEARTBEAT_INTERVAL", "5000"))
# P1-4: dead-man's-switch period — a ConsumerHeartbeat metric is pushed every
# HEARTBEAT_PERIOD_S of WALL-CLOCK time (not events), so a silent consumer
# (dead, hung, or simply idle with zero traffic) still proves it is alive.
HEARTBEAT_PERIOD_S = float(os.getenv("STREAMGUARD_HEARTBEAT_PERIOD_S", "60"))

# Phase 9 — CloudWatch custom metrics (boto3 optional so local runs without
# AWS credentials keep working; observability must never take the loop down).
CLOUDWATCH_ENABLED = os.getenv("STREAMGUARD_CLOUDWATCH_ENABLED", "1") != "0"
CLOUDWATCH_NAMESPACE = os.getenv("STREAMGUARD_CLOUDWATCH_NAMESPACE", "StreamGuard/Inference")
AWS_REGION = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))

_cloudwatch_client = None


def _ensure_cloudwatch_client():
    """Lazily create the CloudWatch client; None when disabled/unavailable.

    Importing the module never needs AWS credentials. Every failure degrades to
    a printed WARNING — observability must never take the loop down.
    """
    global _cloudwatch_client
    if not CLOUDWATCH_ENABLED or boto3 is None:
        return None
    if _cloudwatch_client is None:
        try:
            _cloudwatch_client = boto3.client("cloudwatch", region_name=AWS_REGION)
        except Exception as e:  # noqa: BLE001 - observability must degrade, not crash
            print(f"WARNING: CloudWatch client unavailable ({e})")
            return None
    return _cloudwatch_client


def publish_cloudwatch_metrics(scored_count: int, alerts_count: int, error_count: int) -> None:
    """Push one heartbeat's counters as Counts under ``CLOUDWATCH_NAMESPACE``."""
    client = _ensure_cloudwatch_client()
    if client is None:
        return
    try:
        client.put_metric_data(
            Namespace=CLOUDWATCH_NAMESPACE,
            MetricData=[
                {"MetricName": "ScoredTransactions", "Value": scored_count, "Unit": "Count"},
                {"MetricName": "FraudAlertCount", "Value": alerts_count, "Unit": "Count"},
                {"MetricName": "ScorerErrorCount", "Value": error_count, "Unit": "Count"},
            ],
        )
    except Exception as e:  # noqa: BLE001 - observability must degrade, not crash
        print(f"WARNING: Failed to push metrics to CloudWatch ({e})")


def publish_consumer_heartbeat() -> None:
    """Dead-man's-switch: one Count per wall-clock period while the consumer is alive.

    The P1-4 liveness alarm watches for the ABSENCE of these datapoints (a dead
    consumer stops emitting them entirely, which the alarm treats as breaching).
    """
    client = _ensure_cloudwatch_client()
    if client is None:
        return
    try:
        client.put_metric_data(
            Namespace=CLOUDWATCH_NAMESPACE,
            MetricData=[{"MetricName": "ConsumerHeartbeat", "Value": 1, "Unit": "Count"}],
        )
    except Exception as e:  # noqa: BLE001 - observability must degrade, not crash
        print(f"WARNING: Failed to push consumer heartbeat to CloudWatch ({e})")


def flush_metrics(stats: dict, last_published: dict) -> dict:
    """Publish cumulative-vs-last-heartbeat deltas and return the new baseline.

    The alarms evaluate Sum over a 5-minute period, so per-heartbeat deltas
    (not running totals) are what CloudWatch should aggregate.
    """
    deltas = {
        "scored_count": stats["scored"] - last_published["scored"],
        "alerts_count": stats["alerts"] - last_published["alerts"],
        "error_count": stats["score_errors"] - last_published["score_errors"],
    }
    publish_cloudwatch_metrics(**deltas)
    return {k: stats[k] for k in last_published}

# The frozen model was trained ONLY on these two classes (see train/
# extract_gold.py). Everything else is out-of-distribution and skipped, exactly
# like the scorer's 400 type guard.
SCORABLE_TYPES = frozenset({"CASH_OUT", "TRANSFER"})

# Fan-in window: the Gold feature is a RANGE frame over the `step` column
# (PaySim's time unit; 1 step ~ 1 hour), so the tracker windows the last 24
# steps per destination. A wall-clock time.time() window would NOT reproduce
# the trained feature when the producer replays historical rows (they all
# arrive "now").
FAN_IN_STEP_WINDOW = 24

# Destination step-window: {nameDest: deque([step1, step2, ...])} in arrival
# order. Kept strictly in-memory (per the plan); resets to empty on restart,
# and a cold/new destination honestly reads as fan-in 0.
dest_window = defaultdict(deque)

# One keep-alive connection pool for ALL scoring calls. Opening a fresh TCP
# connection per score exhausts the Windows ephemeral-port range (~16K) under
# sustained throughput (WinError 10048) — a Session reuses sockets instead.
http = requests.Session()


def get_and_update_fan_in(name_dest: str, event_step: float) -> int:
    """Return the number of prior events for ``name_dest`` in the window, then
    record the current event.

    The count is computed BEFORE appending the current event (the batch feature
    also excludes the current row). Same-step arrivals count against each other
    in arrival order, matching the inclusive ``CURRENT ROW`` frame boundary.
    """
    cutoff = event_step - FAN_IN_STEP_WINDOW
    dq = dest_window[name_dest]

    while dq and dq[0] < cutoff:
        dq.popleft()

    current_count = len(dq)
    dq.append(event_step)
    return current_count


def score_transaction(txn: dict) -> dict | None:
    """POST a CASH_OUT/TRANSFER transaction to the scoring service.

    Returns the parsed response on success, None on any failure (network,
    non-200, timeout) so one bad score never takes down the loop.
    """
    try:
        resp = http.post(SCORER_URL, json=txn, timeout=SCORER_TIMEOUT)
    except requests.RequestException as e:
        print(f"WARNING: scorer unreachable ({e}) — txn skipped")
        return None
    if resp.status_code != 200:
        print(f"WARNING: scorer HTTP {resp.status_code}: {resp.text[:120]}")
        return None
    return resp.json()


def process_gdpr_erasure(account_id: str) -> bool:
    """Purge a data-subject account from the streaming fan-in window.

    GDPR Article 17 (Right to Erasure): once forgotten, the account must stop
    being tracked. ``dest_window`` is the only account-keyed live state, so
    removing its deque restarts that account's window at the honest prior
    (fan-in 0) for any future transactions. Missing accounts are a no-op.
    """
    if account_id in dest_window:
        del dest_window[account_id]
        print(f"[GDPR] Purged {account_id} from streaming fan-in state.")
        return True
    return False


def handle_gdpr_record(value, stats: dict) -> None:
    """Dispatch a gdpr-deletion-requests payload.

    Payload schema: {"account_id": "C123456789", "request_id": "req-991"}
    """
    if not isinstance(value, dict) or not value.get("account_id"):
        stats["gdpr_bad_payloads"] += 1
        print(f"WARNING: malformed GDPR payload: {value!r}")
        return
    account_id = str(value["account_id"])
    if process_gdpr_erasure(account_id):
        stats["gdpr_purges"] += 1
        request_id = value.get("request_id")
        suffix = f" (request {request_id})" if request_id else ""
        print(f"[GDPR] Erasure completed for {account_id}{suffix}")


def handle_transaction(txn: dict, stats: dict, producer) -> None:
    """Score one transaction: DQ-pass, skip OOD types, enrich fan-in, alert on fraud."""
    ttype = txn.get("type")

    # P2-2: Bronze-layer DQ pass — reject (count + log) rather than score junk.
    problems = dq_reject_reasons(txn)
    if problems:
        stats["dq_rejected"] += 1
        print(f"WARNING: DQ rejected {ttype} — {problems}")
        return

    if ttype not in SCORABLE_TYPES:
        stats["skipped"] += 1
        stats["skipped_by_type"][ttype] += 1
        return

    fan_in = get_and_update_fan_in(txn.get("nameDest"), float(txn.get("step", 0)))
    txn["fan_in_dest_count_24h"] = fan_in
    stats["fan_in_events"] += 1

    result = score_transaction(txn)
    if result is None:
        stats["score_errors"] += 1
        return

    stats["scored"] += 1
    if result.get("is_fraud"):
        stats["alerts"] += 1
        alert = {
            "txn": txn,
            "fraud_probability": result.get("fraud_probability"),
            "threshold": result.get("threshold"),
            "model_version": result.get("model_version"),
        }
        producer.send(ALERTS_TOPIC, value=alert)
        producer.flush()
        print(
            f"ALERT {ttype} {txn.get('nameOrig')} -> "
            f"{txn.get('nameDest')} "
            f"proba={result.get('fraud_probability'):.4f}"
        )


def run_consumer():
    topics = list(dict.fromkeys([TOPIC, GDPR_TOPIC]))
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=[BROKER],
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        value_deserializer=JSONDeserializer(),
        consumer_timeout_ms=POLL_TIMEOUT_MS,
    )
    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=JSONSerializer(),
        acks="all",
    )

    stats = {
        "scored": 0,
        "alerts": 0,
        "score_errors": 0,
        "skipped": 0,
        "dq_rejected": 0,
        "fan_in_events": 0,
        "gdpr_purges": 0,
        "gdpr_bad_payloads": 0,
        "skipped_by_type": defaultdict(int),
    }
    last_published = {"scored": 0, "alerts": 0, "score_errors": 0}
    print(
        f"Streaming scorer started: {TOPIC} -> {SCORER_URL} "
        f"(alerts -> {ALERTS_TOPIC}, gdpr -> {GDPR_TOPIC})"
    )
    # P1-4: emit an immediate heartbeat so the liveness metric is never cold at
    # startup (a fresh ConsumerHeartbeat metric with treat_missing_data =
    # "breaching" would otherwise backfill 3 missing periods and false-alarm).
    publish_consumer_heartbeat()

    try:
        last_heartbeat_at = time.monotonic()
        while True:
            for records in consumer.poll(
                timeout_ms=POLL_TIMEOUT_MS
            ).values():
                for record in records:
                    if record.topic == GDPR_TOPIC:
                        handle_gdpr_record(record.value, stats)
                        continue

                    handle_transaction(record.value, stats, producer)

                    if stats["fan_in_events"] % HEARTBEAT_INTERVAL == 0:
                        last_published = flush_metrics(stats, last_published)
                        print(
                            f"[heartbeat] scored={stats['scored']} "
                            f"alerts={stats['alerts']} skipped={stats['skipped']} "
                            f"dq_rejected={stats['dq_rejected']} "
                            f"score_errors={stats['score_errors']} "
                            f"gdpr_purges={stats['gdpr_purges']} "
                            f"window_events={stats['fan_in_events']}"
                        )

            # P1-4: wall-clock dead-man's switch — independent of traffic, so an
            # idle or hung consumer still proves liveness to the alarm.
            if time.monotonic() - last_heartbeat_at >= HEARTBEAT_PERIOD_S:
                publish_consumer_heartbeat()
                last_heartbeat_at = time.monotonic()
    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")
    except KafkaError as e:
        print(f"Kafka error: {e}")
    finally:
        consumer.close()
        producer.close()
        http.close()
        print(
            f"Session totals: scored={stats['scored']} "
            f"alerts={stats['alerts']} skipped={stats['skipped']} "
            f"dq_rejected={stats['dq_rejected']} "
            f"gdpr_purges={stats['gdpr_purges']} "
            f"skipped_by_type={dict(stats['skipped_by_type'])}"
        )


if __name__ == "__main__":
    run_consumer()
