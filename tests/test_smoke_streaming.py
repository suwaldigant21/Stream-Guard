"""P1-3 — streaming smoke test: produce -> consume -> score -> alert.

Broker-backed (real Redpanda, same pattern as test_producer_integration.py):
produce a mixed batch of PaySim-shaped messages to a throwaway topic, run the
consumer's processing path against a *fake* scorer, and assert exactly the right
transactions are scored/skipped and the fraud alert lands on the alerts topic.
Also covers the PaySim + alert schema-contract checks (drift flagging).

Requires the Redpanda broker on ``localhost:19092`` (standard for the gate).
"""

import uuid
from collections import defaultdict

import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from src.consumer import main
from src.consumer.schema import validate_alert, validate_paysim_txn
from streamguard_serializers import JSONDeserializer, JSONSerializer

BROKER = "localhost:19092"
FRAUD_AMOUNT = 1_000_000.0


@pytest.fixture(autouse=True)
def _clean_window():
    main.dest_window.clear()
    yield
    main.dest_window.clear()


def _pay_sim(**overrides):
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
        "isFraud": 0,
        "isFlaggedFraud": 0,
    }
    base.update(overrides)
    return base


def _new_group():
    return f"smoke-{uuid.uuid4().hex[:8]}"


def _read_messages(topic, max_msgs):
    """Drain up to ``max_msgs`` messages from ``topic`` (fresh group, earliest)."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[BROKER],
        group_id=_new_group(),
        auto_offset_reset="earliest",
        value_deserializer=JSONDeserializer(),
        consumer_timeout_ms=3000,
    )
    out = []
    try:
        for _ in range(max_msgs):
            try:
                out.append(next(consumer).value)
            except StopIteration:
                break
    finally:
        consumer.close()
    return out


@pytest.fixture()
def smoke_topics():
    suffix = uuid.uuid4().hex[:8]
    txn_topic = f"smoke-txn-{suffix}"
    alerts_topic = f"smoke-alerts-{suffix}"
    admin = KafkaAdminClient(bootstrap_servers=[BROKER], client_id="smoke-admin")
    try:
        admin.create_topics(
            [
                NewTopic(txn_topic, num_partitions=1, replication_factor=1),
                NewTopic(alerts_topic, num_partitions=1, replication_factor=1),
            ],
            timeout_ms=10000,
        )
    finally:
        admin.close()
    yield txn_topic, alerts_topic
    admin = KafkaAdminClient(bootstrap_servers=[BROKER], client_id="smoke-admin")
    try:
        admin.delete_topics([txn_topic, alerts_topic], timeout_ms=10000)
    finally:
        admin.close()


@pytest.fixture()
def producer():
    p = KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=JSONSerializer(),
        acks="all",
    )
    yield p
    p.close()


def test_produce_consume_score_alert_smoke(monkeypatch, smoke_topics, producer):
    txn_topic, alerts_topic = smoke_topics
    monkeypatch.setattr(main, "ALERTS_TOPIC", alerts_topic)

    batch = [
        _pay_sim(step=1, type="CASH_OUT", amount=FRAUD_AMOUNT, nameDest="C0000000099", isFraud=1),
        _pay_sim(step=1, type="CASH_OUT", amount=200.0, nameDest="C0000000011"),
        _pay_sim(step=2, type="TRANSFER", amount=5000.0, nameDest="C0000000022"),
        _pay_sim(step=2, type="TRANSFER", amount=80.0, nameDest="C0000000033"),
        _pay_sim(step=3, type="PAYMENT", amount=10.0, nameDest="C0000000044"),
        _pay_sim(step=3, type="DEBIT", amount=40.0, nameDest="C0000000055"),
        _pay_sim(step=4, type="CASH_IN", amount=1000.0, nameDest="C0000000066"),
        _pay_sim(step=5, type="CASH_OUT", amount=-10.0, nameDest="C0000000077"),
    ]
    for txn in batch:
        producer.send(txn_topic, value=txn)
    producer.flush()

    def fake_score(txn):
        fraud = txn["amount"] == FRAUD_AMOUNT
        return {
            "is_fraud": fraud,
            "fraud_probability": 0.9999 if fraud else 0.001,
            "threshold": 0.9866,
            "model_version": "frozen-2026-08-11",
        }

    monkeypatch.setattr(main, "score_transaction", fake_score)

    consumer = KafkaConsumer(
        txn_topic,
        bootstrap_servers=[BROKER],
        group_id=_new_group(),
        auto_offset_reset="earliest",
        value_deserializer=JSONDeserializer(),
        consumer_timeout_ms=3000,
    )
    stats = {
        "scored": 0,
        "alerts": 0,
        "score_errors": 0,
        "skipped": 0,
        "dq_rejected": 0,
        "fan_in_events": 0,
        "skipped_by_type": defaultdict(int),
    }
    processed = []
    try:
        while True:
            try:
                record = next(consumer)
            except StopIteration:
                break
            processed.append(record.value)
            main.handle_transaction(record.value, stats, producer)
    finally:
        consumer.close()

    assert [t["type"] for t in processed] == [
        "CASH_OUT", "CASH_OUT", "TRANSFER", "TRANSFER",
        "PAYMENT", "DEBIT", "CASH_IN", "CASH_OUT",
    ]
    assert stats["scored"] == 4
    assert stats["alerts"] == 1
    assert stats["skipped"] == 3
    assert stats["dq_rejected"] == 1
    assert stats["score_errors"] == 0
    assert stats["fan_in_events"] == 4
    assert dict(stats["skipped_by_type"]) == {"PAYMENT": 1, "DEBIT": 1, "CASH_IN": 1}

    alerts = _read_messages(alerts_topic, max_msgs=2)
    assert len(alerts) == 1, f"expected exactly 1 alert, got {alerts!r}"
    alert = alerts[0]
    assert validate_alert(alert) == []
    assert alert["fraud_probability"] == 0.9999
    assert alert["threshold"] == 0.9866
    assert alert["model_version"] == "frozen-2026-08-11"
    assert validate_paysim_txn(alert["txn"]) == []
    assert alert["txn"]["fan_in_dest_count_24h"] == 0


def test_paysim_schema_contract_flags_drift():
    assert validate_paysim_txn(_pay_sim()) == []

    renamed = _pay_sim(oldbalanceOrig=50.0)
    del renamed["oldbalanceOrg"]
    problems = validate_paysim_txn(renamed)
    assert any("oldbalanceOrg" in p for p in problems)

    bad_type = _pay_sim(amount="large")
    assert any("amount" in p for p in validate_paysim_txn(bad_type))

    unknown = _pay_sim(type="CASH_OUT2")
    assert any("unknown type" in p for p in validate_paysim_txn(unknown))

    missing = {k: v for k, v in _pay_sim().items() if k != "nameDest"}
    assert any("nameDest" in p for p in validate_paysim_txn(missing))


def test_alert_schema_contract_flags_missing_fields():
    alert = {"txn": {}, "fraud_probability": 0.9999}
    problems = validate_alert(alert)
    assert any("threshold" in p for p in problems)
    assert any("model_version" in p for p in problems)

    assert validate_alert("not-a-dict") == ["alert is not a dict"]
