"""P2-2 — Bronze-layer DQ pass: check logic + consumer wiring."""

from collections import defaultdict

import pytest

from src.consumer import main as consumer
from src.dq import DQ_BALANCE_FIELDS, DQ_REQUIRED_FIELDS, dq_reject_reasons, is_dq_clean


def _clean_txn():
    return {
        "step": 1,
        "type": "CASH_OUT",
        "amount": 181.0,
        "nameOrig": "C0000000001",
        "oldbalanceOrg": 1000.0,
        "newbalanceOrig": 819.0,
        "nameDest": "C0000000002",
        "oldbalanceDest": 0.0,
        "newbalanceDest": 181.0,
        "isFraud": 0,
        "isFlaggedFraud": 0,
    }


def test_clean_transaction_passes():
    assert dq_reject_reasons(_clean_txn()) == []
    assert is_dq_clean(_clean_txn()) is True


def test_missing_required_field_is_rejected():
    txn = _clean_txn()
    del txn["amount"]
    assert any("amount" in p for p in dq_reject_reasons(txn))


def test_null_required_field_is_rejected():
    txn = _clean_txn()
    txn["nameDest"] = None
    assert any("nameDest" in p for p in dq_reject_reasons(txn))


def test_negative_amount_is_rejected():
    txn = _clean_txn()
    txn["amount"] = -5.0
    assert any("negative amount" in p for p in dq_reject_reasons(txn))


def test_negative_balance_is_rejected():
    txn = _clean_txn()
    txn["newbalanceDest"] = -1.0
    assert any("newbalanceDest" in p for p in dq_reject_reasons(txn))


def test_non_dict_is_rejected():
    assert dq_reject_reasons("not-a-txn") == ["not a dict"]


def test_all_problems_collected():
    txn = _clean_txn()
    txn["amount"] = -1.0
    txn["nameOrig"] = None
    reasons = dq_reject_reasons(txn)
    assert len(reasons) == 2


def test_check_field_sets_match_expected_names():
    assert set(DQ_REQUIRED_FIELDS) == {"step", "type", "amount", "nameOrig", "nameDest"}
    assert set(DQ_BALANCE_FIELDS) == {
        "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest",
    }


@pytest.fixture(autouse=True)
def _clean_window():
    consumer.dest_window.clear()
    yield
    consumer.dest_window.clear()


def _stats():
    return {
        "scored": 0,
        "alerts": 0,
        "score_errors": 0,
        "skipped": 0,
        "dq_rejected": 0,
        "fan_in_events": 0,
        "skipped_by_type": defaultdict(int),
    }


class _FakeProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, value):
        self.sent.append((topic, value))
        return self

    def flush(self):
        pass


def test_handle_transaction_rejects_bad_row_without_scoring(monkeypatch):
    producer = _FakeProducer()
    stats = _stats()
    score_calls = []
    monkeypatch.setattr(consumer, "score_transaction", lambda txn: score_calls.append(txn))

    bad = _clean_txn()
    bad["amount"] = -10.0
    consumer.handle_transaction(bad, stats, producer)

    assert stats["dq_rejected"] == 1
    assert stats["scored"] == 0
    assert stats["skipped"] == 0
    assert stats["fan_in_events"] == 0
    assert producer.sent == []
    assert score_calls == []


def test_handle_transaction_still_scores_clean_row(monkeypatch):
    producer = _FakeProducer()
    stats = _stats()
    monkeypatch.setattr(
        consumer,
        "score_transaction",
        lambda txn: {"is_fraud": False, "fraud_probability": 0.01, "threshold": 0.5, "model_version": "t"},
    )

    consumer.handle_transaction(_clean_txn(), stats, producer)

    assert stats["dq_rejected"] == 0
    assert stats["scored"] == 1
