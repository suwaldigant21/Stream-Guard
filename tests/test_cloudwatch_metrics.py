import pytest

from src.consumer import main
from src.consumer.main import dest_window, flush_metrics


@pytest.fixture(autouse=True)
def _clean_state():
    dest_window.clear()
    main._cloudwatch_client = None
    yield
    dest_window.clear()
    main._cloudwatch_client = None


def _fake_client(capture):
    class FakeClient:
        def put_metric_data(self, **kwargs):
            capture.append(kwargs)

    return FakeClient()


def test_publish_sends_three_count_metrics(monkeypatch):
    captured = []
    monkeypatch.setattr(main, "boto3", type("b", (), {"client": lambda *a, **k: _fake_client(captured)}))
    monkeypatch.setattr(main, "CLOUDWATCH_ENABLED", True)

    main.publish_cloudwatch_metrics(scored_count=10, alerts_count=3, error_count=2)

    assert len(captured) == 1
    kwargs = captured[0]
    assert kwargs["Namespace"] == "StreamGuard/Inference"
    by_name = {m["MetricName"]: m for m in kwargs["MetricData"]}
    assert set(by_name) == {"ScoredTransactions", "FraudAlertCount", "ScorerErrorCount"}
    assert by_name["ScoredTransactions"]["Value"] == 10
    assert by_name["FraudAlertCount"]["Value"] == 3
    assert by_name["ScorerErrorCount"]["Value"] == 2
    assert all(m["Unit"] == "Count" for m in kwargs["MetricData"])


def test_consumer_heartbeat_publishes_one_count(monkeypatch):
    captured = []
    monkeypatch.setattr(main, "boto3", type("b", (), {"client": lambda *a, **k: _fake_client(captured)}))
    monkeypatch.setattr(main, "CLOUDWATCH_ENABLED", True)

    main.publish_consumer_heartbeat()

    assert len(captured) == 1
    kwargs = captured[0]
    by_name = {m["MetricName"]: m for m in kwargs["MetricData"]}
    assert by_name == {"ConsumerHeartbeat": {"MetricName": "ConsumerHeartbeat", "Value": 1, "Unit": "Count"}}


def test_consumer_heartbeat_noop_when_disabled(monkeypatch):
    calls = []

    def _boom(*a, **k):
        raise AssertionError("boto3 must not be touched when disabled")

    monkeypatch.setattr(main, "boto3", type("b", (), {"client": _boom}))
    monkeypatch.setattr(main, "CLOUDWATCH_ENABLED", False)

    main.publish_consumer_heartbeat()
    assert calls == []


def test_publish_noop_when_disabled(monkeypatch):
    calls = []

    def _boom(*a, **k):
        raise AssertionError("boto3 must not be touched when disabled")

    monkeypatch.setattr(main, "boto3", type("b", (), {"client": _boom}))
    monkeypatch.setattr(main, "CLOUDWATCH_ENABLED", False)

    main.publish_cloudwatch_metrics(scored_count=1, alerts_count=1, error_count=1)
    assert calls == []


def test_publish_noop_without_boto3(monkeypatch):
    monkeypatch.setattr(main, "boto3", None)
    monkeypatch.setattr(main, "CLOUDWATCH_ENABLED", True)

    main.publish_cloudwatch_metrics(scored_count=1, alerts_count=1, error_count=1)


def test_publish_client_creation_failure_degrades_to_warning(monkeypatch, capsys):
    def _boom(*a, **k):
        raise RuntimeError("no creds")

    monkeypatch.setattr(main, "boto3", type("b", (), {"client": _boom}))
    monkeypatch.setattr(main, "CLOUDWATCH_ENABLED", True)

    main.publish_cloudwatch_metrics(scored_count=1, alerts_count=1, error_count=1)
    assert "CloudWatch client unavailable" in capsys.readouterr().out


def test_publish_put_failure_is_warned_not_raised(monkeypatch, capsys):
    class FailingClient:
        def put_metric_data(self, **kwargs):
            raise ConnectionError("network down")

    monkeypatch.setattr(main, "boto3", type("b", (), {"client": lambda *a, **k: FailingClient()}))
    monkeypatch.setattr(main, "CLOUDWATCH_ENABLED", True)

    main.publish_cloudwatch_metrics(scored_count=1, alerts_count=1, error_count=1)
    assert "Failed to push metrics to CloudWatch" in capsys.readouterr().out


def test_flush_metrics_sends_deltas_and_updates_baseline(monkeypatch):
    sent = {}
    monkeypatch.setattr(
        main, "publish_cloudwatch_metrics",
        lambda **kw: sent.update(kw),
    )
    stats = {"scored": 20, "alerts": 5, "score_errors": 2}
    last = {"scored": 15, "alerts": 3, "score_errors": 1}

    new_last = flush_metrics(stats, last)

    assert sent == {"scored_count": 5, "alerts_count": 2, "error_count": 1}
    assert new_last == stats


def test_handle_transaction_counts_score_errors(monkeypatch):
    monkeypatch.setattr(main, "score_transaction", lambda txn: None)
    stats = {
        "skipped": 0,
        "score_errors": 0,
        "scored": 0,
        "alerts": 0,
        "dq_rejected": 0,
        "fan_in_events": 0,
        "gdpr_purges": 0,
        "gdpr_bad_payloads": 0,
        "skipped_by_type": {},
    }

    main.handle_transaction(
        {
            "type": "CASH_OUT",
            "step": 1,
            "amount": 100.0,
            "nameOrig": "C0000000001",
            "nameDest": "C0000000002",
            "oldbalanceOrg": 1000.0,
            "newbalanceOrig": 900.0,
            "oldbalanceDest": 0.0,
            "newbalanceDest": 100.0,
        },
        stats,
        producer=None,
    )

    assert stats["score_errors"] == 1
    assert stats["scored"] == 0
