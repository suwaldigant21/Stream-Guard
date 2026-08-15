import pytest

from src.consumer.main import (
    dest_window,
    get_and_update_fan_in,
    handle_gdpr_record,
    process_gdpr_erasure,
)


@pytest.fixture(autouse=True)
def _clean_window():
    dest_window.clear()
    yield
    dest_window.clear()


def test_erasure_purges_account_from_window():
    get_and_update_fan_in("C0000000001", 1.0)
    get_and_update_fan_in("C0000000001", 2.0)
    assert "C0000000001" in dest_window

    assert process_gdpr_erasure("C0000000001") is True
    assert "C0000000001" not in dest_window


def test_erasure_of_unknown_account_is_noop():
    assert process_gdpr_erasure("C9999999999") is False
    assert "C9999999999" not in dest_window


def test_forgotten_account_restarts_at_honest_prior():
    get_and_update_fan_in("C0000000001", 1.0)
    get_and_update_fan_in("C0000000001", 2.0)
    process_gdpr_erasure("C0000000001")

    assert get_and_update_fan_in("C0000000001", 3.0) == 0


def test_erasure_does_not_affect_other_accounts():
    get_and_update_fan_in("C0000000001", 1.0)
    get_and_update_fan_in("C0000000002", 5.0)
    process_gdpr_erasure("C0000000001")

    assert "C0000000002" in dest_window


def test_handle_gdpr_record_counts_purge():
    stats = {"gdpr_purges": 0, "gdpr_bad_payloads": 0}
    get_and_update_fan_in("C0000000001", 1.0)

    handle_gdpr_record(
        {"account_id": "C0000000001", "request_id": "req-991"}, stats
    )

    assert stats["gdpr_purges"] == 1
    assert stats["gdpr_bad_payloads"] == 0
    assert "C0000000001" not in dest_window


def test_handle_gdpr_record_ignores_malformed_payloads():
    stats = {"gdpr_purges": 0, "gdpr_bad_payloads": 0}

    handle_gdpr_record({"request_id": "req-1"}, stats)
    handle_gdpr_record("not-a-dict", stats)

    assert stats["gdpr_purges"] == 0
    assert stats["gdpr_bad_payloads"] == 2
