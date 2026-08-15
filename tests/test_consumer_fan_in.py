import pytest

from src.consumer.main import FAN_IN_STEP_WINDOW, dest_window, get_and_update_fan_in


@pytest.fixture(autouse=True)
def _clean_window():
    dest_window.clear()
    yield
    dest_window.clear()


def test_fresh_account_returns_zero():
    assert get_and_update_fan_in("C0000000001", 5.0) == 0


def test_window_excludes_current_event():
    get_and_update_fan_in("C0000000001", 5.0)
    assert get_and_update_fan_in("C0000000001", 6.0) == 1


def test_evicts_stale_steps():
    for step in range(100, 101 + FAN_IN_STEP_WINDOW):
        get_and_update_fan_in("C0000000001", float(step))
    # step=100 is now outside the [124-24, 124) window; the 24 inside remain.
    assert get_and_update_fan_in("C0000000001", 125.0) == 24


def test_window_is_per_destination():
    get_and_update_fan_in("C0000000001", 5.0)
    get_and_update_fan_in("C0000000001", 6.0)
    get_and_update_fan_in("C0000000002", 5.0)
    assert get_and_update_fan_in("C0000000002", 6.0) == 1


def test_same_step_counts_in_arrival_order():
    get_and_update_fan_in("C0000000001", 7.0)
    get_and_update_fan_in("C0000000001", 7.0)
    assert get_and_update_fan_in("C0000000001", 7.0) == 2
