"""Unit tests for StreamingClient reconnect and irrecoverable callback behavior.

These tests are intentionally offline (no broker credentials / no network calls).
They validate the bounded reconnect policy and the single-flight reconnect guard.
"""

import threading
from unittest.mock import Mock

import pytest

from tradier_api_client.streaming.streaming_client import StreamingClient


def _make_client(*, stream_type: str, irrecoverable_callback: Mock) -> StreamingClient:
    """Create a StreamingClient instance without running network-heavy __init__.

    The production __init__ creates a RestClient and may call the broker profile endpoint.
    For these unit tests we bypass __init__ using __new__ and populate only the fields
    used by reconnect code.

    :param stream_type: "market" or "account".
    :param irrecoverable_callback: Callback invoked when reconnect budget is exceeded.
    :return: StreamingClient instance.
    """
    client = StreamingClient.__new__(StreamingClient)

    # Minimal fields used by handle_close/reconnect logic
    client.logger = Mock()
    client.stream_type = stream_type
    client.reconnect_attempts = 2
    client.reconnect_base_delay_seconds = 0.0
    client.reconnect_backoff_factor = 1.0
    client.reconnect_jitter_seconds = 0.0
    client.reconnect_max_downtime_seconds = 9999.0
    client.irrecoverable_callback = irrecoverable_callback

    client._reconnect_lock = threading.Lock()
    client._reconnect_in_progress = False
    client._reconnect_first_failure_ts = None
    client._reconnect_attempt_count = 0
    client._irrecoverable_signaled = False

    client._stop_event = threading.Event()
    client.stop_me = False
    client.stream_started = True

    # New reconnect-worker fields
    client._reconnect_thread = None
    client._reconnect_requested = threading.Event()

    # Last close context used by worker
    client._last_close_stream_key = None
    client._last_close_status_code = None
    client._last_close_msg = None

    # Events streams used for session reset (a dict of dicts)
    client.events_streams = {
        "A1": {"session_id": "s1", "session_id_last_updated": 1.0},
        "A2": {"session_id": "s2", "session_id_last_updated": 2.0},
    }

    client.symbols_listened_to = ["SPY"]
    client.event_types = ["trade"]

    # These get replaced per-test
    stop_mock: Mock = Mock()
    restart_mock: Mock = Mock()
    client.stop = stop_mock
    client.restart_streams = restart_mock

    return client


def test_irrecoverable_callback_is_mandatory():
    """Constructor must require irrecoverable_callback."""
    with pytest.raises(Exception):
        StreamingClient(
            main_api_key="k",
            base_url="https://example.com",
            stream_base_url="wss://example.com",
            main_account_id="A1",
            stream_type="account",
            events_callback=lambda _msg: None,
            irrecoverable_callback=None,
        )


def test_handle_close_reconnect_success_resets_session_ids():
    """If restart succeeds, session ids should be preserved for reuse and no irrecoverable callback fires."""
    cb = Mock()
    client = _make_client(stream_type="market", irrecoverable_callback=cb)

    # stop() should set stop_me=True normally; we don't want that in this unit test.
    def _stop_noop():
        client.stop_me = False

    client.stop = Mock(side_effect=_stop_noop)
    client.restart_streams = Mock()

    client.handle_close("A1", close_status_code=1000, close_msg="bye")

    # Trigger worker and wait a moment for it to call restart_streams.
    client._reconnect_requested.set()
    for _ in range(50):
        if client.restart_streams.call_count:
            break
        threading.Event().wait(0.01)

    # Session ids preserved (may be reused within TTL)
    assert client.events_streams["A1"]["session_id"] == "s1"
    assert client.events_streams["A2"]["session_id"] == "s2"

    client.restart_streams.assert_called_once()
    cb.assert_not_called()


def test_handle_close_calls_irrecoverable_after_budget_exceeded():
    """If restart keeps failing beyond attempts budget, irrecoverable_callback should be called once."""
    cb = Mock()
    client = _make_client(stream_type="market", irrecoverable_callback=cb)

    def _stop_noop():
        client.stop_me = False

    client.stop = Mock(side_effect=_stop_noop)

    # Force restart to fail every time.
    client.restart_streams = Mock(side_effect=Exception("boom"))

    client.handle_close("A1", close_status_code=1000, close_msg="bye")

    # Trigger worker and wait for irrecoverable to fire.
    client._reconnect_requested.set()
    for _ in range(200):
        if cb.call_count:
            break
        threading.Event().wait(0.01)

    assert cb.call_count == 1
    args = cb.call_args[0]
    assert args[0] == "market"  # stream_type
    assert args[1] == "reconnect budget exceeded"  # reason
    assert args[2] == "A1"  # stream_key
    assert args[3] == 1000
    assert args[4] == "bye"
    assert isinstance(args[5], int)
    assert isinstance(args[6], float)


def test_handle_close_does_nothing_when_already_stopped():
    """If stop_me is already True, handle_close must not attempt reconnect or invoke callback."""
    cb = Mock()
    client = _make_client(stream_type="market", irrecoverable_callback=cb)
    client.stop_me = True

    # Keep typed refs to mocks for strict analyzers.
    stop_mock: Mock = client.stop  # type: ignore[assignment]
    restart_mock: Mock = client.restart_streams  # type: ignore[assignment]

    client.handle_close("A1", close_status_code=1000, close_msg="bye")

    stop_mock.assert_not_called()
    restart_mock.assert_not_called()
    cb.assert_not_called()


def test_handle_close_is_single_flight_guarded():
    """Concurrent close events should not start multiple reconnect loops."""
    cb = Mock()
    client = _make_client(stream_type="market", irrecoverable_callback=cb)

    # We want one of the calls to block in restart_streams so the other call overlaps.
    gate = threading.Event()

    def _stop_noop():
        client.stop_me = False

    def _restart_blocking():
        gate.wait(timeout=2)

    client.stop = Mock(side_effect=_stop_noop)
    client.restart_streams = Mock(side_effect=_restart_blocking)

    t1 = threading.Thread(target=client.handle_close, args=("A1", 1000, "bye"), daemon=True)
    t2 = threading.Thread(target=client.handle_close, args=("A1", 1000, "bye"), daemon=True)

    t1.start()
    t2.start()

    # Trigger worker and allow the blocking restart to finish.
    client._reconnect_requested.set()
    gate.set()

    t1.join(timeout=2)
    t2.join(timeout=2)

    # Wait briefly for worker to start the restart.
    for _ in range(50):
        if client.restart_streams.call_count:
            break
        threading.Event().wait(0.01)

    assert client.restart_streams.call_count == 1
    cb.assert_not_called()
