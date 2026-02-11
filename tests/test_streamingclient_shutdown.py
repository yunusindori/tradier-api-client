"""Unit tests for StreamingClient shutdown behavior.

These tests are offline and focus on Ctrl-C / SIGINT style shutdown races.
We simulate callback invocations during/after stop() to ensure they are quiet
and do not attempt reconnect.
"""

import threading
from unittest.mock import Mock

from tradier_api_client.streaming.streaming_client import StreamingClient


def _bare_client() -> StreamingClient:
    """Create a minimal StreamingClient instance without running __init__.

    Production __init__ may call REST endpoints to fetch a user profile.
    For unit tests we bypass __init__ using __new__ and populate only fields
    required by shutdown/callback code paths.
    """
    client = StreamingClient.__new__(StreamingClient)
    client.logger = Mock()

    client.stop_me = False
    client._stop_event = threading.Event()
    client._is_shutting_down = False

    client.stream_type = "market"
    client.events_destination = None
    client.events_callback = Mock()

    # stream dict shape expected by handle_open/check_session_id_for_stream
    stream = Mock()
    stream.is_running.return_value = False
    client.events_streams = {
        "A1": {
            "stream": stream,
            "session_id": "s1",
            "session_id_last_updated": 1.0,
            "symbols": ["SPY"],
            "event_types": None,
            "last_event_timestamp": None,
        }
    }

    client.account_id_to_api_key = {"A1": "k"}
    client.rest_client = Mock()

    # Methods used in callbacks
    client.check_session_id_for_stream = Mock()
    return client


def test_callbacks_are_quiet_during_shutdown():
    client = _bare_client()

    client.stop_me = True
    client._stop_event.set()
    client._is_shutting_down = True

    # Should not raise
    client.handle_open("A1")
    client.handle_message("A1", {"foo": "bar"})
    client.handle_error("A1", Exception("boom"))
    client.handle_close("A1", 1000, "bye")

    # Use Mock API explicitly for type checkers.
    check_mock: Mock = client.check_session_id_for_stream  # type: ignore[assignment]
    check_mock.assert_not_called()


def test_stop_is_idempotent_even_without_threads_started():
    client = _bare_client()

    # stop() should not raise or hang
    client.stop()
    client.stop()


def test_handle_open_does_not_raise_when_stream_not_connected():
    client = _bare_client()

    # stream.is_running() is False => handle_open should not raise
    client.handle_open("A1")
