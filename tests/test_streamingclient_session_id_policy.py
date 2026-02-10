"""Unit tests for StreamingClient session id policy.

These tests are offline and validate that session ids are only obtained when missing,
never rotated based on time or inactivity.
"""

import threading
from unittest.mock import Mock
import time

from tradier_api_client.streaming.streaming_client import StreamingClient


def _client_with_stream_dict(session_id):
    client = StreamingClient.__new__(StreamingClient)
    client.logger = Mock()
    client.stop_me = False
    client._stop_event = threading.Event()
    client._is_shutting_down = False

    client.account_id_to_api_key = {"A1": "k"}
    client.rest_client = Mock()
    client.stream_type = "market"
    client.session_id_ttl_seconds = 300.0

    client.events_streams = {
        "A1": {
            "stream": Mock(),
            "session_id": session_id,
            "session_id_last_updated": 1.0,
            "symbols": ["SPY"],
            "event_types": None,
            "last_event_timestamp": 0.0,
        }
    }
    return client


def test_session_id_refresh_condition_only_when_missing_or_expired(monkeypatch):
    now = 1000.0
    monkeypatch.setattr(time, "time", lambda: now)

    client = _client_with_stream_dict(session_id="s1")

    # Within TTL (age=10s) => reuse
    client.events_streams["A1"]["session_id_last_updated"] = now - 10
    assert client._session_id_refresh_condition_met("A1", client.events_streams["A1"]) is False

    # Expired (age=301s) => refresh
    client.events_streams["A1"]["session_id_last_updated"] = now - 301
    assert client._session_id_refresh_condition_met("A1", client.events_streams["A1"]) is True

    client2 = _client_with_stream_dict(session_id=None)
    assert client2._session_id_refresh_condition_met("A1", client2.events_streams["A1"]) is True

    client3 = _client_with_stream_dict(session_id="")
    assert client3._session_id_refresh_condition_met("A1", client3.events_streams["A1"]) is True


def test_handle_open_does_not_fetch_new_session_id_when_one_exists(monkeypatch):
    now = 1000.0
    monkeypatch.setattr(time, "time", lambda: now)

    client = _client_with_stream_dict(session_id="s1")
    # Within TTL so it should not refresh
    client.events_streams["A1"]["session_id_last_updated"] = now - 10

    client._get_session_id_from_server = Mock(return_value="NEW")

    stream = Mock()
    stream.is_running.return_value = False
    client.events_streams["A1"]["stream"] = stream

    client.handle_open("A1")

    client._get_session_id_from_server.assert_not_called()
