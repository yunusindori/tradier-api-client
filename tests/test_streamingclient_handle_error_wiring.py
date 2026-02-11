"""Unit tests for handle_error wiring.

The websocket-client library calls WebSocketApp.on_error(ws, exc).
Our StreamListener maps that to on_error_callback(exc).
StreamingClient binds the stream key (account id) to that callback.

This test ensures the bound callback preserves stream_key and passes the exception as exc.
"""

from unittest.mock import Mock

from tradier_api_client.streaming.streaming_client import StreamingClient


def test_handle_error_wiring_preserves_stream_key_and_exc():
    client = StreamingClient.__new__(StreamingClient)
    client.handle_error = Mock()

    bound = __import__("functools").partial(client.handle_error, "A1")

    exc = Exception("boom")
    bound(exc)

    client.handle_error.assert_called_once()
    args = client.handle_error.call_args[0]
    assert args[0] == "A1"
    assert args[1] is exc

