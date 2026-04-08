import io
import json
import threading
import unittest
from contextlib import redirect_stdout
from queue import Queue
from unittest.mock import Mock, patch

from tradier_api_client.rest.extensions.orders import OrderWrapper
from tradier_api_client.rest.models.orders_fixed import Order, OrderLeg
from tradier_api_client.rest.rest_client import RestClient
from tradier_api_client.streaming.streaming_client import StreamingClient


class DummyResponse:
    def __init__(self, status_code=200, json_data=None, headers=None, text=""):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.reason = "OK" if status_code < 400 else "ERR"
        self.text = text
        self.headers = headers or {}

        class _Req:
            url = "http://example"
            headers = {}
            body = None

        self.request = _Req()

    def json(self):
        return self._json_data


class ContractFixesTests(unittest.TestCase):
    def test_fundamentals_methods_return_payloads(self):
        client = RestClient("http://example", api_key="k", account_number="acct")

        with patch("requests.request", return_value=DummyResponse(200, {"ok": True})):
            methods = [
                client.get_company,
                client.get_corporate_calendars,
                client.get_dividends,
                client.get_corporate_actions,
                client.get_ratios,
                client.get_financial_reports,
                client.get_price_statistics,
            ]
            for method in methods:
                response = method(["AAPL"])
                self.assertTrue(response["ok"])

    def test_get_orders_single_item_is_flat(self):
        client = RestClient("http://example", api_key="k", account_number="acct")
        payload = {
            "orders": {
                "order": {"id": "1", "symbol": "AAPL"},
                "total_pages": 1,
            }
        }

        with patch("requests.request", return_value=DummyResponse(200, payload)):
            response = client.get_orders("acct")

        self.assertEqual(response["orders"]["order"], [{"id": "1", "symbol": "AAPL"}])

    def test_authenticated_method_returns_bool(self):
        client = RestClient("http://example", api_key="k", account_number="acct")
        self.assertTrue(client.authenticated())
        self.assertTrue(client.is_authenticated)

    def test_place_order_forwards_timeout(self):
        client = RestClient("http://example", api_key="k", account_number="acct")
        order = Order(
            class_="equity",
            legs=[OrderLeg(side="buy", type="market", quantity=1, symbol="AAPL", duration="day")],
        )
        seen = {}

        def fake_request(*args, **kwargs):
            seen["timeout"] = kwargs.get("timeout")
            return DummyResponse(200, {"ok": True})

        with patch("requests.request", side_effect=fake_request):
            response = client.place_order(order, timeout=7.5)

        self.assertTrue(response["ok"])
        self.assertEqual(seen["timeout"], 7.5)

    def test_higher_level_methods_default_timeout_to_five(self):
        client = RestClient("http://example", api_key="k", account_number="acct")
        seen = {}

        def fake_get(path, params=None, headers=None, retry_allowed=False, attempts=None, delay_seconds=None,
                     timeout=None):
            seen["timeout"] = timeout
            return {"quotes": {"quote": []}}

        client.get = fake_get
        client.get_quotes(["AAPL"])

        self.assertEqual(seen["timeout"], 5.0)

    def test_private_account_lookup_raises_runtime_error_without_api_key(self):
        client = RestClient.__new__(RestClient)
        client.api_key = None

        with self.assertRaises(RuntimeError):
            client._RestClient__get_account_number()

    def test_order_wrapper_does_not_print_response(self):
        rest_client = Mock()
        rest_client.place_order.return_value = {"ok": True}
        wrapper = OrderWrapper(rest_client)

        captured = io.StringIO()
        with redirect_stdout(captured):
            response = wrapper.place_bracket_order(
                symbol="AAPL",
                base_side="buy",
                quantity=1,
                base_price=100.0,
            )

        self.assertEqual(response, {"ok": True})
        self.assertEqual(captured.getvalue(), "")

    def test_streaming_messages_are_decoded_before_callback(self):
        client = StreamingClient.__new__(StreamingClient)
        client.logger = Mock()
        client.stop_me = False
        client._stop_event = threading.Event()
        client._is_shutting_down = False
        client.events_destination = None
        client.events_callback = Mock()
        client.events_streams = {"A1": {"last_event_timestamp": None}}

        client.handle_message("A1", json.dumps({"symbol": "AAPL", "price": 123.45}))

        client.events_callback.assert_called_once_with({"symbol": "AAPL", "price": 123.45})
        self.assertIsNotNone(client.events_streams["A1"]["last_event_timestamp"])

    def test_streaming_messages_are_decoded_before_queue_delivery(self):
        client = StreamingClient.__new__(StreamingClient)
        client.logger = Mock()
        client.stop_me = False
        client._stop_event = threading.Event()
        client._is_shutting_down = False
        client.events_destination = Queue()
        client.events_callback = None
        client.events_streams = {"A1": {"last_event_timestamp": None}}

        client.handle_message("A1", json.dumps({"event": "trade"}))

        self.assertEqual(client.events_destination.get_nowait(), {"event": "trade"})


if __name__ == "__main__":
    unittest.main()
