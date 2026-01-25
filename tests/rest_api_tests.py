"""
Unit test all the methods in rest_client.py
"""
import unittest
from unittest.mock import patch, MagicMock

from requests import HTTPError

from tradier_api_client.rest.rest_client import RestClient


class TestRestClient(unittest.TestCase):
    """Unit tests for the RestClient class."""

    def setUp(self):
        """Set up common test fixtures (API key, dummy account id, and base URL)."""
        self.api_key = "your_api_key"  # Inject your API key here
        self.account_id = "DUMMY_ACCOUNT"
        self.base_url = "https://example.invalid"

    def _make_client(self):
        # RestClient currently requires base_url and also expects streaming_base_url to exist.
        # Set it at the class level before instantiation so __init__ passes without touching other modules.
        RestClient.streaming_base_url = "wss://example.invalid"
        return RestClient(base_url=self.base_url, api_key=self.api_key, account_id=self.account_id)

    def test_init_uses_passed_base_url(self):
        client = self._make_client()
        self.assertEqual(client.http_base_url, self.base_url)

    @patch("requests.request")
    def test_prepare_and_send_request_get(self, mock_request):
        mock_response = MagicMock()
        mock_response.json.return_value = {"success": True}
        mock_response.status_code = 200
        mock_response.reason = "OK"
        mock_response.text = "{}"
        mock_response.headers = {}
        mock_request.return_value = mock_response

        client = self._make_client()
        response = client.get("/test_endpoint")
        self.assertIn("client_timestamp", response)
        self.assertIn("success", response)

    @patch("requests.request")
    def test_prepare_and_send_request_post(self, mock_request):
        mock_response = MagicMock()
        mock_response.json.return_value = {"success": True}
        mock_response.status_code = 200
        mock_response.reason = "OK"
        mock_response.text = "{}"
        mock_response.headers = {}
        mock_request.return_value = mock_response

        client = self._make_client()
        response = client.post("/test_endpoint", data={"key": "value"})
        self.assertIn("client_timestamp", response)
        self.assertIn("success", response)

    def test_update_rate_limit(self):
        client = self._make_client()

        mock_response = MagicMock()
        mock_response.headers = {
            "X-Ratelimit-Used": "5",
            "X-Ratelimit-Available": "100",
            "X-Ratelimit-Expiry": "1700000000",
        }
        client.update_rate_limit(mock_response)
        self.assertEqual(client.rate_limit["used"], 5)
        self.assertEqual(client.rate_limit["available"], 100)
        self.assertEqual(client.rate_limit["expiry"], 1700000000)

    def test_handle_exception(self):
        client = self._make_client()

        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.reason = "Bad Request"
        mock_response.text = "{}"
        with self.assertRaises(HTTPError):
            client.handle_exception(mock_response)


if __name__ == "__main__":
    unittest.main()
