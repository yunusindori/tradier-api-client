from datetime import datetime, timedelta, timezone

import pytest


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


def test_retry_disabled_does_not_retry(monkeypatch):
    from tradier_api_client.rest.rest_client import RestClient
    from requests import ReadTimeout

    calls = {"n": 0}

    def fake_request(*args, **kwargs):
        calls["n"] += 1
        raise ReadTimeout("timeout")

    monkeypatch.setattr("requests.request", fake_request)

    c = RestClient("http://example", api_key="k", account_number="acct")
    with pytest.raises(ReadTimeout):
        c.get("/x", retry_allowed=False, attempts=3, delay_seconds=0)

    assert calls["n"] == 1


def test_retry_enabled_retries_transient(monkeypatch):
    from tradier_api_client.rest.rest_client import RestClient
    from requests import ReadTimeout

    calls = {"n": 0}

    def fake_request(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] < 3:
            raise ReadTimeout("timeout")
        return DummyResponse(200, {"ok": True})

    monkeypatch.setattr("requests.request", fake_request)

    c = RestClient("http://example", api_key="k", account_number="acct")
    r = c.get("/x", retry_allowed=True, attempts=3, delay_seconds=0)
    assert r["ok"] is True
    assert calls["n"] == 3


def test_retry_enabled_retries_http_5xx(monkeypatch):
    from tradier_api_client.rest.rest_client import RestClient

    calls = {"n": 0}

    def fake_request(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] < 2:
            return DummyResponse(503, {"error": "nope"})
        return DummyResponse(200, {"ok": True})

    monkeypatch.setattr("requests.request", fake_request)

    c = RestClient("http://example", api_key="k", account_number="acct")
    r = c.get("/x", retry_allowed=True, attempts=2, delay_seconds=0)
    assert r["ok"] is True
    assert calls["n"] == 2


def test_rate_limits_are_tracked_by_bucket(monkeypatch):
    from tradier_api_client.rest.rest_client import RestClient

    expiry_ms = int((datetime.now(timezone.utc) + timedelta(minutes=1)).timestamp() * 1000)
    responses = iter([
        DummyResponse(
            200,
            {"quotes": {}},
            headers={
                "X-Ratelimit-Allowed": "120",
                "X-Ratelimit-Used": "120",
                "X-Ratelimit-Available": "0",
                "X-Ratelimit-Expiry": str(expiry_ms),
            },
        ),
        DummyResponse(200, {"profile": {"account": []}}),
    ])
    calls = {"n": 0}

    def fake_request(*args, **kwargs):
        calls["n"] += 1
        return next(responses)

    monkeypatch.setattr("requests.request", fake_request)

    c = RestClient("http://example", api_key="k", account_number="acct")
    c.get("/markets/quotes")
    profile = c.get("/user/profile")

    assert "profile" in profile
    assert calls["n"] == 2

    with pytest.raises(c.RateLimitExceeded):
        c.get("/markets/quotes")

    assert calls["n"] == 2


def test_get_order_requests_use_standard_bucket_while_post_orders_use_trading(monkeypatch):
    from tradier_api_client.rest.rest_client import RestClient

    responses = iter([
        DummyResponse(
            200,
            {"orders": {"order": []}},
            headers={
                "X-Ratelimit-Allowed": "120",
                "X-Ratelimit-Used": "120",
                "X-Ratelimit-Available": "0",
                "X-Ratelimit-Expiry": str(int((datetime.now(timezone.utc) + timedelta(minutes=1)).timestamp() * 1000)),
            },
        ),
        DummyResponse(200, {"order": {"id": "1"}}),
    ])
    calls = {"n": 0}

    def fake_request(*args, **kwargs):
        calls["n"] += 1
        return next(responses)

    monkeypatch.setattr("requests.request", fake_request)

    c = RestClient("http://example", api_key="k", account_number="acct")
    c.get("/accounts/abc/orders")
    created = c.post("/accounts/abc/orders", data={"class": "equity"})

    assert "order" in created
    assert calls["n"] == 2

    with pytest.raises(c.RateLimitExceeded):
        c.get("/accounts/abc/orders")


def test_rate_limit_updates_from_429_response(monkeypatch):
    from requests import HTTPError
    from tradier_api_client.rest.rest_client import RestClient

    expiry_ms = int((datetime.now(timezone.utc) + timedelta(minutes=1)).timestamp() * 1000)
    calls = {"n": 0}

    def fake_request(*args, **kwargs):
        calls["n"] += 1
        return DummyResponse(
            429,
            {"fault": {"message": "rate limit"}},
            headers={
                "X-Ratelimit-Allowed": "120",
                "X-Ratelimit-Used": "120",
                "X-Ratelimit-Available": "0",
                "X-Ratelimit-Expiry": str(expiry_ms),
            },
            text='{"fault":{"message":"rate limit"}}',
        )

    monkeypatch.setattr("requests.request", fake_request)

    c = RestClient("http://example", api_key="k", account_number="acct")

    with pytest.raises(HTTPError):
        c.get("/markets/quotes", retry_allowed=False)

    assert c.rate_limits[c.RATE_LIMIT_MARKET_DATA]["available"] == 0

    with pytest.raises(c.RateLimitExceeded):
        c.get("/markets/quotes", retry_allowed=False)

    assert calls["n"] == 1


def test_single_remaining_request_is_allowed(monkeypatch):
    from tradier_api_client.rest.rest_client import RestClient

    calls = {"n": 0}

    def fake_request(*args, **kwargs):
        calls["n"] += 1
        return DummyResponse(200, {"ok": True})

    monkeypatch.setattr("requests.request", fake_request)

    c = RestClient("http://example", api_key="k", account_number="acct")
    c.rate_limits[c.RATE_LIMIT_MARKET_DATA] = {
        "allowed": 120,
        "used": 119,
        "available": 1,
        "expiry": int((datetime.now(timezone.utc) + timedelta(minutes=1)).timestamp()),
        "expiry_dt": datetime.now(timezone.utc) + timedelta(minutes=1),
    }

    result = c.get("/markets/quotes")

    assert result["ok"] is True
    assert calls["n"] == 1
