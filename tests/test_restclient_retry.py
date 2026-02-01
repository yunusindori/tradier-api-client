import pytest


class DummyResponse:
    def __init__(self, status_code=200, json_data=None):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.reason = "OK" if status_code < 400 else "ERR"
        self.text = ""
        self.headers = {}

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
