import pytest

from tradier_api_client.rest.rest_client import RestClient


@pytest.fixture
def client():
    return RestClient("http://example", api_key="k", account_number="acct")


def test_get_watchlists_routes_to_collection_get(monkeypatch, client):
    captured = {}

    def fake_get(path, params=None, headers=None, retry_allowed=False, attempts=None, delay_seconds=None, timeout=None):
        captured["path"] = path
        captured["retry_allowed"] = retry_allowed
        captured["timeout"] = timeout
        return {"watchlists": {"watchlist": []}}

    monkeypatch.setattr(client, "get", fake_get)

    response = client.get_watchlists(retry=True)

    assert response == {"watchlists": {"watchlist": []}}
    assert captured == {
        "path": "/watchlists",
        "retry_allowed": True,
        "timeout": 5.0,
    }


def test_create_watchlist_posts_name_and_symbols(monkeypatch, client):
    captured = {}

    def fake_post(path, params=None, data=None, headers=None, retry_allowed=False, attempts=None, delay_seconds=None,
                  timeout=None):
        captured["path"] = path
        captured["data"] = data
        captured["retry_allowed"] = retry_allowed
        captured["timeout"] = timeout
        return {"watchlist": {"id": "growth"}}

    monkeypatch.setattr(client, "post", fake_post)

    response = client.create_watchlist("Growth", symbols=["AAPL", "MSFT"], retry=True)

    assert response == {"watchlist": {"id": "growth"}}
    assert captured == {
        "path": "/watchlists",
        "data": {"name": "Growth", "symbols": "AAPL,MSFT"},
        "retry_allowed": True,
        "timeout": 5.0,
    }


def test_get_watchlist_routes_to_resource_get(monkeypatch, client):
    captured = {}

    def fake_get(path, params=None, headers=None, retry_allowed=False, attempts=None, delay_seconds=None, timeout=None):
        captured["path"] = path
        captured["retry_allowed"] = retry_allowed
        captured["timeout"] = timeout
        return {"watchlist": {"id": "default"}}

    monkeypatch.setattr(client, "get", fake_get)

    response = client.get_watchlist("default")

    assert response == {"watchlist": {"id": "default"}}
    assert captured == {
        "path": "/watchlists/default",
        "retry_allowed": False,
        "timeout": 5.0,
    }


def test_update_watchlist_puts_name_and_optional_symbols(monkeypatch, client):
    captured = {}

    def fake_put(path, params=None, data=None, headers=None, retry_allowed=False, attempts=None, delay_seconds=None,
                 timeout=None):
        captured["path"] = path
        captured["data"] = data
        captured["retry_allowed"] = retry_allowed
        captured["timeout"] = timeout
        return {"watchlist": {"id": "default", "name": "Renamed"}}

    monkeypatch.setattr(client, "put", fake_put)

    response = client.update_watchlist("default", "Renamed", symbols=None, retry=True)

    assert response == {"watchlist": {"id": "default", "name": "Renamed"}}
    assert captured == {
        "path": "/watchlists/default",
        "data": {"name": "Renamed", "symbols": None},
        "retry_allowed": True,
        "timeout": 5.0,
    }


def test_delete_watchlist_routes_to_resource_delete(monkeypatch, client):
    captured = {}

    def fake_delete(path, params=None, data=None, headers=None, retry_allowed=False, attempts=None,
                    delay_seconds=None, timeout=None):
        captured["path"] = path
        captured["retry_allowed"] = retry_allowed
        captured["timeout"] = timeout
        return {"watchlists": {"watchlist": []}}

    monkeypatch.setattr(client, "delete", fake_delete)

    response = client.delete_watchlist("default", retry=True)

    assert response == {"watchlists": {"watchlist": []}}
    assert captured == {
        "path": "/watchlists/default",
        "retry_allowed": True,
        "timeout": 5.0,
    }


def test_add_symbols_to_watchlist_posts_symbol_csv(monkeypatch, client):
    captured = {}

    def fake_post(path, params=None, data=None, headers=None, retry_allowed=False, attempts=None, delay_seconds=None,
                  timeout=None):
        captured["path"] = path
        captured["data"] = data
        captured["retry_allowed"] = retry_allowed
        captured["timeout"] = timeout
        return {"watchlist": {"id": "default"}}

    monkeypatch.setattr(client, "post", fake_post)

    response = client.add_symbols_to_watchlist("default", ("SPY", "QQQ"))

    assert response == {"watchlist": {"id": "default"}}
    assert captured == {
        "path": "/watchlists/default/symbols",
        "data": {"symbols": "SPY,QQQ"},
        "retry_allowed": False,
        "timeout": 5.0,
    }


def test_remove_symbol_from_watchlist_routes_to_symbol_delete(monkeypatch, client):
    captured = {}

    def fake_delete(path, params=None, data=None, headers=None, retry_allowed=False, attempts=None,
                    delay_seconds=None, timeout=None):
        captured["path"] = path
        captured["retry_allowed"] = retry_allowed
        captured["timeout"] = timeout
        return {"watchlist": {"id": "default"}}

    monkeypatch.setattr(client, "delete", fake_delete)

    response = client.remove_symbol_from_watchlist("default", "AAPL", retry=True)

    assert response == {"watchlist": {"id": "default"}}
    assert captured == {
        "path": "/watchlists/default/symbols/AAPL",
        "retry_allowed": True,
        "timeout": 5.0,
    }
