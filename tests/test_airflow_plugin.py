import base64
import importlib
import json
import sys
import time
import types

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient  # noqa: E402

from airflow_depgraph.client import ApiError  # noqa: E402

PAYLOAD = {"nodes": [{"id": "dag:orders_dag", "label": "orders_dag", "type": "dag"}], "edges": []}
_STUBS = ("airflow", "airflow.plugins_manager", "airflow.configuration", "airflow.api_fastapi",
          "airflow.api_fastapi.app", "airflow_depgraph.airflow_plugin")


def _jwt(payload):
    enc = lambda d: base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()  # noqa: E731
    return f"{enc({'alg': 'HS512'})}.{enc(payload)}.sig"


class FakeClient:
    error = None

    def __init__(self, url, token=None, timeout=None):
        self.url = url

    def dependencies(self, node_id=None):
        if FakeClient.error:
            raise FakeClient.error
        return PAYLOAD

    def version(self):
        return "3.3.2"

    def list_dags(self):
        return []


@pytest.fixture
def plugin(monkeypatch):
    monkeypatch.setenv("AIRFLOW__API__BASE_URL", "http://host.example.com/airflow")
    FakeClient.error = None
    mods = {n: types.ModuleType(n) for n in _STUBS[:-1]}
    mods["airflow.plugins_manager"].AirflowPlugin = type("AirflowPlugin", (), {})
    conf = types.SimpleNamespace(get=lambda section, key, fallback=None: {
        "base_url": "http://host.example.com/airflow", "port": "8080"}.get(key, fallback))
    mods["airflow.configuration"].conf = conf
    mods["airflow.api_fastapi.app"].get_auth_manager = lambda: types.SimpleNamespace(
        get_url_login=lambda **kw: "/auth/login")
    for n, m in mods.items():
        monkeypatch.setitem(sys.modules, n, m)
    sys.modules.pop("airflow_depgraph.airflow_plugin", None)
    mod = importlib.import_module("airflow_depgraph.airflow_plugin")
    mod._runtime()[1]._client_factory = FakeClient
    yield mod
    for n in _STUBS:
        sys.modules.pop(n, None)


def _get(mod, url="/", token=None):
    client = TestClient(mod.app, follow_redirects=False)
    if token:
        client.cookies.set("_token", token)
    return client.get(url)


LOGIN = "/airflow/auth/login?next=%2Fairflow%2Fdepgraph%2F"


def test_no_token_redirects_to_login(plugin):
    r = _get(plugin)
    assert r.status_code == 302 and r.headers["location"] == LOGIN


def test_expired_token_redirects_to_login(plugin):
    r = _get(plugin, token=_jwt({"exp": time.time() - 10}))
    assert r.status_code == 302 and r.headers["location"] == LOGIN


def test_valid_token_renders_page_not_cached(plugin):
    r = _get(plugin, token=_jwt({"exp": time.time() + 600}))
    assert r.status_code == 200 and "orders_dag" in r.text
    assert r.headers["cache-control"] == "private, no-store"


def test_api_401_redirects_to_login(plugin):
    FakeClient.error = ApiError("nope (401)", 401, "/ui/dependencies")
    r = _get(plugin, token=_jwt({"exp": time.time() + 600}))
    assert r.status_code == 302 and r.headers["location"] == LOGIN


def test_api_403_offers_sign_in_again(plugin):
    FakeClient.error = ApiError("forbidden (403)", 403, "/ui/dependencies")
    r = _get(plugin, token=_jwt({"exp": time.time() + 600}))
    assert r.status_code == 403 and "Sign in again" in r.text and LOGIN in r.text
    assert r.headers["cache-control"] == "private, no-store"


def test_api_500_is_generic_502(plugin):
    FakeClient.error = ApiError("boom http://127.0.0.1:8080/ui/dependencies (500)", 500, "/x")
    r = _get(plugin, token=_jwt({"exp": time.time() + 600}))
    assert r.status_code == 502 and "see the api-server log" in r.text
    assert "127.0.0.1" not in r.text


def test_bad_status_param_is_not_422(plugin):
    r = _get(plugin, "/?status=abc", token=_jwt({"exp": time.time() + 600}))
    assert r.status_code == 200


def test_menu_href_uses_base_path(plugin):
    assert plugin.DepGraphPlugin.external_views[0]["href"] == "/airflow/depgraph/"


SECURITY_HEADERS = {
    "content-security-policy": ("default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
                                "img-src data:; connect-src 'none'; frame-ancestors 'self'; "
                                "base-uri 'none'; form-action 'none'"),
    "x-content-type-options": "nosniff",
    "referrer-policy": "same-origin",
}


def _assert_security_headers(r):
    for name, value in SECURITY_HEADERS.items():
        assert r.headers[name] == value, name


def test_security_headers_on_page(plugin):
    r = _get(plugin, token=_jwt({"exp": time.time() + 600}))
    assert r.status_code == 200
    _assert_security_headers(r)


def test_security_headers_on_redirect(plugin):
    r = _get(plugin)
    assert r.status_code == 302
    _assert_security_headers(r)


def test_security_headers_on_error_message(plugin):
    FakeClient.error = ApiError("forbidden (403)", 403, "/ui/dependencies")
    r = _get(plugin, token=_jwt({"exp": time.time() + 600}))
    assert r.status_code == 403
    _assert_security_headers(r)


def test_cache_hit_does_not_call_build(plugin, monkeypatch):
    builder = plugin._runtime()[1]
    calls = []
    real_build = builder._build

    def counting_build(*args, **kwargs):
        calls.append(args)
        return real_build(*args, **kwargs)

    monkeypatch.setattr(builder, "_build", counting_build)
    token = _jwt({"exp": time.time() + 600})
    assert _get(plugin, token=token).status_code == 200
    assert len(calls) == 1
    r = _get(plugin, token=token)
    assert r.status_code == 200 and "orders_dag" in r.text
    assert len(calls) == 1


def test_busy_limiter_with_long_queue_returns_503(plugin, monkeypatch):
    monkeypatch.setattr(plugin, "_LIMITER", types.SimpleNamespace(available_tokens=0))
    monkeypatch.setattr(plugin, "_WAITING", plugin.BUSY_WORKERS + plugin.MAX_WAITING + 1)
    r = _get(plugin, token=_jwt({"exp": time.time() + 600}))
    assert r.status_code == 503
    assert r.headers["retry-after"] == "30"
    assert "retry in a minute" in r.text
    _assert_security_headers(r)


def test_short_queue_still_builds(plugin, monkeypatch):
    monkeypatch.setattr(plugin, "_WAITING", 0)
    r = _get(plugin, token=_jwt({"exp": time.time() + 600}))
    assert r.status_code == 200 and "orders_dag" in r.text
