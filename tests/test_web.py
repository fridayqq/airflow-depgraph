import hashlib
import threading
import time

import pytest

from airflow_depgraph.client import ApiError
from airflow_depgraph.presets import Settings
from airflow_depgraph.web import PageBuilder, base_path, extract_token, loopback_url

PAYLOAD = {"nodes": [{"id": "dag:a", "label": "a", "type": "dag"},
                     {"id": "asset:1", "label": "orders", "type": "asset"},
                     {"id": "dag:b", "label": "b", "type": "dag"}],
           "edges": [{"source_id": "dag:a", "target_id": "asset:1"},
                     {"source_id": "asset:1", "target_id": "dag:b"}]}


class FakeClient:
    made = []

    def __init__(self, url, token=None, timeout=None, delay=0):
        time.sleep(delay)
        self.url, self.token, self.timeout = url, token, timeout
        FakeClient.made.append(self)

    def dependencies(self, node_id=None):
        if self.token == "bad":
            raise ApiError("authentication failed (401)", 401, "/ui/dependencies")
        return PAYLOAD

    def version(self):
        return "3.1.5"


@pytest.fixture(autouse=True)
def _reset():
    FakeClient.made.clear()


def test_extract_token_prefers_bearer_header():
    assert extract_token({"_token": "cookie"}, {"authorization": "Bearer hdr"}) == "hdr"


def test_extract_token_falls_back_to_cookie():
    assert extract_token({"_token": "cookie"}, {}) == "cookie"


def test_extract_token_none_when_absent():
    assert extract_token({}, {"authorization": "Basic x"}) is None
    assert extract_token({"_token": ""}, {}) is None


@pytest.mark.parametrize("url,expected", [
    (None, ""), ("", ""), ("http://localhost:8080", ""), ("http://localhost:8080/", ""),
    ("https://host.example.com/airflow/", "/airflow"), ("/airflow", "/airflow")])
def test_base_path(url, expected):
    assert base_path(url) == expected


def test_loopback_url_uses_port_and_prefix():
    assert loopback_url("https://host.example.com/airflow/", 8080) == "http://127.0.0.1:8080/airflow"
    assert loopback_url(None, "8081") == "http://127.0.0.1:8081"
    assert loopback_url("x", 1, override="http://api:8080/af/") == "http://api:8080/af"


def test_page_renders_with_token_and_source():
    b = PageBuilder("http://127.0.0.1:8080", Settings(), client_factory=FakeClient)
    html = b.page("tok", "https://host.example.com/airflow", with_status=False)
    assert "dag:b" in html and "https://host.example.com/airflow" in html
    assert FakeClient.made[0].token == "tok" and FakeClient.made[0].url == "http://127.0.0.1:8080"


def test_page_is_cached_per_token_until_ttl():
    t = [0.0]
    b = PageBuilder("u", Settings(), client_factory=FakeClient, clock=lambda: t[0], cache_seconds=10)
    b.page("tok", None, False); b.page("tok", None, False)
    assert len(FakeClient.made) == 1
    b.page("other", None, False)
    assert len(FakeClient.made) == 2
    t[0] = 11
    b.page("tok", None, False)
    assert len(FakeClient.made) == 3


def test_page_propagates_auth_errors_and_caches_nothing():
    b = PageBuilder("u", Settings(), client_factory=FakeClient)
    with pytest.raises(ApiError):
        b.page("bad", None, False)
    with pytest.raises(ApiError):
        b.page("bad", None, False)
    assert len(FakeClient.made) == 2


def test_cache_drops_expired_entries_on_insert_and_respects_cap():
    t = [0.0]
    b = PageBuilder("u", Settings(), client_factory=FakeClient, clock=lambda: t[0],
                    cache_seconds=10, max_entries=2)
    b.page("a", None, False)
    t[0] = 5
    b.page("b", None, False)
    t[0] = 12                       # "a" is expired, "b" is not
    b.page("c", None, False)
    assert len(b._cache) == 2
    assert not any(k[0] == hashlib.sha256(b"a").hexdigest() for k in b._cache)
    b.page("d", None, False)        # cap: oldest ("b") goes
    assert len(b._cache) == 2
    assert hashlib.sha256(b"b").hexdigest() not in {k[0] for k in b._cache}


def test_concurrent_same_token_builds_once_with_short_timeout():
    b = PageBuilder("u", Settings(),
                    client_factory=lambda *a, **kw: FakeClient(*a, delay=0.2, **kw))
    threads = [threading.Thread(target=b.page, args=("tok", None, False)) for _ in range(2)]
    [th.start() for th in threads]
    [th.join() for th in threads]
    assert len(FakeClient.made) == 1
    assert FakeClient.made[0].timeout == 10


def _jwt(payload: dict) -> str:
    import base64, json
    enc = lambda d: base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()
    return f"{enc({'alg': 'HS512'})}.{enc(payload)}.sig"


def test_token_usable_checks_shape_and_expiry():
    from airflow_depgraph.web import token_usable
    assert token_usable(_jwt({"exp": 2000}), now=1000)
    assert not token_usable(_jwt({"exp": 1000}), now=2000)
    assert token_usable(_jwt({"sub": "x"}), now=1)          # no exp claim: let Airflow decide
    assert not token_usable("garbage", now=1)
    assert not token_usable("a.b.c", now=1)


def test_plugin_entry_point_is_declared():
    import tomllib
    from pathlib import Path
    data = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())
    eps = data["project"]["entry-points"]["airflow.plugins"]
    assert eps == {"airflow_depgraph": "airflow_depgraph.airflow_plugin:DepGraphPlugin"}


def test_cli_never_imports_airflow():
    import subprocess, sys
    code = "import sys, airflow_depgraph.cli; print('airflow' in sys.modules, 'fastapi' in sys.modules)"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "False False"
