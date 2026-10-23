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

    def list_dags(self):
        return [{"dag_id": "a", "tags": [{"name": "team-a"}]}, {"dag_id": "lonely", "tags": []}]


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


def test_page_includes_catalog_unlinked_dags():
    b = PageBuilder("u", Settings(tags=("team-a",)), client_factory=FakeClient)
    html = b.page("tok", None, False)
    assert '"id": "dag:lonely"' in html and '"unlinked": true' in html
    assert '"tags": ["team-a"]' in html


def test_page_without_catalog_still_renders():
    class NoCatalog(FakeClient):
        def list_dags(self):
            raise ApiError("403 on /api/v2/dags: reason", 403, "/api/v2/dags")
    html = PageBuilder("u", Settings(), client_factory=NoCatalog).page("tok", None, False)
    assert "dag:b" in html and "dag:lonely" not in html


# ---------- inferred sensor links (background refresh) ----------

class InferClient:
    """Catalog: a, waiter, secret. waiter.wait_a is a PythonSensor that waits on a (rendered)."""
    made = []
    error = None

    def __init__(self, url, token=None, timeout=None):
        self.token = token
        InferClient.made.append(token)

    def list_dags(self):
        if InferClient.error:
            raise InferClient.error
        return [{"dag_id": d} for d in ("a", "waiter", "secret")]

    def tasks(self, dag_id):
        return [{"task_id": "wait_a", "operator_name": "PythonSensor"}] if dag_id == "waiter" else []

    def dag_runs(self, dag_id, limit):
        return [{"dag_run_id": "r1"}]

    def task_instance(self, dag_id, run_id, task_id):
        return {"dag_run_id": run_id, "try_number": 1,
                "rendered_fields": {"op_kwargs": {"external_dag_id": "a"}}}

    def dag_source(self, dag_id):
        return ""

    def task_log(self, *a):
        return ""


def _cache(t, interval=60):
    from airflow_depgraph.web import InferenceCache
    InferClient.made.clear()
    InferClient.error = None
    return InferenceCache("u", interval, client_factory=InferClient, wall_clock=lambda: t[0],
                          start_thread=False)


def test_inference_refresh_uses_token_and_respects_interval():
    t = [1000.0]
    c = _cache(t)
    tok = _jwt({"exp": 5000})
    assert c.current() is None and c.due()
    c.offer(tok)
    assert c.refresh() is True
    inferred, at = c.current()
    assert at == 1000.0 and InferClient.made == [tok]
    assert inferred == [{"dag": "waiter", "task": "wait_a", "operator": "PythonSensor",
                         "source": "rendered", "targets": ["a"], "target_tasks": [], "kind": "data"}]
    t[0] = 1030
    assert not c.due()
    t[0] = 1061
    assert c.due()


def test_inference_without_token_skips_and_stays_due():
    t = [1000.0]
    c = _cache(t)
    c.offer("not-a-jwt")
    c.offer(_jwt({"exp": 10}))          # expired
    assert c.refresh() is False and InferClient.made == [] and c.due()


def test_inference_uses_most_recent_token_and_drops_rejected_one():
    t = [1000.0]
    c = _cache(t)
    first, second = _jwt({"exp": 5000, "sub": "1"}), _jwt({"exp": 5000, "sub": "2"})
    c.offer(first)
    c.offer(second)
    c.refresh()
    assert InferClient.made == [second]
    InferClient.error = ApiError("401 on /api/v2/dags", 401, "/api/v2/dags")
    t[0] = 2000
    assert c.refresh() is False
    assert c.current()[1] == 1000.0          # previous result kept
    t[0] = 3000
    assert c.refresh() is False and InferClient.made == [second, second]  # token forgotten


def test_inference_token_expiring_before_cycle_is_not_used():
    t = [1000.0]
    c = _cache(t)
    c.offer(_jwt({"exp": 1500}))
    t[0] = 1600
    assert c.refresh() is False and InferClient.made == []


class _Stub:
    def __init__(self, result):
        self.result, self.offered = result, []

    def offer(self, token):
        self.offered.append(token)

    def current(self):
        return self.result


INFERRED = [
    {"dag": "b", "task": "wait_a", "source": "rendered", "targets": ["a", "secret"], "kind": "data"},
    {"dag": "secret", "task": "wait_b", "source": "source", "targets": ["b"], "kind": "data"},
    {"dag": "lonely", "task": "guard_a", "source": "rendered", "targets": ["a"], "kind": "guard"},
]


def test_page_merges_inferred_links_visible_to_viewer():
    b = PageBuilder("u", Settings(infer_sensors=True), client_factory=FakeClient)
    assert b.inference is not None
    b.inference = _Stub((INFERRED, 0.0))
    html = b.page("tok", None, False)
    assert b.inference.offered == ["tok"]
    assert '"id": "sensor-inferred:b:wait_a"' in html and '"inferred": "rendered"' in html
    assert '"kind": "guard"' in html                       # guard drawn (toggle on the page)
    assert "secret" not in html                            # viewer's catalog has no "secret"
    assert "sensor links: 1, updated 00:00 UTC" in html


def test_page_before_first_inference_says_so():
    b = PageBuilder("u", Settings(infer_sensors=True), client_factory=FakeClient)
    b.inference = _Stub(None)
    assert "sensor links: first refresh running" in b.page("tok", None, False)


def test_page_without_infer_setting_has_no_inference():
    b = PageBuilder("u", Settings(), client_factory=FakeClient)
    assert b.inference is None and "sensor links:" not in b.page("tok", None, False)
