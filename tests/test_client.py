import json
import urllib.error

import pytest

from airflow_depgraph.client import AirflowClient, ApiError
import http.client
import http.server
import threading
import urllib.parse

from airflow_depgraph import __version__
from airflow_depgraph import client as client_mod
from tests.conftest import _Resp, fake_opener

URL = "https://airflow.example.com"


def test_login_posts_and_stores_token():
    op = fake_opener({"/auth/token": {"access_token": "abc"}, "/ui/dependencies": {"nodes": []}})
    c = AirflowClient.login(URL + "/", "u", "p", opener=op)
    login = op.calls[0]
    assert login["method"] == "POST"
    assert login["url"] == URL + "/auth/token"
    assert login["headers"]["content-type"] == "application/json"
    assert json.loads(login["body"]) == {"username": "u", "password": "p"}
    c.dependencies()
    assert op.calls[1]["headers"]["authorization"] == "Bearer abc"
    assert op.calls[1]["headers"]["accept"] == "application/json"


def test_login_401_message():
    op = fake_opener({"/auth/token": 401})
    with pytest.raises(ApiError, match=r"authentication failed \(401\)") as ei:
        AirflowClient.login(URL, "u", "secret-pw", opener=op)
    assert ei.value.status == 401
    assert "secret-pw" not in str(ei.value)


def test_dependencies_403_message():
    op = fake_opener({"/ui/dependencies": 403})
    with pytest.raises(ApiError, match="cannot see DAGs"):
        AirflowClient(URL, "t", opener=op).dependencies()


def test_other_http_error():
    op = fake_opener({"/api/v2/dags/x": 500})
    with pytest.raises(ApiError, match="500 on /api/v2/dags/x: reason") as ei:
        AirflowClient(URL, "t", opener=op).dag("x")
    assert ei.value.status == 500 and ei.value.path == "/api/v2/dags/x"


def test_urlerror_becomes_apierror(monkeypatch):
    monkeypatch.setattr(client_mod.time, "sleep", lambda s: None)

    def op(req, timeout=None):
        raise urllib.error.URLError("dns")

    with pytest.raises(ApiError, match="no response from") as ei:
        AirflowClient(URL, "t", opener=op).dag("x")
    assert ei.value.status is None


def test_dependencies_node_id_query():
    op = fake_opener({"/ui/dependencies": {}})
    AirflowClient(URL, "t", opener=op).dependencies("dag:a b")
    assert "node_id=dag%3Aa+b" in op.calls[0]["url"]


def test_dag_runs_query():
    op = fake_opener({"/api/v2/dags/d/dagRuns": {"dag_runs": [{"a": 1}]}})
    assert AirflowClient(URL, "t", opener=op).dag_runs("d", 5) == [{"a": 1}]
    assert "limit=5" in op.calls[0]["url"]
    assert "order_by=-run_after" in op.calls[0]["url"]


def test_ids_are_quoted_and_lists_unwrapped():
    op = fake_opener(
        {
            "/api/v2/dags/a%2Fb/dagRuns/r%3A1/taskInstances": {"task_instances": [{"task_id": "t1"}]},
            "/api/v2/dags/a%2Fb/tasks": {"tasks": [2]},
        }
    )
    c = AirflowClient(URL, "t", opener=op)
    assert c.task_instances("a/b", "r:1") == [{"task_id": "t1"}]
    assert c.tasks("a/b") == [2]


def test_version_swallows_errors():
    assert AirflowClient(URL, "t", opener=fake_opener({"/api/v2/version": {"version": "3.1.0"}})).version() == "3.1.0"
    assert AirflowClient(URL, "t", opener=fake_opener({"/api/v2/version": 500})).version() is None


def _raw_opener(body=None, exc=None):
    def opener(req, timeout=None):
        if exc:
            raise exc
        from tests.conftest import _Resp
        return _Resp(body)
    return opener


def test_non_json_body_is_api_error():
    c = AirflowClient(URL, "t", opener=_raw_opener(b"<html>login</html>"))
    with pytest.raises(ApiError, match="non-JSON response from /ui/dependencies"):
        c.dependencies()


def test_timeout_and_os_error_are_api_errors(monkeypatch):
    monkeypatch.setattr(client_mod.time, "sleep", lambda s: None)
    with pytest.raises(ApiError, match="timed out"):
        AirflowClient(URL, "t", opener=_raw_opener(exc=TimeoutError())).dependencies()
    with pytest.raises(ApiError, match="network error"):
        AirflowClient(URL, "t", opener=_raw_opener(exc=ConnectionResetError("reset"))).dependencies()


def test_list_dags_paginates_until_short_page():
    import urllib.parse
    from tests.conftest import _Resp
    seen = []

    def opener(req, timeout=None):
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(req.full_url).query)
        offset = int(q["offset"][0])
        seen.append((urllib.parse.urlsplit(req.full_url).path, int(q["limit"][0]), offset))
        n = 100 if offset == 0 else 3
        return _Resp(json.dumps({"dags": [{"dag_id": f"d{offset + i}"} for i in range(n)]}).encode())

    dags = AirflowClient(URL, "t", opener=opener).list_dags()
    assert len(dags) == 103 and dags[-1]["dag_id"] == "d102"
    assert [s[0] for s in seen] == ["/api/v2/dags"] * 2
    assert [(s[1], s[2]) for s in seen] == [(100, 0), (100, 100)]


def test_sensor_endpoints_quote_ids_and_read_content():
    base = "/api/v2/dags/my%20dag/dagRuns/r%2B1/taskInstances/grp.wait"
    op = fake_opener({base: {"rendered_fields": {"op_kwargs": {}}},
                      "/api/v2/dagSources/my%20dag": {"content": "dag source"},
                      base + "/logs/2": {"content": [{"event": "DAG orders_dag done"}]}})
    c = AirflowClient(URL, token="t", opener=op)
    assert c.task_instance("my dag", "r+1", "grp.wait") == {"rendered_fields": {"op_kwargs": {}}}
    assert c.dag_source("my dag") == "dag source"
    assert "orders_dag" in c.task_log("my dag", "r+1", "grp.wait", 2)
    assert op.calls[2]["url"].endswith("/logs/2?full_content=true")


def test_list_dags_follows_total_entries_past_capped_pages():
    import urllib.parse
    from tests.conftest import _Resp
    seen = []

    def opener(req, timeout=None):
        offset = int(urllib.parse.parse_qs(urllib.parse.urlsplit(req.full_url).query)["offset"][0])
        seen.append(offset)
        n = min(50, 120 - offset)       # the server caps pages at 50 rows
        return _Resp(json.dumps({"dags": [{"dag_id": f"d{offset + i}"} for i in range(n)],
                                 "total_entries": 120}).encode())

    assert len(AirflowClient(URL, "t", opener=opener).list_dags()) == 120
    assert seen == [0, 50, 100]


def test_task_instances_pages_by_offset_and_stops_at_total_entries():
    import urllib.parse
    from tests.conftest import _Resp
    calls = []
    all_items = [{"task_id": f"t{i}"} for i in range(130)]

    def opener(req, timeout=None):
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(req.full_url).query)
        offset, limit = int(q["offset"][0]), int(q["limit"][0])
        calls.append(offset)
        page = all_items[offset:offset + limit]
        return _Resp(json.dumps({"task_instances": page, "total_entries": 130}).encode())

    out = AirflowClient(URL, "t", opener=opener).task_instances("d", "r")
    assert len(out) == 130
    assert out == all_items
    assert calls == [0, 100]


def test_task_instances_state_filter_in_query():
    op = fake_opener({"/api/v2/dags/d/dagRuns/r/taskInstances": {"task_instances": []}})
    AirflowClient(URL, "t", opener=op).task_instances("d", "r", state="failed")
    assert "state=failed" in op.calls[0]["url"]


def test_task_instances_stops_when_page_repeats_without_total():
    from tests.conftest import _Resp
    same = [{"task_id": f"t{i}"} for i in range(100)]
    calls = []

    def opener(req, timeout=None):
        calls.append(req.full_url)
        return _Resp(json.dumps({"task_instances": same}).encode())

    out = AirflowClient(URL, "t", opener=opener).task_instances("d", "r")
    assert len(calls) == 2
    assert len(out) == 100


def test_task_instances_keeps_mapped_instances_sharing_one_task_id():
    import urllib.parse
    from tests.conftest import _Resp
    all_items = [{"task_id": "mapped", "map_index": i} for i in range(150)]
    calls = []

    def opener(req, timeout=None):
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(req.full_url).query)
        offset, limit = int(q["offset"][0]), int(q["limit"][0])
        calls.append(offset)
        return _Resp(json.dumps({"task_instances": all_items[offset:offset + limit]}).encode())

    out = AirflowClient(URL, "t", opener=opener).task_instances("d", "r")
    assert len(out) == 150
    assert calls == [0, 100, 150]


def scripted_opener(*outcomes):
    """Each call pops one outcome: an exception to raise, or a JSON value to return."""
    queue, calls = list(outcomes), []

    def opener(req, timeout=None):
        calls.append(req)
        item = queue.pop(0)
        if isinstance(item, BaseException):
            raise item
        return _Resp(json.dumps(item).encode())

    opener.calls = calls
    return opener


def _http_error(code, headers=None):
    return urllib.error.HTTPError(URL + "/x", code, "reason", headers or {}, None)


@pytest.fixture
def sleeps(monkeypatch):
    seen = []
    monkeypatch.setattr(client_mod.time, "sleep", seen.append)
    return seen


def test_503_is_retried_then_succeeds(sleeps):
    op = scripted_opener(_http_error(503), {"nodes": []})
    assert AirflowClient(URL, "t", opener=op).dependencies() == {"nodes": []}
    assert len(op.calls) == 2
    assert sleeps == [1]


def test_404_is_not_retried(sleeps):
    op = scripted_opener(_http_error(404), {"nodes": []})
    with pytest.raises(ApiError, match="404"):
        AirflowClient(URL, "t", opener=op).dependencies()
    assert len(op.calls) == 1 and sleeps == []


def test_retry_after_header_is_honoured(sleeps):
    op = scripted_opener(_http_error(503, {"Retry-After": "2"}), {"nodes": []})
    AirflowClient(URL, "t", opener=op).dependencies()
    assert sleeps == [2]


def test_retry_after_is_capped_at_30_seconds(sleeps):
    op = scripted_opener(_http_error(429, {"Retry-After": "120"}), {"nodes": []})
    AirflowClient(URL, "t", opener=op).dependencies()
    assert sleeps == [30]


def test_gives_up_after_two_retries_with_last_status(sleeps):
    op = scripted_opener(_http_error(429), _http_error(429), _http_error(502))
    with pytest.raises(ApiError) as ei:
        AirflowClient(URL, "t", opener=op).dependencies()
    assert ei.value.status == 502
    assert len(op.calls) == 3 and sleeps == [1, 3]


def test_timeout_is_retried(sleeps):
    op = scripted_opener(TimeoutError(), {"nodes": []})
    assert AirflowClient(URL, "t", opener=op).dependencies() == {"nodes": []}
    assert sleeps == [1]


def test_login_503_is_not_retried(sleeps):
    op = scripted_opener(_http_error(503), {"access_token": "abc"})
    with pytest.raises(ApiError):
        AirflowClient.login(URL, "u", "p", opener=op)
    assert len(op.calls) == 1 and sleeps == []


def test_user_agent_is_sent_on_every_request(sleeps):
    op = fake_opener({"/auth/token": {"access_token": "abc"}, "/ui/dependencies": {"nodes": []}})
    c = AirflowClient.login(URL, "u", "p", opener=op)
    c.dependencies()
    assert len(op.calls) == 2
    assert all(call["headers"]["user-agent"] == f"airflow-depgraph/{__version__}"
               for call in op.calls)


@pytest.fixture
def local_server():
    """A real keep-alive HTTP/1.1 server; records connections and User-Agent headers."""
    connections, agents = [], []

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def setup(self):
            super().setup()
            connections.append(self.client_address)

        def do_GET(self):
            agents.append(self.headers.get("User-Agent"))
            body = json.dumps({"nodes": [], "path": self.path}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.05}, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", connections, agents
    finally:
        server.shutdown()
        server.server_close()


PROXY_VARS = ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy")


def test_default_path_reuses_one_connection(local_server, monkeypatch):
    for var in PROXY_VARS:
        monkeypatch.delenv(var, raising=False)
    url, connections, agents = local_server
    c = AirflowClient(url, "t")
    assert c.dependencies()["path"] == "/ui/dependencies"
    c.dependencies()
    assert c.dag("x")["path"] == "/api/v2/dags/x"
    assert len(connections) == 1
    assert agents == [f"airflow-depgraph/{__version__}"] * 3


def test_proxy_variable_falls_back_to_urlopen(local_server, monkeypatch):
    for var in PROXY_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.example.com:3128")
    url, connections, _ = local_server
    used = []

    def fake_urlopen(req, timeout=None):
        used.append(req.full_url)
        return _Resp(b'{"nodes": []}')

    monkeypatch.setattr(client_mod.urllib.request, "urlopen", fake_urlopen)
    assert AirflowClient(url, "t").dependencies() == {"nodes": []}
    assert used == [url + "/ui/dependencies"] and connections == []


class _FakeHTTPResponse:
    def __init__(self, status, body=b"", headers=None):
        self.status, self.reason = status, "fake"
        self.headers = headers or {}
        self._body = body

    def read(self):
        return self._body

    def getheader(self, name, default=None):
        return self.headers.get(name, default)


def _fake_connections(monkeypatch, responses):
    """Patch the connection classes: each request pops one response, or raises it if it is an error."""
    made = []

    class FakeConn:
        def __init__(self, host, port=None, timeout=None):
            self.closed, self.targets = False, []
            made.append(self)

        def request(self, method, target, body=None, headers=None):
            self.targets.append(target)

        def getresponse(self):
            item = responses.pop(0)
            if isinstance(item, BaseException):
                raise item
            return item

        def close(self):
            self.closed = True

    monkeypatch.setattr(client_mod.http.client, "HTTPConnection", FakeConn)
    monkeypatch.setattr(client_mod.http.client, "HTTPSConnection", FakeConn)
    return made


def test_redirect_on_pooled_path_goes_through_urlopen_with_one_note(monkeypatch, capsys):
    for var in PROXY_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(client_mod, "_noted_redirects", set())
    location = "https://airflow.example.com/airflow/ui/dependencies"
    _fake_connections(monkeypatch, [_FakeHTTPResponse(302, headers={"Location": location}),
                                    _FakeHTTPResponse(302, headers={"Location": location})])
    used = []

    def fake_urlopen(req, timeout=None):
        used.append(req.full_url)
        return _Resp(b'{"nodes": []}')

    monkeypatch.setattr(client_mod.urllib.request, "urlopen", fake_urlopen)
    c = AirflowClient(URL, "t")
    assert c.dependencies() == {"nodes": []}
    assert c.dependencies() == {"nodes": []}
    assert used == [URL + "/ui/dependencies"] * 2
    assert capsys.readouterr().err.splitlines() == [
        f"note: {URL}/ui/dependencies redirects to {location}; "
        "set the final URL to avoid the extra hop"]


def test_dropped_keepalive_connection_is_reopened_once(monkeypatch):
    for var in PROXY_VARS:
        monkeypatch.delenv(var, raising=False)
    made = _fake_connections(monkeypatch, [http.client.RemoteDisconnected("closed by server"),
                                           _FakeHTTPResponse(200, b'{"nodes": []}')])
    assert AirflowClient(URL, "t").dependencies() == {"nodes": []}
    assert len(made) == 2 and made[0].closed
    assert made[1].targets == ["/ui/dependencies"]


def _body_opener(answer, seen):
    """Opener that records each JSON body and returns answer(path, body)."""
    def opener(req, timeout=None):
        body = json.loads(req.data)
        seen.append((urllib.parse.urlsplit(req.full_url).path, body))
        return _Resp(json.dumps(answer(urllib.parse.urlsplit(req.full_url).path, body)).encode())
    return opener


def test_post_sends_json_and_maps_errors():
    seen = []
    op = _body_opener(lambda path, body: {"ok": body["x"]}, seen)
    assert AirflowClient(URL, "t", opener=op).post("/p", {"x": 1}) == {"ok": 1}
    assert seen == [("/p", {"x": 1})]
    with pytest.raises(ApiError) as ei:
        AirflowClient(URL, "t", opener=fake_opener({"/p": 404})).post("/p", {})
    assert ei.value.status == 404


def test_bulk_dag_runs_chunks_dag_ids_and_stops_at_total():
    seen = []

    def answer(path, body):
        assert path == "/api/v2/dags/~/dagRuns/list"
        return {"dag_runs": [{"dag_id": d, "dag_run_id": f"r-{d}"} for d in body["dag_ids"]],
                "total_entries": len(body["dag_ids"])}

    ids = [f"d{i}" for i in range(150)]
    runs = AirflowClient(URL, "t", opener=_body_opener(answer, seen)).bulk_dag_runs(
        ids, "2026-01-01T00:00:00+00:00")
    assert len(runs) == 150
    assert [len(body["dag_ids"]) for _, body in seen] == [100, 50]
    assert seen[0][1]["order_by"] == "-run_after" and seen[0][1]["page_offset"] == 0
    assert seen[0][1]["run_after_gte"] == "2026-01-01T00:00:00+00:00"


def test_bulk_dag_runs_pages_until_total_entries():
    pages = {0: {"dag_runs": [{"dag_id": "a", "dag_run_id": "r1"}, {"dag_id": "a", "dag_run_id": "r2"}],
                 "total_entries": 3},
             2: {"dag_runs": [{"dag_id": "a", "dag_run_id": "r3"}], "total_entries": 3}}
    seen = []
    runs = AirflowClient(URL, "t", opener=_body_opener(
        lambda path, body: pages[body["page_offset"]], seen)).bulk_dag_runs(["a"], "x")
    assert [r["dag_run_id"] for r in runs] == ["r1", "r2", "r3"]
    assert [body["page_offset"] for _, body in seen] == [0, 2]


def test_bulk_dag_runs_stops_once_every_dag_has_enough_runs():
    seen = []
    page = {"dag_runs": [{"dag_id": "a", "dag_run_id": "r1"}, {"dag_id": "a", "dag_run_id": "r2"}],
            "total_entries": 50}
    runs = AirflowClient(URL, "t", opener=_body_opener(lambda p, b: page, seen)).bulk_dag_runs(
        ["a"], "x", per_dag=1)
    assert len(seen) == 1 and len(runs) == 2


def test_bulk_task_instances_chunks_run_ids_and_skips_empty():
    seen = []

    def answer(path, body):
        assert path == "/api/v2/dags/~/dagRuns/~/taskInstances/list"
        return {"task_instances": [{"dag_id": "a", "dag_run_id": r, "task_id": "t"}
                                   for r in body["dag_run_ids"]],
                "total_entries": len(body["dag_run_ids"])}

    client = AirflowClient(URL, "t", opener=_body_opener(answer, seen))
    tis = client.bulk_task_instances(["a"], [f"r{i}" for i in range(120)], ["failed", "upstream_failed"])
    assert len(tis) == 120
    assert [len(body["dag_run_ids"]) for _, body in seen] == [100, 20]
    assert seen[0][1]["state"] == ["failed", "upstream_failed"]
    seen.clear()
    assert client.bulk_task_instances(["a"], [], ["failed"]) == [] and seen == []


def _post_count(made):
    return sum(len(conn.targets) for conn in made)


def test_login_connection_drop_is_retried_once(monkeypatch, sleeps):
    for var in PROXY_VARS:
        monkeypatch.delenv(var, raising=False)
    made = _fake_connections(monkeypatch, [http.client.RemoteDisconnected("gone"),
                                           _FakeHTTPResponse(200, b'{"access_token": "abc"}')])
    assert AirflowClient.login(URL, "u", "p").token == "abc"
    assert _post_count(made) == 2


def test_login_connection_dropped_twice_gives_up_after_two_posts(monkeypatch, sleeps):
    for var in PROXY_VARS:
        monkeypatch.delenv(var, raising=False)
    made = _fake_connections(monkeypatch, [http.client.RemoteDisconnected("gone"),
                                           http.client.RemoteDisconnected("gone")])
    with pytest.raises(ApiError, match="network error"):
        AirflowClient.login(URL, "u", "p")
    assert _post_count(made) == 2


def test_login_redirect_is_reported_not_replayed(monkeypatch, sleeps):
    for var in PROXY_VARS:
        monkeypatch.delenv(var, raising=False)
    made = _fake_connections(monkeypatch, [_FakeHTTPResponse(
        302, headers={"Location": "https://sso.example.com/login"})])
    replayed = []
    monkeypatch.setattr(client_mod.urllib.request, "urlopen", lambda *a, **k: replayed.append(a))
    with pytest.raises(ApiError, match=r"login redirected to https://sso.example.com/login; "
                                       r"use the final URL"):
        AirflowClient.login(URL, "u", "p")
    assert _post_count(made) == 1 and replayed == []


def test_has_nonempty_task_asks_for_every_nonempty_state():
    seen = []

    def opener(req, timeout=None):
        seen.append(urllib.parse.parse_qs(urllib.parse.urlsplit(req.full_url).query))
        return _Resp(json.dumps({"task_instances": [{"task_id": "a"}]}).encode())

    assert AirflowClient(URL, "t", opener=opener).has_nonempty_task("d", "r") is True
    assert seen[0]["limit"] == ["1"] and seen[0]["state"] == list(client_mod.NONEMPTY_TI_STATES)


def test_network_errors_are_flagged_as_network(monkeypatch):
    monkeypatch.setattr(client_mod.time, "sleep", lambda s: None)
    with pytest.raises(ApiError) as ei:
        AirflowClient(URL, "t", opener=_raw_opener(exc=ConnectionResetError("reset"))).dependencies()
    assert ei.value.network is True and ei.value.status is None
    with pytest.raises(ApiError) as ei:
        AirflowClient(URL, "t", opener=_raw_opener(b"<html>login</html>")).dependencies()
    assert ei.value.network is False
