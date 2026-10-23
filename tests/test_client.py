import json
import urllib.error

import pytest

from airflow_depgraph.client import AirflowClient, ApiError
from tests.conftest import fake_opener

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


def test_urlerror_becomes_apierror():
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
            "/api/v2/dags/a%2Fb/dagRuns/r%3A1/taskInstances": {"task_instances": [1]},
            "/api/v2/dags/a%2Fb/tasks": {"tasks": [2]},
        }
    )
    c = AirflowClient(URL, "t", opener=op)
    assert c.task_instances("a/b", "r:1") == [1]
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


def test_timeout_and_os_error_are_api_errors():
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
