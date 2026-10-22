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
        AirflowClient.login(URL, "u", "p", opener=op)
    assert ei.value.status == 401


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
