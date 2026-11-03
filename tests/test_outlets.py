"""Outlets of a failed run: which assets and triggers it produced, and the kind rewrite."""

from airflow_depgraph import build, outlets
from airflow_depgraph.presets import Settings
from airflow_depgraph.render import build_view
from airflow_depgraph.snapshot import Snapshot
from tests.test_cli import FakeClient


def test_producing_task_by_convention():
    tasks = ["ingest.airbyte", "dbt_tasks.dbt-run-orders", "dbt_tasks.dbt-test-orders", "plain"]
    assert outlets.producing_task("x-my_dag://dbt-run-orders", tasks) == "dbt_tasks.dbt-run-orders"
    assert outlets.producing_task("plain", tasks) == "plain"
    assert outlets.producing_task("s3://bucket/plain", tasks) is None   # a path, not a task
    assert outlets.producing_task("x-my_dag://dbt-run-missing", tasks) is None
    assert outlets.producing_task("x-my_dag://", tasks) is None


def test_producing_task_by_words_in_order():
    tasks = ["check", "dbt_finance.dbt-run-finance-dt", "dbt_finance.dbt-run-finance-cv",
             "dbt_datamarts.dbt-run-da-finance", "dbt_tasks.dbt_master_data_run",
             "dbt_tasks.dbt_master_data_test", "02_dbt.05_dbt_jira-cv-run"]
    assert outlets.producing_task("x-f://dbt-finance-cv", tasks) == "dbt_finance.dbt-run-finance-cv"
    assert outlets.producing_task("x-f://da-finance", tasks) == "dbt_datamarts.dbt-run-da-finance"
    assert outlets.producing_task("x-f://jira-cv-run", tasks) == "02_dbt.05_dbt_jira-cv-run"
    assert outlets.producing_task("x-s://master-data", tasks) == "dbt_tasks.dbt_master_data_run"
    assert outlets.producing_task("x-f://finance", tasks) is None                    # dt and cv tie
    assert outlets.producing_task("x-f://cv-finance", tasks) is None                 # wrong order


def _snap(last_run, nodes=None, edges=None):
    nodes = nodes or [
        {"id": "dag:p", "label": "p", "type": "dag"},
        {"id": "asset:1", "label": "x-p://dbt-run-a", "type": "asset"},
        {"id": "asset:2", "label": "x-p://dbt-run-b", "type": "asset"},
        {"id": "trigger:p:c:fire_c", "label": "fire_c", "type": "trigger"},
        {"id": "dag:c", "label": "c", "type": "dag"},
    ]
    edges = edges or [
        {"source_id": "dag:p", "target_id": "asset:1"}, {"source_id": "dag:p", "target_id": "asset:2"},
        {"source_id": "dag:p", "target_id": "trigger:p:c:fire_c"},
        {"source_id": "trigger:p:c:fire_c", "target_id": "dag:c"},
    ]
    return Snapshot(1, "t", None, None, nodes, edges, dags={"p": {"last_run": last_run}})


TASKS = ["ingest", "dbt.dbt-run-a", "dbt.dbt-test-a", "dbt.dbt-run-b", "fire_c"]


def test_tests_after_the_run_leave_the_outlets_produced():
    snap = _snap({"state": "failed", "kind": "tests", "failed_tasks": ["dbt.dbt-test-a"],
                  "upstream_failed_tasks": []})
    outlets.annotate(snap, {"p": TASKS})
    run = snap.dags["p"]["last_run"]
    assert run["outlets"] == {"asset:1": True, "asset:2": True, "trigger:p:c:fire_c": True}
    assert run["kind"] == "tests"


def test_a_test_that_stops_the_run_task_is_a_pipeline_failure():
    snap = _snap({"state": "failed", "kind": "tests", "failed_tasks": ["dbt.dbt-test-a"],
                  "upstream_failed_tasks": ["dbt.dbt-run-b", "fire_c"]})
    outlets.annotate(snap, {"p": TASKS})
    run = snap.dags["p"]["last_run"]
    assert run["outlets"] == {"asset:1": True, "asset:2": False, "trigger:p:c:fire_c": False}
    assert run["kind"] == "pipeline"


def test_a_failed_trigger_task_is_unknown_it_may_have_fired():
    snap = _snap({"state": "failed", "kind": "tests", "failed_tasks": ["fire_c"],
                  "upstream_failed_tasks": []})
    outlets.annotate(snap, {"p": TASKS})
    run = snap.dags["p"]["last_run"]
    assert run["outlets"]["trigger:p:c:fire_c"] is None
    assert run["kind"] == "tests"


def test_unknown_task_list_gives_unknown_outlets_and_keeps_the_kind():
    snap = _snap({"state": "failed", "kind": "tests", "failed_tasks": ["dbt.dbt-test-a"],
                  "upstream_failed_tasks": []})
    outlets.annotate(snap, {"p": None})
    run = snap.dags["p"]["last_run"]
    # a trigger node names its task, so it is known even without the task list
    assert run["outlets"] == {"asset:1": None, "asset:2": None, "trigger:p:c:fire_c": True}
    assert run["kind"] == "tests"


def test_only_failed_dags_are_annotated():
    snap = _snap({"state": "success", "kind": None, "failed_tasks": []})
    outlets.annotate(snap, {"p": TASKS})
    assert "outlets" not in snap.dags["p"]["last_run"]


def test_render_carries_outlets_and_run_time():
    snap = _snap({"state": "failed", "kind": "pipeline", "run_after": "2026-11-02T09:32:50Z",
                  "failed_tasks": ["ingest"], "upstream_failed_tasks": ["dbt.dbt-run-a"],
                  "outlets": {"asset:1": False, "asset:2": None}})
    snap.dags["c"] = {"last_run": {"state": "success", "run_after": "2026-11-01T00:00:00Z"}}
    view = build_view(snap, Settings())
    p = next(n for n in view["nodes"] if n["id"] == "dag:p")
    c = next(n for n in view["nodes"] if n["id"] == "dag:c")
    assert p["fail"] == {"kind": "pipeline", "paused": False, "date": "2026-11-02T09:32:50Z",
                         "outlets": {"asset:1": False, "asset:2": None}}
    assert p["ran"] == "2026-11-02T09:32:50Z" and c["ran"] == "2026-11-01T00:00:00Z"
    assert "fail" not in c


class FailingClient(FakeClient):
    """x failed in its dbt test; the asset is named after the run task."""
    deps = {
        "nodes": [
            {"id": "dag:x", "label": "x", "type": "dag"},
            {"id": "asset:1", "label": "x-x://dbt-run-orders", "type": "asset"},
            {"id": "dag:y", "label": "y", "type": "dag"},
        ],
        "edges": [{"source_id": "dag:x", "target_id": "asset:1"},
                  {"source_id": "asset:1", "target_id": "dag:y"}],
    }
    task_calls: list[str]

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.task_calls = []

    def dag_runs(self, dag_id, limit):
        if dag_id == "x":
            return [{"dag_run_id": "r1", "state": "failed", "run_after": "2026-11-02T09:00:00Z"}]
        return [{"dag_run_id": "r2", "state": "success", "run_after": "2026-11-02T10:00:00Z"}]

    def task_instances(self, dag_id, run_id):
        if dag_id == "x":
            return [{"task_id": "dbt.dbt-run-orders", "state": "success"},
                    {"task_id": "dbt.dbt-test-orders", "state": "failed"},
                    {"task_id": "notify", "state": "upstream_failed"}]
        return [{"task_id": "a", "state": "success"}]

    def tasks(self, dag_id):
        self.task_calls.append(dag_id)
        return [{"task_id": "dbt.dbt-run-orders"}, {"task_id": "dbt.dbt-test-orders"},
                {"task_id": "notify"}]


def test_fetch_snapshot_reads_the_tasks_of_failed_dags_only(monkeypatch):
    monkeypatch.setattr(build, "AirflowClient", FailingClient)
    client = FailingClient("http://af", token="t")
    lines = []
    snap = build.fetch_snapshot(client, Settings(url="http://af", with_status=True), log=lines.append)
    assert client.task_calls == ["x"]
    run = snap.dags["x"]["last_run"]
    assert run["kind"] == "tests" and run["outlets"] == {"asset:1": True}
    assert run["upstream_failed_tasks"] == ["notify"]
    assert snap.dags["y"]["last_run"]["outlets"] is None     # not a failed run
    assert any("1 failed DAGs" in line for line in lines)


def test_fetch_snapshot_reuses_task_lists_from_with_tasks(monkeypatch):
    monkeypatch.setattr(build, "AirflowClient", FailingClient)
    client = FailingClient("http://af", token="t")
    snap = build.fetch_snapshot(client, Settings(url="http://af", with_status=True, with_tasks=True))
    assert client.task_calls == ["x", "y"]      # --with-tasks read them once; no second read
    assert snap.dags["x"]["last_run"]["outlets"] == {"asset:1": True}
