from airflow_depgraph.client import ApiError
from airflow_depgraph.presets import Settings
from airflow_depgraph.status import classify_runs, fetch_status

RX = Settings().tests_task_regex


def _ti(mapping):
    def get(run_id):
        get.calls.append(run_id)
        return [{"task_id": t, "state": s} for t, s in mapping[run_id].items()]
    get.calls = []
    return get


def _none(run_id):
    raise AssertionError("task_instances must not be called")


def test_no_runs_is_none():
    assert classify_runs([], _none, RX) is None


def test_running_then_failed_picks_failed():
    runs = [
        {"dag_run_id": "r3", "state": "running"},
        {"dag_run_id": "r2", "state": "queued"},
        {"dag_run_id": "r1", "state": "failed", "run_after": "2026-01-01T00:00:00Z"},
    ]
    ti = _ti({"r1": {"load": "failed", "x": "success"}})
    res = classify_runs(runs, ti, RX)
    assert ti.calls == ["r1"]
    assert (res.state, res.kind, res.run_id) == ("failed", "pipeline", "r1")
    assert res.run_after == "2026-01-01T00:00:00Z"
    assert res.failed_tasks == ["load"]


def test_all_skipped_success_is_ignored_then_failed_found():
    runs = [
        {"dag_run_id": "r2", "state": "success"},
        {"dag_run_id": "r1", "state": "failed"},
    ]
    ti = _ti({
        "r2": {"a": "skipped", "b": None, "c": "removed"},
        "r1": {"dbt_test": "failed"},
    })
    res = classify_runs(runs, ti, RX)
    assert res.run_id == "r1"
    assert res.run_after is None


def test_failed_tests_only_kind():
    ti = _ti({"r1": {"dbt_test": "failed", "report_a": "upstream_failed",
                     "report_b": "upstream_failed", "ok": "success"}})
    res = classify_runs([{"dag_run_id": "r1", "state": "failed"}], ti, RX)
    assert res.kind == "tests"
    assert res.failed_tasks == ["dbt_test"]
    assert res.upstream_failed == 2


def test_failed_mixed_kind_pipeline():
    ti = _ti({"r1": {"dbt_test": "failed", "load": "failed"}})
    res = classify_runs([{"dag_run_id": "r1", "state": "failed"}], ti, RX)
    assert res.kind == "pipeline"


def test_success_decisive():
    ti = _ti({"r1": {"a": "success", "b": "skipped"}})
    res = classify_runs([{"dag_run_id": "r1", "state": "success"}], ti, RX)
    assert (res.state, res.kind, res.failed_tasks) == ("success", None, [])


def test_only_skipped_successes_is_none():
    ti = _ti({"r1": {"a": "skipped"}})
    assert classify_runs([{"dag_run_id": "r1", "state": "success"}], ti, RX) is None


class FakeClient:
    def dag(self, dag_id):
        if dag_id == "dag_bad":
            raise ApiError("boom", 500, "/dags/dag_bad")
        return {"is_paused": dag_id == "dag_b", "timetable_summary": None,
                "timetable_description": "Daily", "owners": ["me"]}

    def dag_runs(self, dag_id, limit):
        return [{"dag_run_id": "r1", "state": "success", "run_after": "t"}]

    def task_instances(self, dag_id, run_id):
        return [{"task_id": "a", "state": "success"}]

    def tasks(self, dag_id):
        return [{"task_id": "a"}, {"task_id": "b"}]


def test_fetch_status_isolates_errors():
    msgs = []
    out = fetch_status(FakeClient(), ["dag_a", "dag_b", "dag_bad"], Settings(),
                       with_tasks=True, workers=2, progress=msgs.append)
    assert set(out) == {"dag_a", "dag_b", "dag_bad"}
    assert out["dag_bad"] == {"error": "boom"}
    assert out["dag_a"]["schedule"] == "Daily"
    assert out["dag_a"]["is_paused"] is False and out["dag_b"]["is_paused"] is True
    assert out["dag_a"]["last_run"]["state"] == "success"
    assert out["dag_a"]["tasks"] == ["a", "b"]
    assert len(msgs) == 3 and all("/3 " in m for m in msgs)


def test_fetch_status_without_tasks():
    out = fetch_status(FakeClient(), ["dag_a"], Settings(), with_tasks=False)
    assert "tasks" not in out["dag_a"]


def test_dotted_dbt_test_with_upstream_report_is_tests_kind():
    ti = _ti({"r1": {"dbt_tasks.dbt-test-orders": "failed",
                     "reports.refresh": "upstream_failed"}})
    res = classify_runs([{"dag_run_id": "r1", "state": "failed"}], ti, RX)
    assert res.kind == "tests"
    assert res.failed_tasks == ["dbt_tasks.dbt-test-orders"]
    assert res.upstream_failed == 1


def test_fetch_status_reuses_catalog():
    class NoDag(FakeClient):
        def dag(self, dag_id):
            raise AssertionError("catalog DAGs must not be fetched one by one")
    cat = {"dag_a": {"tags": [], "is_paused": True, "is_stale": False, "schedule": "Daily", "owners": ["me"]}}
    out = fetch_status(NoDag(), ["dag_a"], Settings(), with_tasks=False, catalog=cat)
    assert out["dag_a"]["is_paused"] is True and out["dag_a"]["schedule"] == "Daily"
    assert out["dag_a"]["owners"] == ["me"] and out["dag_a"]["last_run"]["state"] == "success"


class _ExpiredToken(FakeClient):
    def dag_runs(self, dag_id, limit):
        if dag_id == "dag_401":
            raise ApiError("401 on /dagRuns: Unauthorized", 401, "/dagRuns")
        return super().dag_runs(dag_id, limit)


def test_fetch_status_stops_on_401():
    import pytest
    with pytest.raises(ApiError) as ei:
        fetch_status(_ExpiredToken(), ["dag_a", "dag_401", "dag_b"], Settings(),
                     with_tasks=False, workers=1)
    assert ei.value.status == 401
