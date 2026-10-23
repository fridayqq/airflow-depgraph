from collections import Counter

import pytest

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
    def bulk_dag_runs(self, dag_ids, since_iso, per_dag=None):   # an older Airflow
        raise ApiError("404 on dagRuns/list", 404, "/dagRuns/list")

    def bulk_task_instances(self, dag_ids, run_ids, states):
        raise ApiError("404 on taskInstances/list", 404, "/taskInstances/list")

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


def test_task_list_error_keeps_status_and_omits_tasks():
    class Forbidden(FakeClient):
        def tasks(self, dag_id):
            if dag_id == "dag_403":
                raise ApiError("403 on /tasks", 403, "/tasks")
            return super().tasks(dag_id)
    out = fetch_status(Forbidden(), ["dag_a", "dag_403"], Settings(), with_tasks=True, workers=2)
    assert "error" not in out["dag_403"]
    assert out["dag_403"]["last_run"]["state"] == "success"
    assert "tasks" not in out["dag_403"]
    assert out["dag_a"]["tasks"] == ["a", "b"]


class BulkFake:
    """In-memory Airflow for the per-DAG and the bulk endpoints; counts each kind of call."""

    def __init__(self, runs, tis, bulk_status=None):
        self.runs, self.tis, self.bulk_status = runs, tis, bulk_status
        self.calls = Counter()

    def dag(self, dag_id):
        self.calls["dag"] += 1
        return {"is_paused": False, "timetable_summary": "Daily", "owners": ["me"]}

    def dag_runs(self, dag_id, limit):
        self.calls["dag_runs"] += 1
        return self.runs.get(dag_id, [])[:limit]

    def task_instances(self, dag_id, run_id, state=None):
        self.calls["task_instances"] += 1
        return self.tis.get((dag_id, run_id), [])

    def has_successful_task(self, dag_id, run_id):
        self.calls["has_success"] += 1
        return any(ti["state"] == "success" for ti in self.tis.get((dag_id, run_id), []))

    def bulk_dag_runs(self, dag_ids, since_iso, per_dag=None):
        if self.bulk_status:
            raise ApiError("no bulk", self.bulk_status, "/dagRuns/list")
        self.calls["bulk_runs"] += 1
        return [r for d in dag_ids for r in self.runs.get(d, [])]

    def bulk_task_instances(self, dag_ids, run_ids, states):
        if self.bulk_status:
            raise ApiError("no bulk", self.bulk_status, "/taskInstances/list")
        self.calls["bulk_tis"] += 1
        return [dict(ti, dag_id=d, dag_run_id=r) for (d, r), items in self.tis.items()
                if d in dag_ids and r in run_ids for ti in items if ti["state"] in states]


def _run(dag, run_id, state, run_after="2026-10-20T00:00:00+00:00"):
    return {"dag_id": dag, "dag_run_id": run_id, "state": state, "run_after": run_after}


def _cases():
    """One DAG per classify_runs case, with the same data the per-DAG path would see."""
    runs = {
        "d_success": [_run("d_success", "s2", "success"), _run("d_success", "s1", "failed")],
        "d_empty": [_run("d_empty", "e2", "success"), _run("d_empty", "e1", "success")],
        "d_tests": [_run("d_tests", "t1", "failed")],
        "d_pipe": [_run("d_pipe", "p2", "running"), _run("d_pipe", "p1", "failed")],
        "d_running": [_run("d_running", "g1", "running")],
        "d_skipped": [_run("d_skipped", "k1", "success")],
        "d_nodata": [],
    }
    tis = {
        ("d_success", "s2"): [{"task_id": "a", "state": "success"}, {"task_id": "b", "state": "skipped"}],
        ("d_success", "s1"): [{"task_id": "a", "state": "failed"}],
        ("d_empty", "e2"): [{"task_id": "a", "state": "skipped"}, {"task_id": "b", "state": None}],
        ("d_empty", "e1"): [{"task_id": "a", "state": "success"}],
        ("d_tests", "t1"): [{"task_id": "dbt_test", "state": "failed"},
                            {"task_id": "report", "state": "upstream_failed"}],
        ("d_pipe", "p1"): [{"task_id": "dbt_test", "state": "failed"},
                           {"task_id": "load", "state": "failed"}],
        ("d_skipped", "k1"): [{"task_id": "a", "state": "skipped"}],
    }
    return runs, tis


def _catalog(ids):
    return {d: {"is_paused": False, "schedule": "Daily", "owners": ["me"], "is_stale": False}
            for d in ids}


def test_bulk_matches_per_dag_path_case_by_case():
    from airflow_depgraph import status
    runs, tis = _cases()
    ids = list(runs)
    cat = _catalog(ids)
    s = Settings(status_runs_lookback=2)
    fake = BulkFake(runs, tis)
    per = {d: status._one(fake, d, s, False, cat) for d in ids}
    fake.calls.clear()
    bulk = fetch_status(fake, ids, s, with_tasks=False, catalog=cat)
    assert bulk == per
    # d_tests has a decisive run in the listing, so it is bulk; only the DAGs with no decisive run
    # and fewer runs than the lookback are read per DAG: d_running, d_skipped, d_nodata.
    assert fake.calls["bulk_runs"] >= 1 and fake.calls["bulk_tis"] >= 1
    assert fake.calls["dag_runs"] == 3
    assert bulk["d_success"]["last_run"]["state"] == "success"
    assert bulk["d_empty"]["last_run"]["run_id"] == "e1"
    assert bulk["d_tests"]["last_run"]["kind"] == "tests"
    assert bulk["d_pipe"]["last_run"]["kind"] == "pipeline"
    assert bulk["d_running"]["last_run"] is None and bulk["d_skipped"]["last_run"] is None
    assert bulk["d_nodata"]["last_run"] is None


def test_dag_with_fewer_runs_but_a_decisive_one_is_bulk():
    fake = BulkFake({"d": [_run("d", "r2", "success"), _run("d", "r1", "failed")]},
                    {("d", "r2"): [{"task_id": "a", "state": "success"}],
                     ("d", "r1"): [{"task_id": "load", "state": "failed"}]})
    out = fetch_status(fake, ["d"], Settings(), with_tasks=False, catalog=_catalog(["d"]))
    assert out["d"]["last_run"]["run_id"] == "r2"
    assert fake.calls["dag_runs"] == 0 and fake.calls["bulk_runs"] == 1


def test_fewer_runs_all_undecided_still_reads_per_dag():
    fake = BulkFake({"d": [_run("d", "r1", "running")]}, {})
    out = fetch_status(fake, ["d"], Settings(), with_tasks=False, catalog=_catalog(["d"]))
    assert out["d"]["last_run"] is None
    assert fake.calls["dag_runs"] == 1


def test_full_listing_of_undecided_runs_is_final_without_per_dag_read():
    runs = [_run("d", f"r{k}", "success") for k in range(5)]
    fake = BulkFake({"d": runs}, {("d", f"r{k}"): [{"task_id": "a", "state": "skipped"}] for k in range(5)})
    out = fetch_status(fake, ["d"], Settings(), with_tasks=False, catalog=_catalog(["d"]))
    assert out["d"]["last_run"] is None
    assert fake.calls["dag_runs"] == 0 and fake.calls["task_instances"] == 0


def test_bulk_uses_few_calls_for_fifty_dags():
    ids = [f"d{i}" for i in range(50)]
    runs = {d: [_run(d, f"r{d}-{k}", "success") for k in range(5)] for d in ids}
    tis = {(d, f"r{d}-{k}"): [{"task_id": "a", "state": "success"}] for d in ids for k in range(5)}
    fake = BulkFake(runs, tis)
    out = fetch_status(fake, ids, Settings(), with_tasks=False, catalog=_catalog(ids))
    assert all(out[d]["last_run"]["state"] == "success" for d in ids)
    assert fake.calls["bulk_runs"] == 1 and fake.calls["bulk_tis"] == 0
    assert fake.calls["has_success"] == 50          # one parallel check per DAG
    assert fake.calls["dag_runs"] == 0 and fake.calls["task_instances"] == 0


def test_success_checks_run_in_parallel():
    import threading
    seen = set()

    class Parallel(BulkFake):
        def has_successful_task(self, dag_id, run_id):
            seen.add(threading.get_ident())
            import time
            time.sleep(0.02)
            return super().has_successful_task(dag_id, run_id)

    ids = [f"d{i}" for i in range(12)]
    runs = {d: [_run(d, f"r{d}", "success")] for d in ids}
    tis = {(d, f"r{d}"): [{"task_id": "a", "state": "success"}] for d in ids}
    fetch_status(Parallel(runs, tis), ids, Settings(), with_tasks=False, workers=4,
                 catalog=_catalog(ids))
    assert len(seen) > 1


@pytest.mark.parametrize("status_code", [404, 405])
def test_missing_bulk_endpoint_falls_back_to_per_dag(status_code):
    from airflow_depgraph import status
    runs, tis = _cases()
    ids = list(runs)
    cat = _catalog(ids)
    fake = BulkFake(runs, tis, bulk_status=status_code)
    per = {d: status._one(BulkFake(runs, tis), d, Settings(), False, cat) for d in ids}
    assert fetch_status(fake, ids, Settings(), with_tasks=False, catalog=cat) == per
    assert fake.calls["bulk_runs"] == 0 and fake.calls["dag_runs"] == len(ids)


def test_bulk_401_is_raised():
    runs, tis = _cases()
    fake = BulkFake(runs, tis, bulk_status=401)
    with pytest.raises(ApiError) as ei:
        fetch_status(fake, list(runs), Settings(), with_tasks=False)
    assert ei.value.status == 401


def test_bulk_status_off_reads_per_dag():
    runs, tis = _cases()
    ids = list(runs)
    fake = BulkFake(runs, tis)
    fetch_status(fake, ids, Settings(bulk_status=False), with_tasks=False, catalog=_catalog(ids))
    assert fake.calls["bulk_runs"] == 0 and fake.calls["dag_runs"] == len(ids)
