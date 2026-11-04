from collections import Counter

import pytest

from airflow_depgraph import status as status_mod
from airflow_depgraph.client import NONEMPTY_TI_STATES, ApiError
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


def test_gate_skip_success_does_not_fix_the_failure():
    # The gate and its skip sink end green, the work is skipped: the failed task never ran again.
    runs = [{"dag_run_id": "r2", "state": "success", "run_after": "2026-01-02T00:00:00Z"},
            {"dag_run_id": "r1", "state": "failed", "run_after": "2026-01-01T00:00:00Z"}]
    ti = _ti({"r2": {"check_asset_freshness": "success", "dbt_run": "skipped",
                     "dbt_test": "skipped", "skip_execution": "success"},
              "r1": {"check_asset_freshness": "success", "dbt_run": "success",
                     "dbt_test": "failed", "skip_execution": "skipped"}})
    res = classify_runs(runs, ti, RX)
    assert sorted(ti.calls) == ["r1", "r2"]
    assert (res.state, res.kind, res.run_id, res.run_after) == ("failed", "tests", "r1",
                                                                "2026-01-01T00:00:00Z")


def test_success_that_reran_the_failed_task_fixes_it():
    runs = [{"dag_run_id": "r2", "state": "success"}, {"dag_run_id": "r1", "state": "failed"}]
    ti = _ti({"r2": {"dbt_run": "success", "dbt_test": "success"},
              "r1": {"dbt_run": "success", "dbt_test": "failed"}})
    res = classify_runs(runs, ti, RX)
    assert (res.state, res.run_id) == ("success", "r2")


def test_any_success_since_the_failure_may_be_the_fix():
    # The newest run skipped the work, the one before it redid the failed task: fixed, and the
    # newest non-empty success is the decisive run.
    runs = [{"dag_run_id": "r3", "state": "success"}, {"dag_run_id": "r2", "state": "success"},
            {"dag_run_id": "r1", "state": "failed"}]
    ti = _ti({"r3": {"gate": "success", "dbt_test": "skipped"},
              "r2": {"gate": "success", "dbt_test": "success"},
              "r1": {"gate": "success", "dbt_test": "failed"}})
    res = classify_runs(runs, ti, RX)
    assert (res.state, res.run_id) == ("success", "r3")


def test_a_failed_task_gone_from_the_dag_keeps_the_failure():
    runs = [{"dag_run_id": "r2", "state": "success"}, {"dag_run_id": "r1", "state": "failed"}]
    ti = _ti({"r2": {"new_task": "success"}, "r1": {"old_task": "failed"}})
    assert classify_runs(runs, ti, RX).state == "failed"


def test_a_failed_run_without_failed_tasks_is_fixed_by_any_real_success():
    runs = [{"dag_run_id": "r2", "state": "success"}, {"dag_run_id": "r1", "state": "failed"}]
    ti = _ti({"r2": {"a": "success"}, "r1": {"a": "upstream_failed"}})
    assert classify_runs(runs, ti, RX).state == "success"


def test_only_skipped_successes_is_none():
    ti = _ti({"r1": {"a": "skipped"}})
    assert classify_runs([{"dag_run_id": "r1", "state": "success"}], ti, RX) is None


def _gate_skip(run_id, when, state="success"):
    return {"dag_run_id": run_id, "state": state, "run_after": when}


def test_failure_buried_under_many_gate_skips_is_still_found():
    # Thirty green runs that took the skip path, then the failed one: the failure stands, and the
    # task instances of every success since are read until the failed task is seen again.
    runs = [_gate_skip(f"s{i}", f"2026-01-{31 - i:02d}T00:00:00Z") for i in range(30)]
    runs.append(_gate_skip("f", "2026-01-01T00:00:00Z", "failed"))
    tis = {f"s{i}": {"gate": "success", "refresh": "skipped", "skip": "success"} for i in range(30)}
    tis["f"] = {"gate": "success", "refresh": "failed"}
    ti = _ti(tis)
    res = classify_runs(runs, ti, RX)
    assert res.state == "failed" and res.run_id == "f" and res.running is None
    assert len(ti.calls) == 31


def test_reading_stops_once_the_failed_tasks_ran_again():
    runs = [_gate_skip("s3", "2026-01-04T00:00:00Z"), _gate_skip("s2", "2026-01-03T00:00:00Z"),
            _gate_skip("s1", "2026-01-02T00:00:00Z"), _gate_skip("f", "2026-01-01T00:00:00Z", "failed")]
    ti = _ti({"s3": {"gate": "success", "load": "skipped"}, "s2": {"gate": "success", "load": "success"},
              "s1": {"gate": "success", "load": "success"}, "f": {"load": "failed"}})
    res = classify_runs(runs, ti, RX)
    assert res.state == "success" and res.run_id == "s3"      # the newest real success
    assert sorted(ti.calls) == ["f", "s2", "s3"]               # s1 never needed


def test_running_run_newer_than_the_failure_is_reported():
    runs = [{"dag_run_id": "r3", "state": "running", "run_after": "2026-01-03T00:00:00Z"},
            {"dag_run_id": "r2", "state": "queued", "run_after": "2026-01-02T12:00:00Z"},
            {"dag_run_id": "r1", "state": "failed", "run_after": "2026-01-01T00:00:00Z"}]
    res = classify_runs(runs, _ti({"r1": {"load": "failed"}}), RX)
    assert res.state == "failed" and res.running == "2026-01-03T00:00:00Z"


def test_running_run_newer_than_the_success_is_reported_and_older_ones_are_not():
    runs = [{"dag_run_id": "r3", "state": "running", "run_after": "2026-01-03T00:00:00Z"},
            {"dag_run_id": "r2", "state": "success", "run_after": "2026-01-02T00:00:00Z"},
            {"dag_run_id": "r1", "state": "running", "run_after": "2026-01-01T00:00:00Z"}]
    res = classify_runs(runs, _ti({"r2": {"load": "success"}}), RX)
    assert res.state == "success" and res.running == "2026-01-03T00:00:00Z"
    res = classify_runs(runs[1:], _ti({"r2": {"load": "success"}}), RX)
    assert res.running is None


class FakeClient:
    def bulk_dag_runs(self, dag_ids, since_iso, per_dag=None):   # an older Airflow
        raise ApiError("404 on dagRuns/list", 404, "/dagRuns/list")

    def bulk_task_instances(self, dag_ids, run_ids, states, task_ids=None):
        raise ApiError("404 on taskInstances/list", 404, "/taskInstances/list")

    def dag(self, dag_id):
        if dag_id == "dag_bad":
            raise ApiError("boom", 500, "/dags/dag_bad")
        return {"is_paused": dag_id == "dag_b", "timetable_summary": None,
                "timetable_description": "Daily", "owners": ["me"]}

    def dag_runs_since(self, dag_id, since_iso, min_runs):
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
    def dag_runs_since(self, dag_id, since_iso, min_runs):
        if dag_id == "dag_401":
            raise ApiError("401 on /dagRuns: Unauthorized", 401, "/dagRuns")
        return super().dag_runs_since(dag_id, since_iso, min_runs)


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

    def dag_runs_since(self, dag_id, since_iso, min_runs):
        # The window is ignored, as bulk_dag_runs ignores it: the same runs on both paths.
        self.calls["dag_runs"] += 1
        return self.runs.get(dag_id, [])

    def task_instances(self, dag_id, run_id, state=None):
        self.calls["task_instances"] += 1
        return self.tis.get((dag_id, run_id), [])

    def has_nonempty_task(self, dag_id, run_id):
        # Answers from the same instance list the per-DAG path reads.
        self.calls["has_nonempty"] += 1
        return any(ti["state"] in NONEMPTY_TI_STATES for ti in self.tis.get((dag_id, run_id), []))

    def bulk_dag_runs(self, dag_ids, since_iso, per_dag=None):
        if self.bulk_status:
            raise ApiError("no bulk", self.bulk_status, "/dagRuns/list")
        self.calls["bulk_runs"] += 1
        return [r for d in dag_ids for r in self.runs.get(d, [])]

    def bulk_task_instances(self, dag_ids, run_ids, states, task_ids=None):
        if self.bulk_status:
            raise ApiError("no bulk", self.bulk_status, "/taskInstances/list")
        self.calls["bulk_tis"] += 1
        return [dict(ti, dag_id=d, dag_run_id=r) for (d, r), items in self.tis.items()
                if d in dag_ids and r in run_ids for ti in items
                if ti["state"] in states and (task_ids is None or ti["task_id"] in task_ids)]


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
        "d_gate": [_run("d_gate", "g2", "success"), _run("d_gate", "g1", "failed")],
        "d_fixed": [_run("d_fixed", "f3", "success"), _run("d_fixed", "f2", "success"),
                    _run("d_fixed", "f1", "failed")],
        # More skip-path successes than the run lookback, then the failure: the window is by time.
        "d_buried": [_run("d_buried", f"b{i}", "success", f"2026-10-{30 - i:02d}T00:00:00+00:00")
                     for i in range(6)] + [_run("d_buried", "b_f", "failed", "2026-10-01T00:00:00+00:00")],
        "d_rerun": [_run("d_rerun", "u2", "running", "2026-10-21T00:00:00+00:00"),
                    _run("d_rerun", "u1", "failed")],
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
        ("d_gate", "g2"): [{"task_id": "gate", "state": "success"},
                           {"task_id": "dbt_test", "state": "skipped"},
                           {"task_id": "skip_execution", "state": "success"}],
        ("d_gate", "g1"): [{"task_id": "gate", "state": "success"},
                           {"task_id": "dbt_test", "state": "failed"}],
        ("d_fixed", "f3"): [{"task_id": "gate", "state": "success"}, {"task_id": "load", "state": "skipped"}],
        ("d_fixed", "f2"): [{"task_id": "gate", "state": "success"}, {"task_id": "load", "state": "success"}],
        ("d_fixed", "f1"): [{"task_id": "gate", "state": "success"}, {"task_id": "load", "state": "failed"}],
        **{("d_buried", f"b{i}"): [{"task_id": "gate", "state": "success"},
                                   {"task_id": "refresh", "state": "skipped"}] for i in range(6)},
        ("d_buried", "b_f"): [{"task_id": "refresh", "state": "failed"}],
        ("d_rerun", "u1"): [{"task_id": "load", "state": "failed"}],
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
    s = Settings(status_runs_lookback=3)
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
    assert bulk["d_buried"]["last_run"]["run_id"] == "b_f"
    assert bulk["d_pipe"]["last_run"]["running"] == "2026-10-20T00:00:00+00:00"
    assert bulk["d_rerun"]["last_run"]["running"] == "2026-10-21T00:00:00+00:00"
    assert bulk["d_success"]["last_run"]["running"] is None
    # A green run that skipped the failed work is not a fix; one that redid it is.
    assert bulk["d_gate"]["last_run"]["run_id"] == "g1" and bulk["d_gate"]["last_run"]["kind"] == "tests"
    assert bulk["d_fixed"]["last_run"]["run_id"] == "f3"


def test_dag_with_fewer_runs_but_a_decisive_one_is_bulk():
    fake = BulkFake({"d": [_run("d", "r2", "success"), _run("d", "r1", "failed")]},
                    {("d", "r2"): [{"task_id": "load", "state": "success"}],
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
    assert fake.calls["has_nonempty"] == 50         # one parallel check per DAG
    assert fake.calls["dag_runs"] == 0 and fake.calls["task_instances"] == 0


def test_success_checks_run_in_parallel():
    import threading
    seen = set()

    class Parallel(BulkFake):
        def has_nonempty_task(self, dag_id, run_id):
            seen.add(threading.get_ident())
            import time
            time.sleep(0.02)
            return super().has_nonempty_task(dag_id, run_id)

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


def _compare_bulk_with_per_dag(runs, tis, lookback):
    """Bulk and per-DAG answers on the same fake data; the bulk path must have been used."""
    from airflow_depgraph import status
    s = Settings(status_runs_lookback=lookback)
    ids = list(runs)
    cat = _catalog(ids)
    fake = BulkFake(runs, tis)
    per = {d: status._one(fake, d, s, False, cat) for d in ids}
    fake.calls.clear()
    bulk = fetch_status(fake, ids, s, with_tasks=False, catalog=cat)
    assert bulk == per
    assert fake.calls["bulk_runs"] >= 1
    return bulk


def test_mark_success_with_only_a_failed_task_is_decisive_as_per_dag():
    runs = {"d_mark": [_run("d_mark", "m1", "success")]}
    tis = {("d_mark", "m1"): [{"task_id": "load", "state": "failed"},
                              {"task_id": "x", "state": "skipped"}]}
    bulk = _compare_bulk_with_per_dag(runs, tis, lookback=1)
    assert bulk["d_mark"]["last_run"]["run_id"] == "m1"


def test_running_then_success_matches_per_dag():
    runs = {"d_rs": [_run("d_rs", "r2", "running"), _run("d_rs", "r1", "success")]}
    tis = {("d_rs", "r2"): [{"task_id": "a", "state": "running"}],
           ("d_rs", "r1"): [{"task_id": "a", "state": "running"},
                            {"task_id": "b", "state": "skipped"}]}
    bulk = _compare_bulk_with_per_dag(runs, tis, lookback=2)
    assert bulk["d_rs"]["last_run"]["run_id"] == "r1"


def test_success_run_with_only_skipped_tasks_stays_undecided_as_per_dag():
    runs = {"d_skip": [_run("d_skip", "k2", "success"), _run("d_skip", "k1", "success")]}
    tis = {("d_skip", "k2"): [{"task_id": "a", "state": "skipped"}],
           ("d_skip", "k1"): [{"task_id": "a", "state": "removed"}, {"task_id": "b", "state": None}]}
    bulk = _compare_bulk_with_per_dag(runs, tis, lookback=2)
    assert bulk["d_skip"]["last_run"] is None


def test_bulk_settles_a_dag_by_the_window_and_reads_the_rest_per_dag():
    """A DAG with at least status_runs_lookback runs in the window is settled by the listing even
    without a decisive run; one with fewer runs in it is read per DAG, where older runs count."""
    from datetime import datetime, timedelta, timezone
    day = lambda n: (datetime.now(timezone.utc) - timedelta(days=n)).isoformat()  # noqa: E731
    runs = {"busy": [_run("busy", f"q{i}", "running", day(i + 1)) for i in range(3)],
            "quiet": [_run("quiet", "old", "failed", day(300))]}
    tis = {("quiet", "old"): [{"task_id": "a", "state": "failed"}]}

    class Windowed(BulkFake):
        def bulk_dag_runs(self, dag_ids, since_iso, per_dag=None):
            self.calls["bulk_runs"] += 1
            self.since = since_iso
            return [r for d in dag_ids for r in self.runs.get(d, []) if r["run_after"] >= since_iso]

        def dag_runs_since(self, dag_id, since_iso, min_runs):
            self.calls["dag_runs"] += 1
            rs = self.runs.get(dag_id, [])
            return [r for i, r in enumerate(rs) if i < min_runs or r["run_after"] >= since_iso]

    fake = Windowed(runs, tis)
    s = Settings(status_runs_lookback=3, status_lookback_days=7)
    out = fetch_status(fake, ["busy", "quiet"], s, with_tasks=False, catalog=_catalog(runs))
    assert fake.since.endswith("Z") and fake.since[:10] == day(7)[:10]
    assert out["busy"]["last_run"] is None and fake.calls["dag_runs"] == 1
    assert out["quiet"]["last_run"]["state"] == "failed"


def test_window_start_is_days_before_now():
    from datetime import datetime, timezone

    from airflow_depgraph import status
    now = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)
    assert status.window_start(Settings(status_lookback_days=7), now) == "2026-10-22T12:00:00.000000Z"


def test_bulk_walks_the_successes_since_a_failure_newest_first_and_stops_early():
    """A DAG fixed by its newest success costs one task-instance page however long its history;
    a trail of skip-path successes is read page by page to the failed run."""
    def trail(dag, n, fixed_at=None):
        rs = [_run(dag, f"s{i}", "success", f"2026-10-{30 - i // 24:02d}T{23 - i % 24:02d}:00:00+00:00")
              for i in range(n)]
        rs.append(_run(dag, "f", "failed", "2026-10-01T00:00:00+00:00"))
        tis = {(dag, f"s{i}"): [{"task_id": "gate", "state": "success"},
                                {"task_id": "load", "state": "success" if i == fixed_at else "skipped"}]
               for i in range(n)}
        tis[(dag, "f")] = [{"task_id": "load", "state": "failed"}]
        return rs, tis
    runs, tis = {}, {}
    for dag, n, fixed in (("quick", 250, 0), ("slow", 250, None), ("mid", 250, 120)):
        rs, t = trail(dag, n, fixed)
        runs[dag] = rs
        tis.update(t)
    # Scheduled DAGs share run ids: twin's failed run has the id of the others' newest success,
    # and the batch listing filters DAG ids and run ids separately. Its failure must stand.
    runs["twin"] = [_run("twin", "t1", "success", "2026-10-30T00:00:00+00:00"),
                    _run("twin", "s0", "failed", "2026-10-29T00:00:00+00:00")]
    tis[("twin", "t1")] = [{"task_id": "gate", "state": "success"}, {"task_id": "load", "state": "skipped"}]
    tis[("twin", "s0")] = [{"task_id": "load", "state": "failed"}]
    fake = BulkFake(runs, tis)
    out = fetch_status(fake, list(runs), Settings(), with_tasks=False, catalog=_catalog(runs))
    assert out["quick"]["last_run"]["run_id"] == "s0"
    assert out["mid"]["last_run"]["run_id"] == "s0"            # the newest real success is reported
    assert out["slow"]["last_run"]["run_id"] == "f"
    assert out["twin"]["last_run"]["run_id"] == "s0" and out["twin"]["last_run"]["state"] == "failed"
    # One call for the failed runs, then pages of 100 run ids shared by the undecided DAGs:
    # 25 each (quick and twin decided), 50 each (mid decided at its 120th), then 100 for slow.
    assert fake.calls["bulk_tis"] == 1 + 1 + 2 + 2
    per = {d: status_mod._one(BulkFake(runs, tis), d, Settings(), False, _catalog(runs)) for d in runs}
    assert out == per
