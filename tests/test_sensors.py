import pytest

from airflow_depgraph.client import ApiError
from airflow_depgraph.sensors import fetch_tasks, infer_sensor_edges, is_candidate, parse_log, parse_source

KNOWN = {"dag_a", "dag_b", "dag_c", "waiter", "orders_dag", "edw_dbt-prod01-evening_run"}


class FakeClient:
    """runs: dag -> run ids (newest first); tis: (dag, run, task) -> TI; sources/logs by dag/task."""

    def __init__(self, runs=None, tis=None, sources=None, logs=None, broken=(), fail=None):
        self.runs, self.tis = runs or {}, tis or {}
        self.sources, self.logs, self.broken = sources or {}, logs or {}, set(broken)
        self.fail = fail or {}          # call kind -> exception raised by that call
        self.calls = []

    def dag_runs(self, dag_id, limit):
        self.calls.append(("runs", dag_id))
        return [{"dag_run_id": r} for r in self.runs.get(dag_id, [])][:limit]

    def task_instance(self, dag_id, run_id, task_id):
        self.calls.append(("ti", dag_id, run_id, task_id))
        if task_id in self.broken:
            raise RuntimeError("boom")
        try:
            return {"dag_run_id": run_id, "try_number": 1, **self.tis[(dag_id, run_id, task_id)]}
        except KeyError:
            raise ApiError("404", 404, "/ti") from None

    def dag_source(self, dag_id):
        self.calls.append(("source", dag_id))
        if "source" in self.fail:
            raise self.fail["source"]
        return self.sources.get(dag_id, "")

    def task_log(self, dag_id, run_id, task_id, try_number):
        self.calls.append(("log", dag_id, run_id, task_id, try_number))
        if "log" in self.fail:
            raise self.fail["log"]
        return self.logs.get((dag_id, task_id), "")

    def tasks(self, dag_id):
        if dag_id == "expired":
            raise ApiError("401", 401, "/tasks")
        if dag_id == "hidden":
            raise ApiError("404", 404, "/tasks")
        return [{"task_id": "t", "operator_name": "PythonSensor"}]


def sensor(task_id="wait", op="PythonSensor"):
    return {"task_id": task_id, "operator_name": op}


def _one(client, tasks=None):
    out = infer_sensor_edges(client, KNOWN, {"waiter": tasks or [sensor()]})
    assert len(out) == 1
    return out[0]


@pytest.mark.parametrize("op,expected", [
    ("PythonSensor", True), ("_DecoratedSensorOperator", True), ("MyCustomSensor", True),
    ("ExternalTaskSensor", False), ("TimeSensor", False), ("DateTimeSensor", False),
    ("TimeDeltaSensor", False), ("S3KeySensor", False), ("TableauJobStatusSensor", False),
    ("PythonOperator", False), (None, False)])
def test_candidates(op, expected):
    assert is_candidate(op) is expected


def test_rendered_fields_give_targets_and_tasks():
    c = FakeClient(runs={"waiter": ["r2", "r1"]}, tis={("waiter", "r1", "wait"): {"rendered_fields": {
        "op_kwargs": {"external_dag_id": "dag_a", "external_task_id": "group.task"}}}})
    r = _one(c)
    assert r == {"dag": "waiter", "task": "wait", "operator": "PythonSensor", "source": "rendered",
                 "targets": ["dag_a"], "target_tasks": ["group.task"], "kind": "data"}
    assert ("source", "waiter") not in c.calls   # first hit wins


def test_guard_detection_from_states_running():
    c = FakeClient(runs={"waiter": ["r1"]}, tis={("waiter", "r1", "wait"): {"rendered_fields": {
        "op_kwargs": {"dag_id": "dag_b", "states": ["running"], "task_ids": ["x", "y"]}}}})
    r = _one(c)
    assert r["kind"] == "guard" and r["targets"] == ["dag_b"] and r["target_tasks"] == ["x", "y"]


def test_states_other_than_running_only_is_data():
    c = FakeClient(runs={"waiter": ["r1"]}, tis={("waiter", "r1", "wait"): {"rendered_fields": {
        "op_kwargs": {"dag_id": "dag_b", "states": ["running", "queued"]}}}})
    assert _one(c)["kind"] == "data"


def test_source_used_when_rendered_is_empty():
    tis = {("waiter", "r1", "wait"): {"rendered_fields": {"op_kwargs": {}}}}
    c = FakeClient(runs={"waiter": ["r1"]}, tis=tis,
                   sources={"waiter": 'PythonSensor(task_id="wait", '
                                      'op_kwargs={"a": "dag_c", "b": "dag_a"})\n'})
    r = _one(c)
    assert r["source"] == "source" and r["targets"] == ["dag_c", "dag_a"] and r["kind"] == "data"
    assert not any(call[0] == "log" for call in c.calls)


def test_logs_used_last_with_try_number():
    c = FakeClient(runs={"waiter": ["r1"]},
                   tis={("waiter", "r1", "wait"): {"rendered_fields": {}, "try_number": 3}},
                   logs={("waiter", "wait"): "[INFO] DAG orders_dag has successfully executed. dag_a"})
    r = _one(c)
    assert r["source"] == "logs" and r["targets"] == ["orders_dag"]
    assert ("log", "waiter", "r1", "wait", 3) in c.calls


def test_own_dag_and_unknown_ids_are_ignored():
    c = FakeClient(runs={"waiter": ["r1"]}, tis={("waiter", "r1", "wait"): {"rendered_fields": {
        "op_kwargs": {"dag_id": "waiter", "other": "not_a_dag"}}}},
                   sources={"waiter": "'waiter' 'nope_dag'"}, logs={("waiter", "wait"): "waiter nope_dag"})
    r = _one(c)
    assert r["source"] == "none" and r["targets"] == []


def test_empty_rendered_fields_look_at_older_runs():
    c = FakeClient(runs={"waiter": ["r3", "r2", "r1"]}, tis={
        ("waiter", "r3", "wait"): {"rendered_fields": None},
        ("waiter", "r2", "wait"): {"rendered_fields": {}},
        ("waiter", "r1", "wait"): {"rendered_fields": {"op_kwargs": {"dag_id": "dag_a"}}}})
    assert _one(c)["targets"] == ["dag_a"]


def test_failing_stage_does_not_skip_the_next_and_marks_error():
    c = FakeClient(runs={"waiter": ["r1"]}, tis={("waiter", "r1", "wait"): {"rendered_fields": {}}},
                   fail={"source": ApiError("500", 500, "/src")},
                   logs={("waiter", "wait"): "poking dag_id=orders_dag"})
    assert _one(c)["source"] == "logs"
    c.logs = {}
    assert _one(c)["source"] == "error"                      # nothing found, a call failed
    assert _one(FakeClient())["source"] == "none"            # nothing found, nothing failed


@pytest.mark.parametrize("kind", ["source", "log"])
def test_401_aborts_the_run(kind):
    c = FakeClient(runs={"waiter": ["r1"]}, tis={("waiter", "r1", "wait"): {"rendered_fields": {}}},
                   fail={kind: ApiError("401", 401, "/x")})
    with pytest.raises(ApiError):
        infer_sensor_edges(c, KNOWN, {"waiter": [sensor()]})


def test_fetch_tasks_aborts_on_auth_error():
    with pytest.raises(ApiError):
        fetch_tasks(FakeClient(), ["a", "expired"])


def test_no_task_instance_skips_logs():
    c = FakeClient(runs={"waiter": ["r1"]})
    assert _one(c)["source"] == "none"
    assert not any(call[0] == "log" for call in c.calls)


def test_source_fetched_once_per_dag():
    c = FakeClient(sources={"waiter": "x = 'dag_a'"})
    out = infer_sensor_edges(c, KNOWN, {"waiter": [sensor("s1"), sensor("s2")]})
    assert [r["targets"] for r in out] == [["dag_a"], ["dag_a"]]
    assert c.calls.count(("source", "waiter")) == 1


def test_errors_are_isolated_per_sensor():
    c = FakeClient(runs={"waiter": ["r1"]}, sources={"waiter": "'dag_a'"}, broken={"bad"})
    out = infer_sensor_edges(c, KNOWN, {"waiter": [sensor("bad"), sensor("good")]})
    assert [(r["task"], r["source"]) for r in out] == [("bad", "source"), ("good", "source")]
    c.fail["source"] = RuntimeError("boom")
    out = infer_sensor_edges(c, KNOWN, {"other": [sensor("bad"), sensor("good")]})
    assert [(r["task"], r["source"]) for r in out] == [("bad", "error"), ("good", "error")]


def test_non_candidates_are_skipped_and_output_sorted():
    c = FakeClient(sources={"dag_c": "'dag_a'", "dag_b": "'dag_a'"})
    out = infer_sensor_edges(c, KNOWN, {
        "dag_c": [sensor(), sensor("ext", "ExternalTaskSensor"), sensor("op", "PythonOperator")],
        "dag_b": [sensor()]}, workers=2)
    assert [(r["dag"], r["task"]) for r in out] == [("dag_b", "wait"), ("dag_c", "wait")]


def test_log_context_found_for_and_for_dag():
    line = "No successful DAG run found for edw_dbt-prod01-evening_run today."
    assert parse_log(line, KNOWN, "waiter") == ["edw_dbt-prod01-evening_run"]
    assert parse_log("still waiting for DAG orders_dag", KNOWN, "waiter") == ["orders_dag"]
    assert parse_log("nothing found for today, orders_dag", KNOWN, "waiter") == []


def test_log_needs_context_and_exactly_one_dag():
    assert parse_log("dag_a done; waiting for DAG orders_dag.", KNOWN, "x") == ["orders_dag"]
    assert parse_log("dag_id='dag_b' state=running", KNOWN, "x") == ["dag_b"]
    assert parse_log("orders_dag dag_a", KNOWN, "x") == []                  # no context
    assert parse_log("DAG dag_a and DAG dag_b", KNOWN, "x") == []           # ambiguous
    assert parse_log("DAG waiter", KNOWN, "waiter") == []                   # own DAG


TRIGGER_AND_SENSOR = """
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.python import PythonSensor

def _done(dag_id):
    return check("dag_b")

with DAG("waiter", doc_md="Loads after dag_c, see also orders_dag"):
    go = TriggerDagRunOperator(task_id="kick", trigger_dag_id="dag_a")
    wait = PythonSensor(task_id="wait", python_callable=_done)
"""


def test_source_scoped_to_sensor_call_and_callable():
    assert parse_source(TRIGGER_AND_SENSOR, "wait", KNOWN, "waiter") == ["dag_b"]
    # the trigger target, the doc_md mentions: not the sensor's
    assert parse_source(TRIGGER_AND_SENSOR, "kick", KNOWN, "waiter") == ["dag_a"]


def test_source_task_sensor_decorator_and_group_prefix():
    src = """
@task.sensor(poke_interval=60)
def wait_load():
    return done("dag_c")

@task.sensor(task_id="wait_other")
def anything():
    return done("orders_dag")

PythonSensor(task_id="unrelated", op_kwargs={"d": "dag_a"})
"""
    assert parse_source(src, "grp.wait_load", KNOWN, "waiter") == ["dag_c"]
    assert parse_source(src, "wait_other", KNOWN, "waiter") == ["orders_dag"]


def test_source_falls_back_to_file_only_for_a_single_dag():
    factory = "for d in ['dag_a', 'dag_b', 'dag_c']:\n    make(d)\n"
    assert parse_source(factory, "wait", KNOWN, "waiter") == []
    assert parse_source("make('dag_a')\n", "wait", KNOWN, "waiter") == ["dag_a"]
    assert parse_source("def broken(:\n 'dag_a'", "wait", KNOWN, "waiter") == ["dag_a"]
    assert parse_source("def broken(:\n 'dag_a' 'dag_b'", "wait", KNOWN, "waiter") == []


def test_fetch_tasks_maps_errors_to_empty():
    got = fetch_tasks(FakeClient(), ["a", "hidden"], workers=2)
    assert got == {"a": [{"task_id": "t", "operator_name": "PythonSensor"}], "hidden": None}


def test_one_dag_403_is_skipped_as_error_not_aborting():
    class Partial(FakeClient):
        def tasks(self, dag_id):
            if dag_id == "dag_b":
                raise ApiError("403", 403, "/tasks")
            return [sensor()]
    c = Partial(sources={"waiter": "x = 'dag_a'"})
    tasks = fetch_tasks(c, ["waiter", "dag_b"])
    assert tasks["dag_b"] is None
    out = infer_sensor_edges(c, KNOWN, tasks)
    assert [(r["dag"], r["task"], r["source"]) for r in out] == [
        ("dag_b", "*", "error"), ("waiter", "wait", "source")]


@pytest.mark.parametrize("kind", ["source", "log"])
def test_403_on_one_sensor_lookup_is_an_error_entry(kind):
    c = FakeClient(runs={"waiter": ["r1"]}, tis={("waiter", "r1", "wait"): {"rendered_fields": {}}},
                   fail={kind: ApiError("403", 403, "/x")})
    assert _one(c)["source"] == "error"


def test_fetch_tasks_stops_calling_after_a_401():
    calls = []

    class C:
        def tasks(self, dag_id):
            calls.append(dag_id)
            if dag_id == "first":
                raise ApiError("401", 401, "/tasks")
            return []
    with pytest.raises(ApiError):
        fetch_tasks(C(), ["first"] + [f"d{i}" for i in range(50)], workers=1)
    import time
    time.sleep(0.05)
    assert len(calls) < 5


def test_resolve_dag_stops_when_the_run_is_cancelled():
    import threading

    from airflow_depgraph.sensors import _resolve_dag
    stop = threading.Event()
    stop.set()
    c = FakeClient(sources={"waiter": "'dag_a'"})
    assert _resolve_dag(c, "waiter", [sensor("s1"), sensor("s2")], KNOWN, stop) == []
    assert c.calls == []


def test_source_parsed_once_per_dag_and_deep_source_falls_back(monkeypatch):
    import ast

    import airflow_depgraph.sensors as sensors
    parsed = []
    real = ast.parse
    monkeypatch.setattr(sensors.ast, "parse", lambda text, *a, **k: parsed.append(1) or real(text, *a, **k))
    c = FakeClient(sources={"waiter": "x = 'dag_a'"})
    infer_sensor_edges(c, KNOWN, {"waiter": [sensor("s1"), sensor("s2"), sensor("s3")]})
    assert len(parsed) == 1

    def boom(*a, **k):
        raise RecursionError
    monkeypatch.setattr(sensors.ast, "parse", boom)
    assert parse_source("make('dag_a')", "wait", KNOWN, "waiter") == ["dag_a"]
    monkeypatch.setattr(sensors.ast, "parse", lambda *a, **k: (_ for _ in ()).throw(MemoryError()))
    assert parse_source("make('dag_a') + make('dag_b')", "wait", KNOWN, "waiter") == []


def test_task_id_matches_full_id_first_and_short_name_only_if_unique():
    two_groups = """
with TaskGroup("g1"):
    PythonSensor(task_id="wait", op_kwargs={"d": "dag_a"})
with TaskGroup("g2"):
    PythonSensor(task_id="wait", op_kwargs={"d": "dag_b"})
"""
    assert parse_source(two_groups, "g1.wait", KNOWN, "waiter") == []
    full = """
PythonSensor(task_id="g.wait", op_kwargs={"d": "dag_a"})
PythonSensor(task_id="wait", op_kwargs={"d": "dag_b"})
"""
    assert parse_source(full, "g.wait", KNOWN, "waiter") == ["dag_a"]
    assert parse_source(full, "wait", KNOWN, "waiter") == ["dag_b"]


def test_task_sensor_with_task_id_decorator_counted_once():
    src = """
@task.sensor(task_id="wait_x", poke_interval=30)
def first():
    return done("dag_a")

@task.sensor(task_id="wait_y")
def second():
    return done("dag_b")
"""
    assert parse_source(src, "grp.wait_x", KNOWN, "waiter") == ["dag_a"]
    assert parse_source(src, "grp.wait_y", KNOWN, "waiter") == ["dag_b"]
