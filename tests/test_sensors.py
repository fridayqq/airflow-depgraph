import pytest

from airflow_depgraph.client import ApiError
from airflow_depgraph.sensors import (fetch_tasks, infer_sensor_edges, is_candidate,
                                      parse_log, parse_source)

KNOWN = {"dag_a", "dag_b", "dag_c", "waiter", "orders_dag"}


class FakeClient:
    """runs: dag -> run ids (newest first); tis: (dag, run, task) -> TI; sources/logs by dag/task."""

    def __init__(self, runs=None, tis=None, sources=None, logs=None, broken=()):
        self.runs, self.tis = runs or {}, tis or {}
        self.sources, self.logs, self.broken = sources or {}, logs or {}, set(broken)
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
        return self.sources.get(dag_id, "")

    def task_log(self, dag_id, run_id, task_id, try_number):
        self.calls.append(("log", dag_id, run_id, task_id, try_number))
        return self.logs.get((dag_id, task_id), "")

    def tasks(self, dag_id):
        if dag_id == "hidden":
            raise ApiError("403", 403, "/tasks")
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
    c = FakeClient(runs={"waiter": ["r1"]}, tis={("waiter", "r1", "wait"): {"rendered_fields": {"op_kwargs": {}}}},
                   sources={"waiter": 'DAG_ID = "waiter"\nwait_for("dag_c")\nx = \'dag_a\'\n'})
    r = _one(c)
    assert r["source"] == "source" and r["targets"] == ["dag_c", "dag_a"] and r["kind"] == "data"
    assert not any(call[0] == "log" for call in c.calls)


def test_logs_used_last_with_try_number():
    c = FakeClient(runs={"waiter": ["r1"]},
                   tis={("waiter", "r1", "wait"): {"rendered_fields": {}, "try_number": 3}},
                   logs={("waiter", "wait"): "[INFO] DAG orders_dag has successfully executed."})
    r = _one(c)
    assert r["source"] == "logs" and r["targets"] == ["orders_dag"]
    assert ("log", "waiter", "r1", "wait", 3) in c.calls


def test_own_dag_and_unknown_ids_are_ignored():
    c = FakeClient(runs={"waiter": ["r1"]}, tis={("waiter", "r1", "wait"): {"rendered_fields": {
        "op_kwargs": {"dag_id": "waiter", "other": "not_a_dag"}}}},
                   sources={"waiter": "'waiter' 'nope_dag'"}, logs={("waiter", "wait"): "waiter nope_dag"})
    r = _one(c)
    assert r["source"] == "none" and r["targets"] == []


def test_no_task_instance_skips_logs():
    c = FakeClient(runs={"waiter": ["r1"]})
    assert _one(c)["source"] == "none"
    assert not any(call[0] == "log" for call in c.calls)


def test_source_fetched_once_per_dag():
    c = FakeClient(sources={"waiter": "'dag_a'"})
    out = infer_sensor_edges(c, KNOWN, {"waiter": [sensor("s1"), sensor("s2")]})
    assert [r["targets"] for r in out] == [["dag_a"], ["dag_a"]]
    assert c.calls.count(("source", "waiter")) == 1


def test_errors_are_isolated_per_sensor():
    c = FakeClient(runs={"waiter": ["r1"]}, sources={"waiter": "'dag_a'"}, broken={"bad"})
    out = infer_sensor_edges(c, KNOWN, {"waiter": [sensor("bad"), sensor("good")]})
    assert [(r["task"], r["source"]) for r in out] == [("bad", "none"), ("good", "source")]


def test_non_candidates_are_skipped_and_output_sorted():
    c = FakeClient(sources={"dag_c": "'dag_a'", "dag_b": "'dag_a'"})
    out = infer_sensor_edges(c, KNOWN, {
        "dag_c": [sensor(), sensor("ext", "ExternalTaskSensor"), sensor("op", "PythonOperator")],
        "dag_b": [sensor()]}, workers=2)
    assert [(r["dag"], r["task"]) for r in out] == [("dag_b", "wait"), ("dag_c", "wait")]


def test_parsers_need_min_length_and_quotes():
    assert parse_source("dag_a = 1; f('dag_b')", KNOWN, "x") == ["dag_b"]
    assert parse_log("dag_a done; waited for orders_dag.", KNOWN, "x") == ["orders_dag"]


def test_fetch_tasks_maps_errors_to_empty():
    got = fetch_tasks(FakeClient(), ["a", "hidden"], workers=2)
    assert got == {"a": [{"task_id": "t", "operator_name": "PythonSensor"}], "hidden": []}
