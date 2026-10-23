"""Infer DAG -> DAG dependencies from sensors that /ui/dependencies does not know about.

Airflow only links ExternalTaskSensor. PythonSensor, @task.sensor and custom sensors that wait on
another DAG are resolved here from rendered fields, the DAG source or the task log, in that order.
"""
from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

from .client import AirflowClient, ApiError

# Sensors that are already in the graph, or that wait on time, files or BI jobs, not on DAGs.
EXCLUDED_OPERATORS = frozenset({
    "ExternalTaskSensor", "TimeSensor", "DateTimeSensor", "TimeDeltaSensor", "S3KeySensor",
    "TableauJobStatusSensor",
})
RUNS_LOOKBACK = 5
_TASK_KEYS = ("external_task_id", "external_task_ids", "task_ids", "task_id")
_SOURCE_LITERAL = re.compile(r"""["']([A-Za-z0-9_.-]{4,})["']""")
_LOG_TOKEN = re.compile(r"[A-Za-z0-9_.-]{6,}")


def is_candidate(operator: str | None) -> bool:
    op = operator or ""
    return "sensor" in op.lower() and op not in EXCLUDED_OPERATORS


def _strings(value: object) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for v in value.values():
            yield from _strings(v)
    elif isinstance(value, (list, tuple)):
        for v in value:
            yield from _strings(v)


def _by_key(value: object, keys: tuple[str, ...]) -> Iterable[object]:
    if isinstance(value, dict):
        for k, v in value.items():
            if k in keys:
                yield v
            yield from _by_key(v, keys)
    elif isinstance(value, (list, tuple)):
        for v in value:
            yield from _by_key(v, keys)


def _matches(candidates: Iterable[str], known: set[str], own: str) -> list[str]:
    return list(dict.fromkeys(c for c in candidates if c in known and c != own))


def parse_rendered(fields: object, known: set[str], own: str) -> tuple[list[str], list[str], list | None]:
    """(target DAG ids, target task ids, states) from a task instance's rendered_fields."""
    targets = _matches(_strings(fields), known, own)
    tasks = list(dict.fromkeys(_strings(list(_by_key(fields, _TASK_KEYS)))))
    states = None
    if isinstance(fields, dict):
        op_kwargs = fields.get("op_kwargs")
        if isinstance(op_kwargs, dict) and isinstance(op_kwargs.get("states"), list):
            states = op_kwargs["states"]
    return targets, tasks, states


def parse_source(text: str, known: set[str], own: str) -> list[str]:
    return _matches(_SOURCE_LITERAL.findall(text or ""), known, own)


def parse_log(text: str, known: set[str], own: str) -> list[str]:
    return _matches((t.rstrip(".") for t in _LOG_TOKEN.findall(text or "")), known, own)


def _latest_instance(client: AirflowClient, dag_id: str, task_id: str) -> dict | None:
    for run in client.dag_runs(dag_id, RUNS_LOOKBACK):
        try:
            return client.task_instance(dag_id, run["dag_run_id"], task_id)
        except ApiError as exc:
            if exc.status != 404:
                raise
    return None


def resolve_sensor(client: AirflowClient, dag_id: str, task: dict, known: set[str],
                   source_cache: dict[str, str]) -> dict:
    task_id, op = task["task_id"], task.get("operator_name")
    out = {"dag": dag_id, "task": task_id, "operator": op, "source": "none",
           "targets": [], "target_tasks": [], "kind": "data"}
    ti = _latest_instance(client, dag_id, task_id)
    if ti:
        targets, tasks, states = parse_rendered(ti.get("rendered_fields") or {}, known, dag_id)
        if targets:
            kind = "guard" if states == ["running"] else "data"
            return {**out, "source": "rendered", "targets": targets, "target_tasks": tasks,
                    "kind": kind}
    if dag_id not in source_cache:
        try:
            source_cache[dag_id] = client.dag_source(dag_id)
        except ApiError:
            source_cache[dag_id] = ""
    if targets := parse_source(source_cache[dag_id], known, dag_id):
        return {**out, "source": "source", "targets": targets}
    if ti and ti.get("dag_run_id"):
        log = client.task_log(dag_id, ti["dag_run_id"], task_id, max(int(ti.get("try_number") or 1), 1))
        if targets := parse_log(log, known, dag_id):
            return {**out, "source": "logs", "targets": targets}
    return out


def _resolve_dag(client: AirflowClient, dag_id: str, tasks: list[dict], known: set[str]) -> list[dict]:
    cache: dict[str, str] = {}   # the DAG source is fetched once per DAG
    results = []
    for task in tasks:
        try:
            results.append(resolve_sensor(client, dag_id, task, known, cache))
        except Exception:  # noqa: BLE001 - one broken sensor must not stop the others
            results.append({"dag": dag_id, "task": task["task_id"],
                            "operator": task.get("operator_name"), "source": "none",
                            "targets": [], "target_tasks": [], "kind": "data"})
    return results


def fetch_tasks(client: AirflowClient, dag_ids: list[str], workers: int = 8) -> dict[str, list[dict]]:
    """{dag_id: GET /dags/{id}/tasks items}; a DAG whose tasks cannot be read maps to []."""
    def one(d: str) -> list[dict]:
        try:
            return client.tasks(d)
        except ApiError:
            return []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return dict(zip(dag_ids, pool.map(one, dag_ids)))


def infer_sensor_edges(client: AirflowClient, catalog_ids: Iterable[str],
                       tasks_by_dag: dict[str, list[dict]], *, workers: int = 8,
                       progress: Callable[[str], None] | None = None) -> list[dict]:
    """One entry per candidate sensor; `source` "none" when nothing resolved (or it failed)."""
    known = set(catalog_ids)
    jobs = {d: [t for t in tasks if isinstance(t, dict) and t.get("task_id")
                and is_candidate(t.get("operator_name"))]
            for d, tasks in tasks_by_dag.items()}
    jobs = {d: ts for d, ts in jobs.items() if ts}
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_resolve_dag, client, d, ts, known): d for d, ts in jobs.items()}
        for done, fut in enumerate(as_completed(futures), 1):
            results.extend(fut.result())
            if progress:
                progress(f"{done}/{len(futures)} {futures[fut]}")
    return sorted(results, key=lambda r: (r["dag"], r["task"]))
