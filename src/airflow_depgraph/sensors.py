"""Infer DAG -> DAG dependencies from sensors that /ui/dependencies does not know about.

Airflow only links ExternalTaskSensor. PythonSensor, @task.sensor and custom sensors that wait on
another DAG are resolved here from rendered fields, the DAG source or the task log, in that order.
"""
from __future__ import annotations

import ast
import re
import threading
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
# A DAG id in a log line only counts next to "dag_id=", "dag:" or "DAG <id>".
_LOG_DAG = re.compile(r"""\b(?:dag(?:_id)?\s*[=:]\s*['"]?|DAG\s+|dag\s+|found\s+for\s+|for\s+DAG\s+)"""
                      r"""([A-Za-z0-9_.-]+)""")
# Only an expired or invalid token aborts a run. A 403 on one DAG means this user may not read
# that DAG (per-DAG permissions, Airflow >= 3.1.8): that DAG or sensor becomes an "error" entry.
ABORT_STATUSES = (401,)
WHOLE_DAG = "*"     # task of the entry recorded for a DAG whose task list could not be read


def _auth_error(exc: BaseException) -> bool:
    return isinstance(exc, ApiError) and exc.status in ABORT_STATUSES


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


def _single(ids: list[str]) -> list[str]:
    return ids if len(ids) == 1 else []


def _dotted(node: ast.AST) -> str:
    if isinstance(node, ast.Call):
        node = node.func
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _str_kw(call: ast.Call, name: str) -> str | None:
    for kw in call.keywords:
        if kw.arg == name and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
            return kw.value.value
    return None


def _constants(nodes: Iterable[ast.AST]) -> Iterable[str]:
    for root in nodes:
        for n in ast.walk(root):
            if isinstance(n, ast.Constant) and isinstance(n.value, str):
                yield n.value


def _sensor_defs(tree: ast.Module, name: str) -> list[ast.AST]:
    """Functions decorated with *sensor whose task id (decorator task_id= or name) is `name`."""
    out = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for dec in node.decorator_list:
                named = _str_kw(dec, "task_id") if isinstance(dec, ast.Call) else None
                if _dotted(dec).lower().endswith("sensor") and (named or node.name) == name:
                    out.append(node)
                    break
    return out


def _declared(tree: ast.Module, name: str) -> int:
    calls = sum(1 for n in ast.walk(tree) if isinstance(n, ast.Call) and _str_kw(n, "task_id") == name)
    return calls + len(_sensor_defs(tree, name))


def _sensor_scopes(tree: ast.Module, task_id: str) -> list[ast.AST]:
    """The sensor's own call(s), its python_callable and its @task.sensor function. The full
    task id is tried first; the name without a task-group prefix only if unique in the file."""
    short = task_id.rsplit(".", 1)[-1]
    if not _declared(tree, task_id) and short != task_id:
        if _declared(tree, short) != 1:
            return []
        task_id = short
    ids = {task_id}
    defs = {n.name: n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    scopes: list[ast.AST] = list(_sensor_defs(tree, task_id))
    decorated = {n.name for n in scopes}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if _str_kw(node, "task_id") in ids or (isinstance(node.func, ast.Name)
                                                and node.func.id in decorated):
            scopes.append(node)
            for kw in node.keywords:
                if kw.arg == "python_callable" and isinstance(kw.value, ast.Name) \
                        and kw.value.id in defs:
                    scopes.append(defs[kw.value.id])
    return scopes


_UNPARSED = object()


def parse_tree(text: str) -> ast.Module | None:
    """The module AST, or None when it cannot be parsed (bad syntax, too deep, too big)."""
    try:
        return ast.parse(text or "")
    except (SyntaxError, ValueError, RecursionError, MemoryError):
        return None


def parse_source(text: str, task_id: str, known: set[str], own: str,
                 tree: object = _UNPARSED) -> list[str]:
    """DAG ids named in the sensor's own code; else the file's literals if they name one DAG.
    `tree` is parse_tree(text) when the caller already has it."""
    text = text or ""
    whole = _single(_matches(_SOURCE_LITERAL.findall(text), known, own))
    if tree is _UNPARSED:
        tree = parse_tree(text)
    if tree is None:
        return whole
    try:
        scoped = _matches(_constants(_sensor_scopes(tree, task_id)), known, own)
    except RecursionError:
        scoped = []
    return scoped or whole


def parse_log(text: str, known: set[str], own: str) -> list[str]:
    ids = (m.rstrip(".") for m in _LOG_DAG.findall(text or ""))
    return _single(_matches(ids, known, own))


def _latest_instance(client: AirflowClient, dag_id: str, task_id: str) -> dict | None:
    """Newest instance with rendered fields among recent runs, else the newest instance."""
    newest = None
    for run in client.dag_runs(dag_id, RUNS_LOOKBACK):
        try:
            ti = client.task_instance(dag_id, run["dag_run_id"], task_id)
        except ApiError as exc:
            if exc.status != 404:
                raise
            continue
        if ti.get("rendered_fields"):
            return ti
        newest = newest or ti
    return newest


def _entry(dag_id: str, task: dict, source: str = "none", **kw) -> dict:
    return {"dag": dag_id, "task": task["task_id"], "operator": task.get("operator_name"),
            "source": source, "targets": [], "target_tasks": [], "kind": "data", **kw}


_FAILED = object()


def resolve_sensor(client: AirflowClient, dag_id: str, task: dict, known: set[str],
                   source_cache: dict[str, object]) -> dict:
    """Each stage fails on its own; only a 401 aborts everything (that ApiError is re-raised).
    source_cache holds (source text, parsed tree) per DAG, or _FAILED."""
    task_id = task["task_id"]
    failed = False
    ti = None
    try:
        ti = _latest_instance(client, dag_id, task_id)
    except Exception as exc:  # noqa: BLE001
        if _auth_error(exc):
            raise
        failed = True
    if ti:
        targets, tasks, states = parse_rendered(ti.get("rendered_fields") or {}, known, dag_id)
        if targets:
            return _entry(dag_id, task, "rendered", targets=targets, target_tasks=tasks,
                          kind="guard" if states == ["running"] else "data")
    if dag_id not in source_cache:
        try:
            text = client.dag_source(dag_id)
            source_cache[dag_id] = (text, parse_tree(text))
        except Exception as exc:  # noqa: BLE001
            if _auth_error(exc):
                raise
            source_cache[dag_id] = _FAILED
    source = source_cache[dag_id]
    if source is _FAILED:
        failed = True
    elif targets := parse_source(source[0], task_id, known, dag_id, tree=source[1]):
        return _entry(dag_id, task, "source", targets=targets)
    if ti and ti.get("dag_run_id"):
        try:
            log = client.task_log(dag_id, ti["dag_run_id"], task_id,
                                  max(int(ti.get("try_number") or 1), 1))
            if targets := parse_log(log, known, dag_id):
                return _entry(dag_id, task, "logs", targets=targets)
        except Exception as exc:  # noqa: BLE001
            if _auth_error(exc):
                raise
            failed = True
    return _entry(dag_id, task, "error" if failed else "none")


def _resolve_dag(client: AirflowClient, dag_id: str, tasks: list[dict], known: set[str],
                 stop: threading.Event | None = None) -> list[dict]:
    cache: dict[str, object] = {}   # the DAG source is fetched once per DAG
    results = []
    for task in tasks:
        if stop is not None and stop.is_set():   # another DAG hit a 401: stop calling
            break
        try:
            results.append(resolve_sensor(client, dag_id, task, known, cache))
        except Exception as exc:  # noqa: BLE001 - one broken sensor must not stop the others
            if _auth_error(exc):
                raise
            results.append(_entry(dag_id, task, "error"))
    return results


def fetch_tasks(client: AirflowClient, dag_ids: list[str],
                workers: int = 8) -> dict[str, list[dict] | None]:
    """{dag_id: GET /dags/{id}/tasks items}; None when unreadable (403 included), 401 aborts."""
    def one(d: str) -> list[dict] | None:
        try:
            return client.tasks(d)
        except ApiError as exc:
            if exc.status in ABORT_STATUSES:
                raise
            return None
    pool = ThreadPoolExecutor(max_workers=workers)
    try:
        return dict(zip(dag_ids, pool.map(one, dag_ids)))
    finally:   # after a 401, queued calls are dropped instead of being sent
        pool.shutdown(wait=False, cancel_futures=True)


def infer_sensor_edges(client: AirflowClient, catalog_ids: Iterable[str],
                       tasks_by_dag: dict[str, list[dict] | None], *, workers: int = 8,
                       progress: Callable[[str], None] | None = None) -> list[dict]:
    """One entry per candidate sensor, plus one (task "*") per DAG whose tasks were unreadable.
    `source` is "none" when nothing was found and "error" when a call failed on the way;
    a 401 aborts the whole run with ApiError."""
    known = set(catalog_ids)
    results: list[dict] = [_entry(d, {"task_id": WHOLE_DAG}, "error")
                           for d, tasks in tasks_by_dag.items() if tasks is None]
    jobs = {d: [t for t in tasks if isinstance(t, dict) and t.get("task_id")
                and is_candidate(t.get("operator_name"))]
            for d, tasks in tasks_by_dag.items() if tasks is not None}
    jobs = {d: ts for d, ts in jobs.items() if ts}
    pool, stop = ThreadPoolExecutor(max_workers=workers), threading.Event()
    try:
        futures = {pool.submit(_resolve_dag, client, d, ts, known, stop): d for d, ts in jobs.items()}
        for done, fut in enumerate(as_completed(futures), 1):
            results.extend(fut.result())
            if progress:
                progress(f"{done}/{len(futures)} {futures[fut]}")
    except BaseException:
        stop.set()
        raise
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    return sorted(results, key=lambda r: (r["dag"], r["task"]))
