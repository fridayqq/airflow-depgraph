"""Run-status overlay: last decisive run per DAG."""
from __future__ import annotations

import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass

from .client import AirflowClient
from .presets import Settings

_EMPTY_STATES = {"skipped", "removed", None}
_FAILED_STATES = {"failed", "upstream_failed"}


@dataclass
class LastRun:
    state: str
    kind: str | None
    run_after: str | None
    run_id: str
    failed_tasks: list[str]


def classify_runs(runs: list[dict], task_instances: Callable[[str], list[dict]],
                  tests_task_regex: str) -> LastRun | None:
    """Return the newest decisive run (runs are newest first), or None."""
    for run in runs:
        state = run.get("state")
        run_id = run["dag_run_id"]
        if state == "success":
            instances = task_instances(run_id)
            if all(ti.get("state") in _EMPTY_STATES for ti in instances):
                continue  # empty success: nothing really ran
            return LastRun("success", None, run.get("run_after"), run_id, [])
        if state == "failed":
            failed = [ti["task_id"] for ti in task_instances(run_id)
                      if ti.get("state") in _FAILED_STATES]
            only_tests = bool(failed) and all(
                re.search(tests_task_regex, t) for t in failed)
            return LastRun("failed", "tests" if only_tests else "pipeline",
                           run.get("run_after"), run_id, failed)
    return None


def _one(client: AirflowClient, dag_id: str, s: Settings, with_tasks: bool) -> dict:
    d = client.dag(dag_id)
    runs = client.dag_runs(dag_id, s.status_runs_lookback)
    last = classify_runs(runs, lambda rid: client.task_instances(dag_id, rid),
                         s.tests_task_regex)
    out = {
        "is_paused": bool(d.get("is_paused")),
        "schedule": d.get("timetable_summary") or d.get("timetable_description") or None,
        "owners": d.get("owners") or [],
        "last_run": asdict(last) if last else None,
    }
    if with_tasks:
        out["tasks"] = [t["task_id"] for t in client.tasks(dag_id)]
    return out


def fetch_status(client: AirflowClient, dag_ids: list[str], s: Settings, *,
                 with_tasks: bool, workers: int = 8,
                 progress: Callable[[str], None] | None = None) -> dict[str, dict]:
    result: dict[str, dict] = {}
    total = len(dag_ids)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_one, client, d, s, with_tasks): d for d in dag_ids}
        for done, fut in enumerate(as_completed(futures), 1):
            dag_id = futures[fut]
            try:
                result[dag_id] = fut.result()
            except Exception as exc:  # isolate per-DAG failures
                result[dag_id] = {"error": str(exc)}
            if progress:
                progress(f"{done}/{total} {dag_id}")
    return result
