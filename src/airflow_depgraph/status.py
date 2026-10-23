"""Run-status overlay: last decisive run per DAG."""
from __future__ import annotations

import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

from .client import AirflowClient, ApiError
from .presets import Settings

_EMPTY_STATES = {"skipped", "removed", None}
# Run listings are tried over these windows in turn; the short one is cheap, and only DAGs with too
# few runs in it are listed over the long one. Busy DAGs flood a long listing otherwise.
BULK_WINDOWS_DAYS = (3, 14)
_FAILED_STATES = ("failed", "upstream_failed")


@dataclass
class LastRun:
    state: str
    kind: str | None
    run_after: str | None
    run_id: str
    failed_tasks: list[str]
    upstream_failed: int = 0


def _failed_run(run_id: str, run_after: str | None, instances: list[dict],
                tests_task_regex: str) -> LastRun:
    failed = [ti["task_id"] for ti in instances if ti.get("state") == "failed"]
    upstream = sum(1 for ti in instances if ti.get("state") == "upstream_failed")
    only_tests = bool(failed) and all(
        re.search(tests_task_regex, t, re.IGNORECASE) for t in failed)
    return LastRun("failed", "tests" if only_tests else "pipeline",
                   run_after, run_id, failed, upstream)


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
            return LastRun("success", None, run.get("run_after"), run_id, [], 0)
        if state == "failed":
            return _failed_run(run_id, run.get("run_after"), task_instances(run_id),
                               tests_task_regex)
    return None


def _facts(client: AirflowClient, dag_id: str, catalog: dict[str, dict] | None) -> dict:
    d = (catalog or {}).get(dag_id)
    if d is None:
        d = client.dag(dag_id)
        d = {"is_paused": d.get("is_paused"),
             "schedule": d.get("timetable_summary") or d.get("timetable_description"),
             "owners": d.get("owners")}
    return d


def _summary(facts: dict, last: LastRun | None) -> dict:
    return {
        "is_paused": bool(facts.get("is_paused")),
        "schedule": facts.get("schedule") or None,
        "owners": facts.get("owners") or [],
        "last_run": asdict(last) if last else None,
    }


def _one(client: AirflowClient, dag_id: str, s: Settings, with_tasks: bool,
         catalog: dict[str, dict] | None = None) -> dict:
    d = _facts(client, dag_id, catalog)
    runs = client.dag_runs(dag_id, s.status_runs_lookback)
    last = classify_runs(runs, lambda rid: client.task_instances(dag_id, rid),
                         s.tests_task_regex)
    out = _summary(d, last)
    if with_tasks:
        try:
            out["tasks"] = [t["task_id"] for t in client.tasks(dag_id)]
        except Exception as exc:  # a task-list failure keeps the status, without tasks
            if isinstance(exc, ApiError) and exc.status == 401:
                raise
    return out


def _per_dag(client: AirflowClient, dag_ids: list[str], s: Settings, *, with_tasks: bool,
             workers: int, progress: Callable[[str], None] | None,
             catalog: dict[str, dict] | None) -> dict[str, dict]:
    result: dict[str, dict] = {}
    total = len(dag_ids)
    pool = ThreadPoolExecutor(max_workers=workers)
    try:
        futures = {pool.submit(_one, client, d, s, with_tasks, catalog): d for d in dag_ids}
        for done, fut in enumerate(as_completed(futures), 1):
            dag_id = futures[fut]
            try:
                result[dag_id] = fut.result()
            except ApiError as exc:
                if exc.status == 401:   # expired token: every other call would fail too
                    raise
                result[dag_id] = {"error": str(exc)}
            except Exception as exc:  # isolate per-DAG failures
                result[dag_id] = {"error": str(exc)}
            if progress:
                progress(f"{done}/{total} {dag_id}")
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    return result


def _runs_by_dag(dag_ids: list[str], runs: list[dict], lookback: int) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {d: [] for d in dag_ids}
    for run in sorted(runs, key=lambda r: r.get("run_after") or "", reverse=True):
        bucket = grouped.get(run.get("dag_id"))
        if bucket is not None and len(bucket) < lookback:
            bucket.append(run)
    return grouped


def _decide_bulk(client: AirflowClient, runs: dict[str, list[dict]], tests_task_regex: str,
                 workers: int) -> dict[str, LastRun]:
    """The classify_runs rule for every DAG with runs given. A round asks for the task instances
    of the failed candidates in one call and checks each successful candidate in parallel;
    undecided DAGs move to their next run. DAGs without a decisive run are left out."""
    decided: dict[str, LastRun] = {}
    cursor = {d: 0 for d in runs}
    pending = set(runs)
    pool = ThreadPoolExecutor(max_workers=workers)
    try:
        while pending:
            failed: dict[str, dict] = {}
            succeeded: dict[str, dict] = {}
            for d in sorted(pending):
                rs, i = runs[d], cursor[d]
                while i < len(rs) and rs[i].get("state") not in ("failed", "success"):
                    i += 1          # queued, running, ...: cannot decide
                cursor[d] = i
                if i == len(rs):
                    pending.discard(d)
                else:
                    (failed if rs[i]["state"] == "failed" else succeeded)[d] = rs[i]
            if failed:
                by_run: dict[tuple, list[dict]] = {}
                for ti in client.bulk_task_instances(
                        list(failed), [r["dag_run_id"] for r in failed.values()], _FAILED_STATES):
                    by_run.setdefault((ti.get("dag_id"), ti.get("dag_run_id")), []).append(ti)
                for d, run in failed.items():
                    decided[d] = _failed_run(run["dag_run_id"], run.get("run_after"),
                                             by_run.get((d, run["dag_run_id"]), []),
                                             tests_task_regex)
                    pending.discard(d)
            if succeeded:
                # Decisive when some task is not skipped, removed or none: the same rule as
                # classify_runs, asked with the non-empty task states.
                checks = {pool.submit(client.has_nonempty_task, d, run["dag_run_id"]): d
                          for d, run in succeeded.items()}
                for fut in as_completed(checks):
                    d = checks[fut]
                    run = succeeded[d]
                    if fut.result():
                        decided[d] = LastRun("success", None, run.get("run_after"),
                                             run["dag_run_id"], [], 0)
                        pending.discard(d)
                    else:
                        cursor[d] += 1
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    return decided


def _bulk_outcomes(client: AirflowClient, dag_ids: list[str], s: Settings,
                   workers: int) -> tuple[dict[str, LastRun | None], list[str]]:
    """Answers reached in bulk, and the DAGs that still need a per-DAG read.

    A DAG is settled when a decisive run is among its newest runs of a window, or when a window
    lists `status_runs_lookback` runs of it and none is decisive. The windows are prefixes of the
    same newest-first history, so the first decisive run found is the one the per-DAG path finds."""
    lookback = s.status_runs_lookback
    final: dict[str, LastRun | None] = {}
    pending = list(dag_ids)
    for days in BULK_WINDOWS_DAYS:
        if not pending:
            break
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        runs = _runs_by_dag(pending, client.bulk_dag_runs(pending, since, per_dag=lookback),
                            lookback)
        decided = _decide_bulk(client, {d: rs for d, rs in runs.items() if rs},
                               s.tests_task_regex, workers)
        for d in pending:
            if d in decided:
                final[d] = decided[d]
            elif len(runs[d]) == lookback:
                final[d] = None
        pending = [d for d in pending if d not in final]
    return final, pending


def fetch_status_bulk(client: AirflowClient, dag_ids: list[str], s: Settings, *,
                      workers: int = 8, catalog: dict[str, dict] | None = None,
                      progress: Callable[[str], None] | None = None) -> dict[str, dict] | None:
    """The same result as the per-DAG path, in a few calls for all DAGs. Any error from a bulk
    call other than a 401 (for example a 404/405 on an older Airflow) returns None, and the
    caller then uses the per-DAG path. A 401 raises. DAGs the bulk listing cannot settle are
    read per DAG."""
    try:
        final, pending = _bulk_outcomes(client, dag_ids, s, workers)
    except ApiError as exc:
        if exc.status == 401:
            raise
        return None
    result: dict[str, dict] = {}
    settled = [d for d in dag_ids if d in final]
    for done, dag_id in enumerate(settled, 1):
        try:
            result[dag_id] = _summary(_facts(client, dag_id, catalog), final[dag_id])
        except ApiError as exc:
            if exc.status == 401:
                raise
            result[dag_id] = {"error": str(exc)}
        except Exception as exc:  # isolate per-DAG failures
            result[dag_id] = {"error": str(exc)}
        if progress:
            progress(f"{done}/{len(settled)} {dag_id}")
    if pending:
        result.update(_per_dag(client, pending, s, with_tasks=False, workers=workers,
                               progress=progress, catalog=catalog))
    return result


def fetch_status(client: AirflowClient, dag_ids: list[str], s: Settings, *,
                 with_tasks: bool, workers: int = 8,
                 progress: Callable[[str], None] | None = None,
                 catalog: dict[str, dict] | None = None) -> dict[str, dict]:
    """catalog: DAG facts already listed (snapshot `catalog`); saves one GET per DAG.
    Without task lists the bulk endpoints are tried first."""
    if not with_tasks and s.bulk_status:
        bulk = fetch_status_bulk(client, dag_ids, s, workers=workers, catalog=catalog,
                                 progress=progress)
        if bulk is not None:
            return bulk
    return _per_dag(client, dag_ids, s, with_tasks=with_tasks, workers=workers,
                    progress=progress, catalog=catalog)
