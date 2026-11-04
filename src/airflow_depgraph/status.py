"""Run-status overlay: last decisive run per DAG."""
from __future__ import annotations

import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone

from .client import BULK_PAGE, NONEMPTY_TI_STATES, AirflowClient, ApiError
from .presets import Settings

_EMPTY_STATES = {"skipped", "removed", None}
_FAILED_STATES = ("failed", "upstream_failed")


@dataclass
class LastRun:
    state: str
    kind: str | None
    run_after: str | None
    run_id: str
    failed_tasks: list[str]
    upstream_failed: int = 0
    upstream_failed_tasks: list[str] = field(default_factory=list)
    # {link node id: True (produced), False (not produced), None (unknown)} for the assets and
    # triggers the DAG feeds, filled by outlets.annotate for failed runs.
    outlets: dict[str, bool | None] | None = None
    # When a run newer than the decisive one that is still running started, if any: the DAG is
    # running again after that failure or success.
    running: str | None = None


def _failed_run(run_id: str, run_after: str | None, instances: list[dict],
                tests_task_regex: str) -> LastRun:
    failed = [ti["task_id"] for ti in instances if ti.get("state") == "failed"]
    upstream = [ti["task_id"] for ti in instances if ti.get("state") == "upstream_failed"]
    only_tests = bool(failed) and all(
        re.search(tests_task_regex, t, re.IGNORECASE) for t in failed)
    return LastRun("failed", "tests" if only_tests else "pipeline",
                   run_after, run_id, failed, len(upstream), upstream)


def _success(run: dict, running: str | None = None) -> LastRun:
    return LastRun("success", None, run.get("run_after"), run["dag_run_id"], [], 0,
                   running=running)


def _running_before(runs: list[dict], index: int | None) -> str | None:
    """When the newest run still running among the runs newer than `runs[index]` started."""
    return next((r.get("run_after") for r in runs[:index] if r.get("state") == "running"), None)


def window_start(s: Settings, now: datetime | None = None) -> str:
    """The start of the status window, ISO 8601 UTC."""
    return ((now or datetime.now(timezone.utc)) - timedelta(days=s.status_lookback_days)
            ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _redone(failed_tasks: list[str], ran: set[str]) -> bool:
    """Whether every task that failed ran again (`ran`: the task ids in a non-empty state in the
    successes since). A run that took a freshness gate's skip path ends green with the failed
    work still undone, so such a success does not fix the failure. A task gone from the DAG
    never shows up again; its failure stands until the failed run leaves the lookback."""
    return all(t in ran for t in failed_tasks)


def classify_runs(runs: list[dict], task_instances: Callable[[str], list[dict]],
                  tests_task_regex: str) -> LastRun | None:
    """Return the newest decisive run (runs are newest first), or None.

    A success in which some task ran is decisive, unless an older run of the window failed and
    none of the successes since redid the tasks that failed there: then the DAG is still failing
    and the failed run is the answer. Task instances are read for the newest failed run and for
    the successes newer than it, newest first, stopping once the failed tasks are seen again."""
    failed_at = next((i for i, r in enumerate(runs) if r.get("state") == "failed"), None)
    last: LastRun | None = None
    if failed_at is not None:
        run = runs[failed_at]
        last = _failed_run(run["dag_run_id"], run.get("run_after"),
                           task_instances(run["dag_run_id"]), tests_task_regex)
        last.running = _running_before(runs, failed_at)
    first: tuple[int, dict] | None = None   # the newest success in which some task ran
    ran: set[str] = set()
    for i, run in enumerate(runs[:failed_at]):
        if run.get("state") != "success":
            continue
        did = {ti["task_id"] for ti in task_instances(run["dag_run_id"])
               if ti.get("state") not in _EMPTY_STATES}
        if not did:
            continue  # empty success: nothing really ran
        first = first or (i, run)
        if last is None:
            break
        ran |= did
        if _redone(last.failed_tasks, ran):
            last = None
            break
    if last is not None:
        return last
    if first is None:
        return None
    return _success(first[1], _running_before(runs, first[0]))


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
    runs = client.dag_runs_since(dag_id, window_start(s), s.status_runs_lookback)
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


def _runs_by_dag(dag_ids: list[str], runs: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {d: [] for d in dag_ids}
    for run in sorted(runs, key=lambda r: r.get("run_after") or "", reverse=True):
        bucket = grouped.get(run.get("dag_id"))
        if bucket is not None:
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
                    decided[d].running = _running_before(runs[d], cursor[d])
                    pending.discard(d)
            recovering: dict[str, tuple[dict, dict, list[dict]]] = {}
            if succeeded:
                # Decisive when some task is not skipped, removed or none: the same rule as
                # classify_runs, asked with the non-empty task states.
                checks = {pool.submit(client.has_nonempty_task, d, run["dag_run_id"]): d
                          for d, run in succeeded.items()}
                for fut in as_completed(checks):
                    d = checks[fut]
                    run, rs, i = succeeded[d], runs[d], cursor[d]
                    if not fut.result():
                        cursor[d] += 1
                        continue
                    j = next((k for k in range(i + 1, len(rs)) if rs[k].get("state") == "failed"),
                             None)
                    if j is None:
                        decided[d] = _success(run, _running_before(rs, i))
                        pending.discard(d)
                    else:   # a failure in the window: fixed only if its tasks ran since
                        recovering[d] = (run, rs[j], [r for r in rs[i:j]
                                                      if r.get("state") == "success"])
            if recovering:
                for d, last in _recovered(client, recovering, tests_task_regex).items():
                    run, failed_run, _ = recovering[d]
                    rs = runs[d]
                    if last is None:
                        decided[d] = _success(run, _running_before(rs, rs.index(run)))
                    else:
                        last.running = _running_before(rs, rs.index(failed_run))
                        decided[d] = last
                    pending.discard(d)
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    return decided


def _recovered(client: AirflowClient, recovering: dict[str, tuple[dict, dict, list[dict]]],
               tests_task_regex: str) -> dict[str, LastRun | None]:
    """For each DAG with a success newer than a failed run: None when the successes since redid
    the tasks that failed, else the failed run. One call reads the failed runs' task instances;
    the successes are then asked for those task ids newest first, a page of run ids at a time
    shared by the DAGs still undecided, so a DAG fixed by its newest success costs one call
    while a long trail of skip-path successes is walked to its end."""
    dags = list(recovering)
    failed_tis: dict[tuple, list[dict]] = {}
    for ti in client.bulk_task_instances(
            dags, [f["dag_run_id"] for _, f, _ in recovering.values()], _FAILED_STATES):
        failed_tis.setdefault((ti.get("dag_id"), ti.get("dag_run_id")), []).append(ti)
    out: dict[str, LastRun | None] = {}
    last: dict[str, LastRun] = {}
    for d, (_, failed_run, _) in recovering.items():
        last[d] = _failed_run(failed_run["dag_run_id"], failed_run.get("run_after"),
                              failed_tis.get((d, failed_run["dag_run_id"]), []), tests_task_regex)
    pos = {d: 0 for d in dags}
    ran: dict[str, set[str]] = {d: set() for d in dags}
    open_ = [d for d in dags if recovering[d][2]]
    out.update({d: last[d] for d in dags if not recovering[d][2]})
    while open_:
        per = max(1, BULK_PAGE // len(open_))
        page = {d: [r["dag_run_id"] for r in recovering[d][2][pos[d]:pos[d] + per]] for d in open_}
        wanted = sorted({t for d in open_ for t in last[d].failed_tasks})
        # The listing filters by DAG ids and run ids separately, not by pairs: scheduled DAGs
        # share run ids, so another DAG's success run id can bring back this DAG's failed run.
        for ti in client.bulk_task_instances(
                open_, [r for ids in page.values() for r in ids], NONEMPTY_TI_STATES, wanted):
            if ti.get("dag_run_id") in page.get(ti.get("dag_id"), ()):
                ran[ti["dag_id"]].add(ti["task_id"])
        for d in list(open_):
            pos[d] += len(page[d])
            if _redone(last[d].failed_tasks, ran[d]):
                out[d] = None
            elif pos[d] >= len(recovering[d][2]):
                out[d] = last[d]
            else:
                continue
            open_.remove(d)
    return out


def _bulk_outcomes(client: AirflowClient, dag_ids: list[str], s: Settings,
                   workers: int) -> tuple[dict[str, LastRun | None], list[str]]:
    """Answers reached in bulk, and the DAGs that still need a per-DAG read.

    One listing of every run of the window. A DAG with at least `status_runs_lookback` runs in
    it is settled by it, decisive run or not: the per-DAG path would read the same runs. A DAG
    with fewer runs in the window may have older ones that count, and is read per DAG."""
    runs = _runs_by_dag(dag_ids, client.bulk_dag_runs(dag_ids, window_start(s)))
    decided = _decide_bulk(client, {d: rs for d, rs in runs.items() if rs},
                           s.tests_task_regex, workers)
    final: dict[str, LastRun | None] = {}
    for d in dag_ids:
        if d in decided:
            final[d] = decided[d]
        elif len(runs[d]) >= s.status_runs_lookback:
            final[d] = None
    return final, [d for d in dag_ids if d not in final]


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
