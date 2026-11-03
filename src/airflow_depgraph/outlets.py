"""Which outlets a failed run produced: the assets and triggers the DAG feeds.

A failed run blocks a consumer only when the outlet the consumer waits on was not produced. An
asset event is sent by the task that has the asset as an outlet, a trigger by its
TriggerDagRunOperator task; a failure elsewhere in the DAG (a dbt test after the dbt run, a
notification) leaves them produced. The API does not say which task produces an asset for every
user (`/api/v2/assets` may be forbidden), so the task is found by convention: the asset URI's
path (`x-my_dag://dbt-run-orders` -> `dbt-run-orders`) names the task, bare or as the last
dotted segment (`dbt_tasks.dbt-run-orders`); failing that, the task whose last segment contains
the path's words in order (`dbt-musement-finance-cv` -> `dbt_tasks.dbt-run-musement-finance-cv`),
test tasks left out, the closest one when several fit and no tie. A trigger node carries its
task id. An asset whose task did not run to success (failed, upstream_failed) was not produced;
a trigger whose task was upstream_failed never fired, one that failed is unknown (with
`wait_for_completion` the task fails when the DAG it fired fails). An outlet whose task cannot
be found is unknown. Snapshot: `dags[id].last_run.outlets = {link node id: True|False|None}`.

A tests-only failure that left an outlet unproduced is a pipeline failure for the page and the
reports: the tests blocked data. The kind is rewritten to "pipeline" here.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from .snapshot import Snapshot

TESTS_TASK_REGEX = r"(^|[._-])tests?([._-]|$)"       # Settings.tests_task_regex default


def _words(s: str) -> list[str]:
    return [w for w in re.split(r"[^a-z0-9]+", s.lower()) if w]


def _in_order(needle: list[str], hay: list[str]) -> bool:
    i = 0
    for w in hay:
        if i < len(needle) and w == needle[i]:
            i += 1
    return i == len(needle)


def producing_task(uri: str, task_ids: Iterable[str],
                   tests_task_regex: str = TESTS_TASK_REGEX) -> str | None:
    """The task id that produces the asset `uri`, by convention, or None."""
    path = uri.split("://", 1)[1] if "://" in uri else uri
    path = path.strip("/")
    if not path:
        return None
    ids = list(task_ids)
    for t in ids:
        if t == path or t == uri:
            return t
    for t in ids:
        if t.endswith("." + path):
            return t
    words = _words(path)
    if not words:
        return None
    fits = [t for t in ids if not re.search(tests_task_regex, t, re.IGNORECASE)
            and _in_order(words, _words(t.rsplit(".", 1)[-1]))]
    if not fits:
        return None
    fits.sort(key=lambda t: len(_words(t.rsplit(".", 1)[-1])))
    if len(fits) > 1 and len(_words(fits[0])) == len(_words(fits[1])):
        return None                                          # a tie: no guess
    return fits[0]


def _trigger_task(node_id: str) -> str | None:
    parts = node_id.split(":", 3)          # trigger:<source dag>:<target dag>:<task id>
    return parts[3] if len(parts) == 4 and parts[0] == "trigger" else None


def outlet_states(snap: Snapshot, dag_id: str, task_ids: list[str] | None,
                  tests_task_regex: str = TESTS_TASK_REGEX) -> dict[str, bool | None]:
    """{link node id: produced?} for the assets and triggers `dag_id` feeds in its last run."""
    run = (snap.dags or {}).get(dag_id, {}).get("last_run") or {}
    failed = set(run.get("failed_tasks") or [])
    upstream = set(run.get("upstream_failed_tasks") or [])
    by_id = {n["id"]: n for n in snap.nodes}
    out: dict[str, bool | None] = {}
    for e in snap.edges:
        if e["source_id"] != "dag:" + dag_id:
            continue
        node = by_id.get(e["target_id"])
        if not node or node["type"] not in ("asset", "trigger"):
            continue
        if node["type"] == "trigger":
            task = _trigger_task(node["id"])
            if task is None or (task_ids is not None and task not in task_ids):
                out[node["id"]] = None
            else:
                out[node["id"]] = None if task in failed else task not in upstream
        else:
            task = producing_task(node["label"], task_ids or [], tests_task_regex)
            out[node["id"]] = None if task is None else task not in (failed | upstream)
    return out


def failed_dag_ids(snap: Snapshot) -> list[str]:
    return [d for d, v in (snap.dags or {}).items()
            if isinstance(v, dict) and (v.get("last_run") or {}).get("state") == "failed"]


def annotate(snap: Snapshot, tasks_by_dag: dict[str, list[str] | None],
             tests_task_regex: str = TESTS_TASK_REGEX) -> None:
    """Fill `last_run.outlets` of every failed DAG and turn a tests-only failure that blocked an
    outlet into a pipeline failure. `tasks_by_dag`: task ids per DAG, None when unreadable."""
    for dag_id in failed_dag_ids(snap):
        run = snap.dags[dag_id]["last_run"]
        run["outlets"] = outlet_states(snap, dag_id, tasks_by_dag.get(dag_id), tests_task_regex)
        if run.get("kind") == "tests" and any(v is False for v in run["outlets"].values()):
            run["kind"] = "pipeline"
