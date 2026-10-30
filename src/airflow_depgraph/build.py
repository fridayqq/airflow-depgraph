"""The library entry point: a snapshot or the finished page from a live Airflow in one call.

    from airflow_depgraph import Settings, build_page

    html = build_page(Settings(url="https://airflow.example.com", with_status=True),
                      user="svc-user", password=secret)

`build_page` logs in (a token wins over a user and password, as in the CLI), fetches the graph,
the DAG catalog, the run status when `with_status` is set and the inferred sensor links when
`infer_sensors` is set, then renders the self-contained HTML page. Progress and warnings go to
the `log` callback, which is silent by default; the CLI passes one that prints to stderr.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping

from . import snapshot as snapmod
from .auth import resolve_password, resolve_user
from .client import AirflowClient, ApiError
from .presets import Settings
from .render import render
from .sensors import fetch_tasks, infer_sensor_edges
from .snapshot import Snapshot, SnapshotError
from .status import fetch_status

Log = Callable[[str], None]


def _quiet(message: str) -> None:
    pass


def connect(s: Settings, *, token: str | None = None, user: str | None = None,
            password: str | None = None, env: Mapping[str, str] | None = None) -> AirflowClient:
    """A logged-in client. A token (argument, then settings) skips the login; otherwise the user
    and password come from the arguments, then from the settings and the environment
    (`user_env`, `password_env`, `password_cmd`)."""
    if not s.url:
        raise ValueError("no Airflow URL in the settings")
    env = os.environ if env is None else env
    token = token or s.token
    if token:
        return AirflowClient(s.url, token=token, timeout=s.timeout)
    user = user or resolve_user(s, env)
    if password is None:
        password = resolve_password(s, env, stdin_password=False, stdin=None, isatty=False)
    return AirflowClient.login(s.url, user, password, timeout=s.timeout)


def _live_snapshot(client: AirflowClient, s: Settings, node_id: str | None, log: Log) -> Snapshot:
    raw = client.dependencies(node_id)
    version = client.version()
    try:
        nodes, edges = snapmod.validate_payload(raw, version)
    except SnapshotError as exc:
        raise SnapshotError(f"{exc} (Airflow {version or 'version unknown'})") from None
    snap = snapmod.new(nodes, edges, s.url, version)
    if node_id:
        note = " and no sensor inference" if s.infer_sensors else ""
        log(f"note: --node-id fetches a subgraph: no DAG catalog{note}")
        return snap
    try:
        snap.catalog = snapmod.build_catalog(client.list_dags())
    except ApiError as exc:
        log(f"warning: cannot list DAGs ({exc}); no tag/paused/unlinked data for the page")
    return snap


def _fetch_dags(client: AirflowClient, snap: Snapshot, s: Settings, log: Log,
                tasks_by_dag: dict[str, list[dict] | None] | None = None) -> dict | None:
    ids = snap.graph.dag_ids
    if s.with_status:
        def progress(msg: str) -> None:
            log(f"fetching status {msg.split()[0]}")
        result = fetch_status(client, ids, s, with_tasks=s.with_tasks, progress=progress,
                              catalog=snap.catalog, workers=s.workers)
        errors = [result[d]["error"] for d in ids if "error" in result.get(d, {})]
        if errors:
            log(f"warning: status unavailable for {len(errors)} of {len(ids)} DAGs "
                f"(first error: {errors[0]})")
        if s.with_tasks:
            no_tasks = [d for d in ids if "error" not in result.get(d, {})
                        and "tasks" not in result.get(d, {})]
            if no_tasks:
                log(f"warning: task list unavailable for {len(no_tasks)} DAGs")
        return result
    if s.with_tasks:
        tasks_by_dag = dict(tasks_by_dag or {})
        missing = [d for d in ids if d not in tasks_by_dag]
        if missing:
            tasks_by_dag.update(fetch_tasks(client, missing, workers=s.workers))
        unreadable = [d for d in ids if tasks_by_dag.get(d) is None]
        if unreadable:
            log(f"warning: task list unavailable for {len(unreadable)} DAGs")
        return {d: ({"tasks": [t["task_id"] for t in tasks_by_dag[d]]}
                    if tasks_by_dag.get(d) is not None else {})
                for d in ids}
    return None


def _infer(client: AirflowClient, snap: Snapshot, s: Settings,
           log: Log) -> tuple[list[dict], dict[str, list[dict] | None]]:
    """Sensors of every catalog DAG (graph DAGs without a catalog) that wait on another DAG.
    Also returns the task lists it read, so --with-tasks can reuse them."""
    if snap.catalog is not None:
        ids = sorted(d for d, c in snap.catalog.items() if not c.get("is_stale"))
    else:
        ids = snap.graph_for(inferred=False).dag_ids
    log(f"inferring sensors: reading the tasks of {len(ids)} DAGs")
    tasks = fetch_tasks(client, ids, workers=s.workers)

    def progress(msg: str) -> None:
        log(f"inferring sensors {msg.split()[0]}")
    return infer_sensor_edges(client, ids, tasks, workers=s.workers, progress=progress), tasks


def fetch_snapshot(client: AirflowClient, s: Settings, *, node_id: str | None = None,
                   log: Log = _quiet) -> Snapshot:
    """The graph plus, unless `node_id` narrows it to a subgraph, the DAG catalog, the inferred
    sensor links (`infer_sensors`) and the run status or task lists (`with_status`,
    `with_tasks`)."""
    snap = _live_snapshot(client, s, node_id, log)
    tasks = None
    if s.infer_sensors and not node_id:
        snap.inferred, tasks = _infer(client, snap, s, log)
    snap.dags = _fetch_dags(client, snap, s, log, tasks)
    return snap


def build_page(s: Settings, *, client: AirflowClient | None = None, token: str | None = None,
               user: str | None = None, password: str | None = None,
               env: Mapping[str, str] | None = None, log: Log = _quiet) -> str:
    """The rendered HTML page for the Airflow in `s.url`. Pass a `client` to reuse a login;
    otherwise see `connect` for how the credentials are found."""
    client = client or connect(s, token=token, user=user, password=password, env=env)
    return render(fetch_snapshot(client, s, log=log), s)
