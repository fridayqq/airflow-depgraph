"""Command line interface: argparse plus thin command functions."""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from importlib import resources
from pathlib import Path

from airflow_depgraph import snapshot as snapmod
from airflow_depgraph.auth import CredentialsError, resolve_password, resolve_user
from airflow_depgraph.cli_format import TITLES, format_hits, hits_json, stamp
from airflow_depgraph.client import AirflowClient, ApiError
from airflow_depgraph.graph import Hit, walk
from airflow_depgraph.presets import PresetError, Settings, list_presets, resolve
from airflow_depgraph.render import render
from airflow_depgraph.snapshot import Snapshot, SnapshotError
from airflow_depgraph.status import fetch_status

DEFAULT_SNAPSHOT = "airflow-depgraph.snapshot.json"
DEFAULT_HTML = "airflow-depgraph.html"


class CliError(Exception):
    pass


# ---------- shared plumbing ----------


def _settings(args: argparse.Namespace) -> Settings:
    cli = {k: getattr(args, k, None)
           for k in ("url", "user", "token", "with_status", "with_tasks", "title", "highlight")}
    return resolve(cli, os.environ, args.preset)


def _client(s: Settings, args: argparse.Namespace) -> AirflowClient:
    if not s.url:
        raise CliError("no Airflow URL: pass --url, set AIRFLOW_DEPGRAPH_URL or use a preset")
    if s.token:
        return AirflowClient(s.url, token=s.token)
    user = resolve_user(s, os.environ)
    password = resolve_password(s, os.environ, stdin_password=args.password_stdin,
                                stdin=sys.stdin, isatty=sys.stdin.isatty())
    return AirflowClient.login(s.url, user, password)


def _live_snapshot(client: AirflowClient, s: Settings, node_id: str | None) -> Snapshot:
    nodes, edges = snapmod.validate_payload(client.dependencies(node_id))
    return snapmod.new(nodes, edges, s.url, client.version())


def _walk(snap: Snapshot, dag: str, direction: str, depth: int) -> list[Hit]:
    try:
        return walk(snap.graph, dag, direction, depth)
    except KeyError:
        raise CliError(f"dag '{dag}' is not in the dependency graph "
                       f"({len(snap.graph.dag_ids)} DAGs known)") from None


def _fetch_dags(client: AirflowClient, snap: Snapshot, s: Settings) -> dict | None:
    ids = snap.graph.dag_ids
    if s.with_status:
        def progress(msg: str) -> None:
            print(f"fetching status {msg.split()[0]}", file=sys.stderr)
        return fetch_status(client, ids, s, with_tasks=s.with_tasks, progress=progress)
    if s.with_tasks:
        with ThreadPoolExecutor(max_workers=8) as pool:
            tasks = pool.map(lambda d: [t["task_id"] for t in client.tasks(d)], ids)
            return {d: {"tasks": t} for d, t in zip(ids, tasks)}
    return None


def _fetch_snapshot(args: argparse.Namespace, s: Settings) -> Snapshot:
    client = _client(s, args)
    snap = _live_snapshot(client, s, args.node_id)
    snap.dags = _fetch_dags(client, snap, s)
    return snap


# ---------- commands ----------


def cmd_fetch(args: argparse.Namespace) -> int:
    s = _settings(args)
    snap = _fetch_snapshot(args, s)
    snapmod.dump(snap, Path(args.output))
    print(f"wrote {args.output} ({len(snap.graph.dag_ids)} DAGs)")
    return 0


def _load_override(path: str | None) -> dict | None:
    if not path:
        return None
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotError(f"cannot read status file {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise SnapshotError(f"status file {path} must be a JSON object")
    return data


def _check_html_output(output: str) -> None:
    if Path(output).suffix.lower() == ".json":
        raise CliError(f"output must be an .html file (got {output})")


def _write_html(snap: Snapshot, s: Settings, args: argparse.Namespace) -> None:
    try:
        page = render(snap, s, _load_override(args.status))
    except ValueError as exc:
        if isinstance(exc, SnapshotError):
            raise
        raise CliError(str(exc)) from exc
    Path(args.output).write_text(page, encoding="utf-8")
    print(f"wrote {args.output}")


def cmd_render(args: argparse.Namespace) -> int:
    _check_html_output(args.output)
    _write_html(snapmod.load(Path(args.snapshot)), _settings(args), args)
    return 0


def cmd_graph(args: argparse.Namespace) -> int:
    _check_html_output(args.output)
    s = _settings(args)
    snap = _fetch_snapshot(args, s)
    _write_html(snap, s, args)
    json_path = Path(args.output).with_suffix(".json")
    snapmod.dump(snap, json_path)
    print(f"wrote {json_path}")
    return 0


def _compile(pattern: str) -> re.Pattern:
    try:
        return re.compile(pattern)
    except re.error as exc:
        raise CliError(f"invalid --tasks-matching regex {pattern!r}: {exc}") from exc


def _dag_info(client: AirflowClient | None, snap: Snapshot, dag_id: str) -> dict:
    if client is not None:
        d = client.dag(dag_id)
        return {"is_paused": bool(d.get("is_paused")),
                "schedule": d.get("timetable_summary") or d.get("timetable_description") or None,
                "owners": d.get("owners") or []}
    d = (snap.dags or {}).get(dag_id) or {}
    return {"is_paused": d.get("is_paused"), "schedule": d.get("schedule"),
            "owners": d.get("owners")}


def _task_ids(client: AirflowClient | None, snap: Snapshot, dag_id: str) -> list[str] | None:
    if client is not None:
        return [t["task_id"] for t in client.tasks(dag_id)]
    return ((snap.dags or {}).get(dag_id) or {}).get("tasks")


def _match_tasks(client, snap, hits, regex, info) -> list[Hit]:
    kept = []
    for h in hits:
        tasks = _task_ids(client, snap, h.dag_id)
        if tasks is None:
            raise CliError("snapshot has no task lists; re-run fetch with --with-tasks")
        found = [t for t in tasks if regex.search(t)]
        info[h.dag_id]["matching_tasks"] = found
        if found:
            kept.append(h)
    return kept


def _query(args: argparse.Namespace, direction: str) -> int:
    s = _settings(args)
    pattern = args.tasks_matching
    if pattern == "":
        raise CliError("--tasks-matching needs a non-empty REGEX")
    if pattern is True:
        pattern = s.task_query_regex
        if not pattern:
            raise CliError("--tasks-matching needs a REGEX or task_query_regex in the preset")
    regex = _compile(pattern) if pattern else None
    client = None
    if args.snapshot:
        snap = snapmod.load(Path(args.snapshot))
    else:
        client = _client(s, args)
        snap = _live_snapshot(client, s, None)
    hits = _walk(snap, args.dag, direction, args.depth)
    info = {h.dag_id: _dag_info(client, snap, h.dag_id) for h in hits}
    if regex:
        hits = _match_tasks(client, snap, hits, regex, info)
    if args.json:
        print(hits_json(args.dag, direction, args.depth, hits, info, regex is not None))
    else:
        header = f"{TITLES[direction]} of {args.dag} — {len(hits)} (depth ≤ {args.depth}, {stamp(snap if args.snapshot else None)})"
        print(format_hits(hits, info, snap.source or s.url, direction, header))
    return 0


def cmd_tasks(args: argparse.Namespace) -> int:
    s = _settings(args)
    client = None
    if args.snapshot:
        snap = snapmod.load(Path(args.snapshot))
    else:
        client = _client(s, args)
        snap = snapmod.new([], [], s.url, None)
    tasks = _task_ids(client, snap, args.dag)
    if tasks is None:
        raise CliError("snapshot has no task list for this DAG; re-run fetch with --with-tasks")
    if args.regex:
        regex = _compile(args.regex)
        tasks = [t for t in tasks if regex.search(t)]
    print(json.dumps(tasks, indent=2) if args.json else "\n".join(tasks))
    return 0


def cmd_presets(args: argparse.Namespace) -> int:
    if args.example:
        print((resources.files("airflow_depgraph") / "presets" / "example.toml").read_text(encoding="utf-8"), end="")
        return 0
    for name, path in list_presets(os.environ):
        print(f"{name}  {path}")
    if args.preset:
        print()
        s = _settings(args)
        for f in dataclasses.fields(Settings):
            value = getattr(s, f.name)
            if f.name == "token" or (f.name == "user" and not value):
                continue
            print(f"{f.name} = {'(unset)' if value is None else value}")
    return 0


# ---------- argparse ----------


def _parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--preset", help="preset name or path to a .toml file")
    common.add_argument("--url", help="Airflow base URL")
    common.add_argument("--user", help="Airflow user name")
    common.add_argument("--password-stdin", action="store_true", help="read the password from stdin")
    common.add_argument("--token", help="bearer token (skips login)")

    fetch_opts = argparse.ArgumentParser(add_help=False)
    fetch_opts.add_argument("--with-status", action="store_true", default=None)
    fetch_opts.add_argument("--with-tasks", action="store_true", default=None)
    fetch_opts.add_argument("--node-id", help="fetch only the subgraph around this node")

    render_opts = argparse.ArgumentParser(add_help=False)
    render_opts.add_argument("--title")
    render_opts.add_argument("--highlight", help="regex of DAG ids to highlight")
    render_opts.add_argument("--status", metavar="FILE", help="JSON {dag_id: {kind, paused, date}}")

    p = argparse.ArgumentParser(prog="airflow-depgraph",
                                description="Airflow 3 cross-DAG dependency graph")
    sub = p.add_subparsers(dest="command", required=True)

    f = sub.add_parser("fetch", parents=[common, fetch_opts], help="save a snapshot")
    f.add_argument("-o", "--output", default=DEFAULT_SNAPSHOT)
    f.set_defaults(func=cmd_fetch)

    r = sub.add_parser("render", parents=[common, render_opts], help="render a snapshot to HTML")
    r.add_argument("snapshot")
    r.add_argument("-o", "--output", default=DEFAULT_HTML)
    r.set_defaults(func=cmd_render)

    g = sub.add_parser("graph", parents=[common, fetch_opts, render_opts],
                       help="fetch live and write HTML plus JSON")
    g.add_argument("-o", "--output", default=DEFAULT_HTML)
    g.set_defaults(func=cmd_graph)

    for name in ("downstream", "upstream"):
        q = sub.add_parser(name, parents=[common], help=f"list {name} DAGs")
        q.add_argument("dag")
        q.add_argument("snapshot", nargs="?", help="snapshot file; omit to query live")
        q.add_argument("--depth", type=int, default=5)
        q.add_argument("--json", action="store_true")
        q.add_argument("--tasks-matching", nargs="?", const=True, metavar="REGEX",
                       help="keep DAGs having a task matching REGEX (default: preset task_query_regex)")
        q.set_defaults(func=lambda a, d=name: _query(a, d))

    t = sub.add_parser("tasks", parents=[common], help="list the tasks of a DAG")
    t.add_argument("dag")
    t.add_argument("snapshot", nargs="?")
    t.add_argument("--regex")
    t.add_argument("--json", action="store_true")
    t.set_defaults(func=cmd_tasks)

    pr = sub.add_parser("presets", parents=[common], help="list presets")
    pr.add_argument("--example", action="store_true", help="print the example preset")
    pr.set_defaults(func=cmd_presets)
    return p


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        return args.func(args)
    except (SnapshotError, PresetError, CredentialsError, ApiError, CliError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
