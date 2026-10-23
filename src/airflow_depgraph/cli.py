"""Command line interface: argparse plus thin command functions."""

from __future__ import annotations

import argparse
import dataclasses
import difflib
import getpass
import json
import os
import re
import sys
from importlib import resources
from pathlib import Path

from airflow_depgraph import __version__, report, wizard
from airflow_depgraph import snapshot as snapmod
from airflow_depgraph.auth import CredentialsError, resolve_password, resolve_user
from airflow_depgraph.cli_format import TITLES, format_hits, format_tree, hits_json, stamp
from airflow_depgraph.client import AirflowClient, ApiError
from airflow_depgraph.graph import Hit, walk
from airflow_depgraph.labels import state_label
from airflow_depgraph.presets import PresetError, Settings, list_presets, resolve
from airflow_depgraph.render import render
from airflow_depgraph.sensors import fetch_tasks, infer_sensor_edges
from airflow_depgraph.snapshot import Snapshot, SnapshotError
from airflow_depgraph.status import fetch_status

DEFAULT_SNAPSHOT = "airflow-depgraph.snapshot.json"
DEFAULT_HTML = "airflow-depgraph.html"


class CliError(Exception):
    pass


class DagNotFound(CliError):
    pass


class UsageError(CliError):
    pass


# Exit codes (2 is argparse's usage error).
EXIT_OK, EXIT_ERROR, EXIT_USAGE = 0, 1, 2
EXIT_CREDENTIALS, EXIT_NETWORK, EXIT_DAG_NOT_FOUND, EXIT_INTERRUPTED = 3, 4, 5, 130
_AUTH_PATHS = ("/auth/token", "/ui/dependencies")


# ---------- shared plumbing ----------


def _settings(args: argparse.Namespace) -> Settings:
    cli = {k: getattr(args, k, None)
           for k in ("url", "user", "token", "with_status", "with_tasks", "title", "highlight",
                     "infer_sensors", "timeout", "workers", "bulk_status",
                     "user_env", "password_env", "password_cmd")}
    if getattr(args, "tags", None):
        cli["tags"] = tuple(args.tags)
    if getattr(args, "token_stdin", False):
        cli["token"] = sys.stdin.readline().strip()
        if not cli["token"]:
            raise CliError("--token-stdin: no token on the first line of stdin")
    elif getattr(args, "token", None):
        print("warning: --token is visible in shell history and ps; "
              "prefer AIRFLOW_DEPGRAPH_TOKEN or --token-stdin", file=sys.stderr)
    return resolve(cli, os.environ, args.preset)


def _client(s: Settings, args: argparse.Namespace) -> AirflowClient:
    if not s.url:
        raise CliError("no Airflow URL: pass --url, set AIRFLOW_DEPGRAPH_URL or use a preset")
    if s.token:
        return AirflowClient(s.url, token=s.token, timeout=s.timeout)
    user = resolve_user(s, os.environ)
    password = resolve_password(s, os.environ, stdin_password=args.password_stdin,
                                stdin=sys.stdin, isatty=sys.stdin.isatty())
    return AirflowClient.login(s.url, user, password, timeout=s.timeout)


def _live_snapshot(client: AirflowClient, s: Settings, node_id: str | None) -> Snapshot:
    raw = client.dependencies(node_id)
    version = client.version()
    try:
        nodes, edges = snapmod.validate_payload(raw, version)
    except SnapshotError as exc:
        raise SnapshotError(f"{exc} (Airflow {version or 'version unknown'})") from None
    snap = snapmod.new(nodes, edges, s.url, version)
    if node_id:
        note = " and no sensor inference" if s.infer_sensors else ""
        print(f"note: --node-id fetches a subgraph: no DAG catalog{note}", file=sys.stderr)
        return snap
    try:
        snap.catalog = snapmod.build_catalog(client.list_dags())
    except ApiError as exc:
        print(f"warning: cannot list DAGs ({exc}); no tag/paused/unlinked data for the page",
              file=sys.stderr)
    return snap


def _walk(snap: Snapshot, dag: str, direction: str, depth: int,
          inferred: bool = True, guards: bool = False) -> list[Hit] | None:
    """None when the DAG is not a node of the graph (see _unlinked)."""
    g = snap.graph if inferred and not guards else snap.graph_for(inferred, guards)
    if f"dag:{dag}" not in g.nodes:
        return None
    return walk(g, dag, direction, depth)


def _unlinked(snap: Snapshot, dag: str, direction: str, args: argparse.Namespace) -> int:
    """Explain why DAG is not in the traversal graph; exit 0 only for a real DAG without links."""
    inferred = not args.no_inferred
    if f"dag:{dag}" in snap.graph_for(inferred, guards=True).nodes \
            and f"dag:{dag}" not in snap.graph_for(inferred, guards=False).nodes:
        raise CliError(f"dag '{dag}' is only linked by deploy guards; add --include-guards")
    cat = (snap.catalog or {}).get(dag)
    if cat is not None and not cat.get("is_stale"):
        schedule = cat.get("schedule") or "—"
        if args.json:
            print(json.dumps({"dag": dag, "direction": direction, "depth": args.depth,
                              "dags": [], "note": "no explicit dependencies"},
                             indent=2, ensure_ascii=False))
        else:
            print(f"dag '{dag}' exists but has no explicit dependencies (schedule: {schedule})")
        return 0
    g = snap.graph_for(inferred, guards=args.include_guards)
    ids = sorted(set(g.dag_ids) | set((snap.catalog or {}).keys()))
    close = difflib.get_close_matches(dag, ids, n=3, cutoff=0.6)
    suffix = f" — did you mean: {', '.join(close)}?" if close else ""
    raise DagNotFound(f"dag '{dag}' is not in the dependency graph "
                   f"({len(g.dag_ids)} DAGs known){suffix}")


def _fetch_dags(client: AirflowClient, snap: Snapshot, s: Settings,
                tasks_by_dag: dict[str, list[dict] | None] | None = None) -> dict | None:
    ids = snap.graph.dag_ids
    if s.with_status:
        def progress(msg: str) -> None:
            print(f"fetching status {msg.split()[0]}", file=sys.stderr)
        result = fetch_status(client, ids, s, with_tasks=s.with_tasks, progress=progress,
                              catalog=snap.catalog, workers=s.workers)
        errors = [result[d]["error"] for d in ids if "error" in result.get(d, {})]
        if errors:
            print(f"warning: status unavailable for {len(errors)} of {len(ids)} DAGs "
                  f"(first error: {errors[0]})", file=sys.stderr)
        if s.with_tasks:
            no_tasks = [d for d in ids if "error" not in result.get(d, {})
                        and "tasks" not in result.get(d, {})]
            if no_tasks:
                print(f"warning: task list unavailable for {len(no_tasks)} DAGs", file=sys.stderr)
        return result
    if s.with_tasks:
        tasks_by_dag = dict(tasks_by_dag or {})
        missing = [d for d in ids if d not in tasks_by_dag]
        if missing:
            tasks_by_dag.update(fetch_tasks(client, missing, workers=s.workers))
        unreadable = [d for d in ids if tasks_by_dag.get(d) is None]
        if unreadable:
            print(f"warning: task list unavailable for {len(unreadable)} DAGs", file=sys.stderr)
        return {d: ({"tasks": [t["task_id"] for t in tasks_by_dag[d]]}
                    if tasks_by_dag.get(d) is not None else {})
                for d in ids}
    return None


def _infer(client: AirflowClient, snap: Snapshot,
           s: Settings) -> tuple[list[dict], dict[str, list[dict] | None]]:
    """Sensors of every catalog DAG (graph DAGs without a catalog) that wait on another DAG.
    Also returns the task lists it read, so --with-tasks can reuse them."""
    if snap.catalog is not None:
        ids = sorted(d for d, c in snap.catalog.items() if not c.get("is_stale"))
    else:
        ids = snap.graph_for(inferred=False).dag_ids
    print(f"inferring sensors: reading the tasks of {len(ids)} DAGs", file=sys.stderr)
    tasks = fetch_tasks(client, ids, workers=s.workers)

    def progress(msg: str) -> None:
        print(f"inferring sensors {msg.split()[0]}", file=sys.stderr)
    return infer_sensor_edges(client, ids, tasks, workers=s.workers, progress=progress), tasks


def _fetch_snapshot(args: argparse.Namespace, s: Settings) -> Snapshot:
    client = _client(s, args)
    snap = _live_snapshot(client, s, args.node_id)
    tasks = None
    if s.infer_sensors and not args.node_id:
        snap.inferred, tasks = _infer(client, snap, s)
    snap.dags = _fetch_dags(client, snap, s, tasks)
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


def _depth(value: str) -> int:
    try:
        n = int(value)
    except ValueError:
        n = 0
    if n < 1:
        raise argparse.ArgumentTypeError(f"must be an integer >= 1, got {value!r}")
    return n


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


REPORT_TITLES = {
    "critical": "Critical DAGs (most transitive downstream DAGs)",
    "unlinked": "Unlinked DAGs (no explicit or inferred links)",
    "unconsumed-assets": "Unconsumed assets (produced, never consumed)",
    "failing": "Failing DAGs (last decisive run failed, not paused)",
}


def _table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [max(len(str(c)) for c in col) for col in zip(headers, *rows, strict=True)]

    def fmt(cells) -> str:
        return "  ".join(str(c).ljust(w) for c, w in zip(cells, widths, strict=True)).rstrip()
    return "\n".join([fmt(headers)] + [fmt(r) for r in rows])


def _report_table(kind: str, rows: list[dict]) -> tuple[list[str], list[list[str]]]:
    def joined(values) -> str:
        return ", ".join(values) if values else "—"
    if kind == "critical":
        return (["rank", "dag_id", "downstream", "direct consumers", "state"],
                [[r["rank"], r["dag_id"], r["downstream"], r["direct_consumers"],
                  state_label({"state": r["state"], "kind": r["kind"]}) or "—"]
                 for r in rows])
    if kind == "unlinked":
        return (["dag_id", "schedule", "paused", "tags"],
                [[r["dag_id"], r["schedule"] or "—", "yes" if r["paused"] else "no", joined(r["tags"])]
                 for r in rows])
    if kind == "unconsumed-assets":
        return (["asset", "producers"], [[r["asset"], joined(r["producers"])] for r in rows])
    return (["dag_id", "kind", "failed tasks", "downstream", "cron fallback consumers"],
            [[r["dag_id"], r["kind"] or "—", joined(r["failed_tasks"]), r["downstream"],
              joined(r["stale_consumers"])] for r in rows])


def cmd_report(args: argparse.Namespace) -> int:
    s = _settings(args)
    snap = snapmod.load(Path(args.snapshot))
    tags = report.effective_tags(s, args.report_tags, args.all_tags)
    inferred = not args.no_inferred
    try:
        if args.kind == "critical":
            rows = report.critical(snap, s, tags=tags, inferred=inferred, top=args.top)
        elif args.kind == "unlinked":
            rows = report.unlinked(snap, s, tags=tags, inferred=inferred)
        elif args.kind == "unconsumed-assets":
            rows = report.unconsumed_assets(snap, s, tags=tags)
        else:
            rows = report.failing(snap, s, tags=tags, inferred=inferred)
    except report.ReportError as exc:
        raise CliError(str(exc)) from None
    if args.json:
        print(json.dumps(rows, indent=2, ensure_ascii=False))
        return 0
    print(f"{REPORT_TITLES[args.kind]} — {len(rows)} ({stamp(snap)})")
    if rows:
        headers, table = _report_table(args.kind, rows)
        print(_table(headers, table))
    return 0


def _compile(pattern: str) -> re.Pattern:
    try:
        return re.compile(pattern)
    except re.error as exc:
        raise CliError(f"invalid --tasks-matching regex {pattern!r}: {exc}") from exc


def _dag_info(client: AirflowClient | None, snap: Snapshot, dag_id: str) -> dict:
    """Schedule, pause and owners of a DAG, plus its last run status when the snapshot has it."""
    info = _dag_facts(client, snap, dag_id)
    info["last_run"] = None if client is not None else ((snap.dags or {}).get(dag_id) or {}).get("last_run")
    return info


def _dag_facts(client: AirflowClient | None, snap: Snapshot, dag_id: str) -> dict:
    cat = (snap.catalog or {}).get(dag_id) if client is not None else None
    if cat is not None:  # live: the catalog already holds what GET /dags/{id} would return
        return {"is_paused": cat.get("is_paused"), "schedule": cat.get("schedule"),
                "owners": cat.get("owners")}
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
    if args.tree and args.json:
        raise CliError("--tree and --json cannot be used together")
    s = _settings(args)
    if args.tasks_matching is not None and args.tasks_preset:
        raise CliError("use either --tasks-matching REGEX or --tasks-preset")
    pattern = args.tasks_matching
    if pattern == "":
        raise CliError("--tasks-matching needs a non-empty REGEX")
    if pattern and (pattern.endswith(".json") or Path(pattern).is_file()):
        raise CliError(f"--tasks-matching got '{pattern}', which looks like a snapshot file; "
                       "put the snapshot before the flag or pass a REGEX")
    if args.tasks_preset:
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
        if s.infer_sensors:
            snap.inferred, _ = _infer(client, snap, s)
    dag = args.dag.removeprefix("dag:")
    hits = _walk(snap, dag, direction, args.depth,
                 inferred=not args.no_inferred, guards=args.include_guards)
    if hits is None:
        return _unlinked(snap, dag, direction, args)
    info = {h.dag_id: _dag_info(client, snap, h.dag_id) for h in hits}
    if client is not None and hits:  # live: last run status of the hit DAGs only
        status = fetch_status(client, [h.dag_id for h in hits], s, with_tasks=False,
                              catalog=snap.catalog, workers=s.workers)
        for h in hits:
            info[h.dag_id]["last_run"] = (status.get(h.dag_id) or {}).get("last_run")
    if regex:
        hits = _match_tasks(client, snap, hits, regex, info)
    if args.json:
        print(hits_json(dag, direction, args.depth, hits, info, regex is not None))
    else:
        when = stamp(snap if args.snapshot else None)
        header = f"{TITLES[direction]} of {dag} — {len(hits)} (depth ≤ {args.depth}, {when})"
        if args.tree:
            print(header)
            print(format_tree(hits, info, dag, strip_prefix=s.asset_label_strip_prefix))
        else:
            print(format_hits(hits, info, snap.source or s.url, direction, header,
                              strip_prefix=s.asset_label_strip_prefix))
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
        example = resources.files("airflow_depgraph") / "presets" / "example.toml"
        print(example.read_text(encoding="utf-8"), end="")
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
            if isinstance(value, tuple):
                value = ", ".join(value) or None
            print(f"{f.name} = {'(unset)' if value is None else value}")
    return 0


def _init_given(args: argparse.Namespace) -> dict:
    return {k: getattr(args, k) for k in (
        "name", "url", "user_env", "password_env", "password_cmd", "tags", "strip_prefix",
        "highlight", "highlight_label", "title", "with_status", "infer_sensors",
        "tests_task_regex", "task_query_regex", "lookback", "timeout", "workers")}


def _tty() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


def cmd_init(args: argparse.Namespace) -> int:
    if args.json and not args.discover:
        raise UsageError("--json only works with --discover")
    if args.discover:
        return _init_discover(args)
    given = _init_given(args)
    try:
        wizard.validate_given(given)
    except ValueError as exc:
        raise UsageError(str(exc)) from None
    if not args.yes and not _tty():
        raise UsageError("init asks questions; run it in a terminal, "
                         "or pass --yes with flags (see --help)")
    target = wizard.target_dir(args.dir, os.environ)
    try:
        if args.yes:
            return _init_batch(args, given, target)
        color = sys.stdout.isatty() and not os.environ.get("NO_COLOR")
        prompter = wizard.Prompter(input_fn=input, getpass_fn=getpass.getpass,
                                   out=sys.stdout, color=color)
        return wizard.run_interactive(
            prompter, given, env=os.environ, target=target, client_cls=AirflowClient,
            user=args.user, token=_init_token(args), force=args.force, no_check=args.no_check)
    except (KeyboardInterrupt, EOFError):
        print("\ninterrupted, nothing written", file=sys.stderr)
        return EXIT_INTERRUPTED


def _init_token(args: argparse.Namespace) -> str | None:
    return _settings(args).token


def _init_discover(args: argparse.Namespace) -> int:
    if not args.url:
        raise UsageError("--discover needs --url")
    s = _settings(args)
    d = wizard.discover(_client(s, args), s)
    print(json.dumps(d.to_json(), indent=2, ensure_ascii=False) if args.json
          else wizard.format_report(d))
    return 0


def _init_batch(args: argparse.Namespace, given: dict, target: Path) -> int:
    if not args.name:
        raise UsageError("--yes needs a preset NAME")
    if not args.url:
        raise UsageError("--yes needs --url")
    try:
        name = wizard.validate_name(args.name)
        url = wizard.normalize_url(args.url)
    except ValueError as exc:
        raise UsageError(str(exc)) from None
    path = target / f"{name}.toml"
    if path.exists() and not args.force:
        raise FileExistsError(f"{path} already exists; pass --force to overwrite it")
    if not args.no_check:
        wizard.check_health(AirflowClient, url, given["timeout"] or Settings.timeout)
        args.url = url
        s = _settings(args)
        client = _client(s, args)
        if given["tags"]:
            known = {t for t, _ in wizard.tag_counts(client.list_dags())}
            for tag in given["tags"]:
                if tag not in known:
                    print(f"warning: no DAG has tag {tag}", file=sys.stderr)
    values = wizard.build_values(name, url, given)
    wizard.write_preset(path, wizard.render_preset(values), force=args.force)
    print(wizard.written_message(path, name, values, os.environ,
                                 wizard.in_lookup_dirs(target, os.environ)))
    return 0


# ---------- argparse ----------


def _parser() -> argparse.ArgumentParser:
    def connection(with_preset: bool) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(add_help=False)
        if with_preset:
            parser.add_argument("--preset", help="preset name or path to a .toml file")
        parser.add_argument("--url", help="Airflow base URL")
        parser.add_argument("--user", help="Airflow user name")
        parser.add_argument("--password-stdin", action="store_true",
                            help="read the password from stdin")
        group = parser.add_mutually_exclusive_group()
        group.add_argument("--token", help="bearer token (skips login); visible in shell history")
        group.add_argument("--token-stdin", action="store_true",
                           help="read the bearer token from the first line of stdin")
        return parser

    def network(bulk: bool) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument("--timeout", type=float, default=None, metavar="SECONDS",
                            help="seconds to wait for one API request, retries excluded "
                                 "(default 25)")
        parser.add_argument("--workers", type=int, default=None, metavar="N",
                            help="parallel API requests for status, task lists and sensors "
                                 "(default 8)")
        if bulk:
            parser.add_argument("--no-bulk-status", dest="bulk_status", action="store_const",
                                const=False, default=None,
                                help="read the run status of each DAG instead of the bulk listings")
        return parser

    common, net_opts = connection(True), network(True)

    fetch_opts = argparse.ArgumentParser(add_help=False)
    fetch_opts.add_argument("--with-status", action="store_true", default=None,
                            help="also fetch the latest run status of every DAG")
    fetch_opts.add_argument("--with-tasks", action="store_true", default=None,
                            help="also fetch the task ids of every DAG")
    fetch_opts.add_argument("--infer-sensors", action="store_true", default=None,
                            help="also infer DAG dependencies from Python and custom sensors "
                                 "(about 3-8 API calls per sensor)")
    fetch_opts.add_argument("--node-id", help="fetch only the subgraph around this node id, e.g. dag:my_dag")

    render_opts = argparse.ArgumentParser(add_help=False)
    render_opts.add_argument("--title", help="title of the rendered page")
    render_opts.add_argument("--highlight", help="regex of DAG ids to highlight "
                             "(matched against ids without the dag: prefix)")
    render_opts.add_argument("--status", metavar="FILE",
                             help="JSON {dag_id: {kind, paused, date}} that overrides the snapshot status")

    tag_opts = argparse.ArgumentParser(add_help=False)
    tag_opts.add_argument("--tag", dest="tags", action="append", metavar="NAME",
                          help="default page filter: only DAGs with any of these tags (repeatable)")

    p = argparse.ArgumentParser(prog="airflow-depgraph",
                                description="Airflow 3 cross-DAG dependency graph")
    p.add_argument("--version", action="version", version=f"airflow-depgraph {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    def add(name: str, *, parents: list, help: str, description: str,
            examples: list[str]) -> argparse.ArgumentParser:
        return sub.add_parser(
            name, parents=parents, help=help, description=description,
            epilog="Examples:\n" + "\n".join(f"  {e}" for e in examples),
            formatter_class=argparse.RawDescriptionHelpFormatter)

    f = add("fetch", parents=[common, net_opts, fetch_opts, tag_opts],
            help="save a snapshot of the graph to a JSON file",
            description="Fetch the dependency graph from Airflow and save it as a snapshot JSON "
                        "file. downstream, upstream, render and tasks can then work offline.",
            examples=["airflow-depgraph fetch --url https://airflow.example.com -o graph.json",
                      "airflow-depgraph fetch --preset prod --with-status --infer-sensors"])
    f.add_argument("-o", "--output", default=DEFAULT_SNAPSHOT,
                   help=f"snapshot file to write (default {DEFAULT_SNAPSHOT})")
    f.set_defaults(func=cmd_fetch)

    r = add("render", parents=[common, render_opts, tag_opts],
            help="render a snapshot file to an HTML page",
            description="Render a snapshot saved by fetch as a self-contained HTML page. "
                        "No Airflow connection is needed.",
            examples=["airflow-depgraph render graph.json -o graph.html",
                      "airflow-depgraph render graph.json --title 'Prod graph' --tag team-a"])
    r.add_argument("snapshot", help="snapshot JSON file written by fetch")
    r.add_argument("-o", "--output", default=DEFAULT_HTML,
                   help=f"HTML file to write (default {DEFAULT_HTML})")
    r.set_defaults(func=cmd_render)

    g = add("graph", parents=[common, net_opts, fetch_opts, render_opts, tag_opts],
            help="fetch live and write an HTML page plus its JSON snapshot",
            description="Fetch the graph from Airflow and write an HTML page. The JSON snapshot "
                        "goes next to it with a .json suffix.",
            examples=["airflow-depgraph graph --preset prod -o graph.html",
                      "airflow-depgraph graph --url https://airflow.example.com --with-status"])
    g.add_argument("-o", "--output", default=DEFAULT_HTML,
                   help="HTML file to write; the snapshot goes next to it with a .json suffix "
                        f"(default {DEFAULT_HTML})")
    g.set_defaults(func=cmd_graph)

    for name in ("downstream", "upstream"):
        meaning = "what depends on it" if name == "downstream" else "what it waits for"
        q = add(name, parents=[common, net_opts], help=f"list {name} DAGs",
                description=f"List the DAGs {name} of DAG ({meaning}), up to --depth hops. "
                            "Reads a snapshot file if given, otherwise queries Airflow live.",
                examples=[f"airflow-depgraph {name} dag_a graph.json",
                          f"airflow-depgraph {name} dag_a --depth 2 --json --preset prod"])
        q.add_argument("dag", help="DAG id, with or without the dag: prefix")
        q.add_argument("snapshot", nargs="?", help="snapshot file; omit to query live")
        q.add_argument("--depth", type=_depth, default=5, help="maximum DAG hops, at least 1 (default 5)")
        q.add_argument("--json", action="store_true", help="print machine-readable JSON")
        q.add_argument("--tree", action="store_true",
                       help="print the DAGs as an indented tree, each under the DAG it was reached from")
        q.add_argument("--tasks-matching", metavar="REGEX",
                       help="keep DAGs having a task matching REGEX")
        q.add_argument("--tasks-preset", action="store_true", default=None,
                       help="keep DAGs having a task matching the preset's task_query_regex")
        q.add_argument("--no-inferred", action="store_true",
                       help="ignore sensor dependencies inferred by fetch --infer-sensors")
        q.add_argument("--include-guards", action="store_true",
                       help="also follow inferred deploy guards (sensors waiting for a DAG not to run)")
        q.set_defaults(func=lambda a, d=name: _query(a, d))

    t = add("tasks", parents=[common, net_opts], help="list the task ids of a DAG",
            description="Print the task ids of one DAG, from a snapshot made with --with-tasks "
                        "or live from Airflow.",
            examples=["airflow-depgraph tasks dag_a graph.json --regex refresh",
                      "airflow-depgraph tasks dag_a --json --preset prod"])
    t.add_argument("dag", help="DAG id, with or without the dag: prefix")
    t.add_argument("snapshot", nargs="?", help="snapshot file; omit to query live")
    t.add_argument("--regex", help="keep only task ids matching this regex")
    t.add_argument("--json", action="store_true", help="print a JSON list")
    t.set_defaults(func=cmd_tasks)

    rp = add("report", parents=[common, net_opts],
             help="critical, unlinked, unconsumed-assets or failing DAGs",
             description="Reports over a snapshot file: critical (DAGs with the most downstream "
                         "DAGs), unlinked (DAGs with no links), unconsumed-assets (assets nobody "
                         "reads) and failing (DAGs whose last run failed). Works offline.",
             examples=["airflow-depgraph report critical graph.json --top 10",
                       "airflow-depgraph report failing graph.json --tag team-a --json",
                       "airflow-depgraph report unlinked graph.json --all-tags"])
    rp.add_argument("kind", choices=report.KINDS, help="which report to print")
    rp.add_argument("snapshot", help="snapshot JSON file written by fetch")
    rp.add_argument("--top", type=_depth, default=20, help="rows to show for critical (default 20)")
    rp.add_argument("--json", action="store_true", help="print a JSON list instead of a table")
    rp.add_argument("--tag", dest="report_tags", action="append", metavar="NAME",
                    help="keep rows with any of these tags (repeatable; default: the preset's tags)")
    rp.add_argument("--all-tags", action="store_true", help="ignore tags, even the preset's")
    rp.add_argument("--no-inferred", action="store_true",
                    help="ignore sensor dependencies inferred by fetch --infer-sensors")
    rp.set_defaults(func=cmd_report)

    pr = add("presets", parents=[common], help="list presets, or show one resolved",
             description="List the presets found in the preset directories. With --preset, also "
                         "show its resolved settings; the token is never shown.",
             examples=["airflow-depgraph presets",
                       "airflow-depgraph presets --preset prod",
                       "airflow-depgraph presets --example"])
    pr.add_argument("--example", action="store_true", help="print the example preset")
    pr.set_defaults(func=cmd_presets)

    ini = add("init", parents=[connection(False), network(False), tag_opts],
              help="create a preset by answering questions",
              description="Create a preset file by questionnaire. Every answer is checked against "
                          "the live Airflow, which also proposes tags, asset prefixes and "
                          "settings. With --yes everything comes from flags; --discover only "
                          "reads and reports. The password is never stored or taken as a flag.",
              examples=["airflow-depgraph init",
                        "airflow-depgraph init prod --yes --url https://airflow.example.com "
                        "--tag team-a",
                        "airflow-depgraph init --discover --json --url https://airflow.example.com"])
    ini.set_defaults(preset="none")
    ini.add_argument("name", nargs="?", help="preset name, [a-zA-Z0-9_-]+ (asked when omitted)")
    ini.add_argument("--user-env", help="env var that holds the user name (default "
                                        "AIRFLOW_DEPGRAPH_USER)")
    ini.add_argument("--password-env", help="env var that holds the password (default "
                                            "AIRFLOW_DEPGRAPH_PASSWORD)")
    ini.add_argument("--password-cmd", help="command that prints the password, e.g. "
                                            "'pass show airflow/prod'")
    ini.add_argument("--strip-prefix", help="prefix removed from asset labels")
    ini.add_argument("--highlight", help="regex of DAG ids to highlight")
    ini.add_argument("--highlight-label", help="legend label for the highlighted DAGs")
    ini.add_argument("--title", help="title of the rendered page")
    ini.add_argument("--with-status", dest="with_status", action="store_true", default=None,
                     help="fetch the run status of every DAG")
    ini.add_argument("--no-status", dest="with_status", action="store_const", const=False,
                     help="do not fetch the run status")
    ini.add_argument("--infer-sensors", dest="infer_sensors", action="store_true", default=None,
                     help="infer dependencies from sensors")
    ini.add_argument("--no-infer-sensors", dest="infer_sensors", action="store_const", const=False,
                     help="do not infer dependencies from sensors")
    ini.add_argument("--tests-task-regex", help="regex that marks a task as a test task")
    ini.add_argument("--task-query-regex", help="regex used by --tasks-preset")
    ini.add_argument("--lookback", type=int, metavar="N",
                     help="recent runs to look at for the status")
    ini.add_argument("--dir", metavar="DIR",
                     help="write the preset here (default $AIRFLOW_DEPGRAPH_PRESETS, else "
                          "~/.config/airflow-depgraph/presets)")
    ini.add_argument("--force", action="store_true", help="overwrite an existing preset")
    ini.add_argument("--yes", action="store_true",
                     help="ask nothing: take every value from flags")
    ini.add_argument("--no-check", action="store_true",
                     help="do not contact Airflow (no health or sign-in check)")
    ini.add_argument("--discover", action="store_true",
                     help="only read from Airflow and report what a preset could use; "
                          "writes nothing")
    ini.add_argument("--json", action="store_true", help="with --discover: print JSON")
    ini.set_defaults(func=cmd_init)
    return p


def _exit_code(exc: Exception) -> int:
    if isinstance(exc, CredentialsError):
        return EXIT_CREDENTIALS
    if isinstance(exc, UsageError):
        return EXIT_USAGE
    if isinstance(exc, ApiError):
        if exc.status in (401, 403) and exc.path in _AUTH_PATHS:
            return EXIT_CREDENTIALS
        if exc.network:
            return EXIT_NETWORK
    if isinstance(exc, DagNotFound):
        return EXIT_DAG_NOT_FOUND
    return EXIT_ERROR


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        return args.func(args)
    except (SnapshotError, PresetError, CredentialsError, ApiError, CliError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return _exit_code(exc)
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return EXIT_INTERRUPTED


if __name__ == "__main__":
    sys.exit(main())
