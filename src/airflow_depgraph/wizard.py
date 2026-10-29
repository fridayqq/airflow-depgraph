"""`init`: build a preset by questionnaire, with discovery against the live Airflow."""

from __future__ import annotations

import dataclasses
import getpass
import math
import os
import re
import sys
import tempfile
import urllib.parse
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO

from airflow_depgraph import sensors as sensormod
from airflow_depgraph import snapshot as snapmod
from airflow_depgraph.auth import CredentialsError, resolve_password, resolve_user
from airflow_depgraph.client import ApiError
from airflow_depgraph.presets import PRESET_KEYS, PresetError, Settings, _lookup_dirs, find_preset
from airflow_depgraph.snapshot import SnapshotError

DEFAULT_NAME = "prod"
STATUS_LIMIT = 3000         # above this many DAGs the status rings are not suggested
TOP_TAGS = 15
SENSOR_SAMPLE = 40          # DAGs whose task lists discovery reads to suggest infer_sensors
_PREFIX_BOUNDARY = frozenset(":/._-")
_NAME_RE = re.compile(r"[A-Za-z0-9_-]+")
_ENV_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_DEFAULTS = {f.name: f.default for f in dataclasses.fields(Settings)}

# key -> (comment lines, value shown when the key is not set)
_EXAMPLES = {
    "url": "https://airflow.example.com",
    "password_cmd": "pass show airflow/prod",
    "asset_label_strip_prefix": "s3://example-bucket/",
    "highlight": "^example_",
    "task_query_regex": "^(extract|load)_",
    "tags": ["team-a"],
}
_DOC: list[tuple[str, list[str]]] = [
    ("url", ["Base URL of the Airflow webserver."]),
    ("user_env", ["Name of the env var that holds the Airflow user name."]),
    ("password_env", ["Name of the env var that holds the Airflow password."]),
    ("password_cmd", ["Shell command that prints the password to stdout "
                      "(used when the env var is empty)."]),
    ("with_status", ["Also fetch the latest run status of every DAG."]),
    ("with_tasks", ["Also fetch the tasks of every DAG."]),
    ("title", ["Title of the rendered page."]),
    ("asset_label_strip_prefix",
     ["Prefix removed from asset names when they are shown as labels."]),
    ("highlight", ["Regex matched against DAG ids (without the dag: prefix); matching DAG nodes "
                   "are highlighted.", "Assets, triggers and sensors are never highlighted."]),
    ("highlight_label", ["Legend label for the highlighted nodes."]),
    ("tests_task_regex", ["Regex that marks a task as a test task.",
                          "Matches dbt-test-x, dbt_tasks.dbt-test-x, test_models, run_tests "
                          "(case-insensitive)."]),
    ("task_query_regex", ["Regex used by --tasks-preset. Nothing is filtered at load time; "
                          "all tasks are fetched."]),
    ("status_runs_lookback", ["How many recent runs to look at when computing the status."]),
    ("tags", ["Default page filter: only DAGs with any of these tags "
              "(the page can switch it off)."]),
    ("infer_sensors", ["Infer DAG dependencies from Python and custom sensors (fetch/graph; the "
                       "plugin refreshes them",
                       "in the background). Costs about 3-8 API calls per sensor plus one task "
                       "list per DAG."]),
    ("infer_interval_minutes", ["Plugin only: minutes between background refreshes of the "
                                "inferred sensor links."]),
    ("timeout", ["Seconds to wait for one API request before it fails. Timeouts, 429 and "
                 "502-504 are retried twice."]),
    ("workers", ["Parallel API requests for run status, task lists and sensor inference."]),
    ("bulk_status", ["Read the run status from bulk listings (fewer calls); false reads each "
                     "DAG on its own."]),
]


# ---------- validation ----------


def validate_name(name: str) -> str:
    if not _NAME_RE.fullmatch(name or ""):
        raise ValueError(f"preset name {name!r} must match [a-zA-Z0-9_-]+")
    return name


def validate_env_name(name: str) -> str:
    if not _ENV_RE.fullmatch(name or ""):
        raise ValueError(f"{name!r} is not an environment variable name ([A-Z_][A-Z0-9_]*)")
    return name


def validate_regex(pattern: str) -> str:
    try:
        re.compile(pattern)
    except re.error as exc:
        raise ValueError(f"invalid regex {pattern!r}: {exc}") from None
    return pattern


def normalize_url(raw: str, *, assume_https: bool = False) -> str:
    url = (raw or "").strip()
    if url and "://" not in url and assume_https:
        url = "https://" + url
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise ValueError(f"URL must start with http:// or https:// and name a host: {raw!r}")
    return url.rstrip("/")


def validate_given(given: Mapping[str, object]) -> None:
    """Check the flag values that are not asked again; raises ValueError."""
    for key in ("user_env", "password_env"):
        if given.get(key) is not None:
            validate_env_name(str(given[key]))
    for key in ("highlight", "tests_task_regex", "task_query_regex"):
        if given.get(key) is not None:
            validate_regex(str(given[key]))
    if given.get("lookback") is not None and given["lookback"] < 1:
        raise ValueError("--lookback must be at least 1")
    if given.get("timeout") is not None and not (math.isfinite(given["timeout"])
                                                 and given["timeout"] > 0):
        raise ValueError("--timeout must be a finite number greater than 0")
    if given.get("workers") is not None and given["workers"] < 1:
        raise ValueError("--workers must be at least 1")


# ---------- discovery ----------


def tag_counts(dags: list[dict]) -> list[tuple[str, int]]:
    counts: Counter = Counter()
    for entry in snapmod.build_catalog(dags).values():
        counts.update(set(entry["tags"]))
    counts.pop("", None)
    return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))


def _label_prefixes(label: str) -> set[str]:
    """Every proper prefix of the label that ends at a separator: s3:, s3:/, s3://, s3://bucket/ …"""
    return {label[:i + 1] for i, ch in enumerate(label[:-1]) if ch in _PREFIX_BOUNDARY}


def asset_prefixes(labels: list[str], top: int = 5) -> list[tuple[str, int]]:
    """Prefixes shared by at least two labels, most assets first, longest first on a tie.
    s3: and s3:/ are dropped next to s3://: the same labels, only separators added."""
    counts: Counter = Counter()
    for label in labels:
        counts.update(_label_prefixes(label))
    shared = sorted((p for p, n in counts.items() if n >= 2), key=len, reverse=True)
    kept: list[str] = []
    for p in shared:
        if not any(q.startswith(p) and counts[q] == counts[p]
                   and set(q[len(p):]) <= _PREFIX_BOUNDARY for q in kept):
            kept.append(p)
    kept.sort(key=lambda p: (-counts[p], -len(p), p))
    return [(p, counts[p]) for p in kept[:top]]


def sensor_count(nodes: list[dict]) -> int:
    return sum(1 for n in nodes if n.get("type") == "sensor")


def sample_dag_ids(dag_ids: list[str], n: int = SENSOR_SAMPLE) -> list[str]:
    """n ids spread evenly over the sorted list; all of them when there are at most n."""
    ids = sorted(dag_ids)
    if len(ids) <= n:
        return ids
    step = len(ids) / n
    return [ids[int(i * step)] for i in range(n)]


def sample_sensor_tasks(client, dag_ids: list[str], workers: int = 8) -> tuple[int, int]:
    """(DAGs whose task list was read, candidate sensor tasks among them).
    A DAG whose tasks cannot be read is not counted; a 401 propagates as ApiError."""
    if not dag_ids:
        return (0, 0)
    tasks_by_dag = sensormod.fetch_tasks(client, list(dag_ids), workers=workers)
    read = [tasks for tasks in tasks_by_dag.values() if tasks is not None]
    found = sum(1 for tasks in read for t in tasks
                if isinstance(t, dict) and sensormod.is_candidate(t.get("operator_name")))
    return (len(read), found)


def plural(n: int, noun: str) -> str:
    return f"{n} {noun}" + ("" if n == 1 else "s")


def suggest_with_status(n_dags: int) -> bool:
    return n_dags <= STATUS_LIMIT


@dataclass
class Discovery:
    url: str | None
    airflow_version: str | None
    scheduler: str
    dags_total: int
    dags_active: int
    dags_paused: int
    tags: list[tuple[str, int]]
    assets: int
    asset_prefixes: list[tuple[str, int]]
    sensor_nodes: int
    with_status: bool
    infer_sensors: bool = False
    sensor_sample: tuple[int, int] = (0, 0)     # (DAGs read, candidate sensor tasks found)
    dag_ids: list[str] = field(default_factory=list)

    def to_json(self) -> dict:
        return {
            "url": self.url, "airflow_version": self.airflow_version, "scheduler": self.scheduler,
            "dags": {"total": self.dags_total, "active": self.dags_active,
                     "paused": self.dags_paused},
            "tags": [{"name": t, "dags": n} for t, n in self.tags],
            "assets": self.assets,
            "asset_prefixes": [{"prefix": p, "assets": n} for p, n in self.asset_prefixes],
            "sensor_nodes": self.sensor_nodes,
            "sensor_tasks": {"sampled_dags": self.sensor_sample[0],
                             "candidates": self.sensor_sample[1]},
            "suggest": {"with_status": self.with_status, "infer_sensors": self.infer_sensors},
        }


def scheduler_status(health: object) -> str:
    sched = health.get("scheduler") if isinstance(health, dict) else None
    status = sched.get("status") if isinstance(sched, dict) else None
    return status if isinstance(status, str) else "unknown"


def discover(client, settings: Settings) -> Discovery:
    """Read-only: DAG catalog, dependency graph and version of the Airflow behind client."""
    try:
        scheduler = scheduler_status(client.health())
    except ApiError:
        scheduler = "unknown"
    version = client.version()
    dags = client.list_dags()
    nodes, _ = snapmod.validate_payload(client.dependencies(), version)
    catalog = {d: c for d, c in snapmod.build_catalog(dags).items() if not c["is_stale"]}
    live = [d for d in dags if isinstance(d, dict) and d.get("dag_id") in catalog]
    paused = sum(1 for c in catalog.values() if c["is_paused"])
    labels = [str(n.get("label") or "") for n in nodes if n.get("type") == "asset"]
    active = [d for d, c in catalog.items() if not c["is_paused"]] or list(catalog)
    sampled, found = sample_sensor_tasks(client, sample_dag_ids(active), workers=settings.workers)
    return Discovery(
        url=settings.url, airflow_version=version, scheduler=scheduler,
        dags_total=len(catalog), dags_active=len(catalog) - paused, dags_paused=paused,
        tags=tag_counts(live), assets=len(labels), asset_prefixes=asset_prefixes(labels),
        sensor_nodes=sensor_count(nodes), with_status=suggest_with_status(len(catalog)),
        infer_sensors=found > 0, sensor_sample=(sampled, found), dag_ids=sorted(catalog))


def format_report(d: Discovery) -> str:
    lines = [f"Airflow {d.airflow_version or '(version unknown)'} at {d.url}",
             f"scheduler: {d.scheduler}",
             f"DAGs: {d.dags_total} ({d.dags_active} active, {d.dags_paused} paused)"]
    top = ", ".join(f"{t} ({n})" for t, n in d.tags[:TOP_TAGS]) or "none"
    lines.append(f"tags: {top}" + (f" ... {len(d.tags) - TOP_TAGS} more" if len(d.tags) > TOP_TAGS
                                   else ""))
    prefixes = ", ".join(f"{p} ({n})" for p, n in d.asset_prefixes) or "none"
    lines.append(f"assets: {d.assets}; prefixes: {prefixes}")
    lines.append(f"sensor nodes: {d.sensor_nodes}")
    lines.append(f"sensor tasks: {d.sensor_sample[1]} in a sample of "
                 f"{plural(d.sensor_sample[0], 'DAG')}")
    lines.append(f"suggest: with_status={str(d.with_status).lower()}, "
                 f"infer_sensors={str(d.infer_sensors).lower()}")
    return "\n".join(lines)


def check_health(client_cls, url: str, timeout: float) -> str:
    """Scheduler status from the unauthenticated health endpoint; ApiError if not Airflow 3."""
    path = "/api/v2/monitor/health"
    try:
        health = client_cls(url, timeout=timeout).health()
    except ApiError as e:
        if e.status is not None and not e.network:
            raise ApiError(f"not an Airflow 3 API: {path} returned {e.status}",
                           e.status, path) from None
        raise
    if not isinstance(health, dict) or not ({"scheduler", "metadatabase"} & set(health)):
        raise ApiError(f"not an Airflow 3 API: unexpected response from {path}", None, path)
    return scheduler_status(health)


# ---------- rendering and writing ----------


def _toml_str(s: str) -> str:
    plain = not any(ord(c) < 32 or ord(c) == 127 for c in s) and "'" not in s
    if plain and ("\\" in s or '"' in s):
        return f"'{s}'"
    esc = {"\\": "\\\\", '"': '\\"', "\n": "\\n", "\t": "\\t", "\r": "\\r", "\b": "\\b",
           "\f": "\\f"}
    out = "".join(esc.get(c) or (f"\\u{ord(c):04X}" if ord(c) < 32 or ord(c) == 127 else c)
                  for c in s)
    return f'"{out}"'


def _toml_value(v: object) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return str(int(v)) if v.is_integer() else repr(v)
    if isinstance(v, str):
        return _toml_str(v)
    if isinstance(v, (list, tuple)):
        return "[" + ", ".join(_toml_value(x) for x in v) + "]"
    raise ValueError(f"cannot write {v!r} to a preset")


def render_preset(values: Mapping[str, object]) -> str:
    unknown = set(values) - PRESET_KEYS - {"name"}
    if unknown:
        raise ValueError(f"unknown preset keys: {', '.join(sorted(unknown))}")
    lines = ["# airflow-depgraph preset, created by `airflow-depgraph init`.",
             "# Credentials are never stored here, only env var names or a command.",
             "# Keys that are commented out show their default; uncomment one to change it.",
             "", "# Preset name, shown in the output."]
    if "name" in values:
        lines.append(f"name = {_toml_value(values['name'])}")
    for key, comment in _DOC:
        lines.append("")
        lines.extend(f"# {c}" for c in comment)
        if key in values:
            if key == "password_env" and values[key] == "":
                lines.append("# empty: the password comes from password_cmd only")
            lines.append(f"{key} = {_toml_value(values[key])}")
        else:
            shown = _EXAMPLES.get(key, _DEFAULTS.get(key))
            lines.append(f"# {key} = {_toml_value(shown)}")
    return "\n".join(lines) + "\n"


def build_values(name: str, url: str, chosen: Mapping[str, object]) -> dict:
    """Preset keys for the answers; keys left at None are not written."""
    values: dict[str, object] = {"name": name, "url": url,
                                 "user_env": chosen.get("user_env") or _DEFAULTS["user_env"]}
    # an empty name makes resolve_password skip the env lookup and run the command
    values["password_env"] = (chosen.get("password_env")
                              or ("" if chosen.get("password_cmd") else _DEFAULTS["password_env"]))
    if chosen.get("password_cmd"):
        values["password_cmd"] = chosen["password_cmd"]
    values["with_status"] = bool(chosen.get("with_status"))
    values["infer_sensors"] = bool(chosen.get("infer_sensors"))
    renames = {"strip_prefix": "asset_label_strip_prefix", "lookback": "status_runs_lookback"}
    for key in ("tags", "title", "strip_prefix", "highlight", "highlight_label",
                "tests_task_regex", "task_query_regex", "lookback", "timeout", "workers"):
        v = chosen.get(key)
        if v is None or v == [] or v == "":
            continue
        values[renames.get(key, key)] = list(v) if key == "tags" else v
    return values


def target_dir(dir_arg: str | None, env: Mapping[str, str]) -> Path:
    if dir_arg:
        return Path(dir_arg).expanduser()
    return _lookup_dirs(env)[0]


def write_preset(path: Path, text: str, force: bool = False) -> None:
    path = Path(path)
    if path.exists() and not force:
        raise FileExistsError(f"{path} already exists; pass --force to overwrite it")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def _tilde(path: Path) -> str:
    home = str(Path.home())
    s = str(path)
    return "~" + s[len(home):] if s == home or s.startswith(home + os.sep) else s


def written_message(path: Path, name: str, values: Mapping[str, object],
                    env: Mapping[str, str]) -> str:
    lines = [f"✓ wrote {_tilde(path)}", "", "Next:"]
    if values["user_env"] not in env:
        lines.append(f"  export {values['user_env']}=…")
    if (values["password_env"] and "password_cmd" not in values
            and values["password_env"] not in env):
        lines.append(f"  export {values['password_env']}=…")
    try:
        found = find_preset(name, env).resolve() == path.resolve()
    except PresetError:
        found = False
    ref = name if found else str(path)
    lines.append(f"  airflow-depgraph presets --preset {ref}")
    lines.append(f"  airflow-depgraph graph --preset {ref} -o graph.html")
    return "\n".join(lines)


# ---------- interactive driver ----------


@dataclass
class Prompter:
    input_fn: Callable[[str], str] = input
    getpass_fn: Callable[[str], str] = getpass.getpass
    out: TextIO = sys.stdout
    color: bool = False

    def _c(self, code: str, text: str) -> str:
        return f"\033[{code}m{text}\033[0m" if self.color else text

    def say(self, text: str = "") -> None:
        print(text, file=self.out)

    def ok(self, text: str) -> None:
        self.say(f"{self._c('32', '✓')} {text}")

    def bad(self, text: str) -> None:
        self.say(f"{self._c('31', '✗')} {text}")

    def warn(self, text: str) -> None:
        self.say(f"{self._c('33', '!')} {text}")

    def dim(self, text: str) -> None:
        self.say(self._c("2", text))

    def ask(self, label: str, default: str | None = None,
            parse: Callable[[str], object] | None = None):
        suffix = f" [{default}]" if default else ""
        while True:
            raw = self.input_fn(f"{label}{suffix}: ").strip()
            if not raw and default is None:
                self.bad("a value is required")
                continue
            raw = raw or default
            try:
                return parse(raw) if parse else raw
            except ValueError as exc:
                self.bad(str(exc))

    def confirm(self, label: str, default: bool) -> bool:
        hint = "Y/n" if default else "y/N"
        while True:
            raw = self.input_fn(f"{label} [{hint}]: ").strip().lower()
            if not raw:
                return default
            if raw in ("y", "yes"):
                return True
            if raw in ("n", "no"):
                return False
            self.bad("answer y or n")

    def password(self) -> str:
        return self.getpass_fn("Airflow password: ")


def parse_tags(answer: str, listed: list[str], known: set[str] | None) -> tuple[list[str], list[str]]:
    """Tags from numbers (1-based into listed) and names; second item is warnings."""
    if answer.strip() in ("", "-"):
        return [], []
    tags: list[str] = []
    warnings: list[str] = []
    for token in (t.strip() for t in answer.split(",")):
        if not token:
            continue
        if token.isdigit():
            if not 1 <= int(token) <= len(listed):
                raise ValueError(f"no tag number {token}")
            token = listed[int(token) - 1]
        elif known is not None and token not in known:
            warnings.append(f"no DAG has tag {token}")
        if token not in tags:
            tags.append(token)
    return tags, warnings


def _choice(valid: str) -> Callable[[str], str]:
    def parse(raw: str) -> str:
        if raw.lower() not in valid:
            raise ValueError(f"choose one of: {', '.join(valid)}")
        return raw.lower()
    return parse


def _env_state(p: Prompter, name: str, env: Mapping[str, str]) -> None:
    if name in env:
        p.ok("set")
    else:
        p.dim("not set in this shell")


def _positive(kind: Callable, label: str, minimum: float, strict: bool = False):
    def parse(raw: str):
        try:
            v = kind(raw)
        except ValueError:
            raise ValueError(f"{label} must be a number") from None
        if v < minimum or (strict and v == minimum):
            raise ValueError(f"{label} must be {'greater than' if strict else 'at least'} "
                             f"{minimum:g}")
        return v
    return parse


def run_interactive(p: Prompter, given: Mapping[str, object], *, env: Mapping[str, str],
                    target: Path, client_cls, user: str | None = None, token: str | None = None,
                    force: bool = False, no_check: bool = False) -> int:
    """Ask the questions, write the preset. Returns the exit code (1 when the user chose not to
    write); Ctrl-C and EOF propagate as KeyboardInterrupt/EOFError."""
    p.say("airflow-depgraph init — new preset")
    p.dim("Ctrl-C to quit, nothing is written until the end.")
    chosen: dict[str, object] = {}
    timeout = given.get("timeout") or float(_DEFAULTS["timeout"])

    p.say("\n[1/7] Name")
    name, overwrite = _ask_name(p, given.get("name"), target, force)

    p.say("\n[2/7] Airflow URL")
    url = _ask_url(p, given.get("url"), client_cls, timeout, no_check)

    p.say("\n[3/7] User")
    if given.get("user_env") is not None:
        chosen["user_env"] = given["user_env"]
        p.ok(f"user_env: {chosen['user_env']} (from --user-env)")
    else:
        chosen["user_env"] = p.ask("Environment variable that holds the user name",
                                   _DEFAULTS["user_env"], validate_env_name)
    _env_state(p, chosen["user_env"], env)

    p.say("\n[4/7] Password source")
    _ask_password_source(p, given, chosen, env)

    values_preview = build_values("", "", chosen)
    settings = Settings(
        url=url, user=user, user_env=chosen["user_env"],
        password_env=values_preview["password_env"],
        password_cmd=chosen.get("password_cmd"), token=token, timeout=timeout,
        workers=given.get("workers") or _DEFAULTS["workers"])

    p.say("\n[5/7] Sign-in check")
    client = None
    if no_check:
        p.dim("skipped (--no-check): no suggestions from Airflow")
    else:
        client = _sign_in(p, settings, env, client_cls)
    if client is False:     # the user chose to quit
        return 1

    p.say("\n[6/7] Choices")
    disc = None
    if client:
        p.dim("reading DAGs and dependencies…")
        try:
            disc = discover(client, settings)
            p.ok(f"{plural(disc.dags_total, 'DAG')} ({disc.dags_active} active, "
                 f"{disc.dags_paused} paused), {plural(disc.assets, 'asset')}, "
                 f"{plural(disc.sensor_nodes, 'sensor node')}, "
                 f"{plural(disc.sensor_sample[1], 'sensor task')} in "
                 f"{plural(disc.sensor_sample[0], 'sampled DAG')}")
        except (ApiError, SnapshotError) as exc:
            p.warn(f"discovery failed ({exc}); asking without suggestions")
    _ask_choices(p, given, chosen, disc)

    p.say("\n[7/7] Advanced")
    _ask_advanced(p, given, chosen)

    values = build_values(name, url, chosen)
    text = render_preset(values)
    path = target / f"{name}.toml"
    p.say(f"\nThis is the file that will be written ({path}):\n")
    p.say(text)
    if not p.confirm("Write it?", True):
        p.say("not written")
        return 1
    write_preset(path, text, force=overwrite or force)
    p.say()
    p.say(written_message(path, name, values, env))
    return 0


def _ask_name(p: Prompter, given_name: object, target: Path, force: bool) -> tuple[str, bool]:
    name = given_name
    while True:
        if name is None:
            default = DEFAULT_NAME if not (target / f"{DEFAULT_NAME}.toml").exists() else None
            name = p.ask("Preset name", default, validate_name)
        else:
            try:
                validate_name(name)
            except ValueError as exc:
                p.bad(str(exc))
                name = None
                continue
            if given_name is not None and name == given_name:
                p.ok(f"name: {name} (from NAME)")
        path = target / f"{name}.toml"
        if not path.exists():
            return name, False
        if force:
            p.warn(f"{path} exists and will be replaced (--force)")
            return name, True
        if p.confirm(f"{path} exists. Overwrite it?", False):
            return name, True
        name, given_name = None, None


def _ask_url(p: Prompter, given_url: object, client_cls, timeout: float, no_check: bool) -> str:
    raw = given_url
    while True:
        from_flag = raw is not None
        if raw is None:
            raw = p.ask("Airflow URL, e.g. https://airflow.example.com")
        try:
            assume = False
            if "://" not in str(raw).strip() and str(raw).strip():
                assume = p.confirm(f"no scheme given; use https://{str(raw).strip()} ?", True)
            url = normalize_url(str(raw), assume_https=assume)
        except ValueError as exc:
            p.bad(str(exc))
            raw = None
            continue
        if no_check:
            break
        try:
            scheduler = check_health(client_cls, url, timeout)
        except ApiError as exc:
            p.bad(str(exc))
            raw = None
            continue
        if scheduler == "healthy":
            p.ok("Airflow is reachable (scheduler healthy)")
        else:
            p.warn(f"Airflow is reachable, scheduler is {scheduler}")
        break
    if from_flag:
        p.ok(f"url: {url} (from --url)")
    return url


def _ask_password_source(p: Prompter, given: Mapping[str, object], chosen: dict,
                         env: Mapping[str, str]) -> None:
    cmd, penv = given.get("password_cmd"), given.get("password_env")
    if cmd is None and penv is None:
        p.say("  1) environment variable\n  2) command (pass, Keychain, 1Password…)")
        source = p.ask("Where does the password come from?", "1", _choice("12"))
    else:
        source = "2" if cmd is not None else "1"
    if penv is not None or source == "1":
        if penv is not None:
            chosen["password_env"] = penv
            p.ok(f"password_env: {penv} (from --password-env)")
        else:
            chosen["password_env"] = p.ask("Environment variable that holds the password",
                                           _DEFAULTS["password_env"], validate_env_name)
        _env_state(p, chosen["password_env"], env)
    if source == "2":
        if cmd is not None:
            chosen["password_cmd"] = cmd
            p.ok("password_cmd: (from --password-cmd)")
        else:
            p.dim("e.g. pass show airflow/prod\n"
                  "     security find-generic-password -s airflow -w\n"
                  "     op read op://vault/airflow/password")
            chosen["password_cmd"] = p.ask("Command that prints the password")
    p.dim("The password itself is never stored. At run time a token in AIRFLOW_DEPGRAPH_TOKEN\n"
          "     skips the password (README, \"Get a token from the browser\").")


def _sign_in(p: Prompter, s: Settings, env: Mapping[str, str], client_cls):
    """A logged-in client, None when the user skips the check, False when they quit."""
    typed_user = None
    typed_pw = False
    if s.token:
        client = client_cls(s.url, token=s.token, timeout=s.timeout)
        p.ok("using the bearer token from the flags")
        return client
    if s.password_env not in env and not s.password_cmd:
        p.dim("No password variable or command is available: you will be asked once; "
              "it is used for this check only and discarded.")
    while True:
        try:
            try:
                user = resolve_user(s, env)
            except CredentialsError:
                typed_user = p.ask("User name (for this check only, not stored)", typed_user)
                user = typed_user
            if typed_pw:
                password = p.password()
            else:
                password = resolve_password(s, env, stdin_password=False, stdin=None,
                                            isatty=True, prompt=lambda _prompt: p.password())
            client = client_cls.login(s.url, user, password, timeout=s.timeout)
            version = client.version()
            p.ok(f"signed in as {user} — Airflow {version or '(version unknown)'}")
            return client
        except (ApiError, CredentialsError) as exc:
            p.bad(str(exc))
            from_env = not typed_pw and not s.password_cmd and s.password_env in env
            letters = "rsq"
            menu = "r) retry  s) skip checks (no suggestions)  q) quit"
            if from_env:
                p.dim(f"${s.password_env} is read again on retry and will not change.")
                letters = "rtsq"
                menu = ("r) retry  t) type the password for this check  "
                        "s) skip checks (no suggestions)  q) quit")
            answer = p.ask(menu, "r", _choice(letters))
            if answer == "q":
                p.say("not written")
                return False
            if answer == "s":
                return None
            if answer == "t":
                typed_pw = True


def _ask_choices(p: Prompter, given: Mapping[str, object], chosen: dict, disc) -> None:
    known = {t for t, _ in disc.tags} if disc else None
    # tags
    if given.get("tags"):
        chosen["tags"] = list(given["tags"])
        p.ok(f"tags: {', '.join(chosen['tags'])} (from --tag)")
        for t in chosen["tags"]:
            if known is not None and t not in known:
                p.warn(f"no DAG has tag {t}")
    else:
        listed = [t for t, _ in disc.tags[:TOP_TAGS]] if disc else []
        if listed:
            p.say("Tags (the page shows only DAGs with these tags):")
            for i, (t, n) in enumerate(disc.tags[:TOP_TAGS], 1):
                p.say(f" {i:>2}) {t}  ({n})")
            label = "Numbers and/or names, comma-separated, - for none"
        else:
            label = "Tags for the default page filter, comma-separated, Enter for none"
        tags, warnings = p.ask(label, "-" if listed else "",
                               lambda a: parse_tags(a, listed, known if listed else None))
        for w in warnings:
            p.warn(w)
        chosen["tags"] = tags
    # asset prefix
    if given.get("strip_prefix") is not None:
        chosen["strip_prefix"] = given["strip_prefix"]
        p.ok(f"strip prefix: {chosen['strip_prefix']} (from --strip-prefix)")
    elif disc and disc.asset_prefixes:
        p.say("Asset label prefix to strip:")
        p.say(" 0) keep labels as they are")
        for i, (pre, n) in enumerate(disc.asset_prefixes, 1):
            p.say(f" {i:>2}) {pre}  ({plural(n, 'asset')})")
        cands = [pre for pre, _ in disc.asset_prefixes]

        def parse_prefix(raw: str) -> str:
            if raw == "-":
                return ""
            if raw.isdigit():
                if int(raw) == 0:
                    return ""
                if int(raw) > len(cands):
                    raise ValueError(f"no prefix number {raw}")
                return cands[int(raw) - 1]
            return raw
        chosen["strip_prefix"] = p.ask("Number or text, - for none", "0", parse_prefix)
    else:
        chosen["strip_prefix"] = p.ask(
            "Asset label prefix to strip, - for none", "",
            lambda raw: "" if raw in ("-", "0") else raw)
    # status and sensors
    n = disc.dags_total if disc else None
    if given.get("with_status") is not None:
        chosen["with_status"] = given["with_status"]
        p.ok(f"with_status: {str(chosen['with_status']).lower()} (from flag)")
    else:
        p.dim("Run status rings: one bulk request per 100 DAGs.")
        chosen["with_status"] = p.confirm("Show run status rings?",
                                          suggest_with_status(n) if n is not None else False)
    if given.get("infer_sensors") is not None:
        chosen["infer_sensors"] = given["infer_sensors"]
        p.ok(f"infer_sensors: {str(chosen['infer_sensors']).lower()} (from flag)")
    else:
        size = f"reads the tasks of {plural(n, 'DAG')}; " if n is not None else ""
        hint = ""
        if disc and disc.sensor_sample[0]:
            read, found = disc.sensor_sample
            hint = f"a sample of {plural(read, 'DAG')} has {plural(found, 'sensor task')} to check; "
        p.dim(f"Sensor inference finds waits on other DAGs that Airflow does not show: {hint}{size}"
              "about a minute on ~600 DAGs.")
        chosen["infer_sensors"] = p.confirm("Infer sensor dependencies?",
                                            bool(disc and disc.infer_sensors))
    # highlight
    if given.get("highlight") is not None:
        chosen["highlight"] = given["highlight"]
        p.ok(f"highlight: {chosen['highlight']} (from --highlight)")
    else:
        while True:
            pattern = p.ask("Highlight DAG ids matching a regex, - for none", "",
                            lambda raw: "" if raw in ("-", "0") else validate_regex(raw))
            if not pattern or not disc:
                break
            hits = [d for d in disc.dag_ids if re.search(pattern, d)]
            more = "…" if len(hits) > 5 else ""
            p.say(f"matches {plural(len(hits), 'DAG')}: {', '.join(hits[:5])}{more}")
            if p.confirm("Use it?", True):
                break
        chosen["highlight"] = pattern
    if chosen["highlight"]:
        if given.get("highlight_label") is not None:
            chosen["highlight_label"] = given["highlight_label"]
            p.ok(f"highlight_label: {chosen['highlight_label']} (from --highlight-label)")
        else:
            label = p.ask("Legend label for the highlighted DAGs", _DEFAULTS["highlight_label"])
            chosen["highlight_label"] = None if label == _DEFAULTS["highlight_label"] else label
    # title
    if given.get("title") is not None:
        chosen["title"] = given["title"]
        p.ok(f"title: {chosen['title']} (from --title)")
    else:
        title = p.ask("Page title", _DEFAULTS["title"])
        chosen["title"] = None if title == _DEFAULTS["title"] else title


def _ask_advanced(p: Prompter, given: Mapping[str, object], chosen: dict) -> None:
    def text(v: object) -> str:
        return f"{v:g}" if isinstance(v, float) else str(v)

    def timeout_ok(raw: str) -> float:
        v = _positive(float, "timeout", 0, strict=True)(raw)
        if not math.isfinite(v):
            raise ValueError("timeout must be a finite number")
        return v

    # key, question, default value, parser, flag
    items = [
        ("tests_task_regex", "Regex that marks a task as a test task",
         _DEFAULTS["tests_task_regex"], validate_regex, "--tests-task-regex"),
        ("task_query_regex", "Regex for --tasks-preset, Enter for none", "", validate_regex,
         "--task-query-regex"),
        ("lookback", "Recent runs to look at for the status", _DEFAULTS["status_runs_lookback"],
         _positive(int, "lookback", 1), "--lookback"),
        ("timeout", "Seconds to wait for one API request", float(_DEFAULTS["timeout"]),
         timeout_ok, "--timeout"),
        ("workers", "Parallel API requests", _DEFAULTS["workers"],
         _positive(int, "workers", 1), "--workers"),
    ]
    todo = []
    for key, label, default, parse, flag in items:
        if given.get(key) is not None:
            chosen[key] = given[key]
            p.ok(f"{key}: {given[key]} (from {flag})")
        else:
            todo.append((key, label, default, parse))
    if not todo or not p.confirm("Change advanced settings?", False):
        return
    for key, label, default, parse in todo:
        v = p.ask(label, text(default), parse)
        if v != default and v != "":
            chosen[key] = v
