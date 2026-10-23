"""TOML presets and settings resolution (CLI > env > preset file > default)."""

from __future__ import annotations

import dataclasses
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import resources
from pathlib import Path

ENV_PREFIX = "AIRFLOW_DEPGRAPH_"
_TRUE = {"1", "true", "yes"}


class PresetError(ValueError):
    pass


@dataclass(frozen=True)
class Settings:
    url: str | None = None
    user: str | None = None
    user_env: str = "AIRFLOW_DEPGRAPH_USER"
    password_env: str = "AIRFLOW_DEPGRAPH_PASSWORD"
    password_cmd: str | None = None
    token: str | None = None
    with_status: bool = False
    with_tasks: bool = False
    title: str = "Airflow dependency graph"
    asset_label_strip_prefix: str | None = None
    highlight: str | None = None
    highlight_label: str = "Highlight"
    tests_task_regex: str = r"(^|[._-])tests?([._-]|$)"
    task_query_regex: str | None = None
    status_runs_lookback: int = 5
    tags: tuple[str, ...] = ()
    infer_sensors: bool = False
    infer_interval_minutes: int = 30
    timeout: float = 25             # seconds per API request
    workers: int = 8                # parallel API requests (status, task lists, sensors)
    preset_name: str | None = None


_FIELDS = {f.name: f for f in dataclasses.fields(Settings)}
PRESET_KEYS: frozenset[str] = frozenset(_FIELDS) - {"user", "token", "preset_name"}
# "name" is accepted in a preset file and stored as Settings.preset_name.
_FILE_KEYS = PRESET_KEYS | {"name"}


def _lookup_dirs(env: Mapping[str, str]) -> list[Path]:
    dirs: list[Path] = []
    if env.get(ENV_PREFIX + "PRESETS"):
        dirs.append(Path(env[ENV_PREFIX + "PRESETS"]))
    xdg = env.get("XDG_CONFIG_HOME") or str(Path.home() / ".config")
    dirs.append(Path(xdg) / "airflow-depgraph" / "presets")
    dirs.append(Path(str(resources.files("airflow_depgraph") / "presets")))
    return dirs


def find_preset(name_or_path: str, env: Mapping[str, str]) -> Path:
    if "/" in name_or_path or name_or_path.endswith(".toml"):
        path = Path(name_or_path)
        if not path.is_file():
            raise PresetError(f"preset file not found: {path}")
        return path
    dirs = _lookup_dirs(env)
    for d in dirs:
        candidate = d / f"{name_or_path}.toml"
        if candidate.is_file():
            return candidate
    looked = ", ".join(str(d) for d in dirs)
    raise PresetError(f"preset '{name_or_path}' not found; looked in: {looked}")


def load_preset(path: Path) -> dict:
    try:
        data = tomllib.loads(Path(path).read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as e:
        raise PresetError(f"invalid TOML in {path}: {e}") from e
    for key in data:
        if key not in _FILE_KEYS:
            raise PresetError(f"unknown key '{key}' in {path}")
    return data


def list_presets(env: Mapping[str, str]) -> list[tuple[str, Path]]:
    found: dict[str, Path] = {}
    for d in _lookup_dirs(env):
        if not d.is_dir():
            continue
        for f in sorted(d.glob("*.toml")):
            found.setdefault(f.stem, f)
    return sorted(found.items())


def _coerce_tags(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return tuple(t.strip() for t in value.split(",") if t.strip())
    if isinstance(value, (list, tuple)) and all(isinstance(t, str) for t in value):
        return tuple(value)
    raise PresetError(f"tags must be a list of strings, got {value!r}")


def _coerce(field: str, value: object) -> object:
    if field == "tags":
        return _coerce_tags(value)
    ftype = _FIELDS[field].type
    if isinstance(value, str):
        if ftype == "bool":
            return value.strip().lower() in _TRUE
        if ftype == "int":
            try:
                return int(value)
            except ValueError:
                raise PresetError(f"{field} must be an integer, got {value!r}") from None
        if ftype == "float":
            try:
                return float(value)
            except ValueError:
                raise PresetError(f"{field} must be a number, got {value!r}") from None
        return value
    if ftype == "int" and (isinstance(value, bool) or not isinstance(value, int)):
        raise PresetError(f"{field} must be an integer, got {value!r}")
    if ftype == "float" and isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return value


def resolve(cli: Mapping[str, object], env: Mapping[str, str], preset: str | None) -> Settings:
    if preset is None:
        preset = env.get(ENV_PREFIX + "PRESET") or None
    from_file: dict = {}
    if preset is not None and preset != "none":
        from_file = dict(load_preset(find_preset(preset, env)))
        if "name" in from_file:
            from_file["preset_name"] = from_file.pop("name")
    values: dict[str, object] = {}
    for field in _FIELDS:
        if field == "preset_name":
            if "preset_name" in from_file:
                values[field] = from_file["preset_name"]
            continue
        env_key = ENV_PREFIX + field.upper()
        if cli.get(field) is not None:
            values[field] = _coerce(field, cli[field])
        elif env.get(env_key):
            values[field] = _coerce(field, env[env_key])
        elif field in from_file:
            if field == "tags" and not isinstance(from_file[field], list):
                raise PresetError(f"tags must be a list of strings, got {from_file[field]!r}")
            values[field] = _coerce(field, from_file[field])
    settings = Settings(**values)
    if settings.timeout <= 0:
        raise PresetError(f"timeout must be greater than 0, got {settings.timeout:g}")
    if settings.workers < 1:
        raise PresetError(f"workers must be at least 1, got {settings.workers}")
    return settings
