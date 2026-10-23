"""Airflow-free core of the in-Airflow page: token lookup, loopback URL, cached page build."""
from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import logging
import os
import stat
import tempfile
import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping
from urllib.parse import urlparse

from . import snapshot as snapmod
from .client import AirflowClient, ApiError
from .presets import Settings
from .render import render
from .sensors import fetch_tasks, infer_sensor_edges

try:
    import fcntl
except ImportError:  # not POSIX: every process refreshes on its own
    fcntl = None
from .status import fetch_status

TOKEN_COOKIE = "_token"
CACHE_SECONDS = 300
CACHE_ENTRIES = 64
MAX_BUILDS = 3
CLIENT_TIMEOUT = 10
INFER_WORKERS = 4
INFER_POLL_SECONDS = 30
INFER_RETRY_SECONDS = 300
TOKEN_MARGIN_SECONDS = 15 * 60
MAX_ERROR_SHARE = 0.2
MIN_CATALOG_SHARE = 0.8         # a cycle seeing fewer DAGs than this share keeps the old result
CATALOG_COUNT_MAX_AGE = 6 * 3600   # ... unless the previous count is older, or it happens twice
STATE_DIR_ENV = "AIRFLOW_DEPGRAPH_STATE_DIR"
LOCK_WARN_SECONDS = 600
_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
log = logging.getLogger(__name__)


def extract_token(cookies: Mapping[str, str], headers: Mapping[str, str]) -> str | None:
    """The signed-in user's JWT: Authorization: Bearer header first, then the UI cookie."""
    auth = headers.get("authorization") or headers.get("Authorization") or ""
    if auth.lower().startswith("bearer ") and auth[7:].strip():
        return auth[7:].strip()
    return cookies.get(TOKEN_COOKIE) or None


def token_usable(token: str, now: float | None = None) -> bool:
    """False for a token that is not a JWT or is past its `exp` (signature is Airflow's job)."""
    parts = token.split(".")
    if len(parts) != 3:
        return False
    try:
        claims = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
    except (ValueError, TypeError):
        return False
    if not isinstance(claims, dict):
        return False
    exp = claims.get("exp")
    return not isinstance(exp, (int, float)) or exp > (time.time() if now is None else now)


def base_path(base_url: str | None) -> str:
    """Path prefix of `[api] base_url`, e.g. 'http://host/airflow/' -> '/airflow'."""
    return urlparse(base_url or "").path.rstrip("/")


def loopback_url(base_url: str | None, port: int | str, override: str | None = None) -> str:
    """Where this api-server answers its own API from inside the container."""
    if override:
        return override.rstrip("/")
    return f"http://127.0.0.1:{port}{base_path(base_url)}"


def _private_dir(path: Path) -> bool:
    """Create `path` (0700) if missing; True only for a real directory owned by this user that
    nobody else may write to (no symlink, no group/other write bit)."""
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        st = os.lstat(path)
    except OSError:
        return False
    return (stat.S_ISDIR(st.st_mode) and not stat.S_ISLNK(st.st_mode)
            and st.st_uid == os.geteuid() and not st.st_mode & (stat.S_IWGRP | stat.S_IWOTH))


class InferenceCache:
    """Latest inferred sensor links, refreshed in a daemon thread with a recent user's token.

    `offer()` takes tokens Airflow has just accepted and starts the thread. Only one process per
    host and Airflow instance refreshes: the holder of an flock on the lock file in a private
    state dir (owned by this user, mode 0700). It writes the result to the state file there, and
    every process serves the newest content of that file. File names carry a hash of the API URL.
    Without a safe state dir or fcntl, each process refreshes on its own. Tokens stay in the
    process that received them and are never logged or written anywhere.
    """

    def __init__(self, api_url: str, interval_seconds: float, *,
                 client_factory: Callable[..., AirflowClient] = AirflowClient,
                 wall_clock: Callable[[], float] = time.time,
                 start_thread: bool = True, state_dir: str | os.PathLike | None = None,
                 shared: bool | None = None):
        self.api_url, self.interval = api_url, interval_seconds
        self._client_factory, self._wall = client_factory, wall_clock
        self._builds = threading.BoundedSemaphore(1)      # inference has its own slot
        self._start_thread = start_thread
        explicit = state_dir or os.environ.get(STATE_DIR_ENV)
        uid = f"-{os.geteuid()}" if hasattr(os, "geteuid") else ""
        self.state_dir = Path(explicit) if explicit else Path(tempfile.gettempdir()) / f"airflow-depgraph{uid}"
        tag = hashlib.sha256(api_url.encode()).hexdigest()[:12]
        self.lock_name = f"airflow-depgraph-infer-{tag}.lock"
        self.state_name = f"airflow-depgraph-inferred-{tag}.json"
        self._shared = (fcntl is not None) if shared is None else shared
        if self._shared and not _private_dir(self.state_dir):
            log.warning("airflow-depgraph: state dir %s is not a private directory of this user; "
                        "every api-server process refreshes sensor links on its own", self.state_dir)
            self._shared = False
        self._dag_count: int | None = None
        self._low_pending = False          # the last cycle was rejected for a low DAG count
        self._lock_warned: float | None = None
        self._lock = threading.Lock()
        self._token: str | None = None
        self._result: tuple[list[dict], float] | None = None
        self._attempted: float | None = None
        self._file_seen: tuple[int, int] | None = None
        self._lock_fd: int | None = None
        self._thread: threading.Thread | None = None

    def offer(self, token: str) -> None:
        """Remember a token Airflow accepted for the next cycle; start the thread if needed."""
        if not token_usable(token, self._wall()):
            return
        with self._lock:
            self._token = token
            if self._start_thread and self._thread is None:
                self._thread = threading.Thread(target=self._loop, name="depgraph-infer",
                                                daemon=True)
                self._thread.start()

    def current(self) -> tuple[list[dict], float] | None:
        """(inferred list, wall-clock time of the refresh) or None before the first refresh."""
        if self._shared:
            self._reload()
        with self._lock:
            return self._result

    def _reload(self) -> None:
        try:
            fd = os.open(self.state_dir / self.state_name, os.O_RDONLY | _NOFOLLOW)
        except OSError:
            return
        with os.fdopen(fd, "r", encoding="utf-8") as f:
            try:
                st = os.fstat(f.fileno())
                if st.st_uid != os.geteuid() or (st.st_mtime_ns, st.st_size) == self._file_seen:
                    return
                data = json.load(f)
                entries, at = data["entries"], float(data["updated_at"])
                count = data.get("dag_count")
            except (OSError, ValueError, KeyError, TypeError):
                return
        if isinstance(entries, list) and data.get("api_url") == self.api_url:
            with self._lock:
                self._file_seen, self._result = (st.st_mtime_ns, st.st_size), (entries, at)
                if isinstance(count, int):
                    self._dag_count = count

    def _publish(self, entries: list[dict], at: float, dag_count: int) -> None:
        path = self.state_dir / self.state_name
        try:
            fd, tmp = tempfile.mkstemp(dir=self.state_dir, prefix=self.state_name + ".", suffix=".tmp")
        except FileNotFoundError:        # state dir removed meanwhile: recreate (same checks), retry
            if not _private_dir(self.state_dir):
                raise
            fd, tmp = tempfile.mkstemp(dir=self.state_dir, prefix=self.state_name + ".", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump({"api_url": self.api_url, "updated_at": at, "dag_count": dag_count,
                           "entries": entries}, f)
            os.replace(tmp, path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            raise
        st = os.stat(path, follow_symlinks=False)
        with self._lock:
            self._file_seen = (st.st_mtime_ns, st.st_size)

    def _open_in_state_dir(self, name: str, flags: int) -> int:
        """os.open in the state dir; if the dir was removed meanwhile, recreate it (same checks)
        and try once more."""
        try:
            return os.open(self.state_dir / name, flags | _NOFOLLOW, 0o600)
        except FileNotFoundError:
            if not _private_dir(self.state_dir):
                raise
            return os.open(self.state_dir / name, flags | _NOFOLLOW, 0o600)

    def _holds_lock_file(self) -> bool:
        """The held lock is still the file at the lock path (not one removed with its dir)."""
        try:
            held = os.fstat(self._lock_fd)
            now = os.stat(self.state_dir / self.lock_name, follow_symlinks=False)
        except OSError:
            return False
        return (held.st_dev, held.st_ino) == (now.st_dev, now.st_ino)

    def leader(self) -> bool:
        """True when this process refreshes (holds the host-wide lock, or nothing is shared)."""
        if not self._shared:
            return True
        if self._lock_fd is not None:
            if self._holds_lock_file():
                return True
            self.close()
        try:
            fd = self._open_in_state_dir(self.lock_name, os.O_RDWR | os.O_CREAT)
        except OSError as exc:
            now = time.monotonic()
            if self._lock_warned is None or now - self._lock_warned >= LOCK_WARN_SECONDS:
                self._lock_warned = now
                log.warning("airflow-depgraph: cannot open the inference lock file: %s", exc)
            return False
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            return False
        self._lock_fd = fd
        return True

    def close(self) -> None:
        """Give up leadership (a process exit does the same)."""
        if self._lock_fd is not None:
            os.close(self._lock_fd)
            self._lock_fd = None

    def due(self) -> bool:
        with self._lock:
            return self._attempted is None or self._wall() - self._attempted >= self.interval

    def _loop(self) -> None:
        while True:
            if self.due():
                self.refresh()
            time.sleep(INFER_POLL_SECONDS)

    def _retry_later(self) -> None:
        with self._lock:      # failed for another reason than auth: try again in a few minutes
            self._attempted = self._wall() - self.interval + INFER_RETRY_SECONDS

    def refresh(self) -> bool:
        """One cycle. False when skipped (no token with 15 min left, not the leader) or failed;
        the previous result is kept then."""
        with self._lock:
            token = self._token
        if not token or not token_usable(token, self._wall() + TOKEN_MARGIN_SECONDS):
            return False
        if not self.leader():
            return False
        try:
            with self._builds:
                inferred, dag_count = self._infer(token)
        except ApiError as exc:
            # A 401 anywhere, or a 403 from the first call (list_dags): the token is no good.
            if exc.status in (401, 403):
                with self._lock:
                    if self._token == token:
                        self._token = None
                    self._attempted = None          # the next accepted visitor refreshes at once
            else:
                self._retry_later()
            log.warning("airflow-depgraph: sensor inference aborted: %s", exc)
            return False
        except Exception:  # noqa: BLE001 - the thread must survive any one bad cycle
            log.exception("airflow-depgraph: sensor inference failed")
            self._retry_later()
            return False
        with self._lock:
            previous = self._dag_count if self._result is not None else None
            if previous and self._wall() - self._result[1] > CATALOG_COUNT_MAX_AGE:
                previous = None
            low = bool(previous) and dag_count < MIN_CATALOG_SHARE * previous
            reject = low and not self._low_pending   # a second low cycle in a row is believed
            self._low_pending = reject
        if reject:
            log.warning("airflow-depgraph: this refresh saw %d DAGs, the previous one %d (a visitor "
                        "with fewer permissions?); keeping the previous sensor links",
                        dag_count, previous)
            self._retry_later()
            return False
        errors = sum(1 for e in inferred if e.get("source") == "error")
        if errors > MAX_ERROR_SHARE * len(inferred):
            log.warning("airflow-depgraph: %d of %d sensors failed to resolve; keeping the "
                        "previous sensor links", errors, len(inferred))
            self._retry_later()
            return False
        at = self._wall()
        if self._shared:
            try:
                self._publish(inferred, at, dag_count)
            except OSError as exc:
                log.warning("airflow-depgraph: cannot write the inference state file: %s", exc)
        with self._lock:
            self._result, self._attempted, self._dag_count = (inferred, at), at, dag_count
        return True

    def _infer(self, token: str) -> tuple[list[dict], int]:
        """(inferred entries, number of live DAGs this token sees)."""
        client = self._client_factory(self.api_url, token=token, timeout=CLIENT_TIMEOUT)
        catalog = snapmod.build_catalog(client.list_dags())
        ids = sorted(d for d, c in catalog.items() if not c.get("is_stale"))
        tasks = fetch_tasks(client, ids, workers=INFER_WORKERS)
        return infer_sensor_edges(client, ids, tasks, workers=INFER_WORKERS), len(ids)


class PageBuilder:
    """Builds the page with the caller's token; pages are cached per token (bounded LRU, TTL)."""

    def __init__(self, api_url: str, settings: Settings, *,
                 client_factory: Callable[..., AirflowClient] = AirflowClient,
                 clock: Callable[[], float] = time.monotonic, cache_seconds: int = CACHE_SECONDS,
                 max_entries: int = CACHE_ENTRIES):
        self.api_url, self.settings = api_url, settings
        self._client_factory, self._clock, self._ttl = client_factory, clock, cache_seconds
        self._max = max_entries
        self._cache: OrderedDict[tuple[str, bool], tuple[float, str]] = OrderedDict()
        self._lock = threading.Lock()
        self._key_locks: dict[tuple[str, bool], threading.Lock] = {}
        self._builds = threading.BoundedSemaphore(MAX_BUILDS)
        self.inference: InferenceCache | None = None
        if settings.infer_sensors:
            self.inference = InferenceCache(
                api_url, max(1, settings.infer_interval_minutes) * 60,
                client_factory=lambda *a, **kw: self._client_factory(*a, **kw))

    def _hit(self, key: tuple[str, bool]) -> str | None:
        with self._lock:
            hit = self._cache.get(key)
            return hit[1] if hit and self._clock() - hit[0] < self._ttl else None

    def _store(self, key: tuple[str, bool], html: str) -> None:
        with self._lock:
            now = self._clock()
            self._cache[key] = (now, html)
            self._cache.move_to_end(key)
            for k in [k for k, (at, _) in self._cache.items() if now - at >= self._ttl]:
                del self._cache[k]
            while len(self._cache) > self._max:
                self._cache.popitem(last=False)

    def page(self, token: str, public_url: str | None, with_status: bool) -> str:
        key = (hashlib.sha256(token.encode()).hexdigest(), with_status)
        if (html := self._hit(key)) is not None:
            self._offer(token)                  # Airflow accepted this token for that page
            return html
        with self._lock:
            key_lock = self._key_locks.setdefault(key, threading.Lock())
        try:
            with key_lock:                      # one in-flight build per key
                if (html := self._hit(key)) is not None:
                    self._offer(token)
                    return html
                with self._builds:
                    html = self._build(token, public_url, with_status)
                self._store(key, html)
                self._offer(token)
                return html
        finally:
            with self._lock:
                self._key_locks.pop(key, None)

    def _offer(self, token: str) -> None:
        if self.inference is not None:
            self.inference.offer(token)

    def _build(self, token: str, public_url: str | None, with_status: bool) -> str:
        client = self._client_factory(self.api_url, token=token, timeout=CLIENT_TIMEOUT)
        version = client.version()
        nodes, edges = snapmod.validate_payload(client.dependencies(), version)
        snap = snapmod.new(nodes, edges, public_url, version)
        try:
            snap.catalog = snapmod.build_catalog(client.list_dags())
        except ApiError as exc:
            log.warning("airflow-depgraph: cannot list DAGs, page filters degraded: %s", exc)
        note = updated = None
        if self.inference is not None:
            cached = self.inference.current()
            if cached is None:
                note = "inferred links: first refresh running"
            else:
                # What this viewer may see: their own /ui/dependencies DAGs plus their catalog.
                visible = {n["id"][4:] for n in nodes if n["type"] == "dag"} | set(snap.catalog or {})
                snap.inferred = snapmod.visible_inferred(cached[0], visible)
                updated = datetime.fromtimestamp(cached[1], timezone.utc).strftime("%H:%M UTC")
        if with_status:
            snap.dags = fetch_status(client, snap.graph.dag_ids, self.settings, with_tasks=False,
                                     catalog=snap.catalog)
        return render(snap, self.settings, inferred_note=note, inferred_updated=updated)
