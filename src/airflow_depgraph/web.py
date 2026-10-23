"""Airflow-free core of the in-Airflow page: token lookup, loopback URL, cached page build."""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Callable, Mapping
from urllib.parse import urlparse

from . import snapshot as snapmod
from .client import AirflowClient, ApiError
from .presets import Settings
from .render import render
from .sensors import fetch_tasks, infer_sensor_edges
from .status import fetch_status

TOKEN_COOKIE = "_token"
CACHE_SECONDS = 300
CACHE_ENTRIES = 64
MAX_BUILDS = 3
CLIENT_TIMEOUT = 10
INFER_WORKERS = 4
INFER_POLL_SECONDS = 30
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


class InferenceCache:
    """Latest inferred sensor links, refreshed in a daemon thread with a recent user's token.

    The thread starts on the first `offer()`. Each cycle uses the most recent usable token seen;
    without one the cycle is skipped. Tokens are kept in memory only and never logged.
    """

    def __init__(self, api_url: str, interval_seconds: float, *,
                 client_factory: Callable[..., AirflowClient] = AirflowClient,
                 builds: threading.Semaphore | None = None,
                 wall_clock: Callable[[], float] = time.time,
                 start_thread: bool = True):
        self.api_url, self.interval = api_url, interval_seconds
        self._client_factory, self._wall = client_factory, wall_clock
        self._builds = builds or threading.BoundedSemaphore(1)
        self._start_thread = start_thread
        self._lock = threading.Lock()
        self._token: str | None = None
        self._result: tuple[list[dict], float] | None = None
        self._attempted: float | None = None
        self._thread: threading.Thread | None = None

    def offer(self, token: str) -> None:
        """Remember the caller's token for the next cycle and start the thread if needed."""
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
        with self._lock:
            return self._result

    def due(self) -> bool:
        with self._lock:
            return self._attempted is None or self._wall() - self._attempted >= self.interval

    def _loop(self) -> None:
        while True:
            if self.due():
                self.refresh()
            time.sleep(INFER_POLL_SECONDS)

    def refresh(self) -> bool:
        """One cycle; False when it was skipped (no usable token, retried soon) or failed."""
        with self._lock:
            token = self._token
            if not token or not token_usable(token, self._wall()):
                return False
            self._attempted = self._wall()
        try:
            with self._builds:
                inferred = self._infer(token)
        except ApiError as exc:
            if exc.status in (401, 403):
                with self._lock:
                    if self._token == token:
                        self._token = None
            log.warning("airflow-depgraph: sensor inference skipped: %s", exc)
            return False
        except Exception:  # noqa: BLE001 - the thread must survive any one bad cycle
            log.exception("airflow-depgraph: sensor inference failed")
            return False
        with self._lock:
            self._result = (inferred, self._wall())
        return True

    def _infer(self, token: str) -> list[dict]:
        client = self._client_factory(self.api_url, token=token, timeout=CLIENT_TIMEOUT)
        catalog = snapmod.build_catalog(client.list_dags())
        ids = sorted(d for d, c in catalog.items() if not c.get("is_stale"))
        tasks = fetch_tasks(client, ids, workers=INFER_WORKERS)
        return infer_sensor_edges(client, ids, tasks, workers=INFER_WORKERS)


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
                api_url, max(1, settings.infer_interval_minutes) * 60, builds=self._builds,
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
        if self.inference is not None:
            self.inference.offer(token)
        key = (hashlib.sha256(token.encode()).hexdigest(), with_status)
        if (html := self._hit(key)) is not None:
            return html
        with self._lock:
            key_lock = self._key_locks.setdefault(key, threading.Lock())
        try:
            with key_lock:                      # one in-flight build per key
                if (html := self._hit(key)) is not None:
                    return html
                with self._builds:
                    html = self._build(token, public_url, with_status)
                self._store(key, html)
                return html
        finally:
            with self._lock:
                self._key_locks.pop(key, None)

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
                note = "sensor links: first refresh running"
            else:
                # What this viewer may see: their own /ui/dependencies DAGs plus their catalog.
                visible = {n["id"][4:] for n in nodes if n["type"] == "dag"} | set(snap.catalog or {})
                snap.inferred = snapmod.visible_inferred(cached[0], visible)
                updated = datetime.fromtimestamp(cached[1], timezone.utc).strftime("%H:%M UTC")
        if with_status:
            snap.dags = fetch_status(client, snap.graph.dag_ids, self.settings, with_tasks=False,
                                     catalog=snap.catalog)
        return render(snap, self.settings, inferred_note=note, inferred_updated=updated)
