"""Airflow-free core of the in-Airflow page: token lookup, loopback URL, cached page build."""
from __future__ import annotations

import base64
import hashlib
import json
import threading
import time
from collections import OrderedDict
from typing import Callable, Mapping
from urllib.parse import urlparse

from . import snapshot as snapmod
from .client import AirflowClient
from .presets import Settings
from .render import render
from .status import fetch_status

TOKEN_COOKIE = "_token"
CACHE_SECONDS = 300
CACHE_ENTRIES = 64
MAX_BUILDS = 3
CLIENT_TIMEOUT = 10


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
        if with_status:
            snap.dags = fetch_status(client, snap.graph.dag_ids, self.settings, with_tasks=False)
        return render(snap, self.settings)
