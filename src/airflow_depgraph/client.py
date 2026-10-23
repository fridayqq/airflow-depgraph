"""Minimal Airflow 3 REST client (stdlib only)."""

from __future__ import annotations

import http.client
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from io import BytesIO

from . import __version__

USER_AGENT = f"airflow-depgraph/{__version__}"
RETRY_STATUSES = frozenset({429, 502, 503, 504})
RETRIES = 2                 # extra attempts after the first one
BACKOFF = (1, 3)            # seconds before the first and the second retry
MAX_RETRY_AFTER = 30
# Every task instance state except skipped, removed and none (the empty ones): a run with any
# other task state counts as decisive, the same rule classify_runs applies.
NONEMPTY_TI_STATES = ("success", "failed", "upstream_failed", "running", "queued", "scheduled",
                      "up_for_retry", "up_for_reschedule", "deferred", "restarting")
RUNS_PATH = "/api/v2/dags/~/dagRuns/list"
TASKS_PATH = "/api/v2/dags/~/dagRuns/~/taskInstances/list"
BULK_PAGE = 100             # page_limit and the most ids sent in one bulk request
REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_noted_redirects: set[str] = set()   # URLs already reported as redirecting, once per process
_PROXY_VARS = ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None            # the 3xx stays an HTTPError instead of being replayed


_NO_REDIRECT = urllib.request.build_opener(_NoRedirect)


def _proxied() -> bool:
    # http.client ignores proxy variables and urllib honours them, so with a proxy every request
    # goes through urlopen instead of the pooled connection.
    return any(os.environ.get(v) for v in _PROXY_VARS)


def _chunks(items: list, size: int) -> list[list]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def _note_redirect(url: str, location: str) -> None:
    if url in _noted_redirects:
        return
    _noted_redirects.add(url)
    print(f"note: {url} redirects to {location}; set the final URL to avoid the extra hop",
          file=sys.stderr)


def _retry_after(headers) -> float | None:
    """Numeric Retry-After in seconds, capped; None for a missing or HTTP-date value."""
    try:
        seconds = float(headers.get("Retry-After")) if headers is not None else None
    except (TypeError, ValueError):
        return None
    return None if seconds is None else min(max(seconds, 0.0), MAX_RETRY_AFTER)


class ApiError(RuntimeError):
    def __init__(self, message: str, status: int | None = None, path: str = "",
                 network: bool = False):
        super().__init__(message)
        self.status = status
        self.path = path
        self.network = network      # no response at all: timeout, refused, DNS


def _ti_key(ti: dict) -> tuple:
    """Mapped tasks share a task_id; map_index tells their instances apart."""
    map_index = ti.get("map_index")
    return ti.get("task_id"), -1 if map_index is None else map_index


class AirflowClient:
    def __init__(self, url: str, token: str | None = None, timeout: float = 25,
                 opener=None):
        self.url = url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self._opener = opener          # tests inject one; None means the pooled connections
        self._base = urllib.parse.urlsplit(self.url)
        self._local = threading.local()  # one HTTP connection per thread

    @classmethod
    def login(cls, url: str, user: str, password: str, **kw) -> "AirflowClient":
        client = cls(url, **kw)
        path = "/auth/token"
        body = json.dumps({"username": user, "password": password}).encode()
        req = urllib.request.Request(
            client.url + path,
            data=body,
            method="POST",
            headers={"Content-Type": "application/json", "Accept": "application/json",
                     "User-Agent": USER_AGENT},
        )
        try:
            data = client._send(req, path)
        except ApiError as e:
            if e.status in (401, 403):
                raise ApiError(
                    f"authentication failed ({e.status}): check username/password",
                    e.status, path,
                ) from None
            raise
        client.token = data.get("access_token")
        if not client.token:
            raise ApiError("login response has no access_token", None, path)
        return client

    def _send(self, req: urllib.request.Request, path: str):
        # The login POST is sent once; only a connection error before any response is retried.
        login = path == "/auth/token"
        retries = 1 if login else RETRIES
        for attempt in range(retries + 1):
            last = attempt == retries
            try:
                body = self._fetch(req)
                break
            except urllib.error.HTTPError as e:
                if last or login or e.code not in RETRY_STATUSES:
                    raise self._http_error(e, path) from None
                wait = _retry_after(e.headers)
            except (OSError, http.client.HTTPException) as e:
                if last:
                    raise self._network_error(e, path) from None
                wait = None
            time.sleep(BACKOFF[attempt] if wait is None else wait)
        try:
            return json.loads(body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ApiError(
                f"non-JSON response from {path} — wrong URL or a login/proxy page?", None, path
            ) from None

    def _http_error(self, e: urllib.error.HTTPError, path: str) -> ApiError:
        if path == "/auth/token" and e.code in REDIRECT_STATUSES:
            location = e.headers.get("Location", "") if e.headers is not None else ""
            return ApiError(f"login redirected to {location}; use the final URL", e.code, path)
        if e.code == 403 and path == "/ui/dependencies":
            msg = ("403 on /ui/dependencies: this user cannot see DAGs "
                   "(Airflow ≥ 3.1.8 filters by DAG permissions)")
        else:
            msg = f"{e.code} on {path}: {e.reason}"
        return ApiError(msg, e.code, path)

    def _network_error(self, e: Exception, path: str) -> ApiError:
        if isinstance(e, TimeoutError):
            return ApiError(
                f"timed out after {self.timeout:g}s waiting for {self.url}{path}", None, path,
                network=True)
        if isinstance(e, urllib.error.URLError):
            return ApiError(
                f"no response from {self.url} ({e.reason}) — VPN/DNS?", None, path, network=True)
        return ApiError(f"network error on {path}: {e}", None, path, network=True)

    def _fetch(self, req: urllib.request.Request) -> bytes:
        """Response body of one request; HTTP error statuses raise urllib.error.HTTPError."""
        if self._opener is not None:
            with self._opener(req, timeout=self.timeout) as resp:
                return resp.read()
        if _proxied():
            # Only GET requests may be followed; a redirected POST would be replayed as GET.
            opener = urllib.request.urlopen if req.get_method() == "GET" else _NO_REDIRECT.open
            with opener(req, timeout=self.timeout) as resp:
                return resp.read()
        return self._pooled(req)

    def _pooled(self, req: urllib.request.Request) -> bytes:
        parts = urllib.parse.urlsplit(req.full_url)
        target = parts.path + (f"?{parts.query}" if parts.query else "")
        headers = dict(req.header_items())
        for attempt in (1, 2):
            conn = self._connection()
            try:
                conn.request(req.get_method(), target, body=req.data, headers=headers)
                resp = conn.getresponse()
                body = resp.read()
            except (http.client.RemoteDisconnected, BrokenPipeError, ConnectionResetError):
                # The server closed an idle keep-alive connection: a GET is sent again on a new
                # connection; anything else is not replayed here.
                self._drop()
                if attempt == 2 or req.get_method() != "GET":
                    raise
                continue
            except BaseException:
                self._drop()
                raise
            if resp.status in REDIRECT_STATUSES and req.get_method() == "GET":
                # http.client does not follow redirects; this one GET goes through urlopen,
                # which does. Fixing the URL in the configuration saves the extra hop.
                _note_redirect(req.full_url, resp.getheader("Location") or "")
                with urllib.request.urlopen(req, timeout=self.timeout) as followed:
                    return followed.read()
            if not 200 <= resp.status < 300:
                raise urllib.error.HTTPError(req.full_url, resp.status, resp.reason,
                                             resp.headers, BytesIO(body))
            return body
        raise AssertionError("unreachable")

    def _connection(self) -> http.client.HTTPConnection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            cls = (http.client.HTTPSConnection if self._base.scheme == "https"
                   else http.client.HTTPConnection)
            conn = cls(self._base.hostname, self._base.port, timeout=self.timeout)
            self._local.conn = conn
        return conn

    def _drop(self) -> None:
        conn = getattr(self._local, "conn", None)
        self._local.conn = None
        if conn is not None:
            conn.close()

    def get(self, path: str, **query) -> dict:
        full = self.url + path
        if query:
            full += "?" + urllib.parse.urlencode(query, doseq=True)
        headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return self._send(urllib.request.Request(full, headers=headers), path)

    def dependencies(self, node_id: str | None = None) -> dict:
        if node_id:
            return self.get("/ui/dependencies", node_id=node_id)
        return self.get("/ui/dependencies")

    def health(self) -> dict:
        return self.get("/api/v2/monitor/health")

    def version(self) -> str | None:
        try:
            return self.get("/api/v2/version").get("version")
        except ApiError:
            return None

    @staticmethod
    def _q(value: str) -> str:
        return urllib.parse.quote(value, safe="")

    def list_dags(self, page_size: int = 100) -> list[dict]:
        """Every DAG the user may see, stale ones included (they are dropped at render time)."""
        out: list[dict] = []
        while True:
            data = self.get("/api/v2/dags", limit=page_size, offset=len(out), exclude_stale="false")
            page = data.get("dags") or []
            out.extend(page)
            total = data.get("total_entries")
            done = len(out) >= total if isinstance(total, int) else len(page) < page_size
            if not page or done:
                return out

    def post(self, path: str, body: dict) -> dict:
        headers = {"Content-Type": "application/json", "Accept": "application/json",
                   "User-Agent": USER_AGENT}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        req = urllib.request.Request(self.url + path, data=json.dumps(body).encode(),
                                     method="POST", headers=headers)
        return self._send(req, path)

    def bulk_dag_runs(self, dag_ids: list[str], since_iso: str,
                      per_dag: int | None = None) -> list[dict]:
        """Runs of the DAGs started since `since_iso`, newest first overall. With `per_dag`,
        paging stops once every DAG has that many runs (the newest ones come first)."""
        out: list[dict] = []
        for chunk in _chunks(list(dag_ids), BULK_PAGE):
            counts: Counter = Counter()
            offset = 0
            while True:
                data = self.post(RUNS_PATH, {
                    "dag_ids": chunk, "order_by": "-run_after", "page_limit": BULK_PAGE,
                    "page_offset": offset, "run_after_gte": since_iso})
                page = data.get("dag_runs") or []
                out.extend(page)
                counts.update(r.get("dag_id") for r in page)
                offset += len(page)
                total = data.get("total_entries")
                if (not page or (isinstance(total, int) and offset >= total)
                        or (per_dag is not None and all(counts[d] >= per_dag for d in chunk))):
                    break
        return out

    def bulk_task_instances(self, dag_ids: list[str], run_ids: list[str],
                            states: list[str] | tuple[str, ...]) -> list[dict]:
        """Task instances of the given runs in the given states, paged."""
        out: list[dict] = []
        for chunk in _chunks(list(run_ids), BULK_PAGE):
            offset = 0
            while True:
                data = self.post(TASKS_PATH, {
                    "dag_ids": list(dag_ids), "dag_run_ids": chunk, "state": list(states),
                    "page_limit": BULK_PAGE, "page_offset": offset})
                page = data.get("task_instances") or []
                out.extend(page)
                offset += len(page)
                total = data.get("total_entries")
                if not page or (isinstance(total, int) and offset >= total):
                    break
        return out

    def dag(self, dag_id: str) -> dict:
        return self.get(f"/api/v2/dags/{self._q(dag_id)}")

    def dag_runs(self, dag_id: str, limit: int) -> list[dict]:
        data = self.get(f"/api/v2/dags/{self._q(dag_id)}/dagRuns",
                        limit=limit, order_by="-run_after")
        return data["dag_runs"]

    def task_instances(self, dag_id: str, run_id: str, state: str | None = None) -> list[dict]:
        """Every task instance of the run, paged; stops at total_entries or a repeated page."""
        path = f"/api/v2/dags/{self._q(dag_id)}/dagRuns/{self._q(run_id)}/taskInstances"
        out: list[dict] = []
        seen: set = set()
        offset = 0
        while True:
            query: dict = {"limit": 100, "offset": offset}
            if state:
                query["state"] = state
            data = self.get(path, **query)
            page = data.get("task_instances") or []
            fresh = [ti for ti in page if _ti_key(ti) not in seen]
            if not page or not fresh:
                return out
            seen.update(_ti_key(ti) for ti in fresh)
            out.extend(fresh)
            offset += len(page)
            total = data.get("total_entries")
            if isinstance(total, int) and offset >= total:
                return out

    def tasks(self, dag_id: str) -> list[dict]:
        return self.get(f"/api/v2/dags/{self._q(dag_id)}/tasks")["tasks"]

    def has_nonempty_task(self, dag_id: str, run_id: str) -> bool:
        """True when some task of the run is in a state other than skipped, removed or none
        (one call, one row)."""
        path = f"/api/v2/dags/{self._q(dag_id)}/dagRuns/{self._q(run_id)}/taskInstances"
        return bool(self.get(path, limit=1, state=list(NONEMPTY_TI_STATES)).get("task_instances"))

    def task_instance(self, dag_id: str, run_id: str, task_id: str) -> dict:
        return self.get(f"/api/v2/dags/{self._q(dag_id)}/dagRuns/{self._q(run_id)}"
                        f"/taskInstances/{self._q(task_id)}")

    def dag_source(self, dag_id: str) -> str:
        return self.get(f"/api/v2/dagSources/{self._q(dag_id)}").get("content") or ""

    def task_log(self, dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
        data = self.get(f"/api/v2/dags/{self._q(dag_id)}/dagRuns/{self._q(run_id)}"
                        f"/taskInstances/{self._q(task_id)}/logs/{int(try_number)}",
                        full_content="true")
        content = data.get("content")
        return content if isinstance(content, str) else json.dumps(content)
