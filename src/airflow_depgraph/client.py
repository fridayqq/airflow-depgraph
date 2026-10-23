"""Minimal Airflow 3 REST client (stdlib only)."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request


class ApiError(RuntimeError):
    def __init__(self, message: str, status: int | None = None, path: str = ""):
        super().__init__(message)
        self.status = status
        self.path = path


class AirflowClient:
    def __init__(self, url: str, token: str | None = None, timeout: float = 25,
                 opener=urllib.request.urlopen):
        self.url = url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self._open = opener

    @classmethod
    def login(cls, url: str, user: str, password: str, **kw) -> "AirflowClient":
        client = cls(url, **kw)
        path = "/auth/token"
        body = json.dumps({"username": user, "password": password}).encode()
        req = urllib.request.Request(
            client.url + path,
            data=body,
            method="POST",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
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
        try:
            with self._open(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 403 and path == "/ui/dependencies":
                msg = ("403 on /ui/dependencies: this user cannot see DAGs "
                       "(Airflow ≥ 3.1.8 filters by DAG permissions)")
            else:
                msg = f"{e.code} on {path}: {e.reason}"
            raise ApiError(msg, e.code, path) from None
        except urllib.error.URLError as e:
            raise ApiError(
                f"no response from {self.url} ({e.reason}) — VPN/DNS?", None, path
            ) from None
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ApiError(
                f"non-JSON response from {path} — wrong URL or a login/proxy page?", None, path
            ) from None
        except TimeoutError:
            raise ApiError(
                f"timed out after {self.timeout:g}s waiting for {self.url}{path}", None, path
            ) from None
        except OSError as e:
            raise ApiError(f"network error on {path}: {e}", None, path) from None

    def get(self, path: str, **query) -> dict:
        full = self.url + path
        if query:
            full += "?" + urllib.parse.urlencode(query)
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return self._send(urllib.request.Request(full, headers=headers), path)

    def dependencies(self, node_id: str | None = None) -> dict:
        if node_id:
            return self.get("/ui/dependencies", node_id=node_id)
        return self.get("/ui/dependencies")

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
            page = self.get("/api/v2/dags", limit=page_size, offset=len(out),
                            exclude_stale="false").get("dags") or []
            out.extend(page)
            if len(page) < page_size:
                return out

    def dag(self, dag_id: str) -> dict:
        return self.get(f"/api/v2/dags/{self._q(dag_id)}")

    def dag_runs(self, dag_id: str, limit: int) -> list[dict]:
        data = self.get(f"/api/v2/dags/{self._q(dag_id)}/dagRuns",
                        limit=limit, order_by="-run_after")
        return data["dag_runs"]

    def task_instances(self, dag_id: str, run_id: str) -> list[dict]:
        data = self.get(
            f"/api/v2/dags/{self._q(dag_id)}/dagRuns/{self._q(run_id)}/taskInstances"
        )
        return data["task_instances"]

    def tasks(self, dag_id: str) -> list[dict]:
        return self.get(f"/api/v2/dags/{self._q(dag_id)}/tasks")["tasks"]
