"""Airflow 3 plugin: a "DAG Dependencies" page inside the Airflow UI (Browse menu).

The page calls this api-server's own /ui/dependencies and REST API with the signed-in user's
token, so it shows exactly the DAGs that user may see. Settings come from AIRFLOW_DEPGRAPH_*
environment variables or the preset named in AIRFLOW_DEPGRAPH_PRESET.
"""
from __future__ import annotations

import html
import os
import sys
from functools import lru_cache
from urllib.parse import quote

from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from .client import ApiError
from .presets import resolve
from .snapshot import SnapshotError
from .web import PageBuilder, base_path, extract_token, loopback_url, token_usable

URL_PREFIX = "/depgraph"

app = FastAPI(title="airflow-depgraph", docs_url=None, redoc_url=None, openapi_url=None)


def _configured_base_url() -> str:
    """`[api] base_url` without importing airflow.configuration during plugin discovery."""
    env = os.environ.get("AIRFLOW__API__BASE_URL")
    if env is not None:
        return env
    conf_mod = sys.modules.get("airflow.configuration")
    return (conf_mod.conf.get("api", "base_url", fallback="") or "") if conf_mod else ""


@lru_cache(maxsize=1)
def _runtime() -> tuple[str, PageBuilder]:
    # Imported lazily: airflow.configuration at import time re-enters plugin loading.
    from airflow.configuration import conf
    base_url = conf.get("api", "base_url", fallback="") or ""
    api = loopback_url(base_url, conf.get("api", "port", fallback="8080"),
                       os.environ.get("AIRFLOW_DEPGRAPH_LOOPBACK_URL"))
    return base_url, PageBuilder(api, resolve({}, os.environ, None))


def _public_url(request: Request, base_url: str) -> str:
    if base_url.startswith(("http://", "https://")):
        return base_url.rstrip("/")
    return f"{request.url.scheme}://{request.url.netloc}{base_path(base_url)}"


def _login(base_url: str) -> RedirectResponse:
    prefix = base_path(base_url)
    back = f"{prefix}{URL_PREFIX}/"
    try:
        from airflow.api_fastapi.app import get_auth_manager
        login = get_auth_manager().get_url_login(next_url=back)
    except Exception:  # noqa: BLE001 - any auth-manager problem falls back to Airflow's home
        login = "/"
    if login.startswith("/") and not login.startswith(prefix + "/"):
        login = prefix + login
    if "next=" not in login:
        login += ("&" if "?" in login else "?") + "next=" + quote(back, safe="")
    return RedirectResponse(login, status_code=302)


def _message(status: int, title: str, body: str) -> HTMLResponse:
    # Note: Astro's runtime middleware re-wraps text/html responses and drops the status code.
    page = (f"<!doctype html><meta charset=utf-8><title>{html.escape(title)}</title>"
            f"<body style='font:15px system-ui;margin:40px'><h1>{html.escape(title)}</h1>"
            f"<p>{body}</p></body>")
    return HTMLResponse(page, status_code=status)


@app.get("/", response_class=HTMLResponse)
def page(request: Request, status: int | None = None):
    base_url, builder = _runtime()
    token = extract_token(request.cookies, request.headers)
    if not token or not token_usable(token):
        return _login(base_url)
    with_status = builder.settings.with_status if status is None else bool(status)
    try:
        return HTMLResponse(builder.page(token, _public_url(request, base_url), with_status))
    except ApiError as exc:
        if exc.status == 401:
            return _login(base_url)
        if exc.status == 403:
            return _message(403, "Not allowed", "Your Airflow user may not read DAG dependencies.")
        return _message(502, "Airflow API error", html.escape(str(exc)))
    except SnapshotError as exc:
        return _message(502, "Unexpected dependency data", html.escape(str(exc)))


class DepGraphPlugin(AirflowPlugin):
    name = "airflow_depgraph"
    fastapi_apps = [{"app": app, "url_prefix": URL_PREFIX, "name": "DAG Dependencies"}]
    # Absolute path including the base path (/airflow when served under a sub-path): Airflow 3.3
    # syncs the iframe pathname into its own route, 3.1 uses it as is. url_route = in-shell page.
    external_views = [{"name": "DAG Dependencies",
                       "href": f"{base_path(_configured_base_url())}{URL_PREFIX}/",
                       "url_route": "dag-dependencies", "destination": "nav", "category": "browse"}]
