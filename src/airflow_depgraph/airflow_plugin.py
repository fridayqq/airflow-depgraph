"""Airflow 3 plugin: a "DAG Dependencies" page inside the Airflow UI (Browse menu).

The page calls this api-server's own /ui/dependencies and REST API with the signed-in user's
token, so it shows exactly the DAGs that user may see. Settings come from AIRFLOW_DEPGRAPH_*
environment variables or the preset named in AIRFLOW_DEPGRAPH_PRESET.
"""
from __future__ import annotations

import html
import logging
import os
import sys
from functools import lru_cache
from urllib.parse import quote

from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from .client import ApiError
from .presets import PresetError, resolve
from .snapshot import SnapshotError
from .web import PageBuilder, base_path, extract_token, loopback_url, token_usable

URL_PREFIX = "/depgraph"
_GENERIC_502 = "Could not read the dependency graph from Airflow; see the api-server log."

log = logging.getLogger(__name__)
NO_STORE = {"Cache-Control": "private, no-store"}

app = FastAPI(title="airflow-depgraph", docs_url=None, redoc_url=None, openapi_url=None)


def _configured_base_url() -> str:
    """`[api] base_url` without importing airflow.configuration during plugin discovery.

    Only the plain env var and the config file are seen here; the `_CMD` / `_SECRET` variants of
    base_url are not, so set `AIRFLOW__API__BASE_URL` directly for the menu href to be right.
    """
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


def _login_url(base_url: str) -> str:
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
    return login


def _login(base_url: str) -> RedirectResponse:
    return RedirectResponse(_login_url(base_url), status_code=302, headers=NO_STORE)


def _message(status: int, title: str, body: str) -> HTMLResponse:
    # Note: Astro's runtime middleware re-wraps text/html responses and drops the status code.
    page = (f"<!doctype html><meta charset=utf-8><title>{html.escape(title)}</title>"
            f"<body style='font:15px system-ui;margin:40px'><h1>{html.escape(title)}</h1>"
            f"<p>{body}</p></body>")
    return HTMLResponse(page, status_code=status, headers=NO_STORE)


@app.get("/", response_class=HTMLResponse)
def page(request: Request, status: str | None = None):
    try:
        base_url, builder = _runtime()
    except PresetError as exc:
        return _message(500, "Plugin misconfigured", html.escape(str(exc)))
    token = extract_token(request.cookies, request.headers)
    if not token or not token_usable(token):
        return _login(base_url)
    with_status = (builder.settings.with_status if status is None
                   else status.lower() in {"1", "true", "yes"})
    try:
        return HTMLResponse(builder.page(token, _public_url(request, base_url), with_status),
                            headers=NO_STORE)
    except ApiError as exc:
        if exc.status == 401:
            return _login(base_url)
        if exc.status == 403:
            link = html.escape(_login_url(base_url), quote=True)
            return _message(403, "Not allowed", "Your Airflow user may not read DAG dependencies. "
                            f"<a href='{link}'>Sign in again</a>")
        log.warning("depgraph: %s", exc)
        return _message(502, "Airflow API error", _GENERIC_502)
    except SnapshotError as exc:
        log.warning("depgraph: %s", exc)
        return _message(502, "Unexpected dependency data", _GENERIC_502)


class DepGraphPlugin(AirflowPlugin):
    name = "airflow_depgraph"
    fastapi_apps = [{"app": app, "url_prefix": URL_PREFIX, "name": "DAG Dependencies"}]
    # Absolute path including the base path (/airflow when served under a sub-path): Airflow 3.3
    # syncs the iframe pathname into its own route, 3.1 uses it as is. url_route = in-shell page.
    external_views = [{"name": "DAG Dependencies",
                       "href": f"{base_path(_configured_base_url())}{URL_PREFIX}/",
                       "url_route": "dag-dependencies", "destination": "nav", "category": "browse"}]
