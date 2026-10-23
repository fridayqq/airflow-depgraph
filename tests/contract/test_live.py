"""Live contract test against a running Airflow (nightly job). Skipped unless DEPGRAPH_CONTRACT_URL is set.

The job starts `airflow standalone` with tests/contract/dags, runs the four contract DAGs once and
checks what airflow-depgraph reads from them.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import pytest

from airflow_depgraph import cli
from airflow_depgraph.client import AirflowClient

URL = os.environ.get("DEPGRAPH_CONTRACT_URL", "").rstrip("/")
DAG_IDS = ("contract_producer", "contract_consumer", "contract_trigger", "contract_waiter")
TERMINAL = {"success", "failed"}
WAIT_SECONDS = 600

pytestmark = [
    pytest.mark.contract,
    pytest.mark.skipif(not URL, reason="set DEPGRAPH_CONTRACT_URL to run the live contract test"),
]


def _token() -> str:
    token = os.environ.get("DEPGRAPH_CONTRACT_TOKEN")
    if token:
        return token
    client = AirflowClient.login(URL, os.environ["DEPGRAPH_CONTRACT_USER"],
                                 os.environ["DEPGRAPH_CONTRACT_PASSWORD"])
    return client.token


def _api(token: str, method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(URL + path, data=data, method=method, headers={
        "Authorization": f"Bearer {token}", "Content-Type": "application/json",
        "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as exc:
        if method == "GET" and exc.code == 404:
            return {}
        raise


def _wait(predicate, what: str) -> None:
    deadline = time.monotonic() + WAIT_SECONDS
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(5)
    pytest.fail(f"timed out waiting for {what}")


@pytest.fixture(scope="module")
def token():
    return _token()


@pytest.fixture(scope="module")
def contract_runs(token):
    """Unpause and trigger the DAGs, then wait until each has a finished run."""
    def dag_known(dag_id):
        return bool(_api(token, "GET", f"/api/v2/dags/{dag_id}"))

    for dag_id in DAG_IDS:
        _wait(lambda d=dag_id: dag_known(d), f"DAG {dag_id} to be parsed")
        _api(token, "PATCH", f"/api/v2/dags/{dag_id}", {"is_paused": False})
        _api(token, "POST", f"/api/v2/dags/{dag_id}/dagRuns",
             {"logical_date": datetime.now(timezone.utc).isoformat(), "conf": {}})

    def finished(dag_id):
        runs = _api(token, "GET", f"/api/v2/dags/{dag_id}/dagRuns?order_by=-run_after&limit=1")
        return any(r.get("state") in TERMINAL for r in runs.get("dag_runs", []))

    for dag_id in DAG_IDS:
        _wait(lambda d=dag_id: finished(d), f"a finished run of {dag_id}")
    return True


@pytest.fixture(scope="module")
def snapshot(token, contract_runs, tmp_path_factory):
    out = tmp_path_factory.mktemp("contract") / "snapshot.json"
    os.environ["AIRFLOW_DEPGRAPH_TOKEN"] = token
    try:
        assert cli.main(["fetch", "--url", URL, "--with-status", "--infer-sensors", "-o", str(out)]) == 0
    finally:
        os.environ.pop("AIRFLOW_DEPGRAPH_TOKEN", None)
    return json.loads(out.read_text(encoding="utf-8")), out


def test_contract_dags_are_nodes(snapshot):
    snap, _ = snapshot
    dag_nodes = {n["id"] for n in snap["nodes"] if n["type"] == "dag"}
    assert {f"dag:{d}" for d in DAG_IDS} <= dag_nodes


def test_asset_edge_from_producer_to_consumer(snapshot, capsys):
    _, path = snapshot
    assert cli.main(["downstream", "contract_producer", str(path), "--json"]) == 0
    dags = [d["dag"] for d in json.loads(capsys.readouterr().out)["dags"]]
    assert "contract_consumer" in dags


def test_trigger_edge_to_triggered_dag(snapshot, capsys):
    _, path = snapshot
    assert cli.main(["downstream", "contract_trigger", str(path), "--json"]) == 0
    dags = [d["dag"] for d in json.loads(capsys.readouterr().out)["dags"]]
    assert "contract_triggered" in dags


def test_sensor_link_is_inferred(snapshot):
    snap, _ = snapshot
    links = [e for e in snap.get("inferred") or [] if e["dag"] == "contract_waiter"]
    assert any("contract_producer" in e.get("targets", []) for e in links), links


def test_status_has_a_last_run_for_each_contract_dag(snapshot):
    snap, _ = snapshot
    for dag_id in DAG_IDS:
        status = snap["dags"][dag_id]
        assert "error" not in status, status
        assert status["last_run"] is not None, dag_id
