"""build_page / fetch_snapshot / connect: the library entry point."""

import pytest

from airflow_depgraph import Settings, build, build_page, connect, fetch_snapshot
from airflow_depgraph.auth import CredentialsError
from tests.test_cli import FakeClient


@pytest.fixture
def fake(monkeypatch):
    monkeypatch.setattr(build, "AirflowClient", FakeClient)
    return FakeClient


def test_connect_prefers_the_token(fake):
    c = connect(Settings(url="http://af", token="from-settings"), user="u", password="p", env={})
    assert c.token == "from-settings"
    assert connect(Settings(url="http://af"), token="arg", env={}).token == "arg"


def test_connect_logs_in_with_user_and_password(fake, monkeypatch):
    seen = {}

    def login(cls, url, user, password, timeout=None):
        seen.update(url=url, user=user, password=password)
        return cls(url, token="tok")
    monkeypatch.setattr(FakeClient, "login", classmethod(login))
    assert connect(Settings(url="http://af"), user="svc", password="s3", env={}).token == "tok"
    assert seen == {"url": "http://af", "user": "svc", "password": "s3"}


def test_connect_falls_back_to_the_environment(fake, monkeypatch):
    seen = {}

    def login(cls, url, user, password, timeout=None):
        seen.update(user=user, password=password)
        return cls(url, token="tok")
    monkeypatch.setattr(FakeClient, "login", classmethod(login))
    env = {"AIRFLOW_DEPGRAPH_USER": "env-user", "AIRFLOW_DEPGRAPH_PASSWORD": "env-pass"}
    connect(Settings(url="http://af"), env=env)
    assert seen == {"user": "env-user", "password": "env-pass"}


def test_connect_without_credentials_or_url(fake):
    with pytest.raises(ValueError, match="no Airflow URL"):
        connect(Settings(), env={})
    with pytest.raises(CredentialsError, match="no username"):
        connect(Settings(url="http://af"), env={})
    with pytest.raises(CredentialsError, match="no password"):
        connect(Settings(url="http://af"), user="u", env={})


def test_fetch_snapshot_carries_graph_catalog_and_status(fake):
    s = Settings(url="http://af", with_status=True)
    snap = fetch_snapshot(FakeClient("http://af", token="t"), s)
    assert snap.graph.dag_ids == ["x", "y"]
    assert snap.airflow_version == "3.3.2" and snap.source == "http://af"
    assert set(snap.catalog) == {"x", "y", "z"} and snap.catalog["y"]["is_paused"] is True
    assert set(snap.dags) == {"x", "y"} and "last_run" in snap.dags["x"]


def test_fetch_snapshot_is_quiet_unless_given_a_log(fake, capsys):
    s = Settings(url="http://af", with_status=True)
    fetch_snapshot(FakeClient("http://af", token="t"), s)
    assert capsys.readouterr() == ("", "")
    lines = []
    fetch_snapshot(FakeClient("http://af", token="t"), s, log=lines.append)
    assert any(line.startswith("fetching status") for line in lines)


def test_build_page_renders_the_page(fake):
    s = Settings(url="http://af", title="Graph of af", tags=("team-a",))
    html = build_page(s, token="t", env={})
    assert html.startswith("<!doctype html>") or html.lstrip().startswith("<!DOCTYPE")
    assert "<title>Graph of af</title>" in html
    assert '"dag:x"' in html and '"dag:y"' in html
    assert '"catalog": true' in html


def test_build_page_reuses_a_given_client(fake):
    client = FakeClient("http://af", token="t")
    html = build_page(Settings(url="http://af"), client=client)
    assert '"source": "http://af"' in html


def test_status_is_read_for_tagged_catalog_dags_without_links(fake, monkeypatch):
    """z has no link but carries the preset tag: it is on the page, so its status is read too.
    Without tags only the graph's DAGs are read; the catalog could be the whole instance."""
    seen = []

    def fetch_status(client, ids, s, **kw):
        seen.append(list(ids))
        return {d: {"last_run": None} for d in ids}
    monkeypatch.setattr(build, "fetch_status", fetch_status)
    fetch_snapshot(FakeClient("http://af", token="t"), Settings(url="http://af", with_status=True,
                                                                 tags=("team-a",)))
    fetch_snapshot(FakeClient("http://af", token="t"), Settings(url="http://af", with_status=True))
    assert seen == [["x", "y", "z"], ["x", "y"]]


def test_status_dag_ids_skips_stale_and_untagged_catalog_dags():
    from airflow_depgraph.snapshot import Snapshot, new
    snap = new([{"id": "dag:x", "label": "x", "type": "dag"}], [], "http://af", "3.3.2")
    assert isinstance(snap, Snapshot)
    snap.catalog = {"x": {"tags": ["t"], "is_stale": False}, "gone": {"tags": ["t"], "is_stale": True},
                    "other": {"tags": ["u"], "is_stale": False}, "lone": {"tags": ["t"], "is_stale": False}}
    assert build.status_dag_ids(snap, Settings(tags=("t",))) == ["lone", "x"]
    assert build.status_dag_ids(snap, Settings()) == ["x"]
