import json
from pathlib import Path

import pytest

from airflow_depgraph import cli

FIX = str(Path(__file__).parent / "fixtures" / "snapshot_small.json")
URL = "https://airflow.example.com"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch, tmp_path):
    for k in list(__import__("os").environ):
        if k.startswith("AIRFLOW_DEPGRAPH_"):
            monkeypatch.delenv(k)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))


class FakeClient:
    deps = {
        "nodes": [
            {"id": "dag:x", "label": "x", "type": "dag"},
            {"id": "asset:1", "label": "orders", "type": "asset"},
            {"id": "dag:y", "label": "y", "type": "dag"},
        ],
        "edges": [
            {"source_id": "dag:x", "target_id": "asset:1"},
            {"source_id": "asset:1", "target_id": "dag:y"},
        ],
    }

    def __init__(self, url, token=None):
        self.url, self.token = url, token

    @classmethod
    def login(cls, url, user, password):
        return cls(url, token="tok")

    def dependencies(self, node_id=None):
        return self.deps

    def version(self):
        return "3.3.2"

    def dag(self, dag_id):
        return {"is_paused": dag_id == "y", "timetable_summary": "Asset", "owners": ["o"]}

    def tasks(self, dag_id):
        return [{"task_id": "refresh_dashboard"}, {"task_id": "other"}]

    def dag_runs(self, dag_id, limit):
        return []

    def task_instances(self, dag_id, run_id):
        return []


@pytest.fixture
def live(monkeypatch):
    monkeypatch.setattr(cli, "AirflowClient", FakeClient)
    monkeypatch.setenv("AIRFLOW_DEPGRAPH_USER", "u")
    monkeypatch.setenv("AIRFLOW_DEPGRAPH_PASSWORD", "p")


def test_downstream_from_snapshot_text(capsys):
    assert cli.main(["downstream", "dag_a", FIX]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].startswith("Downstream DAGs (asset/trigger/sensor consumers) of dag_a — ")
    assert "(depth ≤ 5, snapshot 2026-10-22 06:00 UTC)" in lines[0]
    assert lines[1] == f"  →1 {'dag_b':<48} via asset:team-orders  sched: —"
    assert lines[2] == f"        {URL}/dags/dag_b"


def test_downstream_json_schema(capsys):
    assert cli.main(["downstream", "dag_a", FIX, "--json", "--depth", "1"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert set(out) == {"dag", "direction", "depth", "dags"}
    assert out["depth"] == 1 and out["direction"] == "downstream"
    assert set(out["dags"][0]) == {"dag_id", "hops", "via", "is_paused", "schedule", "owners"}


def test_upstream_arrow(capsys):
    assert cli.main(["upstream", "dag_d", FIX]) == 0
    out = capsys.readouterr().out
    assert out.startswith("Upstream DAGs (what this DAG waits for) of dag_d")
    assert "  ←1 dag_c" in out


def test_downstream_unknown_dag_exit_1(capsys):
    assert cli.main(["downstream", "nope", FIX]) == 1
    err = capsys.readouterr().err
    assert "error: " in err and "is not in the dependency graph (12 DAGs known)" in err


def test_render_from_snapshot_writes_file(tmp_path):
    out = tmp_path / "g.html"
    assert cli.main(["render", FIX, "-o", str(out)]) == 0
    assert "__DATA__" not in out.read_text()


def test_render_status_override(tmp_path):
    st = tmp_path / "st.json"
    st.write_text(json.dumps({"dag_c": {"kind": "pipeline", "paused": False, "date": "2026-10-22"}}))
    out = tmp_path / "g.html"
    assert cli.main(["render", FIX, "-o", str(out), "--status", str(st)]) == 0
    assert '"fail": {"kind": "pipeline", "paused": false, "date": "2026-10-22"}' in out.read_text()


def test_tasks_matching_offline_without_tasks_errors(capsys):
    assert cli.main(["downstream", "dag_a", FIX, "--tasks-matching", "x"]) == 1
    assert "--with-tasks" in capsys.readouterr().err


def test_tasks_matching_offline_with_tasks(capsys):
    assert cli.main(["downstream", "dag_e", FIX, "--tasks-matching", "refresh", "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["dags"][0]["dag_id"] == "dag_l_report"
    assert out["dags"][0]["matching_tasks"] == ["refresh_dashboard"]


def test_tasks_matching_text_line(capsys):
    assert cli.main(["downstream", "dag_e", FIX, "--tasks-matching", "refresh"]) == 0
    assert "        tasks: refresh_dashboard" in capsys.readouterr().out


def test_fetch_live_writes_snapshot(tmp_path, live, capsys):
    out = tmp_path / "s.json"
    assert cli.main(["fetch", "--url", URL, "-o", str(out), "--with-status"]) == 0
    data = json.loads(out.read_text())
    assert data["format"] == 1 and data["airflow_version"] == "3.3.2"
    assert set(data["dags"]) == {"x", "y"}
    assert "fetching status 2/2" in capsys.readouterr().err


def test_fetch_bad_payload_exit_1(tmp_path, live, monkeypatch, capsys):
    monkeypatch.setattr(FakeClient, "deps", {})
    assert cli.main(["fetch", "--url", URL, "-o", str(tmp_path / "s.json")]) == 1
    err = capsys.readouterr().err
    assert "unexpected /ui/dependencies payload" in err
    assert "(Airflow 3.3.2)" in err


def test_render_missing_snapshot_is_clean_error(tmp_path, capsys):
    assert cli.main(["render", str(tmp_path / "missing.json")]) == 1
    assert capsys.readouterr().err.startswith("error:")


def test_render_output_in_missing_dir_is_clean_error(tmp_path, capsys):
    assert cli.main(["render", FIX, "-o", str(tmp_path / "no" / "dir" / "g.html")]) == 1
    assert capsys.readouterr().err.startswith("error:")


@pytest.mark.parametrize("bad", ["0", "-1", "x"])
def test_depth_must_be_positive(bad, capsys):
    with pytest.raises(SystemExit) as ei:
        cli.main(["downstream", "dag_a", FIX, "--depth", bad])
    assert ei.value.code == 2
    assert ">= 1" in capsys.readouterr().err


def test_downstream_live(live, capsys):
    assert cli.main(["downstream", "x", "--url", URL]) == 0
    out = capsys.readouterr().out
    assert "live)" in out.splitlines()[0]
    assert "⏸ paused  sched: Asset" in out


def test_downstream_live_tasks_matching(live, capsys):
    assert cli.main(["downstream", "x", "--url", URL, "--tasks-matching", "^refresh"]) == 0
    assert "tasks: refresh_dashboard" in capsys.readouterr().out


def test_tasks_matching_without_regex_uses_preset(live, capsys):
    assert cli.main(["downstream", "x", "--url", URL, "--tasks-matching"]) == 1
    assert "task_query_regex" in capsys.readouterr().err


def test_presets_example_prints_toml(capsys):
    assert cli.main(["presets", "--example"]) == 0
    assert "password_env" in capsys.readouterr().out


def test_presets_lists_and_resolves(capsys, monkeypatch, tmp_path):
    d = tmp_path / "p"
    d.mkdir()
    (d / "mine.toml").write_text('url = "https://airflow.example.com"\n')
    monkeypatch.setenv("AIRFLOW_DEPGRAPH_PRESETS", str(d))
    assert cli.main(["presets", "--preset", "mine", "--token", "secret"]) == 0
    out = capsys.readouterr().out
    assert "mine  " in out and "url = https://airflow.example.com" in out
    assert "secret" not in out and "token" not in out


def test_graph_writes_both_files(tmp_path, live):
    out = tmp_path / "x.html"
    assert cli.main(["graph", "--url", URL, "-o", str(out)]) == 0
    assert out.exists() and (tmp_path / "x.json").exists()


def test_missing_url_live_exit_1(capsys):
    assert cli.main(["fetch", "-o", "x.json"]) == 1
    assert "error: " in capsys.readouterr().err


@pytest.mark.parametrize("cmd", ["graph", "render"])
def test_json_output_rejected(tmp_path, live, capsys, cmd):
    out = tmp_path / "x.json"
    argv = [cmd, FIX, "-o", str(out)] if cmd == "render" else [cmd, "--url", URL, "-o", str(out)]
    assert cli.main(argv) == 1
    assert "output must be an .html file" in capsys.readouterr().err
    assert not out.exists()


def test_preset_bool_survives_absent_flag(tmp_path, live):
    preset = tmp_path / "p.toml"
    preset.write_text(f'url = "{URL}"\nwith_status = true\n')
    out = tmp_path / "s.json"
    assert cli.main(["fetch", "--preset", str(preset), "-o", str(out)]) == 0
    assert set(json.loads(out.read_text())["dags"]) == {"x", "y"}


def test_tasks_matching_empty_string_errors(capsys):
    assert cli.main(["downstream", "dag_e", FIX, "--tasks-matching", ""]) == 1
    assert "non-empty" in capsys.readouterr().err


def test_offline_snapshot_without_fetched_at_not_live(tmp_path, capsys):
    data = json.loads(Path(FIX).read_text())
    del data["fetched_at"]
    f = tmp_path / "s.json"
    f.write_text(json.dumps(data))
    assert cli.main(["downstream", "dag_a", str(f)]) == 0
    assert "live" not in capsys.readouterr().out.splitlines()[0]
