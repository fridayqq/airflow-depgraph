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

    list_dags_error = None

    def list_dags(self):
        if self.list_dags_error:
            raise self.list_dags_error
        return [{"dag_id": "x", "tags": [{"name": "team-a", "dag_id": "x"}], "is_paused": False,
                 "is_stale": False, "timetable_summary": "Asset", "owners": ["o"]},
                {"dag_id": "y", "tags": [], "is_paused": True, "is_stale": False,
                 "timetable_summary": "Asset", "owners": ["o"]},
                {"dag_id": "z", "tags": ["team-a"], "is_paused": False, "is_stale": False,
                 "timetable_summary": None, "owners": []}]

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
    assert set(out["dags"][0]) == {"dag_id", "hops", "via", "is_paused", "schedule", "owners",
                                   "parent", "state", "kind", "last_run_at"}


def test_upstream_arrow(capsys):
    assert cli.main(["upstream", "dag_d", FIX]) == 0
    out = capsys.readouterr().out
    assert out.startswith("Upstream DAGs (what this DAG waits for) of dag_d")
    assert "  ←1 dag_c" in out


def test_downstream_unknown_dag_exit_1(capsys):
    assert cli.main(["downstream", "nope", FIX]) == 1
    err = capsys.readouterr().err
    assert "error: " in err and "is not in the dependency graph (13 DAGs known)" in err


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


def test_tasks_preset_offline_uses_preset_regex(tmp_path, capsys):
    preset = tmp_path / "p.toml"
    preset.write_text('task_query_regex = "refresh"\n')
    assert cli.main(["downstream", "dag_e", FIX, "--preset", str(preset),
                     "--tasks-preset", "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["dags"][0]["matching_tasks"] == ["refresh_dashboard"]


def test_tasks_matching_requires_a_value(capsys):
    with pytest.raises(SystemExit) as ei:
        cli.main(["downstream", "dag_e", FIX, "--tasks-matching"])
    assert ei.value.code == 2


def test_tasks_matching_and_tasks_preset_together_errors(capsys):
    assert cli.main(["downstream", "dag_e", FIX, "--tasks-matching", "refresh",
                     "--tasks-preset"]) == 1
    assert "use either --tasks-matching REGEX or --tasks-preset" in capsys.readouterr().err


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


def test_tasks_preset_without_regex_errors(live, capsys):
    assert cli.main(["downstream", "x", "--url", URL, "--tasks-preset"]) == 1
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


def test_fetch_live_writes_catalog(tmp_path, live):
    out = tmp_path / "s.json"
    assert cli.main(["fetch", "--url", URL, "-o", str(out), "--tag", "team-a"]) == 0
    cat = json.loads(out.read_text())["catalog"]
    assert set(cat) == {"x", "y", "z"} and cat["x"]["tags"] == ["team-a"] and cat["y"]["is_paused"] is True


def test_fetch_without_catalog_warns_and_continues(tmp_path, live, monkeypatch, capsys):
    from airflow_depgraph.client import ApiError
    monkeypatch.setattr(FakeClient, "list_dags_error", ApiError("403 on /api/v2/dags: reason", 403))
    out = tmp_path / "s.json"
    assert cli.main(["fetch", "--url", URL, "-o", str(out)]) == 0
    assert "catalog" not in json.loads(out.read_text())
    assert "warning:" in capsys.readouterr().err


def test_render_tag_flag_is_repeatable(tmp_path):
    out = tmp_path / "g.html"
    assert cli.main(["render", FIX, "-o", str(out), "--tag", "team-a", "--tag", "reporting"]) == 0
    assert '"tags": ["team-a", "reporting"]' in out.read_text()


def test_graph_tag_flag_replaces_env(tmp_path, live, monkeypatch):
    monkeypatch.setenv("AIRFLOW_DEPGRAPH_TAGS", "envtag")
    out = tmp_path / "x.html"
    assert cli.main(["graph", "--url", URL, "-o", str(out), "--tag", "clitag"]) == 0
    html = out.read_text()
    assert '"tags": ["clitag"]' in html and "envtag" not in html
    assert '"unlinked": true' in html


def test_presets_prints_tags(capsys, monkeypatch, tmp_path):
    d = tmp_path / "p"
    d.mkdir()
    (d / "mine.toml").write_text('tags = ["a", "b"]\n')
    monkeypatch.setenv("AIRFLOW_DEPGRAPH_PRESETS", str(d))
    assert cli.main(["presets", "--preset", "mine"]) == 0
    assert "tags = a, b" in capsys.readouterr().out


@pytest.fixture
def inferred_file(tmp_path, inferred_snapshot):
    from airflow_depgraph.snapshot import dump
    p = tmp_path / "inf.json"
    dump(inferred_snapshot, p)
    return str(p)


def test_downstream_follows_inferred_sensor(inferred_file, capsys):
    assert cli.main(["downstream", "dag_i", inferred_file, "--json"]) == 0
    dags = json.loads(capsys.readouterr().out)["dags"]
    assert dags == [{"dag_id": "dag_x_cron", "hops": 1, "via": "sensor:wait_i (inferred)",
                     "is_paused": None, "schedule": None, "owners": None, "parent": "dag_i",
                     "state": None, "kind": None, "last_run_at": None}]


def test_no_inferred_flag_ignores_them(inferred_file, capsys):
    assert cli.main(["downstream", "dag_i", inferred_file, "--no-inferred"]) == 0
    assert "dag_x_cron" not in capsys.readouterr().out


def test_guards_need_include_guards(inferred_file, capsys):
    assert cli.main(["upstream", "dag_y_cron", inferred_file]) == 1
    capsys.readouterr()
    assert cli.main(["upstream", "dag_y_cron", inferred_file, "--include-guards"]) == 0
    assert "via sensor:guard_a (guard)" in capsys.readouterr().out


def test_guard_hop_json_keeps_inferred_label(inferred_file, capsys):
    assert cli.main(["upstream", "dag_y_cron", inferred_file, "--include-guards", "--json"]) == 0
    dags = json.loads(capsys.readouterr().out)["dags"]
    assert [d["via"] for d in dags] == ["sensor:guard_a (inferred)"]


def test_normal_inferred_hop_keeps_inferred_marker(inferred_file, capsys):
    assert cli.main(["downstream", "dag_i", inferred_file]) == 0
    out = capsys.readouterr().out
    assert "via sensor:wait_i (inferred)" in out and "(guard)" not in out


def test_version_flag_prints_package_version(capsys):
    from airflow_depgraph import __version__
    with pytest.raises(SystemExit) as exc:
        cli.main(["--version"])
    assert exc.value.code == 0
    assert capsys.readouterr().out == f"airflow-depgraph {__version__}\n"


@pytest.mark.parametrize("command", ["fetch", "render", "graph", "downstream", "upstream",
                                     "tasks", "presets"])
def test_every_subcommand_help_has_examples(command, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main([command, "--help"])
    assert exc.value.code == 0
    assert "Example" in capsys.readouterr().out


def test_strip_prefix_shortens_asset_label_in_text_only(tmp_path, monkeypatch, capsys):
    d = tmp_path / "presets"
    d.mkdir()
    (d / "team.toml").write_text('asset_label_strip_prefix = "team-"\n')
    monkeypatch.setenv("AIRFLOW_DEPGRAPH_PRESETS", str(d))
    assert cli.main(["downstream", "dag_a", FIX, "--preset", "team"]) == 0
    out = capsys.readouterr().out
    assert "via asset:orders" in out and "via asset:team-" not in out
    assert cli.main(["downstream", "dag_a", FIX, "--preset", "team", "--json"]) == 0
    assert '"via": "asset:team-orders"' in capsys.readouterr().out


def test_fetch_infer_sensors_fills_inferred(tmp_path, live, monkeypatch, capsys):
    def tasks(self, dag_id):
        return [{"task_id": "wait_x", "operator_name": "PythonSensor"}] if dag_id == "z" else []
    monkeypatch.setattr(FakeClient, "tasks", tasks)
    monkeypatch.setattr(FakeClient, "dag_runs", lambda self, d, limit: [{"dag_run_id": "r1"}])
    monkeypatch.setattr(FakeClient, "task_instance", lambda self, d, r, t: {
        "dag_run_id": r, "rendered_fields": {"op_kwargs": {"external_dag_id": "x"}}}, raising=False)
    out = tmp_path / "s.json"
    assert cli.main(["fetch", "--url", URL, "-o", str(out), "--infer-sensors"]) == 0
    data = json.loads(out.read_text())
    assert data["inferred"] == [{"dag": "z", "task": "wait_x", "operator": "PythonSensor",
                                 "source": "rendered", "targets": ["x"], "target_tasks": [],
                                 "kind": "data"}]
    assert "inferring sensors 1/1" in capsys.readouterr().err
    assert cli.main(["downstream", "x", str(out)]) == 0
    assert "z" in capsys.readouterr().out


def test_fetch_without_infer_flag_has_no_inferred(tmp_path, live):
    out = tmp_path / "s.json"
    assert cli.main(["fetch", "--url", URL, "-o", str(out)]) == 0
    assert "inferred" not in json.loads(out.read_text())


def _sensor_on_z(monkeypatch):
    def tasks(self, dag_id):
        return [{"task_id": "wait_x", "operator_name": "PythonSensor"}] if dag_id == "z" else []
    monkeypatch.setattr(FakeClient, "tasks", tasks)
    monkeypatch.setattr(FakeClient, "dag_runs", lambda self, d, limit: [{"dag_run_id": "r1"}])
    monkeypatch.setattr(FakeClient, "task_instance", lambda self, d, r, t: {
        "dag_run_id": r, "rendered_fields": {"op_kwargs": {"external_dag_id": "x"}}}, raising=False)


def test_node_id_skips_catalog_and_inference(tmp_path, live, monkeypatch, capsys):
    _sensor_on_z(monkeypatch)
    out = tmp_path / "s.json"
    assert cli.main(["fetch", "--url", URL, "-o", str(out), "--infer-sensors", "--node-id", "dag:x"]) == 0
    data = json.loads(out.read_text())
    assert "catalog" not in data and "inferred" not in data
    assert "no sensor inference" in capsys.readouterr().err


def test_live_query_infers_when_setting_is_on(live, monkeypatch, capsys):
    _sensor_on_z(monkeypatch)
    assert cli.main(["downstream", "x", "--url", URL]) == 0
    assert " z " not in capsys.readouterr().out
    monkeypatch.setenv("AIRFLOW_DEPGRAPH_INFER_SENSORS", "true")
    assert cli.main(["downstream", "x", "--url", URL]) == 0
    captured = capsys.readouterr()
    assert "via sensor:wait_x (inferred)" in captured.out and "inferring sensors 1/1" in captured.err


def test_fetch_warns_on_stderr_when_status_unavailable(tmp_path, live, monkeypatch, capsys):
    from airflow_depgraph.client import ApiError

    def dag_runs(self, dag_id, limit):
        if dag_id == "y":
            raise ApiError("500 on /dagRuns", 500, "/dagRuns")
        return []

    monkeypatch.setattr(FakeClient, "dag_runs", dag_runs)
    assert cli.main(["fetch", "--url", URL, "-o", str(tmp_path / "s.json"), "--with-status"]) == 0
    captured = capsys.readouterr()
    assert "warning: status unavailable for 1 of 2 DAGs (first error: 500 on /dagRuns)" in captured.err
    assert "warning" not in captured.out


def test_tasks_matching_rejects_snapshot_path_as_regex(capsys):
    assert cli.main(["downstream", "dag_e", "--tasks-matching", FIX]) == 1
    assert "looks like a snapshot file" in capsys.readouterr().err


def test_tasks_matching_rejects_json_name_as_regex(capsys):
    assert cli.main(["downstream", "dag_e", "--tasks-matching", "snapshot.json"]) == 1
    assert "looks like a snapshot file" in capsys.readouterr().err


def test_catalog_only_dag_says_no_dependencies(capsys):
    assert cli.main(["downstream", "dag_x_cron", FIX]) == 0
    out = capsys.readouterr().out
    assert "dag 'dag_x_cron' exists but has no explicit dependencies (schedule: 0 1 * * *)" in out


def test_catalog_only_dag_json_note(capsys):
    assert cli.main(["downstream", "dag_x_cron", FIX, "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out == {"dag": "dag_x_cron", "direction": "downstream", "depth": 5,
                   "dags": [], "note": "no explicit dependencies"}


def test_guard_only_dag_hints_include_guards(tmp_path, inferred_snapshot, capsys):
    from airflow_depgraph import snapshot as snapmod
    path = tmp_path / "s.json"
    snapmod.dump(inferred_snapshot, path)
    assert cli.main(["downstream", "dag_y_cron", str(path)]) == 1
    assert "only linked by deploy guards; add --include-guards" in capsys.readouterr().err


def test_unknown_dag_suggests_close_names(capsys):
    assert cli.main(["downstream", "dag_x_crom", FIX]) == 1
    err = capsys.readouterr().err
    assert "is not in the dependency graph" in err
    assert "did you mean: dag_x_cron" in err


def test_unknown_dag_without_close_names_has_no_suffix(capsys):
    assert cli.main(["downstream", "zzzzzzzz", FIX]) == 1
    err = capsys.readouterr().err
    assert "is not in the dependency graph" in err
    assert "did you mean" not in err


def test_node_id_form_is_accepted(capsys):
    assert cli.main(["downstream", "dag:dag_e", FIX, "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["dag"] == "dag_e"
    assert out["dags"][0]["dag_id"] == "dag_l_report"


def test_fetch_with_tasks_survives_one_forbidden_dag(tmp_path, live, monkeypatch, capsys):
    from airflow_depgraph.client import ApiError

    def tasks(self, dag_id):
        if dag_id == "y":
            raise ApiError("403 on /tasks", 403, "/tasks")
        return [{"task_id": "refresh_dashboard"}]
    monkeypatch.setattr(FakeClient, "tasks", tasks)
    out = tmp_path / "s.json"
    assert cli.main(["fetch", "--url", URL, "-o", str(out), "--with-tasks"]) == 0
    data = json.loads(out.read_text())
    assert data["dags"]["x"] == {"tasks": ["refresh_dashboard"]}
    assert data["dags"]["y"] == {}
    assert "warning: task list unavailable for 1 DAGs" in capsys.readouterr().err


def test_main_returns_130_on_ctrl_c(tmp_path, live, monkeypatch, capsys):
    def interrupted(args):
        raise KeyboardInterrupt
    monkeypatch.setattr(cli, "cmd_fetch", interrupted)
    assert cli.main(["fetch", "--url", URL, "-o", str(tmp_path / "s.json")]) == 130
    assert "interrupted" in capsys.readouterr().err


def test_infer_and_with_tasks_read_each_dag_once(tmp_path, live, monkeypatch):
    calls = []

    def tasks(self, dag_id):
        calls.append(dag_id)
        if dag_id == "z":
            return [{"task_id": "wait_x", "operator_name": "PythonSensor"}]
        return [{"task_id": "refresh_dashboard"}, {"task_id": "other"}]
    monkeypatch.setattr(FakeClient, "tasks", tasks)
    monkeypatch.setattr(FakeClient, "dag_runs", lambda self, d, limit: [{"dag_run_id": "r1"}])
    monkeypatch.setattr(FakeClient, "task_instance", lambda self, d, r, t: {
        "dag_run_id": r, "rendered_fields": {"op_kwargs": {"external_dag_id": "x"}}}, raising=False)
    out = tmp_path / "s.json"
    assert cli.main(["fetch", "--url", URL, "-o", str(out), "--infer-sensors", "--with-tasks"]) == 0
    assert sorted(calls) == ["x", "y", "z"]
    assert json.loads(out.read_text())["dags"]["x"] == {"tasks": ["refresh_dashboard", "other"]}


def test_stale_catalog_dag_gets_unlinked_error_not_no_dependencies(tmp_path, capsys):
    import dataclasses
    from airflow_depgraph import snapshot as snapmod
    snap = snapmod.load(Path(FIX))
    catalog = dict(snap.catalog, dag_old={"tags": [], "is_paused": True, "is_stale": True,
                                          "schedule": None, "owners": []})
    path = tmp_path / "s.json"
    snapmod.dump(dataclasses.replace(snap, catalog=catalog), path)
    assert cli.main(["downstream", "dag_old", str(path)]) == 1
    captured = capsys.readouterr()
    assert "is not in the dependency graph" in captured.err
    assert "exists but has no explicit dependencies" not in captured.out


def test_walk_keyerror_for_existing_node_propagates(monkeypatch):
    def broken_walk(g, start, direction, depth):
        raise KeyError("x")
    monkeypatch.setattr(cli, "walk", broken_walk)
    with pytest.raises(KeyError):
        cli.main(["downstream", "dag_e", FIX])


def test_status_task_list_error_warns_and_keeps_status(tmp_path, live, monkeypatch, capsys):
    from airflow_depgraph.client import ApiError

    def tasks(self, dag_id):
        if dag_id == "y":
            raise ApiError("403 on /tasks", 403, "/tasks")
        return [{"task_id": "refresh_dashboard"}]
    monkeypatch.setattr(FakeClient, "tasks", tasks)
    out = tmp_path / "s.json"
    assert cli.main(["fetch", "--url", URL, "-o", str(out), "--with-status", "--with-tasks"]) == 0
    data = json.loads(out.read_text())
    assert "last_run" in data["dags"]["y"] and "tasks" not in data["dags"]["y"]
    assert data["dags"]["x"]["tasks"] == ["refresh_dashboard"]
    assert "warning: task list unavailable for 1 DAGs" in capsys.readouterr().err


def test_text_row_shows_state_and_last_run(capsys):
    assert cli.main(["downstream", "dag_a", FIX]) == 0
    lines = capsys.readouterr().out.splitlines()
    dag_d = next(line for line in lines if "dag_d " in line and "→" in line)
    assert dag_d.endswith(" · FAILED (tests) · last run 2026-10-22 05:30")
    dag_l = next(line for line in lines if "dag_l_report " in line and "→" in line)
    assert dag_l.endswith(" · ok · last run 2026-10-22 06:00")
    dag_b = next(line for line in lines if "dag_b " in line and "→" in line)
    assert dag_b.endswith("sched: —")


def test_text_row_marks_paused_dag_once(capsys):
    assert cli.main(["upstream", "dag_g", FIX]) == 0
    line = next(x for x in capsys.readouterr().out.splitlines() if "dag_h " in x)
    assert "⏸ paused" in line and line.endswith("sched: —")
    assert "· paused" not in line


def test_json_state_is_normalised_to_success_failed_or_null():
    from airflow_depgraph.cli_format import hits_json
    from airflow_depgraph.graph import Hit
    info = {"b": {"last_run": {"state": "queued", "kind": None, "run_after": None}},
            "c": {"last_run": {"state": "success", "kind": None, "run_after": None}}}
    rows = {r["dag_id"]: r for r in json.loads(hits_json(
        "a", "downstream", 1, [Hit("b", 1, None), Hit("c", 1, None)], info, False))["dags"]}
    assert rows["b"]["state"] is None
    assert rows["c"]["state"] == "success"


def test_live_downstream_shows_status_of_hit_dags(live, monkeypatch, capsys):
    class RunsClient(FakeClient):
        def dag_runs(self, dag_id, limit):
            if dag_id == "y":
                return [{"dag_run_id": "r1", "state": "failed", "run_after": "2026-10-22T05:00:00Z"}]
            return []

        def task_instances(self, dag_id, run_id):
            return [{"task_id": "load", "state": "failed"}]

    monkeypatch.setattr(cli, "AirflowClient", RunsClient)
    assert cli.main(["downstream", "x", "--url", URL]) == 0
    line = next(x for x in capsys.readouterr().out.splitlines() if "dag_y" in x or " y " in x)
    assert line.endswith("· FAILED (pipeline) · last run 2026-10-22 05:00")


def test_json_row_has_parent_and_status(capsys):
    assert cli.main(["downstream", "dag_a", FIX, "--json"]) == 0
    rows = {r["dag_id"]: r for r in json.loads(capsys.readouterr().out)["dags"]}
    assert rows["dag_d"]["parent"] == "dag_c"
    assert rows["dag_d"]["state"] == "failed"
    assert rows["dag_d"]["kind"] == "tests"
    assert rows["dag_d"]["last_run_at"] == "2026-10-22T05:30:00Z"
    assert rows["dag_b"]["parent"] == "dag_a" and rows["dag_b"]["state"] is None


def test_tree_shows_parent_structure(capsys):
    assert cli.main(["downstream", "dag_a", FIX, "--tree"]) == 0
    lines = capsys.readouterr().out.splitlines()
    body = lines[1:]
    assert body[0] == "dag_a"
    assert body[1] == "└ dag_b  via asset:team-orders"
    assert "  └ dag_c  via asset:orders_clean" in body
    assert "    └ dag_d  via trigger:orders_ready  · FAILED (tests)" in body


def test_tree_and_json_are_mutually_exclusive(capsys):
    assert cli.main(["downstream", "dag_a", FIX, "--tree", "--json"]) == 1
    assert "--tree and --json cannot be used together" in capsys.readouterr().err


@pytest.mark.parametrize("kind,header", [
    ("critical", "Critical DAGs (most transitive downstream DAGs) — 4"),
    ("unlinked", "Unlinked DAGs (no explicit or inferred links) — 2"),
    ("unconsumed-assets", "Unconsumed assets (produced, never consumed) — 0"),
    ("failing", "Failing DAGs (last decisive run failed, not paused) — 2"),
])
def test_report_header_and_exit_code(kind, header, capsys):
    extra = ["--top", "4"] if kind == "critical" else []
    assert cli.main(["report", kind, FIX, *extra]) == 0
    assert capsys.readouterr().out.startswith(header)


def test_report_json_is_a_list(capsys):
    assert cli.main(["report", "failing", FIX, "--json"]) == 0
    rows = json.loads(capsys.readouterr().out)
    assert [r["dag_id"] for r in rows] == ["dag_a", "dag_d"]


def test_report_unlinked_without_catalog_exits_1(tmp_path, capsys):
    from dataclasses import replace
    from airflow_depgraph import snapshot as snapmod
    path = tmp_path / "nocat.json"
    snapmod.dump(replace(snapmod.load(Path(FIX)), catalog=None), path)
    assert cli.main(["report", "unlinked", str(path)]) == 1
    assert "snapshot has no catalog" in capsys.readouterr().err
