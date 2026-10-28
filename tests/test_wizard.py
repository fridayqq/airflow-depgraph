import getpass
import io
import json
import os
import tomllib
from pathlib import Path

import pytest

from airflow_depgraph import cli, presets, wizard
from airflow_depgraph.client import ApiError

URL = "https://airflow.example.com"
PW = "s3cr3t-PW"
HEALTHY = {"metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy"}}


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch, tmp_path):
    for k in list(os.environ):
        if k.startswith("AIRFLOW_DEPGRAPH_"):
            monkeypatch.delenv(k)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))


class FakeAirflow:
    health_result: object = HEALTHY
    health_error: Exception | None = None
    login_errors: list = []        # popped one per login attempt
    logins: list = []

    def __init__(self, url, token=None, timeout=None):
        self.url, self.token = url, token

    def health(self):
        if self.health_error:
            raise self.health_error
        return self.health_result

    @classmethod
    def login(cls, url, user, password, timeout=None):
        cls.logins.append((url, user, password))
        if cls.login_errors:
            raise cls.login_errors.pop(0)
        return cls(url, token="tok")

    def version(self):
        return "3.3.2"

    def list_dags(self):
        def dag(i, tags, paused=False, stale=False):
            return {"dag_id": i, "tags": [{"name": t} for t in tags], "is_paused": paused,
                    "is_stale": stale}
        return [dag("orders_load", ["team-a", "daily"]), dag("orders_clean", ["team-a"]),
                dag("orders_report", ["team-a"], paused=True), dag("other", ["team-b"]),
                dag("plain", []), dag("old", ["gone"], stale=True)]

    def tasks(self, dag_id):
        if dag_id == "orders_clean":
            return [{"task_id": "clean", "operator_name": "PythonOperator"},
                    {"task_id": "wait_load", "operator_name": "PythonSensor"}]
        if dag_id == "other":
            raise ApiError("forbidden", status=403, path=f"/dags/{dag_id}/tasks")
        return [{"task_id": "run", "operator_name": "EmptyOperator"}]

    def dependencies(self, node_id=None):
        labels = ["s3://bucket/a", "s3://bucket/b", "s3://bucket/c", "postgres://db/t"]
        nodes = [{"id": f"asset:{i}", "label": lab, "type": "asset"}
                 for i, lab in enumerate(labels)]
        nodes.append({"id": "sensor:1", "label": "wait", "type": "sensor"})
        return {"nodes": nodes, "edges": []}


def fake(**attrs):
    attrs.setdefault("login_errors", [])
    attrs.setdefault("logins", [])
    return type("Fake", (FakeAirflow,), attrs)


class Script:
    """Scripted input(); records the prompts that were asked."""

    def __init__(self, answers):
        self.answers = list(answers)
        self.prompts = []

    def __call__(self, prompt=""):
        self.prompts.append(prompt)
        if not self.answers:
            raise EOFError
        answer = self.answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer


def prompter(answers, passwords=(), final=True):
    """final: also answer the closing "Write it?" with Enter."""
    script = Script([*answers, ""] if final else answers)
    pws = list(passwords)
    out = io.StringIO()
    p = wizard.Prompter(input_fn=script, getpass_fn=lambda _p: pws.pop(0), out=out, color=False)
    return p, script, out


def run(p, tmp_path, given=None, client=None, env=None, **kw):
    env = {} if env is None else env
    return wizard.run_interactive(p, given or {}, env=env, target=tmp_path / "presets",
                                  client_cls=client or fake(), **kw)


def load(path):
    return tomllib.loads(path.read_text(encoding="utf-8"))


# ---------- pure helpers ----------


def test_tag_counts_sorted_by_count_then_name():
    dags = [{"dag_id": "a", "tags": [{"name": "y"}, "x"]}, {"dag_id": "b", "tags": [{"name": "y"}]},
            {"dag_id": "c", "tags": [{"name": "x"}]}, {"dag_id": "d", "tags": []}]
    assert wizard.tag_counts(dags) == [("x", 2), ("y", 2)]
    assert wizard.tag_counts([{"dag_id": "a", "tags": ["z"]}, {"dag_id": "b", "tags": ["z", "a"]},
                              {"dag_id": "c", "tags": ["a"]}, {"dag_id": "d", "tags": ["z"]}]) \
        == [("z", 3), ("a", 2)]
    assert wizard.tag_counts([]) == []


def test_asset_prefixes():
    labels = ["s3://bucket/a/b", "s3://bucket/c", "s3://other/x", "postgres://db/t1",
              "postgres://db/t2", "x-team://one", "x-team://two", "orders_clean", "/data/a/b",
              "/data/c/d", "s3://bare"]
    got = dict(wizard.asset_prefixes(labels, top=20))
    assert got["s3://bucket/"] == 2
    assert got["postgres://db/"] == 2
    assert got["s3://"] == 4
    assert got["x-team://"] == 2
    assert got["/data/"] == 2
    assert "s3://other/" not in got and "orders_clean" not in got
    ordered = wizard.asset_prefixes(labels)
    assert ordered[0] == ("s3://", 4)
    assert wizard.asset_prefixes(labels, top=2) == ordered[:2]
    assert wizard.asset_prefixes(["a", "b", "s3://only/x"]) == []


def test_asset_prefixes_offers_prefixes_inside_the_scheme():
    labels = ["x-dp-orders://dbt-run", "x-dp-orders://dbt-test", "x-dp-sales://dbt-run",
              "x-dp-customers://load", "x-legacy://load"]
    got = wizard.asset_prefixes(labels)
    assert got[0] == ("x-", 5) and got[1] == ("x-dp-", 4)
    got = dict(wizard.asset_prefixes(labels, top=20))
    assert got["x-dp-orders://"] == 2 and got["x-dp-orders://dbt-"] == 2
    # x-dp-orders: and x-dp-orders:/ strip the same labels as x-dp-orders:// and are left out
    assert not any(p.endswith(":") or p.endswith(":/") for p in got)
    assert wizard.asset_prefixes(["s3://bucket/a", "s3://bucket/b"]) == [("s3://bucket/", 2),
                                                                          ("s3://", 2)]
    assert wizard.asset_prefixes(["order_a", "orders_b"]) == []
    assert wizard.asset_prefixes(["same://x", "same://x"]) == [("same://", 2)]
    assert wizard.asset_prefixes(["only"]) == [] and wizard.asset_prefixes([]) == []


def test_sample_dag_ids_spreads_over_the_sorted_list():
    ids = [f"dag_{i:03d}" for i in range(100)]
    sample = wizard.sample_dag_ids(ids, n=10)
    assert len(sample) == 10 and sample[0] == "dag_000" and sample[-1] == "dag_090"
    assert sample == sorted(sample) and len(set(sample)) == 10
    assert wizard.sample_dag_ids(["b", "a"], n=10) == ["a", "b"]
    assert wizard.sample_dag_ids([], n=10) == []


def test_sample_sensor_tasks_counts_candidates_and_skips_unreadable_dags():
    client = fake()(URL, token="t")
    assert wizard.sample_sensor_tasks(client, ["orders_load", "orders_clean", "other"]) == (2, 1)
    assert wizard.sample_sensor_tasks(client, []) == (0, 0)


def test_sensor_count_and_suggest_with_status():
    nodes = [{"type": "sensor"}, {"type": "dag"}, {"type": "sensor"}]
    assert wizard.sensor_count(nodes) == 2
    assert wizard.suggest_with_status(3000) and not wizard.suggest_with_status(3001)


@pytest.mark.parametrize("raw, want", [
    ("https://airflow.example.com/", URL),
    ("  http://127.0.0.1:8080  ", "http://127.0.0.1:8080"),
    ("https://airflow.example.com/prefix//", "https://airflow.example.com/prefix"),
])
def test_normalize_url_ok(raw, want):
    assert wizard.normalize_url(raw) == want


@pytest.mark.parametrize("raw", ["", "airflow.example.com", "ftp://airflow.example.com",
                                 "https://", "file:///etc/passwd"])
def test_normalize_url_rejects(raw):
    with pytest.raises(ValueError):
        wizard.normalize_url(raw)


def test_normalize_url_assume_https():
    assert wizard.normalize_url("airflow.example.com/", assume_https=True) == URL


@pytest.mark.parametrize("name", ["prod", "team-a_2", "A"])
def test_validate_name_ok(name):
    assert wizard.validate_name(name) == name


@pytest.mark.parametrize("name", ["", "a b", "a/b", "../x", "a.toml", "é"])
def test_validate_name_rejects(name):
    with pytest.raises(ValueError):
        wizard.validate_name(name)


def test_parse_tags():
    listed = ["team-a", "daily"]
    assert wizard.parse_tags("1, daily", listed, {"team-a", "daily"}) == (["team-a", "daily"], [])
    assert wizard.parse_tags("-", listed, None) == ([], [])
    assert wizard.parse_tags("x", listed, {"team-a"}) == (["x"], ["no DAG has tag x"])
    assert wizard.parse_tags("1,1", listed, None) == (["team-a"], [])
    with pytest.raises(ValueError):
        wizard.parse_tags("3", listed, None)


# ---------- render_preset ----------

TRICKY = ["plain", r"(^|[._-])tests?([._-]|$)", r"back\slash", 'quo"te', "it's", "it's \\ \"both\"",
          "ünï☃code", "tab\there", "line\nbreak", "ctl\x01\x7f", "", "  spaced  ", "# not a comment"]


@pytest.mark.parametrize("text", TRICKY)
def test_render_round_trips_strings(text, tmp_path):
    values = {"name": "t", "url": URL, "title": text, "highlight": text,
              "asset_label_strip_prefix": text, "task_query_regex": text,
              "password_cmd": text, "tags": [text, "b"]}
    path = tmp_path / "t.toml"
    path.write_text(wizard.render_preset(values), encoding="utf-8")
    s = presets.resolve({}, {}, str(path))
    assert s.title == text and s.highlight == text and s.asset_label_strip_prefix == text
    assert s.task_query_regex == text and s.password_cmd == text
    assert s.tags == (text, "b")
    assert s.preset_name == "t"


def test_render_round_trips_every_type(tmp_path):
    values = {"name": "t", "url": URL, "user_env": "MY_USER", "password_env": "MY_PW",
              "with_status": True, "with_tasks": False, "infer_sensors": True,
              "highlight_label": "Mine", "tests_task_regex": r"(^|[._-])tests?([._-]|$)",
              "status_runs_lookback": 9, "infer_interval_minutes": 5, "timeout": 12.5,
              "workers": 3, "bulk_status": False}
    path = tmp_path / "t.toml"
    path.write_text(wizard.render_preset(values), encoding="utf-8")
    s = presets.resolve({}, {}, str(path))
    for key, value in values.items():
        assert getattr(s, "preset_name" if key == "name" else key) == value, key
    assert load(path)["timeout"] == 12.5


def test_render_unset_keys_are_commented_defaults():
    text = wizard.render_preset({"name": "t", "url": URL})
    data = tomllib.loads(text)
    assert set(data) == {"name", "url"}
    assert "# workers = 8" in text and "# bulk_status = true" in text
    assert "# tags = [\"team-a\"]" in text
    assert text.startswith("# airflow-depgraph preset")


def test_render_prefers_literal_strings_for_backslashes():
    text = wizard.render_preset({"tests_task_regex": r"a\sb"})
    assert "tests_task_regex = 'a\\sb'" in text


def test_render_rejects_unknown_key():
    with pytest.raises(ValueError):
        wizard.render_preset({"nope": 1})


def test_doc_covers_every_preset_key():
    assert {k for k, _ in wizard._DOC} == presets.PRESET_KEYS


# ---------- write_preset ----------


def test_write_preset_atomic_and_refuses_existing(tmp_path):
    path = tmp_path / "a" / "b" / "p.toml"
    wizard.write_preset(path, "x = 1\n")
    assert path.read_text() == "x = 1\n"
    assert path.stat().st_mode & 0o777 == 0o644
    with pytest.raises(FileExistsError, match="--force"):
        wizard.write_preset(path, "x = 2\n")
    assert path.read_text() == "x = 1\n"
    wizard.write_preset(path, "x = 2\n", force=True)
    assert path.read_text() == "x = 2\n"
    assert [f.name for f in path.parent.iterdir()] == ["p.toml"]


def test_target_dir_precedence(tmp_path, monkeypatch):
    assert wizard.target_dir("/x/y", {"AIRFLOW_DEPGRAPH_PRESETS": "/p"}) == Path("/x/y")
    assert wizard.target_dir(None, {"AIRFLOW_DEPGRAPH_PRESETS": "/p"}) == Path("/p")
    assert wizard.target_dir(None, {"XDG_CONFIG_HOME": "/c"}) == Path("/c/airflow-depgraph/presets")


# ---------- discovery ----------


def test_discover_summarises_the_instance():
    d = wizard.discover(fake()(URL, token="t"), presets.Settings(url=URL))
    j = d.to_json()
    assert j["airflow_version"] == "3.3.2" and j["scheduler"] == "healthy"
    assert j["dags"] == {"total": 5, "active": 4, "paused": 1}
    assert j["tags"] == [{"name": "team-a", "dags": 3}, {"name": "daily", "dags": 1},
                         {"name": "team-b", "dags": 1}]
    assert j["assets"] == 4 and j["sensor_nodes"] == 1
    assert {"prefix": "s3://bucket/", "assets": 3} in j["asset_prefixes"]
    # the sample covers the 4 active DAGs; "other" is unreadable, orders_clean has a PythonSensor
    assert j["sensor_tasks"] == {"sampled_dags": 3, "candidates": 1}
    assert j["suggest"] == {"with_status": True, "infer_sensors": True}
    assert "old" not in d.dag_ids


# ---------- interactive ----------

HAPPY = ["prod", URL, "MY_USER", "1", "MY_PW",       # name, url, user env, source, password env
         "bob",                                       # user for the check (env not set)
         "1", "1",                                    # tags: team-a; prefix: 1st candidate
         "y", "n",                                    # status, sensors
         "^orders_", "y", "Orders",                   # highlight, confirm, label
         "My graph",                                  # title
         "y", "(^|[._-])t([._-]|$)", "", "7", "10", "4"]   # advanced


def test_interactive_happy_path(tmp_path):
    p, script, out = prompter(HAPPY, passwords=[PW])
    fk = fake()
    assert run(p, tmp_path, client=fk) == 0
    path = tmp_path / "presets" / "prod.toml"
    data = load(path)
    assert data == {
        "name": "prod", "url": URL, "user_env": "MY_USER", "password_env": "MY_PW",
        "with_status": True, "infer_sensors": False, "tags": ["team-a"],
        "asset_label_strip_prefix": "s3://bucket/", "highlight": "^orders_",
        "highlight_label": "Orders", "title": "My graph",
        "tests_task_regex": "(^|[._-])t([._-]|$)", "status_runs_lookback": 7,
        "timeout": 10.0, "workers": 4}
    text = out.getvalue()
    assert "[1/7]" in text and "[7/7]" in text
    assert "✓ Airflow is reachable (scheduler healthy)" in text
    assert "✓ signed in as bob — Airflow 3.3.2" in text
    assert "matches 3 DAGs: orders_clean, orders_load, orders_report" in text
    assert "1) team-a  (3)" in text
    assert "export MY_USER=…" in text and "export MY_PW=…" in text
    assert f"presets --preset {path}" in text   # not a lookup dir: the path
    assert fk.logins == [(URL, "bob", PW)]
    assert PW not in text and PW not in path.read_text()
    assert len(script.answers) == 0


def test_flags_are_not_asked_again(tmp_path):
    given = {"name": "p1", "url": URL, "user_env": "U", "password_env": "P", "tags": ["team-a"],
             "strip_prefix": "s3://bucket/", "with_status": False, "infer_sensors": True,
             "highlight": "^o", "highlight_label": "L", "title": "T", "tests_task_regex": "t",
             "task_query_regex": "q", "lookback": 3, "timeout": 9.0, "workers": 2}
    p, script, out = prompter([], passwords=[PW])
    env = {"U": "alice", "P": PW}
    assert run(p, tmp_path, given, env=env) == 0
    assert script.prompts == ["Write it? [Y/n]: "]     # nothing else is asked
    data = load(tmp_path / "presets" / "p1.toml")
    assert data["highlight_label"] == "L" and data["workers"] == 2 and data["tags"] == ["team-a"]
    assert "(from --url)" in out.getvalue() and "export" not in out.getvalue()


def test_only_missing_values_are_asked(tmp_path):
    given = {"name": "p1", "url": URL, "user_env": "U", "password_env": "P"}
    answers = ["-", "0", "n", "n", "", "", "n"]   # tags, prefix, status, sensors, hl, title, adv
    p, script, _ = prompter(answers)
    assert run(p, tmp_path, given, env={"U": "a", "P": PW}) == 0
    assert len(script.prompts) == 8 and script.answers == []
    data = load(tmp_path / "presets" / "p1.toml")
    assert "tags" not in data and "title" not in data and "asset_label_strip_prefix" not in data


def test_password_cmd_source(tmp_path):
    answers = ["p1", URL, "", "2", "pass show airflow/prod", "bob", "-", "0", "n", "n", "", "", "n"]
    p, _, out = prompter(answers)
    env = {"AIRFLOW_DEPGRAPH_USER": "bob"}
    ran = []

    def run_cmd(cmd, **kw):
        ran.append(cmd)
        return type("R", (), {"returncode": 0, "stdout": PW + "\n", "stderr": ""})()
    import airflow_depgraph.wizard as w
    orig = w.resolve_password
    w.resolve_password = lambda *a, **k: orig(*a, run_cmd=run_cmd, **k)
    try:
        assert run(p, tmp_path, env=env) == 0
    finally:
        w.resolve_password = orig
    data = load(tmp_path / "presets" / "p1.toml")
    assert data["password_cmd"] == "pass show airflow/prod" and data["password_env"] == ""
    assert ran == ["pass show airflow/prod"] and PW not in out.getvalue()
    assert "export AIRFLOW_DEPGRAPH_PASSWORD" not in out.getvalue()


def test_existing_file_refused_then_overwritten(tmp_path):
    path = tmp_path / "presets" / "prod.toml"
    path.parent.mkdir()
    path.write_text("# old\n")
    given = {"name": "prod", "url": URL, "user_env": "U", "password_env": "P"}
    rest = ["-", "0", "n", "n", "", "", "n"]
    env = {"U": "a", "P": PW}
    # decline, then a different name
    p, _, _ = prompter(["n", "other"] + rest)
    assert run(p, tmp_path, given, env=env) == 0
    assert path.read_text() == "# old\n" and (path.parent / "other.toml").exists()
    # accept the overwrite
    p, _, _ = prompter(["y"] + rest)
    assert run(p, tmp_path, given, env=env) == 0
    assert load(path)["url"] == URL
    # --force asks nothing about it
    path.write_text("# old\n")
    p, script, _ = prompter(rest)
    assert run(p, tmp_path, given, env=env, force=True) == 0
    assert load(path)["url"] == URL and len(script.prompts) == len(rest) + 1


def test_default_name_is_prod_unless_taken(tmp_path):
    p, script, _ = prompter(["", URL, "", "1", "", "bob", "-", "0", "n", "n", "", "", "n"],
                            passwords=[PW])
    assert run(p, tmp_path) == 0
    assert script.prompts[0] == "Preset name [prod]: "
    assert (tmp_path / "presets" / "prod.toml").exists()
    p, script, _ = prompter(["mine", URL, "", "1", "", "bob", "-", "0", "n", "n", "", "", "n"],
                            passwords=[PW])
    assert run(p, tmp_path) == 0
    assert script.prompts[0] == "Preset name: "


def test_url_is_asked_again_until_airflow_answers(tmp_path):
    not_airflow = fake(health_error=ApiError("404 on /api/v2/monitor/health: Not Found", 404,
                                             "/api/v2/monitor/health"))
    answers = ["p1", "ftp://x", "airflow.example.com", "y", URL, "U", "1", "P", "-", "0", "n", "n",
               "", "", "n"]
    p, _, out = prompter(answers)
    calls = []

    class Flaky(not_airflow):
        def health(self):
            calls.append(self.url)
            if len(calls) == 1:
                return super().health()
            return HEALTHY
    assert run(p, tmp_path, client=Flaky, env={"U": "a", "P": PW}) == 0
    text = out.getvalue()
    assert "must start with http:// or https://" in text
    assert "not an Airflow 3 API: /api/v2/monitor/health returned 404" in text
    assert calls[:2] == [URL, URL]
    assert load(tmp_path / "presets" / "p1.toml")["url"] == URL


def test_unhealthy_scheduler_is_only_a_warning(tmp_path):
    sick = fake(health_result={"scheduler": {"status": "unhealthy"}})
    p, _, out = prompter(["p1", URL, "U", "1", "P", "-", "0", "n", "n", "", "", "n"])
    assert run(p, tmp_path, client=sick, env={"U": "a", "P": PW}) == 0
    assert "scheduler is unhealthy" in out.getvalue()


def test_sign_in_failure_retry_then_skip(tmp_path):
    bad = ApiError("authentication failed (401): check username/password", 401, "/auth/token")
    fk = fake(login_errors=[bad, bad])
    answers = ["p1", URL, "U", "1", "P",
               "r",            # retry (fails again)
               "s",            # skip checks
               "tag1, tag2", "s3://x/", "y", "n", "", "", "n"]
    p, script, out = prompter(answers)
    assert run(p, tmp_path, client=fk, env={"U": "a", "P": PW}) == 0
    assert len(fk.logins) == 2
    text = out.getvalue()
    assert "authentication failed (401)" in text
    assert PW not in text
    data = load(tmp_path / "presets" / "p1.toml")
    assert data["tags"] == ["tag1", "tag2"] and data["asset_label_strip_prefix"] == "s3://x/"
    assert "1) " not in text.split("[6/7]")[1]        # no discovery lists
    assert any(q.startswith("Tags for the default page filter") for q in script.prompts)


def test_sign_in_quit_writes_nothing(tmp_path):
    bad = ApiError("authentication failed (401)", 401, "/auth/token")
    p, _, out = prompter(["p1", URL, "U", "1", "P", "q"])
    assert run(p, tmp_path, client=fake(login_errors=[bad]), env={"U": "a", "P": PW}) == 1
    assert not (tmp_path / "presets").exists()


def test_no_check_skips_all_network(tmp_path):
    class Boom(FakeAirflow):
        def __init__(self, *a, **k):
            raise AssertionError("no network")
    p, _, _ = prompter(["p1", URL, "U", "1", "P", "-", "", "n", "n", "", "", "n"])
    assert run(p, tmp_path, client=Boom, env={"U": "a"}, no_check=True) == 0


@pytest.mark.parametrize("fail", [EOFError(), KeyboardInterrupt()])
def test_interrupt_midway_writes_nothing(tmp_path, fail):
    p, _, _ = prompter(["p1", URL, "U", "1", "P", fail])
    with pytest.raises((EOFError, KeyboardInterrupt)):
        run(p, tmp_path, env={"U": "a", "P": PW})
    assert not list((tmp_path / "presets").glob("*")) if (tmp_path / "presets").exists() else True


def test_declining_the_final_confirmation_writes_nothing(tmp_path):
    given = {"name": "p1", "url": URL, "user_env": "U", "password_env": "P"}
    p, _, out = prompter(["-", "0", "n", "n", "", "", "n", "n"], final=False)
    assert run(p, tmp_path, given, env={"U": "a", "P": PW}) == 1
    assert not (tmp_path / "presets" / "p1.toml").exists()
    assert "not written" in out.getvalue()


# ---------- CLI ----------


@pytest.fixture
def live(monkeypatch):
    fk = fake()
    monkeypatch.setattr(cli, "AirflowClient", fk)
    monkeypatch.setenv("AIRFLOW_DEPGRAPH_USER", "u")
    monkeypatch.setenv("AIRFLOW_DEPGRAPH_PASSWORD", PW)
    return fk


def yes(tmp_path, *extra):
    return ["init", "--yes", "--dir", str(tmp_path / "d"), *extra]


def test_yes_without_name_or_url_is_a_usage_error(tmp_path, live, capsys):
    assert cli.main(yes(tmp_path, "--url", URL)) == 2
    assert "NAME" in capsys.readouterr().err
    assert cli.main(yes(tmp_path, "p1")) == 2
    assert "--url" in capsys.readouterr().err
    assert not (tmp_path / "d").exists()


def test_yes_url_without_scheme_is_a_usage_error(tmp_path, live, capsys):
    assert cli.main(yes(tmp_path, "p1", "--url", "airflow.example.com")) == 2
    assert "http" in capsys.readouterr().err


def test_yes_bad_flag_values_are_usage_errors(tmp_path, live, capsys):
    for flags in (["--highlight", "("], ["--lookback", "0"], ["--user-env", "1x"],
                  ["--timeout", "0"], ["--workers", "0"]):
        assert cli.main(yes(tmp_path, "p1", "--url", URL, *flags)) == 2, flags
    assert not (tmp_path / "d").exists()


def test_yes_happy_path(tmp_path, live, capsys):
    argv = yes(tmp_path, "p1", "--url", URL + "/", "--tag", "team-a", "--tag", "nope",
               "--strip-prefix", "s3://bucket/", "--with-status", "--title", "T",
               "--lookback", "3", "--password-cmd", "echo hunter2", "--highlight", "^o")
    assert cli.main(argv) == 0
    cap = capsys.readouterr()
    path = tmp_path / "d" / "p1.toml"
    assert load(path) == {
        "name": "p1", "url": URL, "user_env": "AIRFLOW_DEPGRAPH_USER",
        "password_cmd": "echo hunter2", "password_env": "", "with_status": True, "infer_sensors": False,
        "tags": ["team-a", "nope"], "title": "T",
        "asset_label_strip_prefix": "s3://bucket/", "highlight": "^o",
        "status_runs_lookback": 3}
    assert "warning: no DAG has tag nope" in cap.err and "tag team-a" not in cap.err
    assert f"wrote {path}" in cap.out and "Next:" in cap.out
    # the command was run for the check, not the (set) default password variable
    assert live.logins == [(URL, "u", "hunter2")]
    assert "is read before" not in cap.err
    assert PW not in cap.out + cap.err + path.read_text()
    assert path.stat().st_mode & 0o777 == 0o644


def test_yes_refuses_existing_without_force(tmp_path, live, capsys):
    path = tmp_path / "d" / "p1.toml"
    path.parent.mkdir()
    path.write_text("# old\n")
    assert cli.main(yes(tmp_path, "p1", "--url", URL)) == 1
    assert "--force" in capsys.readouterr().err and path.read_text() == "# old\n"
    assert cli.main(yes(tmp_path, "p1", "--url", URL, "--force")) == 0
    assert load(path)["url"] == URL


def test_yes_login_401_exits_3_and_writes_nothing(tmp_path, monkeypatch, capsys):
    bad = ApiError("authentication failed (401): check username/password", 401, "/auth/token")
    monkeypatch.setattr(cli, "AirflowClient", fake(login_errors=[bad]))
    monkeypatch.setenv("AIRFLOW_DEPGRAPH_USER", "u")
    monkeypatch.setenv("AIRFLOW_DEPGRAPH_PASSWORD", PW)
    assert cli.main(yes(tmp_path, "p1", "--url", URL)) == 3
    assert not (tmp_path / "d").exists()
    assert PW not in capsys.readouterr().err


def test_yes_missing_credentials_exit_3(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "AirflowClient", fake())
    assert cli.main(yes(tmp_path, "p1", "--url", URL)) == 3
    assert not (tmp_path / "d").exists()


def test_yes_network_error_exits_4(tmp_path, monkeypatch, capsys):
    down = ApiError("no response from x (refused) — VPN/DNS?", None, "/api/v2/monitor/health",
                    network=True)
    monkeypatch.setattr(cli, "AirflowClient", fake(health_error=down))
    assert cli.main(yes(tmp_path, "p1", "--url", URL)) == 4
    assert not (tmp_path / "d").exists()


def test_yes_not_airflow_exits_1(tmp_path, monkeypatch, capsys):
    err = ApiError("404 on /api/v2/monitor/health: Not Found", 404, "/api/v2/monitor/health")
    monkeypatch.setattr(cli, "AirflowClient", fake(health_error=err))
    assert cli.main(yes(tmp_path, "p1", "--url", URL)) == 1
    assert "not an Airflow 3 API" in capsys.readouterr().err


def test_yes_no_check_never_touches_the_network(tmp_path, monkeypatch):
    class Boom:
        def __init__(self, *a, **k):
            raise AssertionError("no network")
    monkeypatch.setattr(cli, "AirflowClient", Boom)
    assert cli.main(yes(tmp_path, "p1", "--url", "http://127.0.0.1:9", "--no-check")) == 0
    assert (tmp_path / "d" / "p1.toml").exists()


def test_yes_writes_to_presets_env_dir(tmp_path, monkeypatch, live):
    monkeypatch.setenv("AIRFLOW_DEPGRAPH_PRESETS", str(tmp_path / "envdir"))
    assert cli.main(["init", "--yes", "p1", "--url", URL, "--no-check"]) == 0
    assert (tmp_path / "envdir" / "p1.toml").exists()


def test_written_preset_works_with_other_commands(tmp_path, live, capsys):
    assert cli.main(yes(tmp_path, "p1", "--url", URL)) == 0
    capsys.readouterr()
    assert cli.main(["presets", "--preset", str(tmp_path / "d" / "p1.toml")]) == 0
    assert f"url = {URL}" in capsys.readouterr().out


def test_no_tty_without_yes_exits_2(tmp_path, live, capsys):
    assert cli.main(["init", "--dir", str(tmp_path / "d")]) == 2
    err = capsys.readouterr().err
    assert "init asks questions; run it in a terminal, or pass --yes with flags" in err
    assert not (tmp_path / "d").exists()


def test_init_has_no_preset_flag(capsys):
    with pytest.raises(SystemExit) as ei:
        cli.main(["init", "--preset", "x"])
    assert ei.value.code == 2


def test_discover_json_shape_and_writes_nothing(tmp_path, live, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert cli.main(["init", "--discover", "--json", "--url", URL]) == 0
    out = capsys.readouterr().out
    data = json.loads(out)
    assert set(data) == {"url", "airflow_version", "scheduler", "dags", "tags", "assets",
                         "asset_prefixes", "sensor_nodes", "sensor_tasks", "suggest"}
    assert data["url"] == URL and data["dags"]["total"] == 5
    assert data["tags"][0] == {"name": "team-a", "dags": 3}
    assert PW not in out
    assert not (tmp_path / "xdg").exists() and list(tmp_path.iterdir()) == []


def test_discover_text_report(tmp_path, live, capsys):
    assert cli.main(["init", "--discover", "--url", URL]) == 0
    out = capsys.readouterr().out
    assert "Airflow 3.3.2" in out and "DAGs: 5 (4 active, 1 paused)" in out
    assert "team-a (3)" in out and "s3://bucket/ (3)" in out
    assert "sensor tasks: 1 in a sample of 3 DAGs" in out


def test_discover_needs_url_and_credentials(live, monkeypatch, capsys):
    assert cli.main(["init", "--discover"]) == 2
    monkeypatch.delenv("AIRFLOW_DEPGRAPH_PASSWORD")
    assert cli.main(["init", "--discover", "--url", URL]) == 3


def test_json_without_discover_is_a_usage_error(capsys):
    assert cli.main(["init", "--json", "--yes", "p", "--url", URL]) == 2


def test_cli_interactive_end_to_end(tmp_path, live, monkeypatch, capsys):
    monkeypatch.setattr(cli, "_tty", lambda: True)
    script = Script(["p1", URL, "", "1", "", "-", "0", "n", "n", "", "", "n", ""])
    monkeypatch.setattr("builtins.input", script)
    monkeypatch.setattr(getpass, "getpass", lambda prompt="": pytest.fail("password prompted"))
    assert cli.main(["init", "--dir", str(tmp_path / "d")]) == 0
    out = capsys.readouterr().out
    assert "airflow-depgraph init — new preset" in out
    assert load(tmp_path / "d" / "p1.toml")["url"] == URL


def test_cli_interactive_ctrl_c_and_eof_exit_130(tmp_path, live, monkeypatch, capsys):
    monkeypatch.setattr(cli, "_tty", lambda: True)
    for fail in (KeyboardInterrupt(), EOFError()):
        monkeypatch.setattr("builtins.input", Script(["p1", fail]))
        assert cli.main(["init", "--dir", str(tmp_path / "d")]) == 130
        assert "interrupted, nothing written" in capsys.readouterr().err
    assert not (tmp_path / "d").exists()


def test_cli_interactive_password_prompt_is_never_echoed(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(cli, "AirflowClient", fake())
    monkeypatch.setattr(cli, "_tty", lambda: True)
    monkeypatch.setattr("builtins.input", Script(["p1", URL, "", "1", "", "bob", "-", "0", "n",
                                                  "n", "", "", "n", ""]))
    monkeypatch.setattr(getpass, "getpass", lambda prompt="": PW)
    assert cli.main(["init", "--dir", str(tmp_path / "d")]) == 0
    cap = capsys.readouterr()
    assert PW not in cap.out + cap.err + (tmp_path / "d" / "p1.toml").read_text()


def test_init_help_has_examples(capsys):
    with pytest.raises(SystemExit):
        cli.main(["init", "--help"])
    assert "Examples:" in capsys.readouterr().out


# ---------- fix round 1 ----------


def _fixed_run_cmd(monkeypatch, out):
    import airflow_depgraph.wizard as w
    orig = w.resolve_password
    ran = []

    def run_cmd(cmd, **kw):
        ran.append(cmd)
        return type("R", (), {"returncode": 0, "stdout": out + "\n", "stderr": ""})()
    monkeypatch.setattr(w, "resolve_password", lambda *a, **k: orig(*a, run_cmd=run_cmd, **k))
    return ran


def test_command_source_check_and_later_runs_use_the_command(tmp_path, monkeypatch):
    from airflow_depgraph import auth
    ran = _fixed_run_cmd(monkeypatch, "from-cmd")
    fk = fake()
    answers = ["p1", URL, "U", "2", "pass show a", "-", "0", "n", "n", "", "", "n"]
    p, _, out = prompter(answers)
    env = {"U": "bob", "AIRFLOW_DEPGRAPH_PASSWORD": "unrelated"}
    assert run(p, tmp_path, client=fk, env=env) == 0
    assert ran == ["pass show a"] and fk.logins == [(URL, "bob", "from-cmd")]
    path = tmp_path / "presets" / "p1.toml"
    assert load(path)["password_env"] == ""
    assert "# empty: the password comes from password_cmd only" in path.read_text()
    assert "export AIRFLOW_DEPGRAPH_PASSWORD" not in out.getvalue()
    # a later run resolves the written preset to the same source
    s = presets.resolve({}, env, str(path))
    got = auth.resolve_password(s, env, stdin_password=False, stdin=None, isatty=False,
                                run_cmd=lambda cmd, **kw: type("R", (), {
                                    "returncode": 0, "stdout": "from-cmd\n", "stderr": ""})())
    assert got == "from-cmd"


def test_empty_password_env_error_does_not_mention_a_variable():
    from airflow_depgraph import auth
    s = presets.Settings(password_env="")
    with pytest.raises(auth.CredentialsError) as ei:
        auth.resolve_password(s, {"": "x"}, stdin_password=False, stdin=None, isatty=False)
    assert "set $" not in str(ei.value) and "password_cmd" in str(ei.value)


def test_presets_shows_empty_password_env(tmp_path, capsys):
    path = tmp_path / "p.toml"
    path.write_text('password_env = ""\npassword_cmd = "x"\n')
    assert cli.main(["presets", "--preset", str(path)]) == 0
    assert "password_env = (empty)" in capsys.readouterr().out


def test_failing_password_cmd_offers_retry_and_never_prints_secrets(tmp_path, monkeypatch):
    import airflow_depgraph.wizard as w
    orig = w.resolve_password

    def run_cmd(cmd, **kw):
        return type("R", (), {"returncode": 1, "stdout": PW, "stderr": "no such entry"})()
    monkeypatch.setattr(w, "resolve_password", lambda *a, **k: orig(*a, run_cmd=run_cmd, **k))
    answers = ["p1", URL, "U", "2", "pass show a", "s", "-", "0", "n", "n", "", "", "n"]
    p, script, out = prompter(answers)
    assert run(p, tmp_path, env={"U": "bob"}) == 0
    text = out.getvalue()
    assert "password_cmd failed (1): no such entry" in text and PW not in text
    assert any(q.startswith("r) retry  s) skip") for q in script.prompts)   # no "type" option


def test_retry_asks_the_user_again_and_offers_typing_the_password(tmp_path):
    bad = ApiError("authentication failed (401): check username/password", 401, "/auth/token")
    fk = fake(login_errors=[bad])
    answers = ["p1", URL, "U", "1", "P", "bob",       # first user
               "t", "bobby",                          # type the password; user asked again
               "-", "0", "n", "n", "", "", "n"]
    p, script, out = prompter(answers, passwords=[PW])
    env = {"P": "wrong"}
    assert run(p, tmp_path, client=fk, env=env) == 0
    assert fk.logins == [(URL, "bob", "wrong"), (URL, "bobby", PW)]
    retry = [q for q in script.prompts if q.startswith("r) retry")][0]
    assert "t) type the password" in retry
    assert "$P is read again on retry and will not change" in out.getvalue()
    again = [q for q in script.prompts if q.startswith("User name")]
    assert again == ["User name (for this check only, not stored): ",
                     "User name (for this check only, not stored) [bob]: "]
    assert PW not in out.getvalue()


def test_stdin_flags_are_rejected_without_yes(tmp_path, live, capsys):
    for flag in ("--password-stdin", "--token-stdin"):
        assert cli.main(["init", flag, "--dir", str(tmp_path / "d")]) == 2
        assert "--password-stdin/--token-stdin need --yes or --discover" in capsys.readouterr().err


@pytest.mark.parametrize("none", ["-", "0", ""])
def test_free_text_none_answers_without_candidates(tmp_path, none):
    class NoPrefixes(FakeAirflow):
        def dependencies(self, node_id=None):
            return {"nodes": [{"id": "dag:a", "label": "a", "type": "dag"}], "edges": []}
    given = {"name": "p1", "url": URL, "user_env": "U", "password_env": "P"}
    answers = ["-", none, "n", "n", none, "", "n"]    # tags, prefix, status, sensors, hl, title
    p, script, _ = prompter(answers)
    assert run(p, tmp_path, given, client=fake_from(NoPrefixes), env={"U": "a", "P": PW}) == 0
    data = load(tmp_path / "presets" / "p1.toml")
    assert "asset_label_strip_prefix" not in data and "highlight" not in data
    assert "Asset label prefix to strip, - for none: " in script.prompts or \
        any("- for none" in q for q in script.prompts)


def fake_from(base):
    return type("F", (base,), {"login_errors": [], "logins": []})


def _given():
    return {"name": "p1", "url": URL, "user_env": "U", "password_env": "P"}


def test_numbered_prefix_choice_and_out_of_range(tmp_path):
    answers = ["-", "9", "abc-not-a-number", "n", "n", "", "", "n"]
    # 9 is out of range -> asked again; the text is then taken literally
    p, _, out = prompter(answers)
    assert run(p, tmp_path, _given(), env={"U": "a", "P": PW}) == 0
    assert "no prefix number 9" in out.getvalue()
    assert load(tmp_path / "presets" / "p1.toml")["asset_label_strip_prefix"] == "abc-not-a-number"


def test_highlight_decline_and_reenter_and_invalid_regex(tmp_path):
    answers = ["-", "0", "n", "n",
               "(", "^orders_", "n",          # invalid, then a regex that is declined
               "^other", "y",                 # accepted
               "", "", "n"]                   # label default, title default, no advanced
    p, _, out = prompter(answers)
    assert run(p, tmp_path, _given(), env={"U": "a", "P": PW}) == 0
    text = out.getvalue()
    assert "invalid regex '('" in text
    assert "matches 3 DAGs" in text and "matches 1 DAG: other" in text
    assert load(tmp_path / "presets" / "p1.toml")["highlight"] == "^other"


def test_advanced_reasks_on_invalid_and_default_writes_nothing(tmp_path):
    answers = ["-", "0", "n", "n", "", "",
               "y",
               "(", "tests",                  # regex: invalid then valid
               "",                            # task_query_regex: none
               "0", "abc", "5",               # lookback: below minimum, not a number, the default
               "nan", "inf", "-1", "25",      # timeout: rejected x3, then the default
               "0", "6"]                      # workers: rejected, then 6
    p, _, out = prompter(answers)
    assert run(p, tmp_path, _given(), env={"U": "a", "P": PW}) == 0
    data = load(tmp_path / "presets" / "p1.toml")
    assert data["tests_task_regex"] == "tests" and data["workers"] == 6
    assert "timeout" not in data and "status_runs_lookback" not in data
    assert "task_query_regex" not in data
    assert "timeout must be" in out.getvalue()


def test_timeout_must_be_finite():
    for bad in (float("nan"), float("inf"), 0.0, -1.0):
        with pytest.raises(ValueError):
            wizard.validate_given({"timeout": bad})


def test_cli_rejects_nan_timeout(tmp_path, live, capsys):
    assert cli.main(yes(tmp_path, "p1", "--url", URL, "--timeout", "nan")) == 2
    assert cli.main(["init", "--discover", "--url", URL, "--timeout", "inf"]) == 2


def test_tag_warning_in_interactive_mode(tmp_path):
    given = {**_given(), "tags": ["team-a", "nope"]}
    p, _, out = prompter(["0", "n", "n", "", "", "n"])
    assert run(p, tmp_path, given, env={"U": "a", "P": PW}) == 0
    assert "no DAG has tag nope" in out.getvalue()
    assert "no DAG has tag team-a" not in out.getvalue()


def test_interrupt_inside_write_leaves_no_temp_file(tmp_path, monkeypatch):
    import os as _os

    def boom(src, dst):
        raise KeyboardInterrupt
    monkeypatch.setattr(_os, "replace", boom)
    path = tmp_path / "d" / "p.toml"
    with pytest.raises(KeyboardInterrupt):
        wizard.write_preset(path, "x = 1\n")
    assert list(path.parent.iterdir()) == []


def test_next_block_uses_the_name_only_when_it_resolves_to_the_file(tmp_path):
    values = {"user_env": "U", "password_env": "P"}
    lookup = tmp_path / "xdg" / "airflow-depgraph" / "presets"
    path = lookup / "p1.toml"
    wizard.write_preset(path, "name = \"p1\"\n")
    env = {"XDG_CONFIG_HOME": str(tmp_path / "xdg"), "U": "a", "P": "b"}
    assert "--preset p1\n" in wizard.written_message(path, "p1", values, env) + "\n"
    # a different directory with the same name: another file would win, so use the path
    other = tmp_path / "other" / "p1.toml"
    wizard.write_preset(other, "name = \"p1\"\n")
    assert f"--preset {other}" in wizard.written_message(other, "p1", values, env)
    # not found anywhere in the lookup dirs
    elsewhere = tmp_path / "e" / "zz.toml"
    wizard.write_preset(elsewhere, "")
    assert f"--preset {elsewhere}" in wizard.written_message(elsewhere, "zz", values, env)


def test_discover_normalises_the_url(tmp_path, live, capsys):
    assert cli.main(["init", "--discover", "--json", "--url", URL + "/"]) == 0
    assert json.loads(capsys.readouterr().out)["url"] == URL
    assert cli.main(["init", "--discover", "--url", "airflow.example.com"]) == 2


@pytest.mark.parametrize("extra", [["--yes"], ["p1"], ["--force"]])
def test_discover_cannot_be_combined(live, extra, capsys):
    assert cli.main(["init", "--discover", "--url", URL, *extra]) == 2
    assert "--discover" in capsys.readouterr().err


def test_yes_tags_survive_a_failing_list_dags(tmp_path, monkeypatch, capsys):
    class Forbidden(FakeAirflow):
        logins: list = []

        def list_dags(self):
            raise ApiError("403 on /api/v2/dags: Forbidden", 403, "/api/v2/dags")
    monkeypatch.setattr(cli, "AirflowClient", Forbidden)
    monkeypatch.setenv("AIRFLOW_DEPGRAPH_USER", "u")
    monkeypatch.setenv("AIRFLOW_DEPGRAPH_PASSWORD", PW)
    assert cli.main(yes(tmp_path, "p1", "--url", URL, "--tag", "team-a")) == 0
    assert "warning: cannot list DAGs" in capsys.readouterr().err
    assert load(tmp_path / "d" / "p1.toml")["tags"] == ["team-a"]


def test_yes_command_source_checks_with_the_command(tmp_path, live, capsys):
    assert cli.main(yes(tmp_path, "p1", "--url", URL, "--password-cmd", "echo hunter2")) == 0
    assert live.logins == [(URL, "u", "hunter2")]
    assert load(tmp_path / "d" / "p1.toml")["password_env"] == ""
    assert "Next:" in capsys.readouterr().out


def test_yes_command_with_explicit_env_var_keeps_it(tmp_path, live):
    assert cli.main(yes(tmp_path, "p1", "--url", URL, "--no-check", "--password-cmd", "x",
                        "--password-env", "MY_PW")) == 0
    assert load(tmp_path / "d" / "p1.toml")["password_env"] == "MY_PW"


@pytest.mark.parametrize("extra", [["--no-check"], ["--tag", "t"], ["--dir", "/x"]])
def test_discover_rejects_init_only_flags(live, extra, capsys):
    assert cli.main(["init", "--discover", "--url", URL, *extra]) == 2


def test_color_only_when_no_color_is_unset(tmp_path, live, monkeypatch):
    seen = []
    monkeypatch.setattr(cli, "_tty", lambda: True)
    monkeypatch.setattr(cli.sys.stdout, "isatty", lambda: True, raising=False)

    def spy(prompter, *a, **k):
        seen.append(prompter.color)
        return 0
    monkeypatch.setattr(cli.wizard, "run_interactive", spy)
    cli.main(["init", "--dir", str(tmp_path)])
    monkeypatch.setenv("NO_COLOR", "")
    cli.main(["init", "--dir", str(tmp_path)])
    assert seen == [True, False]


def test_plurals():
    assert wizard.plural(1, "asset") == "1 asset" and wizard.plural(0, "DAG") == "0 DAGs"
    assert wizard.plural(2, "sensor node") == "2 sensor nodes"
