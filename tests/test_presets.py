from pathlib import Path

import pytest

from airflow_depgraph.presets import (
    PRESET_KEYS,
    PresetError,
    Settings,
    find_preset,
    list_presets,
    load_preset,
    resolve,
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch, tmp_path):
    for var in ("AIRFLOW_DEPGRAPH_PRESETS", "AIRFLOW_DEPGRAPH_PRESET"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg-empty"))
    # Callers passing a bare {} env fall back to Path.home(); keep that off the real home.
    monkeypatch.setenv("HOME", str(tmp_path / "home-empty"))


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def test_find_by_path(tmp_path):
    f = write(tmp_path / "x.toml", 'name = "x"\n')
    assert find_preset(str(f), {}) == f


def test_find_env_dir_before_xdg(tmp_path, monkeypatch):
    env_dir = tmp_path / "envdir"
    xdg = tmp_path / "xdg"
    a = write(env_dir / "p.toml", 'url = "A"\n')
    write(xdg / "airflow-depgraph" / "presets" / "p.toml", 'url = "B"\n')
    monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg))
    monkeypatch.setenv("AIRFLOW_DEPGRAPH_PRESETS", str(env_dir))
    import os

    assert find_preset("p", dict(os.environ)) == a


def test_find_xdg(tmp_path, monkeypatch):
    xdg = tmp_path / "xdg"
    b = write(xdg / "airflow-depgraph" / "presets" / "p.toml", "")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg))
    import os

    assert find_preset("p", dict(os.environ)) == b


def test_find_builtin_example():
    assert find_preset("example", {}).name == "example.toml"


def test_find_missing_lists_dirs():
    with pytest.raises(PresetError, match="looked in"):
        find_preset("nope-nope", {})


def test_load_rejects_unknown_key(tmp_path):
    f = write(tmp_path / "bad.toml", 'foo = 1\n')
    with pytest.raises(PresetError, match=r"unknown key 'foo' in .*bad\.toml"):
        load_preset(f)


def test_load_example_is_valid():
    assert load_preset(find_preset("example", {})) == {"name": "example"}


def test_preset_keys_exclude_secrets():
    assert not {"user", "token", "preset_name"} & PRESET_KEYS
    assert "url" in PRESET_KEYS


def test_resolve_precedence(tmp_path):
    f = write(tmp_path / "p.toml", 'name = "p"\nurl = "A"\n')
    env = {"AIRFLOW_DEPGRAPH_URL": "B"}
    assert resolve({"url": "C"}, env, str(f)).url == "C"
    assert resolve({"url": None}, env, str(f)).url == "B"
    assert resolve({}, {}, str(f)).url == "A"
    assert resolve({}, {}, str(f)).preset_name == "p"


def test_resolve_preset_none_overrides_env(tmp_path):
    f = write(tmp_path / "p.toml", 'url = "A"\n')
    env = {"AIRFLOW_DEPGRAPH_PRESET": str(f)}
    assert resolve({}, env, None).url == "A"
    s = resolve({}, env, "none")
    assert s.url is None and s.preset_name is None


def test_resolve_bool_env():
    assert resolve({}, {"AIRFLOW_DEPGRAPH_WITH_STATUS": "yes"}, None).with_status is True
    assert resolve({}, {"AIRFLOW_DEPGRAPH_WITH_STATUS": "No"}, None).with_status is False


def test_resolve_int_and_bad_int():
    assert resolve({}, {"AIRFLOW_DEPGRAPH_STATUS_RUNS_LOOKBACK": "7"}, None).status_runs_lookback == 7
    with pytest.raises(PresetError):
        resolve({}, {"AIRFLOW_DEPGRAPH_STATUS_RUNS_LOOKBACK": "x"}, None)


def test_resolve_no_preset_by_default():
    s = resolve({}, {}, None)
    assert s.preset_name is None
    assert s == Settings()


def test_list_presets_dedup_first_wins(tmp_path, monkeypatch):
    env_dir = tmp_path / "envdir"
    a = write(env_dir / "example.toml", "")
    write(env_dir / "b.toml", "")
    monkeypatch.setenv("AIRFLOW_DEPGRAPH_PRESETS", str(env_dir))
    import os

    got = list_presets(dict(os.environ))
    assert [n for n, _ in got] == ["b", "example"]
    assert dict(got)["example"] == a


def test_bare_env_never_reads_real_home():
    assert "home-empty" in str(Path.home())


def test_load_invalid_toml(tmp_path):
    f = write(tmp_path / "broken.toml", "url = \n")
    with pytest.raises(PresetError, match=r"invalid TOML in .*broken\.toml"):
        load_preset(f)


def test_resolve_bad_int_in_preset_file(tmp_path):
    f = write(tmp_path / "p.toml", 'status_runs_lookback = "five"\n')
    with pytest.raises(PresetError, match="status_runs_lookback"):
        resolve({}, {}, str(f))


def test_example_mentions_every_key():
    text = find_preset("example", {}).read_text()
    missing = [k for k in sorted(PRESET_KEYS | {"name"}) if k not in text]
    assert not missing
