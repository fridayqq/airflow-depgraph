import io

import pytest

from airflow_depgraph.auth import CredentialsError, resolve_password, resolve_user
from airflow_depgraph.presets import Settings


class _Done:
    def __init__(self, rc=0, out="", err=""):
        self.returncode, self.stdout, self.stderr = rc, out, err


def _pw(s=None, env=None, **kw):
    kw.setdefault("stdin_password", False)
    kw.setdefault("stdin", io.StringIO(""))
    kw.setdefault("isatty", False)
    return resolve_password(s or Settings(), env or {}, **kw)


def test_user_from_settings_then_env_then_error():
    assert resolve_user(Settings(user="a"), {"AIRFLOW_DEPGRAPH_USER": "b"}) == "a"
    assert resolve_user(Settings(), {"AIRFLOW_DEPGRAPH_USER": "b"}) == "b"
    with pytest.raises(CredentialsError, match="AIRFLOW_DEPGRAPH_USER"):
        resolve_user(Settings(), {})


def test_password_stdin_wins_over_env():
    got = _pw(
        env={"AIRFLOW_DEPGRAPH_PASSWORD": "env"},
        stdin_password=True,
        stdin=io.StringIO("fromstdin\nrest\n"),
    )
    assert got == "fromstdin"


def test_password_env_used():
    assert _pw(env={"AIRFLOW_DEPGRAPH_PASSWORD": "env"}) == "env"


def test_password_cmd_used_when_env_missing():
    seen = {}

    def run(cmd, **kw):
        seen["cmd"], seen["kw"] = cmd, kw
        return _Done(out="s3cret\n")

    assert _pw(Settings(password_cmd="pass show x"), run_cmd=run) == "s3cret"
    assert seen["cmd"] == "pass show x"
    assert seen["kw"] == {"shell": True, "capture_output": True, "text": True}


def test_password_cmd_failure():
    run = lambda cmd, **kw: _Done(rc=2, err="boom\n")  # noqa: E731
    with pytest.raises(CredentialsError, match=r"password_cmd failed \(2\): boom"):
        _pw(Settings(password_cmd="x"), run_cmd=run)


def test_password_non_tty_without_sources_errors():
    with pytest.raises(CredentialsError) as ei:
        _pw()
    msg = str(ei.value)
    for needle in ("--password-stdin", "AIRFLOW_DEPGRAPH_PASSWORD", "password_cmd", "prompt"):
        assert needle in msg


def test_password_prompt_when_tty():
    assert _pw(isatty=True, prompt=lambda *a: "typed") == "typed"
