"""Credential resolution. No network code; secrets never appear in messages."""

from __future__ import annotations

import getpass
import subprocess
from collections.abc import Mapping

from airflow_depgraph.presets import Settings


class CredentialsError(RuntimeError):
    pass


TOKEN_HINT = ("a token needs neither: take the _token cookie from the browser "
              "(README, \"Get a token from the browser\") and set AIRFLOW_DEPGRAPH_TOKEN "
              "or use --token-stdin")


def resolve_user(s: Settings, env: Mapping[str, str]) -> str:
    user = s.user or env.get(s.user_env)
    if not user:
        raise CredentialsError(f"no username: pass --user or set ${s.user_env}; {TOKEN_HINT}")
    return user


def resolve_password(
    s: Settings,
    env: Mapping[str, str],
    *,
    stdin_password: bool,
    stdin,
    isatty: bool,
    run_cmd=subprocess.run,
    prompt=getpass.getpass,
) -> str:
    if stdin_password:
        return stdin.readline().rstrip("\r\n")
    if s.password_env and env.get(s.password_env):
        return env[s.password_env]
    if s.password_cmd:
        done = run_cmd(s.password_cmd, shell=True, capture_output=True, text=True)
        if done.returncode != 0:
            raise CredentialsError(
                f"password_cmd failed ({done.returncode}): {done.stderr.strip()}"
            )
        return done.stdout.strip()
    if isatty:
        return prompt("Airflow password: ")
    source = f"set ${s.password_env}, " if s.password_env else ""
    raise CredentialsError(
        f"no password: use --password-stdin, {source}set password_cmd in the preset, "
        f"or run on a TTY for an interactive prompt; {TOKEN_HINT}"
    )
