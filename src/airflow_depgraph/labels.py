"""Labels shared by the page, the CLI tables and the reports."""
from __future__ import annotations

RUN_STATES = ("success", "failed")


def normalise_state(state: object) -> str | None:
    """'success', 'failed' or None; other run states are not reported."""
    return state if state in RUN_STATES else None


def state_label(last_run: dict | None) -> str | None:
    """'ok', 'FAILED (pipeline)', 'FAILED (tests)', 'FAILED' without a kind, or None."""
    state = normalise_state((last_run or {}).get("state"))
    if state == "success":
        return "ok"
    if state == "failed":
        kind = last_run.get("kind")
        return f"FAILED ({kind})" if kind else "FAILED"
    return None


def strip_asset_prefix(label: str, prefix: str | None) -> str:
    """Asset label without the preset's prefix; the prefix is only ever stripped from asset labels."""
    return label.removeprefix(prefix) if prefix else label
