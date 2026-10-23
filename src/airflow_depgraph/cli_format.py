"""Text and JSON formatting for the downstream/upstream commands."""

from __future__ import annotations

import json

from airflow_depgraph.graph import Hit
from airflow_depgraph.snapshot import Snapshot

TITLES = {
    "downstream": "Downstream DAGs (asset/trigger/sensor consumers)",
    "upstream": "Upstream DAGs (what this DAG waits for)",
}
ARROWS = {"downstream": "→", "upstream": "←"}


def _text_via(hit: Hit, strip_prefix: str | None) -> str:
    """The hop label for text output: asset prefix stripped, guard hops marked as (guard)."""
    if hit.via is None:
        return "—"
    via = hit.via
    if strip_prefix and via.startswith("asset:"):
        via = "asset:" + via[len("asset:"):].removeprefix(strip_prefix)
    if hit.guard:
        via = via.removesuffix(" (inferred)") + " (guard)"
    return via


def _state_text(d: dict) -> str | None:
    """ok, FAILED (kind), or another run state as is; None without status."""
    last = d.get("last_run")
    if not last:
        return None
    state = last.get("state")
    if state == "success":
        return "ok"
    if state == "failed":
        return f"FAILED ({last['kind']})" if last.get("kind") else "FAILED"
    return state


def _last_run_text(d: dict) -> str | None:
    run_after = (d.get("last_run") or {}).get("run_after")
    return f"last run {run_after.replace('T', ' ')[:16]}" if run_after else None


def _row_suffix(d: dict) -> str:
    return "".join(f" · {part}" for part in (_state_text(d), _last_run_text(d)) if part)


def format_hits(hits: list[Hit], info: dict[str, dict], url: str | None,
                direction: str, header: str, strip_prefix: str | None = None) -> str:
    lines = [header]
    for h in hits:
        d = info.get(h.dag_id) or {}
        paused = " ⏸ paused" if d.get("is_paused") else ""
        sched = d.get("schedule") or "—"
        lines.append(f"  {ARROWS[direction]}{h.hops} {h.dag_id:<48} via {_text_via(h, strip_prefix)}"
                     f"{paused}  sched: {sched}{_row_suffix(d)}")
        if url:
            lines.append(f"        {url.rstrip('/')}/dags/{h.dag_id}")
        if d.get("matching_tasks"):
            lines.append(f"        tasks: {', '.join(d['matching_tasks'])}")
    return "\n".join(lines)


def format_tree(hits: list[Hit], info: dict[str, dict], start: str,
                strip_prefix: str | None = None) -> str:
    """Hits as an indented tree following each hit's parent; children sorted by DAG id.
    A hit whose parent is not in the list (filtered out) hangs under the start DAG."""
    ids = {h.dag_id for h in hits}
    children: dict[str, list[Hit]] = {}
    for h in hits:
        parent = h.parent if h.parent in ids else start
        children.setdefault(parent, []).append(h)
    lines = [start]

    def walk(node: str, depth: int) -> None:
        for h in sorted(children.get(node, []), key=lambda x: x.dag_id):
            row = f"{'  ' * (depth - 1)}└ {h.dag_id}"
            if h.via is not None:
                row += f"  via {_text_via(h, strip_prefix)}"
            state = _state_text(info.get(h.dag_id) or {})
            row += f"  · {state}" if state else ""
            lines.append(row)
            walk(h.dag_id, depth + 1)

    walk(start, 1)
    return "\n".join(lines)


def stamp(snap: Snapshot | None) -> str:
    if snap is None:
        return "live"
    if not snap.fetched_at:
        return "snapshot (time unknown)"
    return "snapshot " + snap.fetched_at.replace("T", " ")[:16] + " UTC"


def hits_json(dag: str, direction: str, depth: int, hits: list[Hit],
               info: dict[str, dict], with_tasks: bool) -> str:
    rows = []
    for h in hits:
        d = info.get(h.dag_id) or {}
        last = d.get("last_run") or {}
        row = {"dag_id": h.dag_id, "hops": h.hops, "via": h.via,
               "is_paused": d.get("is_paused"), "schedule": d.get("schedule"),
               "owners": d.get("owners"), "parent": h.parent,
               "state": last.get("state") if last.get("state") in ("success", "failed") else None,
               "kind": last.get("kind"),
               "last_run_at": last.get("run_after")}
        if with_tasks:
            row["matching_tasks"] = d.get("matching_tasks", [])
        rows.append(row)
    return json.dumps({"dag": dag, "direction": direction, "depth": depth, "dags": rows},
                      indent=2, ensure_ascii=False)
