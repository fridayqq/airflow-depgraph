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


def format_hits(hits: list[Hit], info: dict[str, dict], url: str | None,
                direction: str, header: str, strip_prefix: str | None = None) -> str:
    lines = [header]
    for h in hits:
        d = info.get(h.dag_id) or {}
        paused = " ⏸ paused" if d.get("is_paused") else ""
        sched = d.get("schedule") or "—"
        lines.append(f"  {ARROWS[direction]}{h.hops} {h.dag_id:<48} via {_text_via(h, strip_prefix)}"
                     f"{paused}  sched: {sched}")
        if url:
            lines.append(f"        {url.rstrip('/')}/dags/{h.dag_id}")
        if d.get("matching_tasks"):
            lines.append(f"        tasks: {', '.join(d['matching_tasks'])}")
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
        row = {"dag_id": h.dag_id, "hops": h.hops, "via": h.via,
               "is_paused": d.get("is_paused"), "schedule": d.get("schedule"),
               "owners": d.get("owners")}
        if with_tasks:
            row["matching_tasks"] = d.get("matching_tasks", [])
        rows.append(row)
    return json.dumps({"dag": dag, "direction": direction, "depth": depth, "dags": rows},
                      indent=2, ensure_ascii=False)
