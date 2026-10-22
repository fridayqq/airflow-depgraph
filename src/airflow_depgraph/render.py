"""Render a snapshot to one self-contained HTML page (d3 inlined, no network)."""

from __future__ import annotations

import html
import json
import re
from pathlib import Path

from airflow_depgraph.graph import reach_dags
from airflow_depgraph.presets import Settings
from airflow_depgraph.snapshot import Snapshot

_PKG = Path(__file__).parent
TEMPLATE = _PKG / "templates" / "graph.html"
VENDOR_D3 = _PKG / "vendor" / "d3.v7.min.js"

_PLACEHOLDER = re.compile(r"__(TITLE|HL_LABEL|STATS|DATA|D3)__")


def build_view(snap: Snapshot, s: Settings, status_override: dict | None = None) -> dict:
    g = snap.graph
    dags = snap.dags or {}
    override = status_override or {}
    try:
        highlight = re.compile(s.highlight) if s.highlight else None
    except re.error as exc:
        raise ValueError(f"invalid highlight regex {s.highlight!r}: {exc}") from exc

    nodes = []
    for raw in snap.nodes:
        nid, ntype = raw["id"], raw["type"]
        label = raw["label"]
        if ntype != "dag" and s.asset_label_strip_prefix:
            label = label.removeprefix(s.asset_label_strip_prefix)
        node: dict = {
            "id": nid,
            "label": label,
            "type": ntype,
            "down": reach_dags(g, nid, "downstream"),
            "up": reach_dags(g, nid, "upstream"),
            "indeg": len(g.inc[nid]),
            "outdeg": len(g.out[nid]),
        }
        if ntype == "dag":
            dag_id = nid.removeprefix("dag:")
            info = dags.get(dag_id) or {}
            paused = bool(info.get("is_paused"))
            if paused:
                node["paused"] = True
            run = info.get("last_run") or {}
            if dag_id in override:
                node["fail"] = dict(override[dag_id])
            elif run.get("state") == "failed":
                node["fail"] = {"kind": run.get("kind") or "pipeline", "paused": paused, "date": run.get("run_after")}
            if highlight and highlight.search(dag_id):
                node["hl"] = True
        nodes.append(node)

    edges = [{"s": e["source_id"], "t": e["target_id"]} for e in snap.edges]
    count = lambda t: sum(1 for n in nodes if n["type"] == t)  # noqa: E731
    stats = {
        "dags": count("dag"),
        "assets": count("asset"),
        "triggers": count("trigger"),
        "sensors": count("sensor"),
        "edges": len(edges),
        "failing": sum(1 for n in nodes if n.get("fail") and not n["fail"].get("paused")),
        "snapshot": snap.fetched_at,
        "source": snap.source,
    }
    return {"nodes": nodes, "edges": edges, "stats": stats}


def _plural(n: int, one: str, many: str) -> str:
    return f"{n} {one if n == 1 else many}"


def _stats_line(st: dict) -> str:
    return " · ".join(
        [
            _plural(st["dags"], "DAG", "DAGs"),
            _plural(st["assets"], "asset", "assets"),
            _plural(st["triggers"], "trigger", "triggers"),
            _plural(st["sensors"], "sensor", "sensors"),
            _plural(st["edges"], "edge", "edges"),
            f"failing now: {st['failing']}",
            f"snapshot {st['snapshot']}",
        ]
    )


def _json_for_script(obj: object) -> str:
    # "<" escaped so no value can close the script element or open a comment.
    return json.dumps(obj).replace("<", "\\u003c")


def render(snap: Snapshot, s: Settings, status_override: dict | None = None) -> str:
    view = build_view(snap, s, status_override)
    data = {**view, "meta": {"source": snap.source, "airflow_version": snap.airflow_version}}
    values = {
        "TITLE": html.escape(s.title),
        "HL_LABEL": html.escape(s.highlight_label),
        "STATS": html.escape(_stats_line(view["stats"])),
        "DATA": _json_for_script(data),
        "D3": VENDOR_D3.read_text(encoding="utf-8"),
    }
    # One pass, so substituted content (labels, d3 source) is never re-scanned for placeholders.
    return _PLACEHOLDER.sub(lambda m: values[m.group(1)], TEMPLATE.read_text(encoding="utf-8"))
