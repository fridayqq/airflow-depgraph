"""Render a snapshot to one self-contained HTML page (d3 inlined, no network)."""

from __future__ import annotations

import html
import json
import re
from collections import Counter
from pathlib import Path

from airflow_depgraph.graph import Graph, reach_dags
from airflow_depgraph.labels import strip_asset_prefix
from airflow_depgraph.presets import Settings
from airflow_depgraph.snapshot import Snapshot, with_inferred

_PKG = Path(__file__).parent
TEMPLATE = _PKG / "templates" / "graph.html"
VENDOR_D3 = _PKG / "vendor" / "d3.v7.min.js"
CORE_JS = _PKG / "templates" / "graph-core.js"

# Search syntax "[N+]text[+[N]]"; the page reads this exact pattern, and a test pins it.
SEARCH_PATTERN = r"^(?:(\d*)\+)?(.+?)(?:\+(\d*))?$"

_PLACEHOLDER = re.compile(r"__(TITLE|HL_LABEL|STATS|DATA|D3|CORE|SEARCH_RE)__")


def build_view(snap: Snapshot, s: Settings, status_override: dict | None = None) -> dict:
    dags = snap.dags or {}
    catalog = snap.catalog or {}
    override = status_override or {}
    try:
        highlight = re.compile(s.highlight) if s.highlight else None
    except re.error as exc:
        raise ValueError(f"invalid highlight regex {s.highlight!r}: {exc}") from exc

    # Stale DAGs (gone from the DAG folder) are dropped with their edges.
    # Inferred deploy guards are on the page (behind a toggle) but never counted as reach.
    stale = {"dag:" + d for d, c in catalog.items() if c.get("is_stale")}
    def live(e: object) -> bool:      # sensor DAG not stale, and at least one target not stale
        targets = e.get("targets") if isinstance(e, dict) else None
        return (isinstance(e, dict) and "dag:" + str(e.get("dag")) not in stale
                and not (targets and all("dag:" + str(t) in stale for t in targets)))
    inferred = [e for e in snap.inferred or [] if live(e)]
    all_nodes, all_edges = with_inferred(snap.nodes, snap.edges, inferred, include_guards=True)
    raw_nodes = [n for n in all_nodes if n["id"] not in stale]
    raw_edges = [e for e in all_edges if e["source_id"] not in stale and e["target_id"] not in stale]
    guards = {n["id"] for n in raw_nodes if n.get("kind") == "guard"}
    g = Graph.from_payload(raw_nodes, [e for e in raw_edges if e["source_id"] not in guards
                                       and e["target_id"] not in guards])

    def dag_fields(node: dict, dag_id: str) -> None:
        info = dags.get(dag_id) or {}
        cat = catalog.get(dag_id)
        paused = bool(cat["is_paused"] if cat and "is_paused" in cat else info.get("is_paused"))
        node["paused"] = paused
        node["tags"] = list((cat or {}).get("tags") or [])
        node["schedule"] = (cat or {}).get("schedule") or info.get("schedule")
        run = info.get("last_run") or {}
        if dag_id in override:
            node["fail"] = dict(override[dag_id])
        elif run.get("state") == "failed":
            node["fail"] = {"kind": run.get("kind") or "pipeline", "paused": paused,
                            "date": run.get("run_after")}
        if highlight and highlight.search(dag_id):
            node["hl"] = True

    indeg = Counter(e["target_id"] for e in raw_edges)
    outdeg = Counter(e["source_id"] for e in raw_edges)
    nodes = []
    for raw in raw_nodes:
        nid, ntype = raw["id"], raw["type"]
        label = raw["label"]
        if ntype != "dag":
            label = strip_asset_prefix(label, s.asset_label_strip_prefix)
        node: dict = {
            "id": nid,
            "label": label,
            "type": ntype,
            "down": reach_dags(g, nid, "downstream"),
            "up": reach_dags(g, nid, "upstream"),
            "indeg": indeg[nid],
            "outdeg": outdeg[nid],
        }
        if ntype == "dag":
            dag_fields(node, nid.removeprefix("dag:"))
            if indeg[nid] + outdeg[nid] and not g.inc[nid] and not g.out[nid]:
                node["guard_only"] = True       # unlinked while the page hides deploy guards
        if raw.get("inferred"):
            node["inferred"], node["kind"] = raw["inferred"], raw.get("kind") or "data"
        nodes.append(node)

    # Catalog DAGs with no cross-DAG dependency at all.
    in_graph = {n["id"] for n in all_nodes}
    for dag_id in sorted(catalog):
        nid = "dag:" + dag_id
        if nid in in_graph or nid in stale:
            continue
        node = {"id": nid, "label": dag_id, "type": "dag", "unlinked": True,
                "down": 0, "up": 0, "indeg": 0, "outdeg": 0}
        dag_fields(node, dag_id)
        nodes.append(node)

    edges = [{"s": e["source_id"], "t": e["target_id"]} for e in raw_edges]
    count = lambda t: sum(1 for n in nodes if n["type"] == t)  # noqa: E731
    dag_nodes = [n for n in nodes if n["type"] == "dag"]
    stats = {
        "dags": len(dag_nodes),
        "assets": count("asset"),
        "triggers": count("trigger"),
        "sensors": count("sensor"),
        "edges": len(edges),
        "failing": sum(1 for n in nodes if n.get("fail") and not n["fail"].get("paused")),
        "unlinked": sum(1 for n in dag_nodes if n.get("unlinked")),
        "paused": sum(1 for n in dag_nodes if n["paused"]),
        "tagged": sum(1 for n in dag_nodes if set(n["tags"]) & set(s.tags)),
        "inferred": None if snap.inferred is None else sum(
            1 for n in nodes if n.get("inferred") and n["kind"] != "guard"),
        "guards": sum(1 for n in nodes if n.get("kind") == "guard"),
        "status_errors": sum(1 for v in dags.values() if isinstance(v, dict) and "error" in v),
        "snapshot": snap.fetched_at,
        "source": snap.source,
    }
    return {"nodes": nodes, "edges": edges, "stats": stats}


def _plural(n: int, one: str, many: str) -> str:
    return f"{n} {one if n == 1 else many}"


def _snapshot_text(fetched_at: str | None) -> str:
    """'snapshot 2026-10-22 06:00 UTC'; the page adds the age to it."""
    if not fetched_at:
        return "snapshot time unknown"
    return "snapshot " + fetched_at.replace("T", " ")[:16] + " UTC"


def _stats_line(st: dict, inferred_note: str | None = None, inferred_updated: str | None = None) -> str:
    if inferred_note is None and st.get("inferred") is not None:
        inferred_note = f"inferred links: {st['inferred']}"
        if inferred_updated:
            inferred_note += f", updated {inferred_updated}"
    status_note = []
    if st.get("status_errors"):
        status_note = [f"status unavailable for {st['status_errors']} DAGs"]
    return " · ".join(
        [
            _plural(st["dags"], "DAG", "DAGs"),
            _plural(st["assets"], "asset", "assets"),
            _plural(st["triggers"], "trigger", "triggers"),
            _plural(st["edges"], "edge", "edges"),
            f"failing at snapshot: {st['failing']}",
            f"sensors: {st['sensors']} (Airflow)",
        ] + ([inferred_note] if inferred_note else []) + [_snapshot_text(st["snapshot"])] + status_note
    )


def _json_for_script(obj: object) -> str:
    # "<" escaped so no value can close the script element or open a comment.
    return json.dumps(obj).replace("<", "\\u003c")


def render(snap: Snapshot, s: Settings, status_override: dict | None = None, *,
           inferred_note: str | None = None, inferred_updated: str | None = None) -> str:
    """Header gets "sensor links: N[, updated <inferred_updated>]"; inferred_note replaces it."""
    view = build_view(snap, s, status_override)
    meta = {"source": snap.source, "airflow_version": snap.airflow_version, "tags": list(s.tags),
            "catalog": snap.catalog is not None, "snapshot": snap.fetched_at or None}
    data = {**view, "meta": meta}
    values = {
        "TITLE": html.escape(s.title),
        "HL_LABEL": html.escape(s.highlight_label),
        "STATS": html.escape(_stats_line(view["stats"], inferred_note, inferred_updated)),
        "DATA": _json_for_script(data),
        "SEARCH_RE": SEARCH_PATTERN,
        "D3": VENDOR_D3.read_text(encoding="utf-8"),
        "CORE": CORE_JS.read_text(encoding="utf-8"),
    }
    # One pass, so substituted content (labels, d3 source) is never re-scanned for placeholders.
    return _PLACEHOLDER.sub(lambda m: values[m.group(1)], TEMPLATE.read_text(encoding="utf-8"))
