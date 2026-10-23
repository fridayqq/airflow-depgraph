from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import cached_property
from pathlib import Path
from typing import Any

from airflow_depgraph.graph import Graph

FORMAT = 1
_NODE_KEYS = ("id", "label", "type")
_EDGE_KEYS = ("source_id", "target_id")


class SnapshotError(ValueError):
    pass


def _before_3_1_8(version: str | None) -> bool:
    m = re.match(r"\s*(\d+)\.(\d+)\.(\d+)", version or "")
    return bool(m) and tuple(int(g) for g in m.groups()) < (3, 1, 8)


def _is_trigger_target(other: object, dag_node: str, by_id: dict) -> bool:
    node = by_id.get(other)
    parts = other.split(":", 3) if isinstance(other, str) else []
    return bool(node and node.get("type") == "trigger" and len(parts) == 4
                and parts[0] == "trigger" and parts[2] == dag_node[4:])


def validate_payload(raw: dict, airflow_version: str | None = None) -> tuple[list[dict], list[dict]]:
    prefix = "unexpected /ui/dependencies payload"
    nodes = raw.get("nodes") if isinstance(raw, dict) else None
    edges = raw.get("edges") if isinstance(raw, dict) else None
    if not isinstance(nodes, list):
        raise SnapshotError(f"{prefix}: 'nodes' missing or not a list")
    if not isinstance(edges, list):
        raise SnapshotError(f"{prefix}: 'edges' missing or not a list")

    for node in nodes:
        if not isinstance(node, dict) or any(k not in node for k in _NODE_KEYS):
            raise SnapshotError(f"{prefix}: node without {'/'.join(_NODE_KEYS)}: {node!r}")
    for edge in edges:
        if not isinstance(edge, dict) or any(k not in edge for k in _EDGE_KEYS):
            raise SnapshotError(f"{prefix}: edge without source_id/target_id: {edge!r}")

    # Airflow < 3.1.8 lists a trigger edge to a DAG it omits from nodes: add that DAG node.
    # Every other dangling edge (e.g. permission-filtered) is dropped.
    nodes = list(nodes)
    node_ids = {n["id"] for n in nodes}
    if _before_3_1_8(airflow_version):
        by_id = {n["id"]: n for n in nodes}
        for edge in edges:
            for end, other in ((edge["source_id"], edge["target_id"]),
                               (edge["target_id"], edge["source_id"])):
                if (isinstance(end, str) and end.startswith("dag:") and end not in node_ids
                        and _is_trigger_target(other, end, by_id)):
                    nodes.append({"id": end, "label": end[4:], "type": "dag"})
                    node_ids.add(end)
    kept = [e for e in edges if e["source_id"] in node_ids and e["target_id"] in node_ids]
    return nodes, kept


@dataclass
class Snapshot:
    format: int
    fetched_at: str
    source: str | None
    airflow_version: str | None
    nodes: list[dict]
    edges: list[dict]
    dags: dict[str, dict] | None = None
    catalog: dict[str, dict] | None = None
    inferred: list[dict] | None = None

    @cached_property
    def graph(self) -> Graph:
        """Explicit plus inferred dependencies; deploy guards are left out."""
        return self.graph_for()

    def graph_for(self, inferred: bool = True, guards: bool = False) -> Graph:
        if not inferred:
            return Graph.from_payload(self.nodes, self.edges)
        return Graph.from_payload(*with_inferred(self.nodes, self.edges, self.inferred,
                                                 include_guards=guards))


def _usable(entry: object) -> bool:
    return (isinstance(entry, dict) and isinstance(entry.get("dag"), str)
            and isinstance(entry.get("task"), str) and isinstance(entry.get("targets"), list)
            and entry.get("source") not in (None, "none") and bool(entry["targets"]))


def with_inferred(nodes: list[dict], edges: list[dict], inferred: list[dict] | None, *,
                  include_guards: bool = False) -> tuple[list[dict], list[dict]]:
    """Add one sensor link node per inferred sensor: dag:<target> -> node -> dag:<sensor DAG>."""
    if not inferred:
        return nodes, edges
    nodes, edges = list(nodes), list(edges)
    ids = {n["id"] for n in nodes}

    def add(node: dict) -> None:
        if node["id"] not in ids:
            ids.add(node["id"])
            nodes.append(node)

    for e in inferred:
        if not _usable(e) or (e.get("kind") == "guard" and not include_guards):
            continue
        link = f"sensor-inferred:{e['dag']}:{e['task']}"
        if link in ids:
            continue
        add({"id": link, "label": e["task"], "type": "sensor", "inferred": e["source"],
             "kind": e.get("kind") or "data"})
        add({"id": "dag:" + e["dag"], "label": e["dag"], "type": "dag"})
        edges.append({"source_id": link, "target_id": "dag:" + e["dag"]})
        for target in dict.fromkeys(t for t in e["targets"] if isinstance(t, str)):
            add({"id": "dag:" + target, "label": target, "type": "dag"})
            edges.append({"source_id": "dag:" + target, "target_id": link})
    return nodes, edges


def visible_inferred(inferred: list[dict], visible: set[str]) -> list[dict]:
    """Inferred sensors reduced to the DAGs one viewer may see (sensor DAG and targets)."""
    out = []
    for e in inferred:
        if not _usable(e) or e["dag"] not in visible:
            continue
        targets = [t for t in e["targets"] if t in visible]
        if targets:
            out.append({**e, "targets": targets})
    return out


def _tag_name(tag: object) -> str:
    return str(tag.get("name", "")) if isinstance(tag, dict) else str(tag)


def build_catalog(dags: list[dict]) -> dict[str, dict]:
    """{dag_id: tags/is_paused/is_stale/schedule/owners} from GET /api/v2/dags items."""
    return {
        d["dag_id"]: {
            "tags": [_tag_name(t) for t in d.get("tags") or []],
            "is_paused": bool(d.get("is_paused")),
            "is_stale": bool(d.get("is_stale")),
            "schedule": d.get("timetable_summary") or d.get("timetable_description") or None,
            "owners": d.get("owners") or [],
        }
        for d in dags if isinstance(d, dict) and d.get("dag_id")
    }


def load(path: Path) -> Snapshot:
    try:
        data: Any = json.loads(Path(path).read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SnapshotError(f"snapshot is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise SnapshotError("snapshot must be a JSON object")

    fmt = data.get("format")
    if fmt != FORMAT:
        raise SnapshotError(f"snapshot format {fmt!r} is not supported (this version reads format {FORMAT})")

    nodes, edges = validate_payload(data)
    inferred = data.get("inferred")
    if inferred is not None and not isinstance(inferred, list):
        raise SnapshotError("snapshot 'inferred' must be a list")
    return Snapshot(
        format=fmt,
        fetched_at=data.get("fetched_at", ""),
        source=data.get("source"),
        airflow_version=data.get("airflow_version"),
        nodes=nodes,
        edges=edges,
        dags=data.get("dags"),
        catalog=data.get("catalog"),
        inferred=inferred,
    )


def dump(snap: Snapshot, path: Path) -> None:
    payload: dict[str, Any] = {
        "format": snap.format,
        "fetched_at": snap.fetched_at,
        "source": snap.source,
        "airflow_version": snap.airflow_version,
        "nodes": snap.nodes,
        "edges": snap.edges,
    }
    if snap.dags is not None:
        payload["dags"] = snap.dags
    if snap.catalog is not None:
        payload["catalog"] = snap.catalog
    if snap.inferred is not None:
        payload["inferred"] = snap.inferred

    path = Path(path)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def new(
    nodes: list[dict],
    edges: list[dict],
    source: str | None,
    airflow_version: str | None,
    dags: dict[str, dict] | None = None,
) -> Snapshot:
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return Snapshot(
        format=FORMAT,
        fetched_at=fetched_at,
        source=source,
        airflow_version=airflow_version,
        nodes=nodes,
        edges=edges,
        dags=dags,
    )
