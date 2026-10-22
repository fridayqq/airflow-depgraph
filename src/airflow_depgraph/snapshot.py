from __future__ import annotations

import json
import os
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


def validate_payload(raw: dict) -> tuple[list[dict], list[dict]]:
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

    # Permission-filtered payloads may reference nodes they omit; drop those edges.
    node_ids = {n["id"] for n in nodes}
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

    @cached_property
    def graph(self) -> Graph:
        return Graph.from_payload(self.nodes, self.edges)


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
    return Snapshot(
        format=fmt,
        fetched_at=data.get("fetched_at", ""),
        source=data.get("source"),
        airflow_version=data.get("airflow_version"),
        nodes=nodes,
        edges=edges,
        dags=data.get("dags"),
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
