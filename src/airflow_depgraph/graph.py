from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Literal

Direction = Literal["downstream", "upstream"]


@dataclass(frozen=True)
class Node:
    id: str
    label: str
    type: str
    inferred: bool = False
    guard: bool = False


@dataclass(frozen=True)
class Hit:
    dag_id: str
    hops: int
    via: str | None
    guard: bool = False  # the hop goes through an inferred deploy guard
    parent: str | None = None  # the DAG one hop closer to the start (the start DAG for hop 1)


@dataclass
class Graph:
    nodes: dict[str, Node] = field(default_factory=dict)
    out: dict[str, list[str]] = field(default_factory=dict)
    inc: dict[str, list[str]] = field(default_factory=dict)
    dag_ids: list[str] = field(default_factory=list)

    @classmethod
    def from_payload(cls, nodes: list[dict], edges: list[dict]) -> Graph:
        g = cls()
        for n in nodes:
            node = Node(id=n["id"], label=n["label"], type=n["type"], inferred=bool(n.get("inferred")),
                        guard=n.get("kind") == "guard")
            g.nodes[node.id] = node
            g.out.setdefault(node.id, [])
            g.inc.setdefault(node.id, [])
        for e in edges:
            g.out[e["source_id"]].append(e["target_id"])
            g.inc[e["target_id"]].append(e["source_id"])
        g.dag_ids = sorted(n.id.removeprefix("dag:") for n in g.nodes.values() if n.type == "dag")
        return g


def _is_dag(g: Graph, node_id: str) -> bool:
    return g.nodes[node_id].type == "dag"


def _neighbours(g: Graph, node_id: str, direction: Direction) -> list[str]:
    adj = g.out if direction == "downstream" else g.inc
    return sorted(adj.get(node_id, []))


def _link_label(g: Graph, node_id: str) -> str:
    node = g.nodes[node_id]
    return f"{node.type}:{node.label}" + (" (inferred)" if node.inferred else "")


def walk(g: Graph, start: str, direction: Direction, depth: int = 5) -> list[Hit]:
    start_id = f"dag:{start}"
    if start_id not in g.nodes:
        raise KeyError(start)

    hits: list[Hit] = []
    seen_dags = {start_id}
    visited_links: set[str] = set()
    frontier = [start_id]
    hops = 0

    while frontier and hops < depth:
        # found: DAG -> (via, guard, parent); the queue carries the DAG each branch started from.
        # FIFO queue over the sorted frontier: the first branch to reach a DAG is the lowest-id
        # parent on equal-length paths.
        found: dict[str, tuple[str | None, bool, str]] = {}
        queue: deque[tuple[str, str | None, bool, str]] = deque(
            (d, None, False, d) for d in frontier)
        while queue:
            current, via, guard, origin = queue.popleft()
            for nxt in _neighbours(g, current, direction):
                if _is_dag(g, nxt):
                    if nxt not in seen_dags and nxt not in found:
                        found[nxt] = (via, guard, origin)
                elif nxt not in visited_links:
                    visited_links.add(nxt)
                    queue.append((nxt, _link_label(g, nxt), g.nodes[nxt].guard, origin))

        hops += 1
        next_frontier = []
        for dag_node in sorted(found):
            seen_dags.add(dag_node)
            via, guard, origin = found[dag_node]
            hits.append(Hit(dag_id=dag_node.removeprefix("dag:"), hops=hops, via=via, guard=guard,
                            parent=origin.removeprefix("dag:")))
            next_frontier.append(dag_node)
        frontier = next_frontier

    return sorted(hits, key=lambda h: (h.hops, h.dag_id))


def reach_dags(g: Graph, node_id: str, direction: Direction) -> int:
    seen = {node_id}
    stack = [node_id]
    count = 0
    while stack:
        current = stack.pop()
        for nxt in _neighbours(g, current, direction):
            if nxt in seen:
                continue
            seen.add(nxt)
            stack.append(nxt)
            if _is_dag(g, nxt):
                count += 1
    return count
