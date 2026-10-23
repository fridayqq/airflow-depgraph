"""Reports over a snapshot: critical DAGs, unlinked DAGs, unconsumed assets, failing DAGs.

Pure functions: each takes a Snapshot and Settings and returns a list of dicts.
"""
from __future__ import annotations

import re

from airflow_depgraph.graph import Graph, reach_dags, walk
from airflow_depgraph.presets import Settings
from airflow_depgraph.snapshot import Snapshot

KINDS = ("critical", "unlinked", "unconsumed-assets", "failing")
_CRON_FALLBACK = re.compile(r"\bor\b", re.IGNORECASE)


class ReportError(ValueError):
    pass


def effective_tags(s: Settings, cli_tags: list[str] | None, all_tags: bool) -> tuple[str, ...] | None:
    """None disables the tag filter; otherwise the --tag values or the preset's tags."""
    if all_tags:
        return None
    tags = tuple(cli_tags) if cli_tags else s.tags
    return tags or None


def _require_catalog(snap: Snapshot) -> dict[str, dict]:
    if snap.catalog is None:
        raise ReportError("snapshot has no catalog; re-run fetch against a live Airflow")
    return snap.catalog


def _check_tags(snap: Snapshot, tags: tuple[str, ...] | None) -> None:
    if tags is not None and snap.catalog is None:
        raise ReportError("snapshot has no catalog, so tags cannot be checked; use --all-tags")


def _tag_ok(snap: Snapshot, dag: str, tags: tuple[str, ...] | None) -> bool:
    if tags is None:
        return True
    return bool(set((snap.catalog or {}).get(dag, {}).get("tags") or []) & set(tags))


def _status(snap: Snapshot, dag: str) -> dict:
    last = ((snap.dags or {}).get(dag) or {}).get("last_run") or {}
    state = last.get("state")
    return {"state": state if state in ("success", "failed") else None, "kind": last.get("kind")}


def _schedule(snap: Snapshot, dag: str) -> str | None:
    if snap.catalog is not None and dag in snap.catalog:
        return snap.catalog[dag].get("schedule")
    return ((snap.dags or {}).get(dag) or {}).get("schedule")


def _downstream_ids(g: Graph, dag: str) -> list[str]:
    if f"dag:{dag}" not in g.nodes:
        return []
    return [h.dag_id for h in walk(g, dag, "downstream", depth=len(g.nodes))]


def critical(snap: Snapshot, s: Settings, *, tags: tuple[str, ...] | None = None,
             inferred: bool = True, top: int = 20) -> list[dict]:
    """DAGs ranked by transitive downstream DAG count (guards excluded)."""
    _check_tags(snap, tags)
    g = snap.graph_for(inferred=inferred, guards=False)
    rows = []
    for dag in g.dag_ids:
        if not _tag_ok(snap, dag, tags):
            continue
        rows.append({"dag_id": dag,
                     "downstream": reach_dags(g, f"dag:{dag}", "downstream"),
                     "direct_consumers": len(walk(g, dag, "downstream", depth=1)),
                     **_status(snap, dag)})
    rows.sort(key=lambda r: (-r["downstream"], r["dag_id"]))
    rows = rows[:top]
    return [{"rank": i, **r} for i, r in enumerate(rows, 1)]


def unlinked(snap: Snapshot, s: Settings, *, tags: tuple[str, ...] | None = None,
             inferred: bool = True) -> list[dict]:
    """Active catalog DAGs with no explicit or inferred link (deploy guards count as links)."""
    catalog = _require_catalog(snap)
    g = snap.graph_for(inferred=inferred, guards=True)
    rows = []
    for dag in sorted(catalog):
        entry = catalog[dag]
        if entry.get("is_stale") or not _tag_ok(snap, dag, tags):
            continue
        node = f"dag:{dag}"
        if node in g.nodes and (g.out.get(node) or g.inc.get(node)):
            continue
        rows.append({"dag_id": dag, "schedule": entry.get("schedule"),
                     "paused": bool(entry.get("is_paused")), "tags": entry.get("tags") or []})
    return rows


def unconsumed_assets(snap: Snapshot, s: Settings, *,
                      tags: tuple[str, ...] | None = None) -> list[dict]:
    """Assets with at least one producing DAG and no consumer (explicit edges only)."""
    _check_tags(snap, tags)
    g = snap.graph_for(inferred=False)
    rows = []
    for node in g.nodes.values():
        if node.type != "asset" or g.out.get(node.id):
            continue
        producers = sorted(n.removeprefix("dag:") for n in g.inc.get(node.id, [])
                           if g.nodes[n].type == "dag")
        if not producers or (tags is not None and not any(_tag_ok(snap, p, tags) for p in producers)):
            continue
        label = node.label
        if s.asset_label_strip_prefix:
            label = label.removeprefix(s.asset_label_strip_prefix)
        rows.append({"asset": label, "producers": producers})
    return sorted(rows, key=lambda r: r["asset"])


def failing(snap: Snapshot, s: Settings, *, tags: tuple[str, ...] | None = None,
            inferred: bool = True) -> list[dict]:
    """DAGs whose last decisive run failed and that are not paused, with their downstream DAGs.
    stale_consumers lists downstream DAGs whose schedule has a cron fallback ("... or ...")."""
    if snap.dags is None:
        raise ReportError("snapshot has no status; re-run fetch with --with-status")
    _check_tags(snap, tags)
    g = snap.graph_for(inferred=inferred, guards=False)
    rows = []
    for dag, st in sorted(snap.dags.items()):
        last = (st or {}).get("last_run")
        if not last or last.get("state") != "failed":
            continue
        paused = bool((snap.catalog or {}).get(dag, {}).get("is_paused") or (st or {}).get("is_paused"))
        if paused or not _tag_ok(snap, dag, tags):
            continue
        down = _downstream_ids(g, dag)
        rows.append({"dag_id": dag, "kind": last.get("kind"),
                     "failed_tasks": (last.get("failed_tasks") or [])[:3],
                     "downstream": len(down),
                     "stale_consumers": [d for d in down
                                         if _CRON_FALLBACK.search(_schedule(snap, d) or "")]})
    rows.sort(key=lambda r: (-r["downstream"], r["dag_id"]))
    return rows
