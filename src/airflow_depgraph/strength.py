"""Hard or soft asset dependencies, read from each consumer DAG's schedule.

An asset edge (asset -> DAG) is *hard* when the DAG runs only once that asset (and the others in
an "all" schedule) is updated, so a missing upstream run blocks it. It is *soft* when the DAG runs
anyway: an "Asset or <cron>" timetable, an "any" asset condition, or an asset the DAG only reads
as a task inlet without scheduling on it. The rule follows the auto-status lineage builder.

The asset condition is only in GET /api/v2/dags/{id}/details (`asset_expression`), one request
per consumer DAG, fetched in parallel.
"""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from .client import ApiError

HARD, SOFT = "hard", "soft"


def asset_uris(expression: object) -> set[str]:
    """Every asset URI in an `asset_expression` tree ({"all": [...]}, {"any": [...]}, {"asset": {...}})."""
    out: set[str] = set()
    if not isinstance(expression, dict):
        return out
    asset = expression.get("asset")
    if isinstance(asset, dict):
        # Both: /ui/dependencies labels the asset by name, while `uri` is normalised (an
        # `Asset("contract://orders")` has the uri `contract://orders/`).
        out.update(v for v in (asset.get("uri"), asset.get("name")) if isinstance(v, str))
        return out
    for key in ("all", "any"):
        for child in expression.get(key) or []:
            out |= asset_uris(child)
    return out


def strength_of(details: dict, asset_uri: str) -> str:
    """HARD or SOFT for one asset the DAG in `details` depends on."""
    expression = details.get("asset_expression")
    if asset_uri not in asset_uris(expression):
        return SOFT                                         # read as an inlet, not scheduled on
    summary = details.get("timetable_summary") or ""
    if summary.startswith("Asset or "):
        return SOFT                                         # AssetOrTimeSchedule
    if isinstance(expression, dict) and "any" in expression:
        return SOFT
    return HARD


def fetch_details(client, dag_ids: list[str], *, workers: int = 8,
                  progress: Callable[[str], None] | None = None) -> dict[str, dict]:
    """{dag_id: details} for the DAGs that could be read; unreadable ones are left out."""
    out: dict[str, dict] = {}
    if not dag_ids:
        return out

    def one(dag_id: str) -> tuple[str, dict | None]:
        try:
            return dag_id, client.dag_details(dag_id)
        except ApiError:
            return dag_id, None

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        for i, (dag_id, details) in enumerate(pool.map(one, dag_ids), 1):
            if isinstance(details, dict):
                out[dag_id] = details
            if progress and (i % 25 == 0 or i == len(dag_ids)):
                progress(f"{i}/{len(dag_ids)}")
    return out


def consumers(nodes: list[dict], edges: list[dict]) -> list[str]:
    """DAG ids with at least one incoming asset edge, in order of first appearance."""
    assets = {n["id"] for n in nodes if n.get("type") == "asset"}
    seen: dict[str, None] = {}
    for e in edges:
        if e["source_id"] in assets and str(e["target_id"]).startswith("dag:"):
            seen.setdefault(e["target_id"][4:], None)
    return list(seen)


def annotate(nodes: list[dict], edges: list[dict], details: dict[str, dict]) -> list[dict]:
    """The edges with "strength" set on every asset -> DAG edge whose DAG has details. The input
    edges are left as they are."""
    label = {n["id"]: n.get("label") for n in nodes if n.get("type") == "asset"}
    out = []
    for e in edges:
        uri, dag_id = label.get(e["source_id"]), str(e["target_id"])[4:]
        if uri is None or dag_id not in details:
            out.append(e)
        else:
            out.append({**e, "strength": strength_of(details[dag_id], str(uri))})
    return out
