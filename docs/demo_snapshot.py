"""Write the fictional snapshot behind the README demo and screenshots: python3 docs/demo_snapshot.py out.json

Render it with AIRFLOW_DEPGRAPH_HIGHLIGHT_LABEL=Reports airflow-depgraph render demo.json \
--highlight '_report$' -o demo.html; docs/record_demo.mjs records the GIF and docs/screenshots.mjs
takes page.png and impact.png (the steps are at the top of record_demo.mjs).

Two failures: payments_ingest (pipeline, its outlet not produced) blocks the finance chain, which
has not run since; customers_dedupe (tests only, outlet produced) is followed by the customer and
order DAGs, which ran after it on unvalidated data.
"""

from __future__ import annotations

import json
import random
import sys

FETCHED = "2026-11-06T06:00:00Z"
TEAMS = {"orders": "team-orders", "customers": "team-customers", "finance": "team-finance",
         "marketing": "team-marketing"}
VERTICAL = {"orders": "Commerce", "customers": "Customer", "finance": "Finance", "marketing": "Marketing"}
EXTRA_VERTICALS = {"exec_dashboard_report": ["Commerce", "Customer"], "churn_report": ["Marketing"],
                   "attribution_model": ["Commerce"]}

# (dag_id, teams, cron, outlets)  cron None → asset-driven
PIPELINES = [
    ("orders_ingest", ["orders"], "0 4 * * *", ["warehouse://orders/raw"]),
    ("orders_clean", ["orders"], None, ["warehouse://orders/clean"]),
    ("orders_enrich", ["orders"], None, ["warehouse://orders/enriched"]),
    ("orders_daily_report", ["orders"], "0 7 * * *", []),
    ("customers_ingest", ["customers"], "0 3 * * *", ["warehouse://customers/raw"]),
    ("customers_dedupe", ["customers"], None, ["warehouse://customers/golden"]),
    ("customers_segments", ["customers"], None, ["warehouse://customers/segments"]),
    ("churn_model", ["customers"], None, ["warehouse://customers/churn_scores"]),
    ("churn_report", ["customers"], None, []),
    ("payments_ingest", ["finance"], "15 4 * * *", ["warehouse://finance/payments"]),
    ("invoices_build", ["finance"], None, ["warehouse://finance/invoices"]),
    ("revenue_model", ["finance"], None, ["warehouse://finance/revenue"]),
    ("revenue_report", ["finance"], None, []),
    ("finance_month_close", ["finance"], "0 6 1 * *", []),
    ("ledger_export", ["finance"], None, []),
    ("campaigns_ingest", ["marketing"], "0 5 * * *", ["warehouse://marketing/campaigns"]),
    ("attribution_model", ["marketing"], None, ["warehouse://marketing/attribution"]),
    ("attribution_report", ["marketing"], None, []),
    ("marketing_weekly_report", ["marketing"], "0 8 * * 1", []),
    ("products_ingest", ["orders"], "0 2 * * *", ["warehouse://catalog/products"]),
    ("exec_dashboard_report", ["finance"], None, []),
    ("data_quality_checks", [], "0 9 * * *", []),
    ("gdpr_erasure", ["customers"], "0 1 * * 0", []),
]
# consumer → assets it waits for
CONSUMES = {
    "orders_clean": ["warehouse://orders/raw"],
    "orders_enrich": ["warehouse://orders/clean", "warehouse://customers/golden",
                      "warehouse://catalog/products"],
    "orders_daily_report": ["warehouse://orders/enriched"],
    "customers_dedupe": ["warehouse://customers/raw"],
    "customers_segments": ["warehouse://customers/golden", "warehouse://orders/enriched"],
    "churn_model": ["warehouse://customers/segments"],
    "churn_report": ["warehouse://customers/churn_scores"],
    "invoices_build": ["warehouse://finance/payments", "warehouse://orders/enriched"],
    "revenue_model": ["warehouse://finance/invoices", "warehouse://finance/payments"],
    "revenue_report": ["warehouse://finance/revenue"],
    "ledger_export": ["warehouse://finance/invoices"],
    "attribution_model": ["warehouse://marketing/campaigns", "warehouse://orders/enriched"],
    "attribution_report": ["warehouse://marketing/attribution"],
    "exec_dashboard_report": ["warehouse://finance/revenue", "warehouse://marketing/attribution",
                              "warehouse://customers/churn_scores"],
}
TRIGGERS = [("finance_month_close", "ledger_export")]
SENSORS = [("marketing_weekly_report", "wait_attribution", "attribution_model")]
# dag → (kind, when the failed run started, which of its outlets that run produced)
FAILED = {"payments_ingest": ("pipeline", "2026-11-06T04:15:00Z", {"warehouse://finance/payments": False}),
          "customers_dedupe": ("tests", "2026-11-06T03:30:00Z", {"warehouse://customers/golden": True})}
# customers_dedupe is already running again after its failure.
RUNNING = {"customers_dedupe": "2026-11-06T05:50:00Z"}
# When each DAG last ran. Everything after both failures by default, so the DAGs downstream of
# the tests failure count as having run on unvalidated data; the finance chain and the marketing
# DAGs ran before, so the pipeline failure still blocks them.
RAN_BEFORE = {"invoices_build": "2026-11-05T04:40:00Z", "revenue_model": "2026-11-05T05:10:00Z",
              "revenue_report": "2026-11-05T05:30:00Z", "ledger_export": "2026-11-01T06:20:00Z",
              "exec_dashboard_report": "2026-11-05T06:00:00Z", "finance_month_close": "2026-11-01T06:00:00Z",
              "attribution_model": "2026-11-05T05:20:00Z", "attribution_report": "2026-11-05T05:40:00Z",
              "marketing_weekly_report": "2026-11-05T08:00:00Z", "gdpr_erasure": "2026-11-04T01:00:00Z"}
PAUSED = {"gdpr_erasure"}

random.seed(7)
nodes, edges, catalog, dags = [], [], {}, {}
asset_ids: dict[str, str] = {}


def asset(label: str) -> str:
    if label not in asset_ids:
        asset_ids[label] = f"asset:{len(asset_ids) + 1}"
        nodes.append({"id": asset_ids[label], "label": label, "type": "asset"})
    return asset_ids[label]


for dag_id, teams, cron, outlets in PIPELINES:
    nodes.append({"id": f"dag:{dag_id}", "label": dag_id, "type": "dag"})
    consumed = CONSUMES.get(dag_id, [])
    schedule = "Asset" if consumed and not cron else (f"Asset or {cron}" if consumed else cron)
    tags = [TEAMS[t] for t in teams] + (["reporting"] if dag_id.endswith("_report") else [])
    verticals = [VERTICAL[t] for t in teams] + EXTRA_VERTICALS.get(dag_id, [])
    tags += [f"vertical:{v}" for v in dict.fromkeys(verticals)]
    owners = [TEAMS[teams[0]]] if teams else ["platform"]
    catalog[dag_id] = {"tags": tags, "is_paused": dag_id in PAUSED, "is_stale": False,
                       "schedule": schedule, "owners": owners}
    kind, failed_at, produced = FAILED.get(dag_id, (None, None, None))
    ran = failed_at or RAN_BEFORE.get(dag_id) or f"2026-11-06T05:{random.randint(10, 59):02d}:00Z"
    run = {"state": "failed" if kind else "success", "kind": kind or "ok",
           "run_after": ran, "run_id": f"scheduled__{ran.replace('Z', '+00:00')}",
           "failed_tasks": ["load"] if kind == "pipeline" else (["dbt_test"] if kind else []),
           "upstream_failed_tasks": ["publish"] if kind == "pipeline" else []}
    if kind:
        run["outlets"] = {asset(label): ok for label, ok in produced.items()}
    if dag_id in RUNNING:
        run["running"] = RUNNING[dag_id]
    dags[dag_id] = {"is_paused": dag_id in PAUSED, "schedule": schedule, "owners": owners,
                    "last_run": run}
    for label in outlets:
        edges.append({"source_id": f"dag:{dag_id}", "target_id": asset(label)})
    for label in consumed:
        # A DAG with a cron next to its assets runs without them: a soft edge.
        edges.append({"source_id": asset(label), "target_id": f"dag:{dag_id}",
                      "strength": "soft" if cron else "hard"})

for i, (src, dst) in enumerate(TRIGGERS, 1):
    tid = f"trigger:{i}"
    nodes.append({"id": tid, "label": f"trigger_{dst}", "type": "trigger"})
    edges += [{"source_id": f"dag:{src}", "target_id": tid},
              {"source_id": tid, "target_id": f"dag:{dst}"}]
for i, (waiter, task, target) in enumerate(SENSORS, 1):
    sid = f"sensor:{i}"
    nodes.append({"id": sid, "label": task, "type": "sensor"})
    edges += [{"source_id": f"dag:{target}", "target_id": sid},
              {"source_id": sid, "target_id": f"dag:{waiter}"}]

# The order of nodes and edges seeds the force layout; this order gives the spread-out picture.
random.seed(2)
random.shuffle(nodes)
random.shuffle(edges)

snapshot = {"format": 1, "fetched_at": FETCHED, "source": "https://airflow.example.com",
            "airflow_version": "3.3.2", "nodes": nodes, "edges": edges, "dags": dags,
            "catalog": catalog}
out = sys.argv[1] if len(sys.argv) > 1 else "demo.json"
with open(out, "w", encoding="utf-8") as fh:
    json.dump(snapshot, fh, indent=1)
print(f"wrote {out}: {len(catalog)} DAGs, {len(asset_ids)} assets, {len(edges)} edges")
