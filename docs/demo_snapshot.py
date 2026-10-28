"""Write a fictional snapshot for the README screenshot: python3 docs/demo_snapshot.py demo.json

Then: airflow-depgraph render demo.json --highlight '_report$' --highlight-label Reports -o demo.html
"""

from __future__ import annotations

import json
import random
import sys

random.seed(7)
FETCHED = "2026-10-28T06:00:00Z"
TEAMS = {"orders": "team-orders", "customers": "team-customers", "finance": "team-finance",
         "marketing": "team-marketing"}

# (dag_id, tags, schedule, outlets)  schedule None → asset-driven
PIPELINES = [
    ("orders_ingest", ["orders"], "0 4 * * *", ["warehouse://orders/raw"]),
    ("orders_clean", ["orders"], None, ["warehouse://orders/clean"]),
    ("orders_enrich", ["orders"], None, ["warehouse://orders/enriched"]),
    ("orders_daily_report", ["orders"], "0 7 * * *", []),
    ("returns_ingest", ["orders"], "30 4 * * *", ["warehouse://orders/returns"]),
    ("returns_report", ["orders"], None, []),
    ("customers_ingest", ["customers"], "0 3 * * *", ["warehouse://customers/raw"]),
    ("customers_dedupe", ["customers"], None, ["warehouse://customers/golden"]),
    ("customers_segments", ["customers"], None, ["warehouse://customers/segments"]),
    ("churn_model", ["customers"], None, ["warehouse://customers/churn_scores"]),
    ("churn_report", ["customers"], None, []),
    ("crm_sync", ["customers", "marketing"], None, []),
    ("payments_ingest", ["finance"], "15 4 * * *", ["warehouse://finance/payments"]),
    ("invoices_build", ["finance"], None, ["warehouse://finance/invoices"]),
    ("revenue_model", ["finance"], None, ["warehouse://finance/revenue"]),
    ("revenue_report", ["finance"], None, []),
    ("finance_month_close", ["finance"], "0 6 1 * *", []),
    ("ledger_export", ["finance"], None, []),
    ("campaigns_ingest", ["marketing"], "0 5 * * *", ["warehouse://marketing/campaigns"]),
    ("adspend_ingest", ["marketing"], "10 5 * * *", ["warehouse://marketing/adspend"]),
    ("attribution_model", ["marketing"], None, ["warehouse://marketing/attribution"]),
    ("attribution_report", ["marketing"], None, []),
    ("marketing_weekly_report", ["marketing"], "0 8 * * 1", []),
    ("newsletter_send", ["marketing"], None, []),
    ("products_ingest", ["orders"], "0 2 * * *", ["warehouse://catalog/products"]),
    ("inventory_snapshot", ["orders"], None, ["warehouse://catalog/inventory"]),
    ("stock_alerts", ["orders"], None, []),
    ("pricing_model", ["orders", "finance"], None, ["warehouse://catalog/prices"]),
    ("pricing_report", ["finance"], None, []),
    ("exec_dashboard_report", ["finance"], None, []),
    ("data_quality_checks", [], "0 9 * * *", []),
    ("gdpr_erasure", ["customers"], "0 1 * * 0", []),
    ("warehouse_vacuum", [], "0 0 * * *", []),
]
# consumer → assets it waits for
CONSUMES = {
    "orders_clean": ["warehouse://orders/raw"],
    "orders_enrich": ["warehouse://orders/clean", "warehouse://customers/golden",
                      "warehouse://catalog/products"],
    "orders_daily_report": ["warehouse://orders/enriched"],
    "returns_report": ["warehouse://orders/returns", "warehouse://orders/enriched"],
    "customers_dedupe": ["warehouse://customers/raw"],
    "customers_segments": ["warehouse://customers/golden", "warehouse://orders/enriched"],
    "churn_model": ["warehouse://customers/segments"],
    "churn_report": ["warehouse://customers/churn_scores"],
    "crm_sync": ["warehouse://customers/segments", "warehouse://customers/churn_scores"],
    "invoices_build": ["warehouse://finance/payments", "warehouse://orders/enriched"],
    "revenue_model": ["warehouse://finance/invoices", "warehouse://finance/payments"],
    "revenue_report": ["warehouse://finance/revenue"],
    "ledger_export": ["warehouse://finance/invoices"],
    "attribution_model": ["warehouse://marketing/campaigns", "warehouse://marketing/adspend",
                          "warehouse://orders/enriched"],
    "attribution_report": ["warehouse://marketing/attribution"],
    "newsletter_send": ["warehouse://customers/segments"],
    "inventory_snapshot": ["warehouse://catalog/products"],
    "stock_alerts": ["warehouse://catalog/inventory"],
    "pricing_model": ["warehouse://catalog/inventory", "warehouse://finance/revenue"],
    "pricing_report": ["warehouse://catalog/prices"],
    "exec_dashboard_report": ["warehouse://finance/revenue", "warehouse://marketing/attribution",
                              "warehouse://customers/churn_scores"],
}
TRIGGERS = [("finance_month_close", "ledger_export"), ("orders_enrich", "stock_alerts")]
SENSORS = [("marketing_weekly_report", "wait_attribution", "attribution_model")]
FAILED = {"payments_ingest": "pipeline", "customers_dedupe": "tests", "adspend_ingest": "pipeline"}
PAUSED = {"gdpr_erasure", "newsletter_send"}

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
    owners = [TEAMS[teams[0]]] if teams else ["platform"]
    catalog[dag_id] = {"tags": tags, "is_paused": dag_id in PAUSED, "is_stale": False,
                       "schedule": schedule, "owners": owners}
    kind = FAILED.get(dag_id)
    state = "failed" if kind else "success"
    hour = random.randint(2, 8)
    run = {"state": state, "kind": kind or "ok", "run_after": f"2026-10-28T0{hour}:00:00Z",
           "run_id": f"scheduled__2026-10-28T0{hour}:00:00+00:00",
           "failed_tasks": ["load"] if kind == "pipeline" else (["dbt_test"] if kind else [])}
    dags[dag_id] = {"is_paused": dag_id in PAUSED, "schedule": schedule, "owners": owners,
                    "last_run": run}
    for label in outlets:
        edges.append({"source_id": f"dag:{dag_id}", "target_id": asset(label)})
    for label in consumed:
        edges.append({"source_id": asset(label), "target_id": f"dag:{dag_id}"})

for i, (src, dst) in enumerate(TRIGGERS, 1):
    tid = f"trigger:{i}"
    nodes.append({"id": tid, "label": f"trigger_{dst}", "type": "trigger"})
    edges += [{"source_id": f"dag:{src}", "target_id": tid}, {"source_id": tid, "target_id": f"dag:{dst}"}]
for i, (waiter, task, target) in enumerate(SENSORS, 1):
    sid = f"sensor:{i}"
    nodes.append({"id": sid, "label": task, "type": "sensor"})
    edges += [{"source_id": f"dag:{target}", "target_id": sid}, {"source_id": sid, "target_id": f"dag:{waiter}"}]

snapshot = {"format": 1, "fetched_at": FETCHED, "source": "https://airflow.example.com",
            "airflow_version": "3.3.2", "nodes": nodes, "edges": edges, "dags": dags,
            "catalog": catalog}
out = sys.argv[1] if len(sys.argv) > 1 else "demo.json"
with open(out, "w", encoding="utf-8") as fh:
    json.dump(snapshot, fh, indent=1)
print(f"wrote {out}: {len(catalog)} DAGs, {len(asset_ids)} assets, {len(edges)} edges")
