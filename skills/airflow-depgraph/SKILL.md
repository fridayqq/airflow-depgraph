---
name: airflow-depgraph
description: Use when someone wants to see, draw, export or query the cross-DAG dependency graph of an Airflow 3 instance - "which DAGs depend on X", "what does X wait for", "show me the asset/trigger graph", "what breaks if X fails", "which reports refresh after X", impact of a failed DAG - or has a /ui/dependencies JSON snapshot to visualise.
---

# airflow-depgraph

## Overview

`airflow-depgraph` is a stdlib-only CLI that pulls the Airflow 3 dependency graph
(DAG → asset / trigger / sensor → DAG) from `/ui/dependencies`, saves a JSON snapshot, answers
downstream/upstream questions and renders one offline HTML page. Use it instead of writing your
own parser or graph script: it already handles hop counting, the `via` link, run status and
escaping.

## Workflow

1. **CLI present?** `airflow-depgraph --help`. If missing:
   `uv tool install git+https://github.com/fridayqq/airflow-depgraph` (or `pipx install git+…`).
   Needs Python ≥ 3.11. Airflow 2 is not supported.
2. **Connection.** Run `airflow-depgraph presets`. A listed preset is only right if its URL is
   the user's Airflow: check with `airflow-depgraph presets --preset NAME` (prints the resolved
   settings) before using it. Without a matching preset you need the base URL and a user name:
   `airflow-depgraph fetch --url https://airflow.example.com --user viewer --with-status -o deps.json`
   (password from `AIRFLOW_DEPGRAPH_PASSWORD`, or piped with `--password-stdin`). The password goes in an environment
   variable (`AIRFLOW_DEPGRAPH_PASSWORD`) or via `--password-stdin`; there is no `--password`
   flag. Never write the password into a command, file, preset or chat reply. If the user will
   query this Airflow again, offer a preset: `airflow-depgraph presets --example` →
   `~/.config/airflow-depgraph/presets/<name>.toml` with `url`, `user_env`, `password_env` (or
   `password_cmd` for a keychain / password-manager command). `--preset ./any/path.toml` also
   works, so the file can live anywhere.
3. **Snapshot.** `airflow-depgraph fetch --preset NAME --with-status -o deps.json`
   (`--url U --user X` instead of `--preset`). Add `--with-tasks` when the question is about
   tasks (report refreshes). Reuse a snapshot younger than ~30 min instead of refetching.
4. **Answer in text** from the snapshot, print the tool output as is, and state the snapshot
   time from the header line (it may be hours or days old):
   - `airflow-depgraph downstream DAG deps.json [--depth N] [--json]`
   - `airflow-depgraph upstream DAG deps.json`
   - `airflow-depgraph downstream DAG deps.json --tasks-matching 'refresh|report'`
5. **Render and open:** `airflow-depgraph render deps.json [--preset NAME] -o graph.html`, then
   `open "file:///absolute/path/graph.html#q=DAG+"` (macOS; `xdg-open` on Linux). A bare path
   with `#…` fails: the hash needs an absolute `file://` URL. `--preset` on `render` only adds
   cosmetics (title, asset-prefix strip, highlight); the page works without it. One step instead of 3–5:
   `airflow-depgraph graph --preset NAME -o graph.html` (also writes `graph.json`).

## Page flags (URL hash)

`q=<text>` focus a node (`+X` upstream, `X+` downstream, `2+X+3` limited depth) · `onlyFail`
failing DAGs and their chain · `hideAssets` DAG→DAG view · `hl` preset highlight. Combine with
commas: `#q=dag_a+,hideAssets`.

## Reading the output

- `via asset:…` names the asset/trigger/sensor joining two DAGs; links never count as hops.
- Schedule `Asset` → that DAG did not run while the upstream was broken (nothing stale).
  `Asset or <cron>` → its cron run used stale data. `⏸ paused` → not an incident.
- Rings: red = pipeline failure, amber = only test tasks failed. No ring = healthy, paused, or no
  decisive run in the lookback.

## Common errors

| Message | Meaning / fix |
|---|---|
| `no response from … — VPN/DNS?` | Airflow unreachable from here; answer from an existing snapshot and say how old it is |
| `authentication failed (401)` | wrong user/password |
| `403 on /ui/dependencies` | user may not see DAGs (Airflow ≥ 3.1.8 filters by permission) |
| `dag 'x' is not in the dependency graph` | typo, or a cron-only DAG: those have no edges and are never in the graph |
| `snapshot has no task lists` | refetch with `--with-tasks` |

Only explicit Airflow dependencies are visible; table-level coupling without an asset is not.
