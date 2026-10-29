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
2. **Connection.** Credentials in this order of preference; a token always wins over a
   user and password when both are present.
   - **Token.** Preferred. If `AIRFLOW_DEPGRAPH_TOKEN` is set, use it: every command takes
      `--url U` and the token from the environment (or from `--token-stdin`, e.g.
      `pbpaste | airflow-depgraph fetch --url U --token-stdin …`). No login call is made; status,
      catalog and sensor inference all work. If it is not set and the user has no password
      variable, walk them through getting one, in these words: log in to Airflow in the browser;
      open DevTools (F12, ⌥⌘I on a Mac); *Application* tab (Firefox/Safari: *Storage*), expand
      *Cookies*, click the Airflow origin; click the row `_token` and copy the value from the
      *Cookie Value* box below the table; then `export AIRFLOW_DEPGRAPH_TOKEN='…'` in their
      shell and re-run. README, "Get a token from the browser", has a screenshot. The token
      lives a day by default; on a 401 (exit code 2) ask for a fresh one the same way. Never
      write the token into a command, file, preset or chat reply.
   - **No credentials at all.** The user saves `U/ui/dependencies` from a logged-in browser
      tab as `deps.json`; `render`, `downstream`, `upstream` and `report` take that file as the
      snapshot (no status, tags or sensor links in it), so skip to step 4.
   - **User and password.** Run `airflow-depgraph presets`. A listed preset is only right if its URL is
   the user's Airflow: check with `airflow-depgraph presets --preset NAME` (prints the resolved
   settings) before using it. For a one-off without a preset you need the base URL and a user name:
   `airflow-depgraph fetch --url https://airflow.example.com --user viewer --with-status -o deps.json`
   (password from `AIRFLOW_DEPGRAPH_PASSWORD` or `--password-stdin`; there is no `--password`
   flag). Never write the password into a command, file, preset or chat reply.
   **No matching preset and the user will come back to this Airflow:** build one with them.
   a. Ask for the URL and which env var or command holds the user and the password (never the
      password itself). If those variables are not set in Claude's shell, tell the user to add
      `export` lines to their shell profile, or to use `password_cmd`, then re-run.
   b. `airflow-depgraph init --discover --json --url U` (add `--user-env`, `--password-env` or
      `--password-cmd` for non-default names). It reads only, and prints tags with DAG counts,
      asset prefixes, sensor nodes and suggestions.
   c. Ask with AskUserQuestion, options from the JSON: tags (multiSelect, top 4 by count plus
      "none"; Other to type), asset prefix (top candidates plus "keep labels"), status rings
      (recommended per `suggest.with_status`), sensor inference (recommended per
      `suggest.infer_sensors`; `sensor_tasks` says how many sensor tasks a sample of DAGs has).
   d. `airflow-depgraph init NAME --yes --url U --tag team-a --strip-prefix 's3://bucket/'
      --with-status …` (same credential flags as in b).
   e. Verify with `airflow-depgraph presets --preset NAME`.
   The user can also run `airflow-depgraph init` in their own terminal. `--preset` also takes a path.
3. **Snapshot.** `airflow-depgraph fetch --preset NAME --with-status -o deps.json`
   (`--url U --user X` instead of `--preset`). Add `--with-tasks` when the question is about
   tasks (report refreshes). Add `--infer-sensors` to recover dependencies Airflow does not
   show: `PythonSensor`, `@task.sensor` and custom sensors waiting on another DAG (about a
   minute on ~600 DAGs). Reuse a snapshot younger than ~30 min instead of refetching.
4. **Answer in text** from the snapshot, print the tool output as is, and state the snapshot
   time from the header line (it may be hours or days old):
   - `airflow-depgraph downstream DAG deps.json [--depth N] [--json]`
   - `airflow-depgraph upstream DAG deps.json`
   - `airflow-depgraph downstream DAG deps.json --tasks-matching 'refresh|report'`
   - `airflow-depgraph report critical|unlinked|unconsumed-assets|failing deps.json [--json]`
5. **Render and open:** `airflow-depgraph render deps.json [--preset NAME] -o graph.html`, then
   `open "file:///absolute/path/graph.html#q=DAG+"` (macOS; `xdg-open` on Linux). A bare path
   with `#…` fails: the hash needs an absolute `file://` URL. `--preset` on `render` only adds
   cosmetics (title, asset-prefix strip, highlight); the page works without it. One step instead of 3–5:
   `airflow-depgraph graph --preset NAME -o graph.html` (also writes `graph.json`).

## Page flags (URL hash)

`q=<text>` focus a node (`+X` upstream, `X+` downstream, `2+X+3` limited depth) · `onlyFail`
failing DAGs and their chain · `hideAssets` DAG→DAG view · `hl` preset highlight. Combine with
commas: `#q=dag_a+,hideAssets`.

By default the page shows only linked, active DAGs with the preset `tags`. Negations: `allLinks`
(unlinked too), `paused`, `allTags`; `conn` adds DAGs one link away from tagged ones; `guards`
shows inferred deploy guards. Text queries are never filtered: they follow inferred links unless
`--no-inferred`, and deploy guards only with `--include-guards`.

## Reading the output

- `via asset:…` names the asset/trigger/sensor joining two DAGs; links never count as hops.
  `sensor:<task> (inferred)` comes from `--infer-sensors`.
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

Exit codes: 3 = credentials, 4 = Airflow unreachable, 5 = unknown DAG.

Only explicit Airflow dependencies are visible; table-level coupling without an asset is not.
