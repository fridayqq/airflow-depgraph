# airflow-depgraph

Fetch, query and render the Airflow 3 cross-DAG dependency graph (DAG → asset / trigger / sensor → DAG) from any Airflow 3 instance. It uses only the Python standard library. The result is a JSON snapshot you can query offline, plus one self-contained HTML page.

![How the pieces fit: Airflow → fetch → snapshot → render → HTML, with presets and credentials feeding fetch](docs/images/overview.png)

Things to know before you use it:

- **Airflow 3 only.** Airflow 2 has no `/ui/dependencies` endpoint, so it is not supported.
- **Internal endpoint.** `/ui/dependencies` belongs to the Airflow UI, not the public API. It may change between releases. For example, 3.1.8 started filtering it by DAG permissions. The tool was developed against Airflow 3.3.2.
- **Pure cron DAGs are absent.** A DAG with no asset, trigger or sensor link has no edges, so it does not appear in the graph.

## What the graph contains

![Graph model: DAGs linked through assets, triggers and sensors; link nodes do not count as hops and are reported as "via"](docs/images/graph-model.png)

Only explicit Airflow dependencies are visible: asset events, `TriggerDagRunOperator` triggers and sensors. `downstream`/`upstream` count hops between DAGs; the asset, trigger or sensor that joins two DAGs is reported as `via`. A DAG reachable by several paths is listed once, at its shortest hop count. Table-level coupling without an asset is invisible here.

## Install

```bash
uv tool install git+https://github.com/fridayqq/airflow-depgraph
# or
pipx install git+https://github.com/fridayqq/airflow-depgraph
```

To run from a checkout, use `uv run airflow-depgraph ...`.

## Quick start

```bash
export AIRFLOW_DEPGRAPH_URL=https://airflow.example.com
export AIRFLOW_DEPGRAPH_USER=alice
export AIRFLOW_DEPGRAPH_PASSWORD=...

# Save a snapshot, then render it to a page
airflow-depgraph fetch -o deps.json
airflow-depgraph render deps.json -o graph.html

# Or both in one go: writes graph.html and graph.json
airflow-depgraph graph -o graph.html

# Which DAGs run downstream of my_dag?
airflow-depgraph downstream my_dag deps.json

# Only those that have a task matching a regex (snapshot needs --with-tasks)
airflow-depgraph fetch --with-tasks -o deps.json
airflow-depgraph downstream my_dag --tasks-matching 'refresh|report' deps.json
```

`upstream` works the same way. `tasks <dag> [snapshot]` lists the tasks of a DAG. If you omit the snapshot argument, `downstream`, `upstream` and `tasks` query the live instance. `--depth N` limits the traversal and `--json` prints machine-readable output.

## Credentials

The password is never accepted as a flag, so it stays out of shell history and process lists. It is read from the first of these that applies:

1. `--password-stdin`, which reads one line from stdin.
2. The environment variable named by `password_env`, `AIRFLOW_DEPGRAPH_PASSWORD` by default.
3. The command in `password_cmd`, for example `pass show airflow/example`, taking its stdout.
4. An interactive prompt, when stdin is a terminal.

The user comes from `--user`, the variable named by `user_env` (`AIRFLOW_DEPGRAPH_USER` by default), or the preset. `--token` supplies a bearer token and skips the login.

## Presets

A preset is a TOML file of defaults for one Airflow instance. Credentials are never stored in it, only the names of environment variables or a command.

Settings are resolved in this order, first match wins: command-line flag, environment variable `AIRFLOW_DEPGRAPH_<KEY>` (for example `AIRFLOW_DEPGRAPH_URL`, `AIRFLOW_DEPGRAPH_WITH_STATUS`), preset file, built-in default.

A preset name is looked up in these directories, in order:

1. `$AIRFLOW_DEPGRAPH_PRESETS`
2. `$XDG_CONFIG_HOME/airflow-depgraph/presets`, which is `~/.config/airflow-depgraph/presets` by default
3. the presets bundled with the package

`--preset` also accepts a path to a `.toml` file. `--preset none` ignores any preset, including one set through `AIRFLOW_DEPGRAPH_PRESET`. `airflow-depgraph presets` lists what is found.

```bash
mkdir -p ~/.config/airflow-depgraph/presets
airflow-depgraph presets --example > ~/.config/airflow-depgraph/presets/prod.toml
# edit prod.toml, then:
airflow-depgraph graph --preset prod -o graph.html
```

The example preset:

```toml
# Preset name, shown in the output.
name = "example"
# Base URL of the Airflow webserver.
# url = "https://airflow.example.com"
# Name of the env var that holds the Airflow user name.
# user_env = "AIRFLOW_USER"
# Name of the env var that holds the Airflow password.
# password_env = "AIRFLOW_PASSWORD"
# Shell command that prints the password to stdout (used when the env var is empty).
# password_cmd = "pass show airflow/example"
# Also fetch the latest run status of every DAG.
# with_status = false
# Also fetch the tasks of every DAG.
# with_tasks = false
# Title of the rendered page.
# title = "Airflow dependency graph"
# Prefix removed from asset names when they are shown as labels.
# asset_label_strip_prefix = "s3://example-bucket/"
# Regex of node ids to highlight in the rendered graph.
# highlight = "^example_"
# Legend label for the highlighted nodes.
# highlight_label = "Highlight"
# Regex that marks a task as a test task (case-insensitive).
# tests_task_regex = '(^|[._-])tests?([._-]|$)'
# Regex that selects which tasks to load; all tasks when unset.
# task_query_regex = "^(extract|load)_"
# How many recent runs to look at when computing the status.
# status_runs_lookback = 5
```

## Page features

The page is a single HTML file with d3 inlined, so it works offline.

- **Layout.** Node size grows with the number of transitive downstream DAGs. The shape shows the node type: DAG, asset, trigger or sensor.
- **Status.** With `--with-status`, a red ring marks a pipeline failure and an amber ring marks a failure that is tests only. Dark mode follows the system setting.
- **Click.** Selecting a node highlights its upstream and downstream chain and fills the side lists.
- **Search.** Type a DAG name to focus it. `+dag` adds everything upstream, `dag+` everything downstream, and `2+dag+3` limits that to 2 levels up and 3 down.
- **Toggles.** Hide assets, show only failing DAGs, and highlight the DAGs matched by the preset `highlight` regex.
- **Hash flags.** The state lives in the URL fragment so a view can be shared: `#q=<name>,onlyFail,hideAssets,hl`.

## How failing is decided

Status needs `--with-status` (or `with_status = true`). For each DAG:

![Decision flow: newest runs first, skip running/queued and empty successes, the first decisive run wins; failed runs are "tests only" when every failed task matches tests_task_regex](docs/images/status-rule.png)

- Runs are examined newest first, up to `status_runs_lookback`.
- Runs that are still running or queued are skipped. So are successful runs in which every task was skipped.
- The first remaining run is the decisive one. If it succeeded, the DAG is healthy. If it failed, the DAG is failing.
- A failing DAG is of kind `tests` when every task in state `failed` matches `tests_task_regex`. Otherwise it is a pipeline failure.
- Tasks in state `upstream_failed` do not count, because they only inherit a failure.

## Snapshot format

Snapshots are JSON, format 1:

```json
{
  "format": 1,
  "fetched_at": "2026-10-22T08:00:00Z",
  "source": "https://airflow.example.com",
  "airflow_version": "3.3.2",
  "nodes": [
    {"id": "dag:my_dag", "label": "my_dag", "type": "dag"},
    {"id": "asset:1", "label": "my_asset", "type": "asset"}
  ],
  "edges": [{"source_id": "dag:my_dag", "target_id": "asset:1"}],
  "dags": {
    "my_dag": {
      "is_paused": false,
      "schedule": "Asset",
      "owners": ["alice"],
      "last_run": {"state": "failed", "kind": "tests", "failed_tasks": ["dbt_test"]},
      "tasks": ["extract", "dbt_test"]
    }
  }
}
```

`dags` is optional. `last_run` is present with `--with-status` and `tasks` with `--with-tasks`. The `last_run` object is abbreviated here. Edges whose endpoints are missing from `nodes` are dropped on load.

## Development

```bash
uv run pytest
```

The diagrams in this README are drawn with Excalidraw. Sources live in `docs/diagrams/*.excalidraw`; re-export the PNG/SVG files in `docs/images/` after editing them.

## License

MIT. d3 7.9.0 is vendored under its ISC license, see `src/airflow_depgraph/vendor/D3-LICENSE`.
