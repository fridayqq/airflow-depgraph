# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses semantic versioning.

## [Unreleased]

### Added
- README, *Get a token from the browser*: how to copy the `_token` cookie from DevTools, with a screenshot, and pass it with `AIRFLOW_DEPGRAPH_TOKEN` or `--token-stdin`.
- The CLI points at that route: the "no username" and "no password" errors mention it, a rejected login or token prints a `hint:` line, and `init` says that a token skips the password at run time.

### Changed
- Skill and README: a bearer token is the preferred credential and takes precedence over user and password; the skill walks the user through copying it.

## [0.5.3] - 2026-10-29

### Added
- `render`, `report`, `downstream`, `upstream` and `tasks` accept the raw `/ui/dependencies` JSON saved from a logged-in browser tab, so the graph can be built without giving the tool a password. README has a *Without credentials* section.

### Changed
- README screenshot shows blast radius: the failing DAGs and everything downstream of them, with the side panel listing them.

## [0.5.2] - 2026-10-28

### Changed
- README screenshot redone on a smaller fictional snapshot (23 DAGs) so the labels are readable; `docs/demo_snapshot.py` writes exactly that snapshot.

### Fixed
- `fetch_tasks` stops its queued calls deterministically after a 401. The test that counts those calls depended on thread timing and failed on a slow CI runner (the v0.5.1 tag build).

## [0.5.1] - 2026-10-28

### Added
- `init` proposes every asset prefix shared by at least two labels, cut at a separator (`:/._-`), not only `scheme://` and `scheme://segment/`: for example `x-dp-` when most assets start with it. A shorter prefix that strips exactly the same assets as a longer one is left out.
- `--discover --json` reports `sensor_tasks`: the candidate sensor tasks found in the task lists of a sample of 40 active DAGs, which is what `suggest.infer_sensors` is based on.
- A screenshot of the rendered page in the README, `open graph.html` in the quick start, a plugin icon, and `docs/demo_snapshot.py` (writes the fictional snapshot the screenshot was taken from).

### Fixed
- `init` suggested sensor inference as "no" on every instance: the suggestion was never computed. It now comes from the sample above, and the question says what the sample found.

## [0.5.0] - 2026-10-23

### Added
- `init` command: creates a preset by questionnaire, checking each answer against the live Airflow (health, sign-in) and proposing tags, an asset prefix and settings from what it finds. `--yes` with flags is the scriptable form; `--discover` (with `--json`) only reads and reports.
- `AirflowClient.health()`, a call to `/api/v2/monitor/health`.

### Changed
- An empty `password_env` (`password_env = ""`) skips the environment and reads the password from `password_cmd` only; `init` writes it when the password comes from a command.

## [0.4.0] - 2026-10-23

### Breaking
- `--tasks-matching` needs a REGEX argument. Use `--tasks-preset` to take the regex from the preset's `task_query_regex`.

### Added
- `report` command: critical, unlinked, unconsumed-assets and failing DAGs.
- `status`, last run, parent and `--tree` in downstream and upstream.
- Unlinked DAGs say "no dependencies", hint at deploy guards, and suggest close names.
- Page: fit to content with a Fit button, labels for the focused set and on zoom, a side panel by hop with copy, blast-radius mode, an unlinked list, the selection in the URL (`sel=`), clearer wording, a compact phone layout, and a snapshot age in the header.
- Plugin: a limiter on page builds, 503 with `Retry-After` when busy, and security headers on every response.
- `--token-stdin` reads the bearer token from stdin. `--token` still works but prints a warning; the two cannot be combined.
- Distinct exit codes: 2 usage, 3 credentials, 4 Airflow unreachable, 5 unknown DAG, 130 interrupted; 1 for other errors.
- `--timeout` and `--workers` on fetch, graph, downstream, upstream, tasks and report, with presets and `AIRFLOW_DEPGRAPH_TIMEOUT` / `AIRFLOW_DEPGRAPH_WORKERS`.
- Bulk run status from the `dagRuns` and `taskInstances` list endpoints, with the same result as reading each DAG; `--no-bulk-status` turns it off.
- CI on Python 3.11, 3.12 and 3.13 (tests, ruff, node tests), and a nightly contract test against `airflow standalone`.
- The page's pure logic lives in `graph-core.js`, with node tests in `tests/js`.

### Changed
- The HTTP client keeps connections alive, retries 429 and 502 to 504 and timeouts with backoff, and sends a `User-Agent`.
- The login request is sent once; only a connection error before any response is retried.
- The page reads colours once, debounces the search box, and lays out graphs above 800 nodes before the first frame.
- The version is read from `src/airflow_depgraph/__init__.py` (single source).

### Fixed
- Status is paged through task instances, so large DAGs get a correct status; task instances are deduplicated by task id and map index.
- An expired token (401) stops the run instead of showing a healthy graph; status errors are shown, not hidden.
- A task-list error no longer wipes a DAG's status; a per-DAG error no longer aborts the fetch.
- Ctrl-C exits 130 at once.
- Bulk run status follows the same decisive-run rule as the per-DAG path.
- Redirects are followed for GET requests only; a redirected login is reported instead of replayed.
- Plugin page: a burst of requests can no longer take Airflow's own threadpool; excess builds get a 503.

## [0.3.0]

### Added
- Page filters for linked, active and tagged DAGs; a DAG catalog in snapshots.
- Dependencies inferred from Python and custom sensors (CLI and a background refresh in the plugin).

### Fixed
- Per-DAG 403 skips the DAG instead of failing the run.
- Sensor inference and page filters hardened after review; private state directory with per-instance state files.

## [0.2.0]

### Added
- Airflow 3 plugin with an in-UI "DAG Dependencies" page under Browse.
- Claude Code skill and plugin marketplace entry.
- Trigger-only DAGs stay in the graph.

### Fixed
- Plugin cache bounds, concurrency limits, wrapped OS, JSON and timeout errors, depth validation.

## [0.1.0]

First CLI release (no git tag): `fetch`, `render`, `graph`, `downstream`, `upstream`, `tasks` and `presets`, the graph model, snapshot format, TOML presets, credential resolution, the Airflow REST client, the run-status overlay with the decisive-run rule, and the self-contained HTML renderer.
