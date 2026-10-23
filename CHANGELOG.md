# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses semantic versioning.

## [Unreleased]

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
