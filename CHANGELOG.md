# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses semantic versioning.

## [Unreleased]

### Added
- `--token-stdin` reads the bearer token from stdin. `--token` still works but prints a warning; the two cannot be combined.
- Distinct exit codes: 2 usage, 3 credentials, 4 Airflow unreachable, 5 unknown DAG, 130 interrupted; 1 for other errors.
- `--timeout` and `--workers` on fetch, graph, downstream, upstream, tasks and report (presets and `AIRFLOW_DEPGRAPH_TIMEOUT` / `AIRFLOW_DEPGRAPH_WORKERS` too).
- Bulk run status from the `dagRuns` and `taskInstances` list endpoints, with the same result as reading each DAG; `--no-bulk-status` turns it off.
- Plugin: side panel by hop with copy, blast-radius mode, unlinked list, selection in the URL, fit-to-content and clearer wording.
- `report` command: critical, unlinked, unconsumed-assets and failing DAGs.
- CI on Python 3.11, 3.12 and 3.13, ruff, and a nightly contract test against `airflow standalone`.

### Changed
- The HTTP client keeps connections alive, retries 429 and 502 to 504 and timeouts with backoff, and sends a `User-Agent`.
- The login request is sent once; only a connection error before any response is retried.
- Redirects on GET requests are followed, with a one-time note to use the final URL.
- The plugin page runs its builds in a bounded pool and answers 503 with `Retry-After` when busy, and sends security headers.
- The version is read from `src/airflow_depgraph/__init__.py` (single source).

### Fixed
- Plugin page: a burst of requests can no longer take Airflow's own threadpool; excess builds get a 503.
- Bulk run status follows the same decisive-run rule as the per-DAG path, and a login POST is sent once.
- Redirects are followed for GET requests only; a redirected login is reported instead of replayed.

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
