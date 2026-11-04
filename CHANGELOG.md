# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses semantic versioning.

## [Unreleased]

### Added
- *What failed and what it blocks* on the page (hash flag `impact`): the failing DAGs in columns with what they **blocked** (orange) and what **ran on unvalidated data** (violet), hop by hop, the failures that affected nothing on a row under the graph, one side-panel card per failure with the affected DAGs and the link they came through, and the counts in the header. A failure blocks a consumer only when the outlet it waits on was not produced in the failed run and the consumer has not run since (hard edges, triggers and sensors; soft consumers run anyway); a tests-only failure whose outlets were produced marks the consumers that ran since as unvalidated.
- Snapshot: `last_run.upstream_failed_tasks` (the task ids; `upstream_failed` stays the count) and `last_run.outlets`, which of the assets and triggers a failed DAG feeds were produced in its failed run (`outlets.py`: the producing task by naming convention from the task list, one `GET /dags/{id}/tasks` per failed DAG unless the list was already read). A tests-only failure that left an outlet unproduced is stored as `kind: pipeline`.
- *pipeline failed* / *tests failed* switches (hash flags `noPipeline`, `noTests`) choose which failures count on the page: rings, the failing chain, the impact view.
- Preset key `status_lookback_days` (default 7): the status examines every run of the last that many days, and at least the newest `status_runs_lookback` runs. A monthly PowerBI refresh that failed and was followed by thirty asset-triggered runs that took a freshness gate's skip path used to fall out of a five-run window and look healthy.
- Snapshot: `last_run.running`, when a run newer than the decisive one that is still running started. The page takes it as when the DAG last ran: a DAG running again after its failure, or running on data whose tests just failed, counts in *what failed and what it blocks*.
- Status is also read for the active catalog DAGs that carry a preset tag but have no cross-DAG link (`fetch`, `graph`, the plugin). They were on the page without a ring whatever their runs did.

### Changed
- The *blast radius* switch is replaced by *what failed and what it blocks*; a `blast` hash flag opens the new view.
- Status: a successful run no longer fixes an older failed run of the lookback unless the tasks that failed there ran in it or in another success since. A run that takes a freshness gate's skip path ends green with the work skipped, and used to hide the failure; now the failed run stays the DAG's last decisive run. Per-DAG and bulk paths alike; the bulk path makes two extra `taskInstances/list` calls per round only for the DAGs with a failure in their window.
- The bulk run listing covers the whole status window in one pass (`dagRuns/list`, paged), instead of three-day then fourteen-day listings capped at the run lookback; DAGs with fewer runs in the window than `status_runs_lookback` are read per DAG (`GET /dags/{id}/dagRuns`, paged with `offset`), where the newest runs count whatever their age. The per-DAG path reads the failed run's task instances first and the successes since it newest first, stopping as soon as the failed tasks are seen again.

## [0.8.0] - 2026-11-03

### Added
- Hard/soft asset edges: `fetch`, `graph`, the plugin and the library read each consumer DAG's asset schedule (`GET /api/v2/dags/{id}/details`, parallel) and mark every asset -> DAG edge `strength: hard|soft` in the snapshot. Soft means the DAG runs without the asset: `Asset or <cron>`, an `any` condition, or an inlet-only read. The page draws soft edges dashed, counts them in the header, adds a *hard edges only* toggle (hash flag `hard`) and a legend entry. Preset key `edge_strength` (default true) switches the extra calls off.
- With *linked DAGs only* off, the DAGs without any link are drawn on a shelf under the graph (a square grid of dots with a caption, outside the force layout) instead of only being listed in the side panel.
- Library entry point: `build_page(settings, user=..., password=...)` returns the page for a live Airflow in one call; `fetch_snapshot`, `connect`, `render`, `load` and `Settings` are exported from the package. The CLI runs through the same code.

### Changed
- README opens with a recorded demo of the page (`docs/images/demo.gif`, recorded by `docs/record_demo.mjs`); the static screenshot moved to *Page features*.
- Airflow plugin section: no settings are needed; the variables that tune the page are listed, including `AIRFLOW_DEPGRAPH_TAG_FACET` and `AIRFLOW_DEPGRAPH_TAGS`.

## [0.7.0] - 2026-10-29

### Added
- Facet chips: tags of one family, such as `vertical:Sales`, become chips in the header. Any of the chosen values lights a DAG, the rest dims but stays, and the side panel sums up the choice and the links to other values. Preset key `tag_facet` (`--tag-facet`, `AIRFLOW_DEPGRAPH_TAG_FACET`), detected when unset; hash `v=`.

## [0.6.0] - 2026-10-29

### Changed
- The page is restyled: a gridded drafting surface for the graph, a three-row header (title and search, counts as figures, switches grouped into *Show* and *Focus*), and a side panel that lists the chain hop by hop on a rail. Dark mode follows the same scheme. No external fonts or requests; the logic is unchanged.

### Added
- *asset names* switch (hash flag `names`, off by default): names the assets, triggers and sensors of the focused chain. The selected node is always named.

## [0.5.4] - 2026-10-29

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
