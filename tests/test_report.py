import dataclasses
from pathlib import Path

import pytest

from airflow_depgraph import report
from airflow_depgraph.presets import Settings
from airflow_depgraph.snapshot import load

FIX = Path(__file__).parent / "fixtures" / "snapshot_small.json"


@pytest.fixture
def snap():
    return load(FIX)


def ids(rows, key="dag_id"):
    return [r[key] for r in rows]


# ---- critical ----

def test_critical_ranks_by_transitive_downstream(snap):
    rows = report.critical(snap, Settings())
    assert rows[0]["dag_id"] == "dag_a" and rows[0]["rank"] == 1
    assert rows[0]["downstream"] == 9
    assert rows[0]["direct_consumers"] == 3
    assert rows[0]["state"] == "failed" and rows[0]["kind"] == "pipeline"
    assert [r["rank"] for r in rows] == list(range(1, len(rows) + 1))


def test_critical_top_limits_rows(snap):
    assert len(report.critical(snap, Settings(), top=2)) == 2


def test_critical_tag_filter_keeps_only_tagged_dags(snap):
    rows = report.critical(snap, Settings(), tags=("team-a",))
    assert set(ids(rows)) == {"dag_a", "dag_b", "dag_h"}


def test_critical_excludes_stale_dags(snap):
    rows = report.critical(snap, Settings())
    assert "dag_m_stale" not in ids(rows)


def _with_inferred_and_guard(snap):
    entries = [
        # dag_q's sensor waits for dag_a: a data link, so dag_q is downstream of dag_a
        {"dag": "dag_q", "task": "wait_a", "operator": "PythonSensor", "source": "rendered",
         "targets": ["dag_a"], "target_tasks": [], "kind": "data"},
        # dag_r's deploy guard on dag_a: must not count as downstream
        {"dag": "dag_r", "task": "guard_a", "operator": "PythonSensor", "source": "rendered",
         "targets": ["dag_a"], "target_tasks": [], "kind": "guard"},
    ]
    return dataclasses.replace(snap, inferred=entries)


def test_critical_counts_inferred_links_and_skips_guards(snap):
    snap = _with_inferred_and_guard(snap)
    rows = {r["dag_id"]: r for r in report.critical(snap, Settings())}
    assert rows["dag_a"]["downstream"] == 10  # 9 explicit + dag_q, not dag_r


def test_critical_no_inferred_drops_inferred_links(snap):
    snap = _with_inferred_and_guard(snap)
    rows = {r["dag_id"]: r for r in report.critical(snap, Settings(), inferred=False)}
    assert rows["dag_a"]["downstream"] == 9 and "dag_q" not in rows


def test_critical_without_catalog_and_tags_is_an_error(snap):
    with pytest.raises(report.ReportError, match="no catalog"):
        report.critical(dataclasses.replace(snap, catalog=None), Settings(), tags=("team-a",))


# ---- unlinked ----

def test_unlinked_lists_active_catalog_dags_without_links(snap):
    rows = report.unlinked(snap, Settings())
    assert ids(rows) == ["dag_x_cron", "dag_y_cron"]  # dag_m_stale is stale and left out
    assert rows[0] == {"dag_id": "dag_x_cron", "schedule": "0 1 * * *", "paused": False,
                       "tags": ["team-a"]}


def test_unlinked_tag_filter(snap):
    assert ids(report.unlinked(snap, Settings(), tags=("team-a",))) == ["dag_x_cron"]
    assert report.unlinked(snap, Settings(), tags=("reporting",)) == []


def test_unlinked_without_catalog_is_an_error(snap):
    with pytest.raises(report.ReportError, match="no catalog"):
        report.unlinked(dataclasses.replace(snap, catalog=None), Settings())


# ---- unconsumed-assets ----

def test_unconsumed_assets_empty_on_fixture(snap):
    assert report.unconsumed_assets(snap, Settings()) == []


def _with_orphan_asset(snap):
    return dataclasses.replace(
        snap,
        nodes=snap.nodes + [{"id": "asset:9", "label": "team-a-orphan", "type": "asset"}],
        edges=snap.edges + [{"source_id": "dag:dag_a", "target_id": "asset:9"}])


def test_unconsumed_asset_strips_preset_prefix(snap):
    rows = report.unconsumed_assets(_with_orphan_asset(snap),
                                    Settings(asset_label_strip_prefix="team-a-"))
    assert rows == [{"asset": "orphan", "producers": ["dag_a"]}]


def test_unconsumed_asset_tag_filter_uses_producer_tags(snap):
    orphan = _with_orphan_asset(snap)
    assert report.unconsumed_assets(orphan, Settings(), tags=("team-a",))[0]["asset"] == "team-a-orphan"
    assert report.unconsumed_assets(orphan, Settings(), tags=("reporting",)) == []


# ---- failing ----

def test_failing_lists_failed_unpaused_dags_with_downstream(snap):
    rows = report.failing(snap, Settings())
    assert ids(rows) == ["dag_a", "dag_d"]  # dag_a has 9 downstream DAGs, dag_d has 1
    assert rows[0] == {"dag_id": "dag_a", "kind": "pipeline", "failed_tasks": ["load"],
                       "downstream": 9, "stale_consumers": []}
    assert rows[1]["kind"] == "tests" and rows[1]["downstream"] == 1


def test_failing_skips_paused_dags(snap):
    paused = dataclasses.replace(snap, catalog={**snap.catalog,
                                                "dag_a": {**snap.catalog["dag_a"], "is_paused": True}})
    assert ids(report.failing(paused, Settings())) == ["dag_d"]


def test_failing_names_cron_fallback_consumers(snap):
    cat = {**snap.catalog, "dag_k_report": {**snap.catalog["dag_k_report"],
                                            "schedule": "Asset or 0 6 * * *"}}
    rows = {r["dag_id"]: r for r in report.failing(dataclasses.replace(snap, catalog=cat), Settings())}
    assert rows["dag_a"]["stale_consumers"] == ["dag_k_report"]
    assert rows["dag_d"]["stale_consumers"] == ["dag_k_report"]


def test_failing_tag_filter(snap):
    assert ids(report.failing(snap, Settings(), tags=("team-a",))) == ["dag_a"]


def test_failing_without_status_is_an_error(snap):
    with pytest.raises(report.ReportError, match="--with-status"):
        report.failing(dataclasses.replace(snap, dags=None), Settings())


def test_effective_tags_precedence():
    s = Settings(tags=("team-a",))
    assert report.effective_tags(s, None, False) == ("team-a",)
    assert report.effective_tags(s, ["reporting"], False) == ("reporting",)
    assert report.effective_tags(s, ["reporting"], True) is None
    assert report.effective_tags(Settings(), None, False) is None
