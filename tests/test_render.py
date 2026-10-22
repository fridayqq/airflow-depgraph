import dataclasses
import re

from airflow_depgraph.presets import Settings
from airflow_depgraph.render import VENDOR_D3, build_view, render
from airflow_depgraph.snapshot import Snapshot


def _by_id(view):
    return {n["id"]: n for n in view["nodes"]}


def test_view_strips_asset_prefix(small_snapshot):
    nodes = _by_id(build_view(small_snapshot, Settings(asset_label_strip_prefix="team-")))
    assert nodes["asset:1"]["label"] == "orders"
    assert nodes["asset:2"]["label"] == "orders_clean"
    assert nodes["dag:dag_a"]["label"] == "dag_a"


def test_view_highlight_regex(small_snapshot):
    view = build_view(small_snapshot, Settings(highlight="_report$"))
    assert {n["id"] for n in view["nodes"] if n.get("hl")} == {"dag:dag_k_report", "dag:dag_l_report"}


def test_view_no_highlight_by_default(small_snapshot):
    assert not any("hl" in n for n in build_view(small_snapshot, Settings())["nodes"])


def test_view_fail_kinds(small_snapshot):
    nodes = _by_id(build_view(small_snapshot, Settings()))
    assert nodes["dag:dag_a"]["fail"] == {"kind": "pipeline", "paused": False, "date": "2026-10-22T05:00:00Z"}
    assert nodes["dag:dag_d"]["fail"]["kind"] == "tests"
    assert "fail" not in nodes["dag:dag_l_report"]
    assert nodes["dag:dag_h"]["paused"] is True
    assert "fail" not in nodes["dag:dag_h"]
    assert "paused" not in nodes["dag:dag_a"]


def test_view_override_wins(small_snapshot):
    over = {"dag_c": {"kind": "pipeline", "paused": False, "date": "x"}, "dag_a": {"kind": "tests", "paused": False, "date": "y"}}
    nodes = _by_id(build_view(small_snapshot, Settings(), status_override=over))
    assert nodes["dag:dag_c"]["fail"] == {"kind": "pipeline", "paused": False, "date": "x"}
    assert nodes["dag:dag_a"]["fail"]["kind"] == "tests"


def test_view_down_counts(small_snapshot):
    nodes = _by_id(build_view(small_snapshot, Settings()))
    assert nodes["dag:dag_a"]["down"] == 9
    assert nodes["dag:dag_a"]["up"] == 0
    assert nodes["asset:1"]["indeg"] == 1 and nodes["asset:1"]["outdeg"] == 2


def test_view_stats(small_snapshot):
    stats = build_view(small_snapshot, Settings())["stats"]
    assert stats["dags"] == 12 and stats["assets"] == 5
    assert stats["triggers"] == 1 and stats["sensors"] == 1
    assert stats["edges"] == 19
    assert stats["failing"] == 2
    assert stats["snapshot"] == "2026-10-22T06:00:00Z"
    assert stats["source"] == "https://airflow.example.com"


def test_view_stats_count_override_and_ignore_paused():
    snap = Snapshot(
        format=1, fetched_at="t", source=None, airflow_version=None,
        nodes=[{"id": "dag:a", "label": "a", "type": "dag"}, {"id": "dag:b", "label": "b", "type": "dag"}],
        edges=[], dags={"a": {"is_paused": True, "last_run": {"state": "failed", "kind": "pipeline", "run_after": "d"}}},
    )
    view = build_view(snap, Settings())
    assert view["stats"]["failing"] == 0
    assert _by_id(view)["dag:a"]["fail"]["paused"] is True
    view = build_view(snap, Settings(), {"b": {"kind": "pipeline", "paused": False, "date": None}})
    assert view["stats"]["failing"] == 1


def test_render_has_no_placeholders_and_inlines_d3(small_snapshot):
    html = render(small_snapshot, Settings(highlight="_report$", highlight_label="Reports"))
    assert not re.search(r"__[A-Z0-9_]+__", html.replace(VENDOR_D3.read_text(encoding="utf-8"), ""))
    assert "__DATA__" not in html
    assert "https://d3js.org v7.9.0" in html
    assert "<script src=" not in html and "<link" not in html
    assert "Reports" in html
    assert "12 DAGs · 5 assets · 1 trigger · 1 sensor · 19 edges · failing now: 2 · snapshot 2026-10-22T06:00:00Z" in html


def test_render_embeds_meta(small_snapshot):
    html = render(small_snapshot, Settings())
    assert '"source": "https://airflow.example.com"' in html
    assert '"airflow_version": "3.3.2"' in html


def test_vendored_d3_is_safe_inside_script_element():
    assert "</script" not in VENDOR_D3.read_text(encoding="utf-8").lower()


def test_render_data_cannot_close_script(small_snapshot):
    snap = dataclasses.replace(small_snapshot, nodes=[{"id": "dag:x", "label": "</script><b>", "type": "dag"}], edges=[], dags=None)
    html = render(snap, Settings())
    assert "</script><b>" not in html


def test_render_title_escaped(small_snapshot):
    html = render(small_snapshot, Settings(title="<x>"))
    assert "<title>&lt;x&gt;</title>" in html
    assert "<x>" not in html.split("<script")[0]


def test_render_no_network_references(small_snapshot):
    head = render(small_snapshot, Settings()).replace(VENDOR_D3.read_text(encoding="utf-8"), "")
    assert "googleapis" not in head and "cdnjs" not in head
