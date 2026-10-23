import dataclasses
import re

from airflow_depgraph.presets import Settings
from airflow_depgraph.render import SEARCH_PATTERN, VENDOR_D3, build_view, render
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
    assert nodes["dag:dag_a"]["paused"] is False


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
    assert stats["dags"] == 14 and stats["assets"] == 6
    assert stats["triggers"] == 1 and stats["sensors"] == 2
    assert stats["edges"] == 22
    assert stats["unlinked"] == 2 and stats["paused"] == 1 and stats["tagged"] == 0
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
    assert "14 DAGs · 6 assets · 1 trigger · 2 sensors · 22 edges · failing now: 2 · snapshot 2026-10-22T06:00:00Z" in html


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


def test_search_pattern_is_embedded_verbatim(small_snapshot):
    html = render(small_snapshot, Settings())
    assert "const SEARCH_RE = /" + SEARCH_PATTERN + "/;" in html


def _parse(text):
    m = re.match(SEARCH_PATTERN, text)
    if m.group(1) is None and m.group(3) is None:
        return ("plain", text)
    return (m.group(1), m.group(2), m.group(3))


def test_search_pattern_keeps_digits_that_belong_to_the_name():
    assert _parse("+dag_2") == ("", "dag_2", None)
    assert _parse("12_hourly+") == (None, "12_hourly", "")
    assert _parse("2+dag+3") == ("2", "dag", "3")
    assert _parse("dag") == ("plain", "dag")


def test_fixture_has_two_link_chain(small_snapshot):
    g = small_snapshot.graph
    assert g.out["dag:dag_j"] == ["asset:2", "asset:6"]
    assert g.out["asset:6"] == ["sensor:2"] and g.out["sensor:2"] == ["dag:dag_k_report"]


def test_view_catalog_unlinked_stale_paused_tags(small_snapshot):
    view = build_view(small_snapshot, Settings(tags=("team-a",)))
    nodes = _by_id(view)
    assert "dag:dag_m_stale" not in nodes
    assert not any("dag:dag_m_stale" in (e["s"], e["t"]) for e in view["edges"])
    assert nodes["asset:5"]["indeg"] == 1
    for dag in ("dag_x_cron", "dag_y_cron"):
        n = nodes["dag:" + dag]
        assert n["unlinked"] is True and n["type"] == "dag"
        assert (n["down"], n["up"], n["indeg"], n["outdeg"]) == (0, 0, 0, 0)
    assert nodes["dag:dag_x_cron"]["tags"] == ["team-a"] and nodes["dag:dag_y_cron"]["tags"] == []
    assert "unlinked" not in nodes["dag:dag_a"]
    assert nodes["dag:dag_b"]["tags"] == ["team-a", "analytics"]
    assert nodes["dag:dag_h"]["paused"] is True and nodes["dag:dag_c"]["paused"] is False
    assert nodes["dag:dag_a"]["down"] == 9
    st = view["stats"]
    assert (st["unlinked"], st["paused"], st["tagged"]) == (2, 1, 4)


def test_view_unlinked_gets_status_fail():
    snap = Snapshot(format=1, fetched_at="t", source=None, airflow_version=None, nodes=[], edges=[],
                    dags={"c": {"last_run": {"state": "failed", "kind": "pipeline", "run_after": "d"}}},
                    catalog={"c": {"tags": [], "is_paused": False, "is_stale": False}})
    n = _by_id(build_view(snap, Settings()))["dag:c"]
    assert n["unlinked"] and n["fail"]["kind"] == "pipeline"


def test_view_without_catalog_has_no_unlinked(small_snapshot):
    view = build_view(dataclasses.replace(small_snapshot, catalog=None), Settings())
    assert not any(n.get("unlinked") for n in view["nodes"])
    assert _by_id(view)["dag:dag_h"]["paused"] is True
    assert _by_id(view)["dag:dag_a"]["tags"] == []
    assert view["stats"]["dags"] == 13
    assert '"catalog": false' in render(dataclasses.replace(small_snapshot, catalog=None), Settings(tags=("team-a",)))


def test_render_meta_has_tags(small_snapshot):
    html = render(small_snapshot, Settings(tags=("reporting", "team-a")))
    assert '"airflow_version": "3.3.2", "tags": ["reporting", "team-a"]' in html
    assert '"airflow_version": "3.3.2", "tags": []' in render(small_snapshot, Settings())
