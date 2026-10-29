import json

import pytest

from airflow_depgraph.graph import reach_dags
from airflow_depgraph.snapshot import SnapshotError, dump, load, new, validate_payload


def test_load_fixture(small_snapshot):
    assert small_snapshot.format == 1 and len(small_snapshot.graph.dag_ids) == 13


def test_fixture_downstream_of_dag_a(small_graph):
    assert reach_dags(small_graph, "dag:dag_a", "downstream") == 9


def test_validate_rejects_missing_edges():
    with pytest.raises(SnapshotError, match="unexpected /ui/dependencies payload"):
        validate_payload({"nodes": []})


def test_validate_rejects_node_without_type():
    with pytest.raises(SnapshotError):
        validate_payload({"nodes": [{"id": "dag:x", "label": "x"}], "edges": []})


def test_validate_drops_dangling_edges():
    raw = {
        "nodes": [{"id": "dag:x", "label": "x", "type": "dag"},
                  {"id": "dag:y", "label": "y", "type": "dag"}],
        "edges": [{"source_id": "dag:x", "target_id": "dag:y"},
                  {"source_id": "dag:x", "target_id": "asset:hidden"},
                  {"source_id": "asset:hidden", "target_id": "dag:y"}],
    }
    nodes, edges = validate_payload(raw)
    assert [n["id"] for n in nodes] == ["dag:x", "dag:y"]
    assert edges == [{"source_id": "dag:x", "target_id": "dag:y"}]


def test_load_rejects_future_format(tmp_path):
    p = tmp_path / "s.json"
    p.write_text('{"format": 2, "nodes": [], "edges": []}')
    with pytest.raises(SnapshotError, match="format 2"):
        load(p)


def test_load_accepts_a_raw_ui_dependencies_payload(tmp_path):
    """The JSON a logged-in browser shows at /ui/dependencies, saved as is."""
    p = tmp_path / "raw.json"
    p.write_text('{"nodes": [{"id": "dag:x", "label": "x", "type": "dag"},'
                 ' {"id": "dag:y", "label": "y", "type": "dag"}],'
                 ' "edges": [{"source_id": "dag:x", "target_id": "dag:y"}]}')
    snap = load(p)
    assert snap.format == 1
    assert [n["id"] for n in snap.nodes] == ["dag:x", "dag:y"]
    assert snap.edges == [{"source_id": "dag:x", "target_id": "dag:y"}]
    assert (snap.fetched_at, snap.source, snap.airflow_version) == ("", None, None)
    assert (snap.dags, snap.catalog, snap.inferred) == (None, None, None)


def test_load_rejects_a_raw_payload_without_edges(tmp_path):
    p = tmp_path / "raw.json"
    p.write_text('{"nodes": []}')
    with pytest.raises(SnapshotError, match="format None"):
        load(p)


def test_dump_then_load_roundtrip(tmp_path, small_snapshot):
    p = tmp_path / "out.json"
    dump(small_snapshot, p)
    assert not (tmp_path / "out.json.tmp").exists()
    again = load(p)
    assert again == small_snapshot
    assert again.graph.dag_ids == small_snapshot.graph.dag_ids


def test_dump_omits_dags_when_none(tmp_path):
    snap = new(nodes=[], edges=[], source="https://airflow.example.com", airflow_version="3.3.2")
    p = tmp_path / "out.json"
    dump(snap, p)
    assert "dags" not in json.loads(p.read_text())


def test_new_stamps_utc_fetched_at():
    snap = new(nodes=[], edges=[], source=None, airflow_version=None)
    assert snap.format == 1
    assert len(snap.fetched_at) == 20 and snap.fetched_at.endswith("Z")


def test_validate_adds_dag_nodes_airflow_omits():
    # Airflow 3.1 lists a trigger edge to a DAG without listing that DAG as a node.
    raw = {"nodes": [{"id": "dag:a", "label": "a", "type": "dag"},
                     {"id": "trigger:a:b:t", "label": "t", "type": "trigger"}],
           "edges": [{"source_id": "dag:a", "target_id": "trigger:a:b:t"},
                     {"source_id": "trigger:a:b:t", "target_id": "dag:b"},
                     {"source_id": "dag:a", "target_id": "asset:99"}]}
    nodes, edges = validate_payload(raw, airflow_version="3.1.5")
    assert {"id": "dag:b", "label": "b", "type": "dag"} in nodes
    assert {"source_id": "trigger:a:b:t", "target_id": "dag:b"} in edges
    assert all(e["target_id"] != "asset:99" for e in edges)


_TRIGGER_RAW = {"nodes": [{"id": "dag:a", "label": "a", "type": "dag"},
                          {"id": "trigger:a:b:t", "label": "t", "type": "trigger"}],
                "edges": [{"source_id": "dag:a", "target_id": "trigger:a:b:t"},
                          {"source_id": "trigger:a:b:t", "target_id": "dag:b"}]}


@pytest.mark.parametrize("version,synthesized", [
    ("3.1.5", True), ("3.1.7+astro.1", True), ("3.1.8", False), ("3.3.2", False), (None, False)])
def test_validate_synthesizes_trigger_target_only_on_old_airflow(version, synthesized):
    nodes, edges = validate_payload(_TRIGGER_RAW, airflow_version=version)
    assert any(n["id"] == "dag:b" for n in nodes) is synthesized
    assert any(e["target_id"] == "dag:b" for e in edges) is synthesized


def test_validate_does_not_synthesize_dag_from_asset_edge():
    raw = {"nodes": [{"id": "asset:1", "label": "x", "type": "asset"}],
           "edges": [{"source_id": "asset:1", "target_id": "dag:hidden"}]}
    nodes, edges = validate_payload(raw, airflow_version="3.1.5")
    assert [n["id"] for n in nodes] == ["asset:1"] and edges == []


def test_catalog_roundtrip_and_optional(tmp_path, small_snapshot):
    assert small_snapshot.catalog["dag_h"]["is_paused"] is True
    p = tmp_path / "out.json"
    dump(small_snapshot, p)
    assert load(p).catalog == small_snapshot.catalog
    snap = new(nodes=[], edges=[], source=None, airflow_version=None)
    dump(snap, p)
    assert "catalog" not in json.loads(p.read_text()) and load(p).catalog is None


def test_build_catalog_normalises_tags_and_schedule():
    from airflow_depgraph.snapshot import build_catalog
    cat = build_catalog([
        {"dag_id": "a", "tags": [{"name": "team-a", "dag_id": "a"}, "analytics"], "is_paused": True,
         "is_stale": False, "timetable_summary": "0 5 * * *", "owners": ["o"]},
        {"dag_id": "b", "timetable_description": "Daily"},
    ])
    assert cat["a"] == {"tags": ["team-a", "analytics"], "is_paused": True, "is_stale": False,
                        "schedule": "0 5 * * *", "owners": ["o"]}
    assert cat["b"] == {"tags": [], "is_paused": False, "is_stale": False, "schedule": "Daily", "owners": []}


def test_with_inferred_adds_link_nodes_and_skips_guards_and_unresolved(inferred_snapshot):
    from airflow_depgraph.snapshot import with_inferred
    s = inferred_snapshot
    nodes, edges = with_inferred(s.nodes, s.edges, s.inferred)
    ids = {n["id"] for n in nodes}
    assert {"sensor-inferred:dag_x_cron:wait_i", "sensor-inferred:dag_k_report:wait_l",
            "dag:dag_x_cron"} <= ids
    assert "sensor-inferred:dag_y_cron:guard_a" not in ids and "dag:dag_y_cron" not in ids
    assert not any(i.startswith("sensor-inferred:dag_b:") for i in ids)
    assert {"source_id": "dag:dag_i", "target_id": "sensor-inferred:dag_x_cron:wait_i"} in edges
    assert {"source_id": "sensor-inferred:dag_x_cron:wait_i", "target_id": "dag:dag_x_cron"} in edges
    link = next(n for n in nodes if n["id"] == "sensor-inferred:dag_x_cron:wait_i")
    assert link == {"id": "sensor-inferred:dag_x_cron:wait_i", "label": "wait_i", "type": "sensor",
                    "inferred": "rendered", "kind": "data"}
    g_nodes, _ = with_inferred(s.nodes, s.edges, s.inferred, include_guards=True)
    assert any(n["id"] == "sensor-inferred:dag_y_cron:guard_a" and n["kind"] == "guard" for n in g_nodes)
    assert len(s.nodes) == 22                       # inputs untouched


def test_snapshot_graph_traverses_inferred_edges(inferred_snapshot, small_snapshot):
    assert reach_dags(inferred_snapshot.graph, "dag:dag_a", "downstream") == 10   # + dag_x_cron
    assert reach_dags(small_snapshot.graph, "dag:dag_a", "downstream") == 9
    assert reach_dags(inferred_snapshot.graph_for(inferred=False), "dag:dag_a", "downstream") == 9
    assert reach_dags(inferred_snapshot.graph_for(guards=True), "dag:dag_a", "downstream") == 11


def test_inferred_roundtrip_and_validation(tmp_path, inferred_snapshot):
    p = tmp_path / "s.json"
    dump(inferred_snapshot, p)
    assert load(p).inferred == inferred_snapshot.inferred
    data = json.loads(p.read_text())
    data["inferred"] = {"x": 1}
    p.write_text(json.dumps(data))
    with pytest.raises(SnapshotError, match="inferred"):
        load(p)


def test_visible_inferred_drops_dags_the_viewer_cannot_see():
    from airflow_depgraph.snapshot import visible_inferred
    inferred = [{"dag": "b", "task": "t", "source": "rendered", "targets": ["a", "hidden"]},
                {"dag": "hidden", "task": "t", "source": "source", "targets": ["a"]},
                {"dag": "b", "task": "u", "source": "logs", "targets": ["hidden"]}]
    assert visible_inferred(inferred, {"a", "b"}) == [
        {"dag": "b", "task": "t", "source": "rendered", "targets": ["a"]}]


def test_graph_is_rebuilt_when_inferred_or_edges_change(small_snapshot, inferred_snapshot):
    import dataclasses
    snap = dataclasses.replace(small_snapshot)
    assert reach_dags(snap.graph, "dag:dag_a", "downstream") == 9
    snap.inferred = inferred_snapshot.inferred
    assert reach_dags(snap.graph, "dag:dag_a", "downstream") == 10
    snap.edges = []
    assert reach_dags(snap.graph, "dag:dag_a", "downstream") == 0


def test_with_inferred_drops_targets_the_sensor_dag_already_feeds():
    from airflow_depgraph.snapshot import with_inferred
    nodes = [{"id": "dag:w", "label": "w", "type": "dag"}, {"id": "dag:t", "label": "t", "type": "dag"},
             {"id": "trigger:w:t:kick", "label": "kick", "type": "trigger"}]
    edges = [{"source_id": "dag:w", "target_id": "trigger:w:t:kick"},
             {"source_id": "trigger:w:t:kick", "target_id": "dag:t"}]
    inferred = [{"dag": "w", "task": "wait", "source": "source", "targets": ["t", "u"]},
                {"dag": "w", "task": "only_t", "source": "source", "targets": ["t"]}]
    n, e = with_inferred(nodes, edges, inferred)
    assert {"source_id": "dag:u", "target_id": "sensor-inferred:w:wait"} in e
    assert not any(x["target_id"] == "sensor-inferred:w:wait" and x["source_id"] == "dag:t" for x in e)
    assert not any(x["id"] == "sensor-inferred:w:only_t" for x in n)
