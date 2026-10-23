import json

import pytest

from airflow_depgraph.graph import reach_dags
from airflow_depgraph.snapshot import SnapshotError, dump, load, new, validate_payload


def test_load_fixture(small_snapshot):
    assert small_snapshot.format == 1 and len(small_snapshot.graph.dag_ids) == 12


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
    nodes, edges = validate_payload(raw)
    assert {"id": "dag:b", "label": "b", "type": "dag"} in nodes
    assert {"source_id": "trigger:a:b:t", "target_id": "dag:b"} in edges
    assert all(e["target_id"] != "asset:99" for e in edges)
