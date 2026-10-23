import pytest

from airflow_depgraph.graph import Graph, reach_dags, walk


NODES = [
    {"id": "dag:a", "label": "a", "type": "dag"},
    {"id": "dag:b", "label": "b", "type": "dag"},
    {"id": "dag:c", "label": "c", "type": "dag"},
    {"id": "dag:d", "label": "d", "type": "dag"},
    {"id": "dag:e", "label": "e", "type": "dag"},
    {"id": "asset:1", "label": "one", "type": "asset"},
    {"id": "trigger:2", "label": "two", "type": "trigger"},
    {"id": "sensor:3", "label": "three", "type": "sensor"},
    {"id": "thing:9", "label": "nine", "type": "thing"},
]
EDGES = [
    {"source_id": "dag:a", "target_id": "asset:1"},
    {"source_id": "asset:1", "target_id": "dag:b"},
    {"source_id": "dag:b", "target_id": "trigger:2"},
    {"source_id": "trigger:2", "target_id": "dag:c"},
    {"source_id": "dag:a", "target_id": "sensor:3"},
    {"source_id": "sensor:3", "target_id": "dag:d"},
    {"source_id": "dag:d", "target_id": "thing:9"},
    {"source_id": "thing:9", "target_id": "dag:e"},
]


@pytest.fixture
def g():
    return Graph.from_payload(NODES, EDGES)


def test_from_payload_builds_indexes(g):
    assert g.dag_ids == ["a", "b", "c", "d", "e"]
    assert g.nodes["asset:1"].label == "one"
    assert g.out["dag:a"] == ["asset:1", "sensor:3"]
    assert g.inc["dag:c"] == ["trigger:2"]


def test_walk_downstream_hops_and_via(g):
    hits = walk(g, "a", "downstream")
    assert [(h.dag_id, h.hops, h.via) for h in hits] == [
        ("b", 1, "asset:one"), ("d", 1, "sensor:three"), ("c", 2, "trigger:two"), ("e", 2, "thing:nine")]


def test_walk_depth_limit(g):
    assert [h.dag_id for h in walk(g, "a", "downstream", depth=1)] == ["b", "d"]


def test_walk_upstream(g):
    assert [(h.dag_id, h.hops) for h in walk(g, "c", "upstream")] == [("b", 1), ("a", 2)]


def test_walk_unknown_dag_raises(g):
    with pytest.raises(KeyError):
        walk(g, "nope", "downstream")


def test_reach_dags_counts_transitively(g):
    assert reach_dags(g, "dag:a", "downstream") == 4
    assert reach_dags(g, "asset:1", "downstream") == 2


def test_dag_reached_via_two_assets_reported_once_at_min_hops():
    nodes = [
        {"id": "dag:s", "label": "s", "type": "dag"},
        {"id": "dag:t", "label": "t", "type": "dag"},
        {"id": "asset:x", "label": "x", "type": "asset"},
        {"id": "asset:y", "label": "y", "type": "asset"},
    ]
    edges = [
        {"source_id": "dag:s", "target_id": "asset:x"},
        {"source_id": "asset:x", "target_id": "dag:t"},
        {"source_id": "dag:s", "target_id": "asset:y"},
        {"source_id": "asset:y", "target_id": "dag:t"},
    ]
    hits = walk(Graph.from_payload(nodes, edges), "s", "downstream")
    assert [(h.dag_id, h.hops) for h in hits] == [("t", 1)]


def test_walk_parent_is_the_dag_one_hop_closer(small_graph):
    from airflow_depgraph.graph import walk
    hits = {h.dag_id: h for h in walk(small_graph, "dag_a", "downstream")}
    assert hits["dag_b"].parent == "dag_a"
    assert hits["dag_l_report"].parent == "dag_e"
    assert hits["dag_d"].parent == "dag_c"
