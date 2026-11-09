"""Hard/soft asset edges from the consumer DAG's asset schedule."""

from airflow_depgraph import Settings, build, fetch_snapshot
from airflow_depgraph.client import ApiError
from airflow_depgraph.render import build_view
from airflow_depgraph.strength import annotate, asset_uris, consumers, strength_of
from tests.test_cli import FakeClient

ALL = {"all": [{"asset": {"uri": "a"}}, {"asset": {"uri": "b"}}]}
ANY = {"any": [{"asset": {"uri": "a"}}, {"asset": {"uri": "b"}}]}
NESTED = {"all": [{"asset": {"uri": "a"}}, {"any": [{"asset": {"uri": "b"}}, {"asset": {"uri": "c"}}]}]}


def test_asset_uris_flattens_the_tree():
    assert asset_uris(NESTED) == {"a", "b", "c"}
    assert asset_uris({"asset": {"name": "only-name"}}) == {"only-name"}
    assert asset_uris(None) == set()


def test_strength_rules():
    assert strength_of({"timetable_summary": "Asset", "asset_expression": ALL}, "a") == "hard"
    one = {"timetable_summary": "Asset", "asset_expression": {"asset": {"uri": "a"}}}
    assert strength_of(one, "a") == "hard"
    assert strength_of({"timetable_summary": "Asset or 0 8 * * *", "asset_expression": ALL}, "a") == "soft"
    assert strength_of({"timetable_summary": "Asset", "asset_expression": ANY}, "a") == "soft"
    # read as a task inlet, not scheduled on: soft; no asset schedule at all: soft
    assert strength_of({"timetable_summary": "Asset", "asset_expression": ALL}, "zzz") == "soft"
    assert strength_of({"timetable_summary": "0 8 * * *", "asset_expression": None}, "a") == "soft"


def test_consumers_and_annotate():
    nodes = [{"id": "dag:p", "type": "dag"}, {"id": "asset:1", "type": "asset", "label": "a"},
             {"id": "dag:c", "type": "dag"}, {"id": "dag:d", "type": "dag"}]
    edges = [{"source_id": "dag:p", "target_id": "asset:1"},
             {"source_id": "asset:1", "target_id": "dag:c"},
             {"source_id": "asset:1", "target_id": "dag:d"},
             {"source_id": "dag:c", "target_id": "dag:d"}]
    assert consumers(nodes, edges) == ["c", "d"]
    out = annotate(nodes, edges, {"c": {"timetable_summary": "Asset", "asset_expression": ALL}})
    assert out[1]["strength"] == "hard"
    assert "strength" not in out[2] and "strength" not in out[0]
    assert all("strength" not in e for e in edges)       # the input is untouched


def test_fetch_snapshot_marks_asset_edges(monkeypatch):
    monkeypatch.setattr(build, "AirflowClient", FakeClient)
    snap = fetch_snapshot(FakeClient("http://af"), Settings(url="http://af"))
    assert [e.get("strength") for e in snap.edges] == [None, "soft"]   # y runs "Asset or cron"
    view = build_view(snap, Settings())
    assert view["edges"][1]["strength"] == "soft"
    assert (view["stats"]["hard"], view["stats"]["soft"]) == (0, 1)


def test_fetch_snapshot_can_skip_strength_and_survives_errors(monkeypatch):
    monkeypatch.setattr(build, "AirflowClient", FakeClient)
    snap = fetch_snapshot(FakeClient("http://af"), Settings(url="http://af", edge_strength=False))
    assert all("strength" not in e for e in snap.edges)

    class Broken(FakeClient):
        def dag_details(self, dag_id):
            raise ApiError("boom", 500, "/details")
    logs = []
    snap = fetch_snapshot(Broken("http://af"), Settings(url="http://af"), log=logs.append)
    assert all("strength" not in e for e in snap.edges)
    assert any("asset schedule unavailable for 1 of 1" in m for m in logs)


def test_asset_named_without_a_path_matches_the_label_not_only_the_normalised_uri():
    # Asset("contract://orders"): name "contract://orders", uri normalised to "contract://orders/";
    # the /ui/dependencies node is labelled by name.
    details = {"timetable_summary": "Asset", "asset_expression": {"all": [
        {"asset": {"uri": "contract://orders/", "name": "contract://orders"}}]}}
    assert asset_uris(details["asset_expression"]) == {"contract://orders/", "contract://orders"}
    assert strength_of(details, "contract://orders") == "hard"
