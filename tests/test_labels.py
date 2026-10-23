from airflow_depgraph.labels import normalise_state, state_label, strip_asset_prefix


def test_state_label_variants():
    assert state_label({"state": "success"}) == "ok"
    assert state_label({"state": "failed", "kind": "tests"}) == "FAILED (tests)"
    assert state_label({"state": "failed", "kind": "pipeline"}) == "FAILED (pipeline)"
    assert state_label({"state": "failed", "kind": None}) == "FAILED"
    assert state_label({"state": "running"}) is None
    assert state_label(None) is None


def test_normalise_state_maps_others_to_none():
    assert normalise_state("success") == "success"
    assert normalise_state("failed") == "failed"
    assert normalise_state("queued") is None
    assert normalise_state(None) is None


def test_strip_asset_prefix():
    assert strip_asset_prefix("team-orders", "team-") == "orders"
    assert strip_asset_prefix("team-orders", None) == "team-orders"
    assert strip_asset_prefix("other", "team-") == "other"
