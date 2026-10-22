from pathlib import Path

import pytest

from airflow_depgraph.snapshot import load

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def small_snapshot():
    return load(FIXTURES / "snapshot_small.json")


@pytest.fixture
def small_graph(small_snapshot):
    return small_snapshot.graph
