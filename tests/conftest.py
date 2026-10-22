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


class _Resp:
    def __init__(self, body: bytes):
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def fake_opener(routes):
    """Build an opener= callable. routes: path -> JSON object (200) or int status."""
    import json
    import urllib.error
    import urllib.parse

    calls = []

    def opener(req, timeout=None):
        calls.append(
            {
                "method": req.get_method(),
                "url": req.full_url,
                "headers": {k.lower(): v for k, v in req.header_items()},
                "body": req.data,
            }
        )
        path = urllib.parse.urlsplit(req.full_url).path
        route = routes[path]
        if isinstance(route, int):
            raise urllib.error.HTTPError(req.full_url, route, "reason", {}, None)
        return _Resp(json.dumps(route).encode())

    opener.calls = calls
    return opener
