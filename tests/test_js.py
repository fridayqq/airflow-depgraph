"""Runs the node tests of the page core (tests/js) when node is on PATH; skipped otherwise."""

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not on PATH")
def test_page_core_node_tests():
    # Node < 26 does not expand a directory argument, so pass the files explicitly.
    files = sorted(str(p.relative_to(ROOT)) for p in (ROOT / "tests" / "js").glob("*.test.mjs"))
    assert files, "no tests/js/*.test.mjs files"
    done = subprocess.run(["node", "--test", *files], cwd=ROOT, capture_output=True,
                          text=True, timeout=120)
    assert done.returncode == 0, done.stdout + done.stderr
