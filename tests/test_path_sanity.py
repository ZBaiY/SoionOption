from __future__ import annotations
from pathlib import Path

def test_repo_layout_exists():
    root = Path(__file__).resolve().parents[1]
    for p in ["artifacts", "configs", "data", "docs", "src", "tests"]:
        assert (root / p).exists()
