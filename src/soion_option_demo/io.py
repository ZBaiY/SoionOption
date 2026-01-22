# DEMO-ONLY MODULE
# Intended final location: src/quant_engine/utils/artifact_io.py (SoionLab utils layer)
"""
I/O utilities for demo pipeline.

Placeholder implementations for data loading and artifact writing.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from quant_engine.utils.logger import get_logger, log_info, log_debug
from quant_engine.utils.paths import resolve_data_root


def load_sample_data(
    *,
    symbol: str,
    interval: str,
    data_root: Path | None = None,
) -> pd.DataFrame | None:
    """Load sample option chain data.

    Placeholder - returns empty DataFrame if no data found.
    """
    logger = get_logger(__name__)

    if data_root is None:
        data_root = resolve_data_root(__file__, levels_up=3)

    # Try to find sample data
    sample_dir = data_root / "data" / "sample" / "option_chain" / symbol / interval

    if not sample_dir.exists():
        log_debug(logger, "No sample data found", path=str(sample_dir))
        return None

    # Find parquet files
    parquet_files = list(sample_dir.rglob("*.parquet"))
    if not parquet_files:
        log_debug(logger, "No parquet files in sample dir", path=str(sample_dir))
        return None

    # Load first file as sample
    df = pd.read_parquet(parquet_files[0])
    log_info(
        logger,
        "Loaded sample data",
        path=str(parquet_files[0]),
        rows=len(df),
    )
    return df


def write_artifact(
    *,
    artifacts_dir: Path,
    name: str,
    data: Any,
    format: str = "json",
) -> Path:
    """Write an artifact to the artifacts directory.

    Supports json and parquet formats.
    """
    logger = get_logger(__name__)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    if format == "json":
        path = artifacts_dir / f"{name}.json"
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
    elif format == "parquet":
        path = artifacts_dir / f"{name}.parquet"
        if isinstance(data, pd.DataFrame):
            data.to_parquet(path)
        else:
            pd.DataFrame(data).to_parquet(path)
    else:
        raise ValueError(f"Unsupported artifact format: {format}")

    log_debug(logger, "Wrote artifact", path=str(path), format=format)
    return path
