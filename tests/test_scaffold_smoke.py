"""
Scaffold Smoke Test

Verifies that the demo scaffolding is runnable:
1. Demo pipeline can be instantiated
2. Entrypoint runs without import errors
3. Artifacts directory can be written to
4. Logs are created
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest


def test_demo_pipeline_import():
    """Verify soion_option_demo package is importable."""
    from soion_option_demo import DemoPipeline
    from soion_option_demo import QCPlaceholder, MCPlaceholder, RegimePlaceholder
    from soion_option_demo import SDEPlaceholder, SurfacePlaceholder

    assert DemoPipeline is not None
    assert QCPlaceholder is not None
    assert MCPlaceholder is not None
    assert RegimePlaceholder is not None
    assert SDEPlaceholder is not None
    assert SurfacePlaceholder is not None


def test_demo_pipeline_run():
    """Verify DemoPipeline.run() executes and writes artifacts."""
    from soion_option_demo import DemoPipeline

    with tempfile.TemporaryDirectory() as tmpdir:
        artifacts_dir = Path(tmpdir) / "artifacts"

        pipeline = DemoPipeline(
            run_id="TEST001",
            symbol="BTC",
            interval="5m",
            mode="qc",
        )

        result = pipeline.run(artifacts_dir=artifacts_dir)

        assert result["status"] == "completed"
        assert result["run_id"] == "TEST001"
        assert result["qc_implemented"] is False

        # Check artifact was written
        summary_path = artifacts_dir / "TEST001_summary.json"
        assert summary_path.exists()

        with open(summary_path) as f:
            summary = json.load(f)
        assert summary["run_id"] == "TEST001"


def test_qc_placeholder():
    """Verify QC placeholder runs."""
    from soion_option_demo import QCPlaceholder

    qc = QCPlaceholder()
    result = qc.run(data=None)

    assert result["implemented"] is False
    assert result["checks_run"] == 0


def test_entrypoint_help():
    """Verify entrypoint runs with --help."""
    result = subprocess.run(
        [sys.executable, "-m", "apps.run_option_qc", "--help"],
        capture_output=True,
        text=True,
        cwd=str(Path(__file__).parent.parent / "src"),
    )
    assert result.returncode == 0
    assert "run_id" in result.stdout


def test_entrypoint_runs():
    """Verify entrypoint runs a complete pipeline."""
    with tempfile.TemporaryDirectory() as tmpdir:
        artifacts_dir = Path(tmpdir) / "artifacts"

        result = subprocess.run(
            [
                sys.executable, "-m", "apps.run_option_qc",
                "--run_id", "SMOKE_TEST",
                "--symbol", "BTC",
                "--interval", "5m",
                "--mode", "qc",
                "--artifacts-dir", str(artifacts_dir),
            ],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent.parent / "src"),
        )

        # Print output for debugging if test fails
        if result.returncode != 0:
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")

        assert result.returncode == 0
        assert "[OK]" in result.stdout or "completed" in result.stdout.lower()

        # Check artifacts created
        status_path = artifacts_dir / "SMOKE_TEST_status.json"
        assert status_path.exists()

        with open(status_path) as f:
            status = json.load(f)
        assert status["status"] == "success"

        # Check log file created
        log_path = artifacts_dir / "SMOKE_TEST.log"
        assert log_path.exists()


def test_frozen_modules_importable():
    """Verify frozen option_chain and iv modules are importable."""
    from quant_engine.data.derivatives.option_chain import (
        OptionChainDataHandler,
        OptionChainSnapshot,
        OptionChainCache,
    )
    from quant_engine.data.derivatives.iv import (
        IVSurfaceDataHandler,
        IVSurfaceSnapshot,
        IVSurfaceCache,
    )

    assert OptionChainDataHandler is not None
    assert OptionChainSnapshot is not None
    assert OptionChainCache is not None
    assert IVSurfaceDataHandler is not None
    assert IVSurfaceSnapshot is not None
    assert IVSurfaceCache is not None


def test_ingestion_contracts_importable():
    """Verify ingestion contracts are importable."""
    from ingestion.contracts import IngestionTick, normalize_tick
    from ingestion.option_chain import OptionChainFileSource

    assert IngestionTick is not None
    assert normalize_tick is not None
    assert OptionChainFileSource is not None
