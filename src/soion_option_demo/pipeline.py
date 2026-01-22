# DEMO-ONLY MODULE
# Intended final location: TBD (orchestration logic will be absorbed into SoionLab runtime/driver layer)
"""
Demo Pipeline - Minimal scaffolding for option QC workflow.

This module orchestrates the demo pipeline components.
All processing logic is placeholder only.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from quant_engine.utils.logger import get_logger, log_info, log_debug


class DemoPipeline:
    """Minimal demo pipeline orchestrator.

    This is scaffolding only - no actual QC/MC/Regime logic implemented.
    """

    def __init__(
        self,
        *,
        run_id: str,
        symbol: str,
        interval: str,
        mode: str = "qc",
        config: dict[str, Any] | None = None,
    ):
        self.run_id = run_id
        self.symbol = symbol
        self.interval = interval
        self.mode = mode
        self.config = config or {}
        self._logger = get_logger(__name__)

    def run(self, *, artifacts_dir: Path) -> dict[str, Any]:
        """Execute the demo pipeline.

        Returns summary dict with run metadata.
        """
        log_info(
            self._logger,
            "DemoPipeline.run started",
            run_id=self.run_id,
            symbol=self.symbol,
            interval=self.interval,
            mode=self.mode,
        )

        # Placeholder: write a simple artifact to show pipeline ran
        summary = {
            "run_id": self.run_id,
            "symbol": self.symbol,
            "interval": self.interval,
            "mode": self.mode,
            "status": "completed",
            "pipeline_version": "0.0.1-scaffold",
            "qc_implemented": False,
            "mc_implemented": False,
            "regime_implemented": False,
        }

        # Write summary artifact
        summary_path = artifacts_dir / f"{self.run_id}_summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)

        log_info(
            self._logger,
            "DemoPipeline.run completed",
            run_id=self.run_id,
            artifact_path=str(summary_path),
        )

        return summary
