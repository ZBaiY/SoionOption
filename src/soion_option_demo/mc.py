# DEMO-ONLY MODULE
# Intended final location: src/quant_engine/models/simulation/monte_carlo.py
"""
Monte Carlo (MC) Placeholder Module.

This module will contain Monte Carlo simulation logic.
Currently scaffolding only - no implementation.
"""

from __future__ import annotations

from typing import Any

from quant_engine.utils.logger import get_logger, log_debug


class MCPlaceholder:
    """Placeholder for Monte Carlo simulation logic.

    Future implementation will include:
    - IV surface sampling
    - Path generation
    - Greeks computation via finite differences
    - Risk metrics (VaR, CVaR)
    """

    def __init__(self, *, config: dict[str, Any] | None = None):
        self.config = config or {}
        self._logger = get_logger(__name__)

    def run(self, *, surface: Any) -> dict[str, Any]:
        """Run Monte Carlo simulation.

        Returns simulation results dict.
        """
        log_debug(self._logger, "MCPlaceholder.run called (no-op)")

        return {
            "mc_version": "placeholder",
            "paths_generated": 0,
            "implemented": False,
        }
