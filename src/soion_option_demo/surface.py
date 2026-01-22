# DEMO-ONLY MODULE
# Intended final location: src/quant_engine/data/derivatives/iv/surface_builder.py
"""
IV Surface Placeholder Module.

This module will contain IV surface construction and interpolation.
Currently scaffolding only - no implementation.
"""

from __future__ import annotations

from typing import Any

from quant_engine.utils.logger import get_logger, log_debug


class SurfacePlaceholder:
    """Placeholder for IV surface construction.

    Future implementation will include:
    - Strike/moneyness interpolation
    - Term structure interpolation
    - Arbitrage-free smoothing
    - SSVI/SABR parametric fits
    """

    def __init__(self, *, config: dict[str, Any] | None = None):
        self.config = config or {}
        self._logger = get_logger(__name__)

    def build(self, *, chain_data: Any) -> dict[str, Any]:
        """Build IV surface from option chain data.

        Returns surface parameters dict.
        """
        log_debug(self._logger, "SurfacePlaceholder.build called (no-op)")

        return {
            "surface_version": "placeholder",
            "atm_iv": None,
            "skew": None,
            "term_structure": None,
            "implemented": False,
        }

    def interpolate(self, *, strike: float, expiry: float) -> float | None:
        """Interpolate IV at given strike and expiry.

        Returns interpolated IV or None if not implemented.
        """
        log_debug(
            self._logger,
            "SurfacePlaceholder.interpolate called (no-op)",
            strike=strike,
            expiry=expiry,
        )
        return None
