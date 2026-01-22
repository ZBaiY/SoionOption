# DEMO-ONLY MODULE
# Intended final location: src/quant_engine/decision/regime/option_regime.py
"""
Regime Detection Placeholder Module.

This module will contain market regime detection logic.
Currently scaffolding only - no implementation.
"""

from __future__ import annotations

from typing import Any

from quant_engine.utils.logger import get_logger, log_debug


class RegimePlaceholder:
    """Placeholder for Regime detection logic.

    Future implementation will include:
    - Volatility regime classification (low/medium/high)
    - Trend detection
    - Mean-reversion vs momentum regimes
    - IV term structure regime (contango/backwardation)
    """

    def __init__(self, *, config: dict[str, Any] | None = None):
        self.config = config or {}
        self._logger = get_logger(__name__)

    def detect(self, *, data: Any) -> dict[str, Any]:
        """Detect current market regime.

        Returns regime classification dict.
        """
        log_debug(self._logger, "RegimePlaceholder.detect called (no-op)")

        return {
            "regime_version": "placeholder",
            "regime": "unknown",
            "confidence": 0.0,
            "implemented": False,
        }
