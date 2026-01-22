from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List
from collections.abc import Mapping

from quant_engine.runtime.modes import EngineMode
from quant_engine.contracts.portfolio import PortfolioState
from quant_engine.utils.guards import ensure_epoch_ms

SCHEMA_VERSION = 2


@dataclass(frozen=True)
class EngineSnapshot:
    """
    Immutable snapshot of a single engine step.
    """
    timestamp: int  # epoch ms
    mode: EngineMode

    # --- strategy perception ---
    features: Dict[str, Any]
    model_outputs: Dict[str, Any]

    # --- decision & execution ---
    decision_score: Any
    target_position: Any
    fills: List[Dict]

    # --- market & accounting ---
    portfolio: PortfolioState

    def __init__(
        self,
        timestamp: int,
        mode: EngineMode,
        features: Dict[str, Any],
        model_outputs: Dict[str, Any],
        decision_score: Any,
        target_position: Any,
        fills: List[Dict],
        portfolio: PortfolioState,
    ):
        ts = ensure_epoch_ms(timestamp)
        features_out = dict(features) if isinstance(features, Mapping) else {"features": features}
        model_outputs_out = dict(model_outputs) if isinstance(model_outputs, Mapping) else {"model_outputs": model_outputs}
        fills_out = list(fills)
        if isinstance(portfolio, PortfolioState):
            portfolio_out = PortfolioState(dict(portfolio.to_dict()))
        else:
            portfolio_out = portfolio
        object.__setattr__(self, "timestamp", int(ts))
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "features", features_out)
        object.__setattr__(self, "model_outputs", model_outputs_out)
        object.__setattr__(self, "decision_score", decision_score)
        object.__setattr__(self, "target_position", target_position)
        object.__setattr__(self, "fills", fills_out)
        object.__setattr__(self, "portfolio", portfolio_out)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": int(self.timestamp),
            "mode": self.mode.value,
            "features": self.features,
            "model_outputs": self.model_outputs,
            "decision_score": self.decision_score,
            "target_position": self.target_position,
            "fills": self.fills,
            "portfolio": self.portfolio.to_dict(),
        }
