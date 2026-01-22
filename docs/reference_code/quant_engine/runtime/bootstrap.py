from __future__ import annotations

from quant_engine.contracts.engine import StrategyEngineProto


def run_bootstrap(
    *,
    engine: StrategyEngineProto,
    anchor_ts: int | None = None,
) -> None:
    """
    Execute the data bootstrap / preload phase.
    NOTE:
      - This is a thin wrapper around StrategyEngine.bootstrap().
      - It intentionally contains no logic of its own.
      - anchor_ts is epoch milliseconds (int) when provided.
    """
    engine.bootstrap(anchor_ts=anchor_ts)
