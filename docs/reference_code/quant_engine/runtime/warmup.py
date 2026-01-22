from __future__ import annotations

from quant_engine.contracts.engine import StrategyEngineProto


def run_warmup(
    *,
    engine: StrategyEngineProto,
    anchor_ts: int | None = None,
) -> None:
    """
    Execute the feature/model warmup phase.

    Semantics:
      - Runtime-level orchestration only.
      - Assumes preload_data() has already been executed.
      - Does NOT ingest data.
      - Does NOT advance time.
      - anchor_ts is epoch milliseconds (int) when provided.
      - Delegates all logic to StrategyEngine.

    This function exists to:
      - Make runtime lifecycle explicit.
      - Centralize warmup invocation semantics.
      - Improve readability of Driver code.
    """
    engine.warmup_features(anchor_ts=anchor_ts)
