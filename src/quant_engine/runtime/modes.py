from __future__ import annotations

from datetime import tzinfo
from enum import Enum
from dataclasses import dataclass
from typing import Any

from quant_engine.data.contracts.protocol_realtime import to_interval_ms


class EngineMode(Enum):
    """
    Runtime execution mode.

    This enum is used by Driver / Runtime for logging, guards,
    and artifact routing. It must NOT be used to branch strategy logic.
    """

    REALTIME = "realtime"
    BACKTEST = "backtest"
    MOCK = "mock"
    SAMPLE = "sample"


@dataclass(frozen=True)
class EngineSpec:
    """
    Strategy observation semantics.

    EngineSpec describes *how the strategy observes time*,
    not how data is ingested.

    Responsibilities:
      - Define the strategy-level observation interval.
      - Provide a deterministic clock advancement rule via `advance(ts)`.
    """

    mode: EngineMode
    interval: str          # e.g. "1m", "5m"
    interval_ms: int       # e.g. 60_000, 300_000
    symbol: str
    timestamp: int | None = None  # epoch ms
    universe: dict[str, Any] | None = None
    timezone: tzinfo | None = None ## optional timezone for multi-timezone strategies in the future

    @classmethod
    def from_interval(
        cls,
        *,
        mode: EngineMode,
        interval: str,
        symbol: str,
        timestamp: int | None = None,
        universe: dict[str, Any] | None = None,
        timezone: tzinfo | None = None,
    ) -> "EngineSpec":
        ms = to_interval_ms(interval)
        if ms is None:
            raise ValueError(f"Invalid interval format: {interval}")
        return cls(
            mode=mode,
            interval=interval,
            interval_ms=int(ms),
            symbol=symbol,
            timestamp=timestamp,
            universe=universe,
            timezone=timezone,
        )

    def advance(self, ts: int) -> int:
        """Deterministically advance the strategy observation clock (epoch ms)."""
        return int(ts) + int(self.interval_ms)
