from __future__ import annotations

from typing import Any, Protocol, runtime_checkable, TYPE_CHECKING

from ingestion.contracts.tick import IngestionTick

# Contract layer must be lightweight.
# Runtime uses millisecond timestamps as int (epoch ms).
# If callers want pandas timestamps, keep it as type-only to avoid runtime dependency.
if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd
    TimestampLike = int | pd.Timestamp
else:
    TimestampLike = int

@runtime_checkable
class DataHandlerProto(Protocol):
    """Runtime-facing handler contract consumed by Engine/FeatureExtractor.

    Runtime timestamps are epoch milliseconds (int).

    Notes
    -----
    - Engine/Driver own lifecycle semantics; handlers just provide the methods.
    - Backtest seeding is done via `RealTimeDataHandler.from_historical(...)` + replay ticks.
    - Some handlers may implement extra methods (e.g., load_history) but Engine should not require them.
    """

    # -------- lifecycle --------

    def load_history(self, *, start_ts: int | None = None, end_ts: int | None = None) -> None:
        """BACKTEST: optional historical preload hook (may be no-op)."""
        ...

    def bootstrap(self, *, anchor_ts: int | None = None, lookback: Any | None = None) -> None:
        """REALTIME/MOCK: preload recent data into cache."""
        ...

    def warmup_to(self, ts: int) -> None:
        """Align internal state for warmup (alias of align_to)."""
        ...

    def align_to(self, ts: int) -> None:
        """Align internal cursor/cache to ts. Must be idempotent."""
        ...

    # -------- runtime --------

    def last_timestamp(self) -> int | None:
        """Return latest available timestamp; None if empty."""
        ...

    def get_snapshot(self, ts: int | None = None) -> Any:
        """Return latest snapshot (dict/dataclass/TypedDict). Must be serializable."""
        ...

    def window(self, ts: int | None = None, n: int = 1) -> Any:
        """Return last n snapshots up to ts (or latest if ts is None)."""
        ...


@runtime_checkable
class RealTimeDataHandler(DataHandlerProto, Protocol):
    """Runtime data handler contract.

    Design intent:
      - Runtime handler is what Engine consumes in ALL modes.
      - Data origin (live / replay) is external to the handler.
      - Handler exposes a single ingestion entrypoint: on_new_tick().
    """
    bootstrap_cfg: dict[str, Any]

    def on_new_tick(self, tick: IngestionTick) -> None:
        """Push one new tick/bar into the runtime cache (live or replay)."""
        ...

@runtime_checkable
class OHLCVHandlerProto(RealTimeDataHandler, Protocol):
    interval: str
    interval_ms: int
    symbol: str

    def window_df(self, window: int | None = None) -> Any:
        """Optional OHLCV-specific window accessor for feature warmup."""
        ...


def to_interval_ms(interval: str | None) -> int | None:
    """Parse common interval strings into milliseconds.

    Examples: "100ms", "1s", "15m", "1h", "1d".
    """
    if interval is None:
        return None

    unit_multipliers_ms = {
        "ms": 1,
        "s": 1_000,
        "m": 60_000,
        "h": 3_600_000,
        "d": 86_400_000,
    }

    s = interval.strip().lower()
    try:
        if s.endswith("ms"):
            value = int(s[:-2])
            return value * unit_multipliers_ms["ms"]
        unit = s[-1]
        value = int(s[:-1])
        return value * unit_multipliers_ms[unit]
    except (ValueError, KeyError):
        return None  # Unknown format

def to_float_interval(interval: str | None) -> float | None:
    """Legacy helper: parse interval into seconds as float.

    Prefer `to_interval_ms()` for runtime arithmetic with epoch-ms timestamps.
    """
    ms = to_interval_ms(interval)
    return None if ms is None else (ms / 1000.0)
