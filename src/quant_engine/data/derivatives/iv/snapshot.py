from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Any, Dict

from quant_engine.data.contracts.snapshot import Snapshot, MarketSpec, ensure_market_spec, MarketInfo
from quant_engine.utils.num import to_float


def to_ms_int(x: Any) -> int:
    """Coerce a timestamp-like value to epoch milliseconds as int."""
    # Keep tolerant: accept int/float/np scalars; interpret numerically as epoch-ms.
    return int(to_float(x))


@dataclass(frozen=True)
class IVSurfaceSnapshot(Snapshot):
    """
    Immutable IV surface snapshot.

    Domain payload represents a fitted or extracted implied-volatility surface
    at a given engine timestamp, derived from market data at `data_ts`.
    """

    # --- common snapshot fields ---
    # timestamp: int
    data_ts: int
    # latency: int
    symbol: str
    market: MarketSpec
    domain: str
    schema_version: int

    # --- IV surface specific payload ---
    expiry: str | None
    model: str | None            # e.g. "SSVI", "SABR"
    atm_iv: float
    skew: float
    curve: Dict[str, float]      # moneyness -> implied volatility
    surface: Dict[str, Any]      # optional model parameters

    @classmethod
    def from_surface_aligned(
        cls,
        *,
        timestamp: int,
        data_ts: int,
        symbol: str,
        market: MarketSpec | None = None,
        atm_iv: float,
        skew: float,
        curve: Mapping[Any, Any],
        surface: Mapping[str, Any] | None = None,
        expiry: str | None = None,
        model: str | None = None,
        schema_version: int = 1,
    ) -> "IVSurfaceSnapshot":
        """
        Tolerant constructor from aligned IV surface quantities.

        All numeric values are normalized to float.
        """
        ts = to_ms_int(timestamp)
        dts = to_ms_int(data_ts)

        return cls(
            # timestamp=ts,
            data_ts=dts,
            # latency=ts - dts,
            symbol=symbol,
            market=ensure_market_spec(market),
            domain="iv_surface",
            schema_version=schema_version,
            expiry=expiry,
            model=model,
            atm_iv=to_float(atm_iv),
            skew=to_float(skew),
            curve={str(k): to_float(v) for k, v in curve.items()},
            surface=dict(surface) if surface is not None else {},
        )

    def to_dict(self) -> Dict[str, Any]:
        assert isinstance(self.market, MarketInfo)
        return {
            # "timestamp": self.timestamp,
            "data_ts": self.data_ts,
            # "latency": self.latency,
            "symbol": self.symbol,
            "market": self.market.to_dict(),
            "domain": self.domain,
            "schema_version": self.schema_version,
            "expiry": self.expiry,
            "model": self.model,
            "atm_iv": self.atm_iv,
            "skew": self.skew,
            "curve": self.curve,
            "surface": self.surface,
        }
    def get_attr(self, key: str) -> Any:
        if not hasattr(self, key):
            raise AttributeError(f"{type(self).__name__} has no attribute {key!r}")
        return getattr(self, key)