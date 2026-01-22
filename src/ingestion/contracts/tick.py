from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Literal, cast, get_args

Domain = Literal[
    "ohlcv",
    "orderbook",
    "trades",
    "option_chain",
    "option_trades",
    "iv_surface",
    "sentiment",
]

_DOMAIN_ALIASES: dict[str, str] = {
    "trade": "trades",
    "option_trade": "option_trades",
}
_ALLOWED_DOMAINS: set[str] = set(get_args(Domain))


def _normalize_domain(domain: Domain | str) -> Domain:
    if isinstance(domain, str) and domain in _DOMAIN_ALIASES:
        domain = _DOMAIN_ALIASES[domain]
    if domain not in _ALLOWED_DOMAINS:
        raise ValueError(f"Invalid domain: {domain!r}. Expected one of: {sorted(_ALLOWED_DOMAINS)}")
    return cast(Domain, domain)


def _normalize_source_id(source_id: Any | None) -> str | None:
    if source_id is None:
        return None
    if isinstance(source_id, str) and not source_id.strip():
        return None
    try:
        return str(source_id)
    except Exception:
        return None


def resolve_source_id(source: Any | None, *, override: Any | None = None) -> str | None:
    if override is not None:
        return _normalize_source_id(override)
    if source is None:
        return None
    for attr in ("source_id", "_source_id", "root", "_root", "base_url", "url", "venue", "provider", "name"):
        if hasattr(source, attr):
            value = getattr(source, attr)
            if value is not None:
                if isinstance(value, Path):
                    parts = value.parts
                    for marker in ("cleaned", "raw"):
                        if marker in parts:
                            idx = parts.index(marker)
                            if idx > 0:
                                return _normalize_source_id(Path(*parts[:idx]))
                    return _normalize_source_id(value)
                return _normalize_source_id(value)
    return None


@dataclass(frozen=True)
class IngestionTick:
    """
    Canonical ingestion tick.

    This is the ONLY object allowed to cross the boundary:
        Ingestion -> Driver -> Engine -> DataHandler

    Semantics:
        - `data_ts`        : event timestamp from source / logical event time (epoch ms int)
        - `timestamp`      : ingestion arrival timestamp (epoch ms int)
        - `domain`         : data domain identifier (e.g. 'ohlcv', 'orderbook')
        - `symbol`         : instrument symbol (e.g. 'BTCUSDT')
        - `payload`        : normalized domain-specific data
        - `source_id`      : optional source identifier (e.g. file root or venue)

    Aliases:
        - event_ts   == data_ts
        - arrival_ts == timestamp
    """

    timestamp: int # ingestion timestamp (epoch ms int)
    data_ts: int # event timestamp (epoch ms int)
    domain: Domain
    symbol: str
    payload: Mapping[str, Any]
    source_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "domain", _normalize_domain(self.domain))
        if not isinstance(self.symbol, str):
            object.__setattr__(self, "symbol", str(self.symbol))
        object.__setattr__(self, "source_id", _normalize_source_id(self.source_id))

    @property
    def event_ts(self) -> int:
        """Event/logical timestamp from source (epoch ms)."""
        return self.data_ts

    @property
    def arrival_ts(self) -> int:
        """Ingestion arrival/observe timestamp (epoch ms)."""
        return self.timestamp

def _to_interval_ms(interval: str) -> int | None:
    """Parse interval strings like '250ms', '1s', '1m', '1h', '1d', '1w' into milliseconds."""
    if not isinstance(interval, str) or not interval:
        return None
    s = interval.strip().lower()
    try:
        if s.endswith("ms"):
            return int(s[:-2])
        if s.endswith("s"):
            return int(s[:-1]) * 1000
        if s.endswith("m"):
            return int(s[:-1]) * 60_000
        if s.endswith("h"):
            return int(s[:-1]) * 3_600_000
        if s.endswith("d"):
            return int(s[:-1]) * 86_400_000
        if s.endswith("w"):
            return int(s[:-1]) * 7 * 86_400_000
    except Exception:
        return None
    return None


def _guard_interval_ms(interval: str | None, interval_ms: int | None) -> None:
    """Guard against accidental unit division when interval strings are used."""
    if interval is None or interval_ms is None:
        return
    s = interval.strip().lower()
    if not s.endswith("ms") and interval_ms < 1000:
        raise ValueError(
            f"Interval {interval!r} parsed to {interval_ms}ms; "
            "expected milliseconds (e.g., '1m' -> 60000)."
        )


def _coerce_epoch_ms(x: Any) -> int:
    """Coerce seconds-or-ms epoch into epoch milliseconds int.

    Heuristic: seconds are ~1e9, ms are ~1e12.
    """
    if x is None:
        raise ValueError("timestamp cannot be None")
    # bool is an int subclass; reject it
    if isinstance(x, bool):
        raise ValueError("invalid timestamp type: bool")
    if isinstance(x, (int, float)):
        v = float(x)
    else:
        try:
            v = float(x)  # strings, numpy scalars
        except Exception as e:
            raise ValueError(f"invalid timestamp: {x!r}") from e

    if v < 10_000_000_000:  # seconds
        return int(round(v * 1000.0))
    return int(round(v))


def normalize_tick(
    *,
    timestamp: Any,
    domain: Domain,
    symbol: str,
    payload: Mapping[str, Any],
    data_ts: Any | None = None,
    source_id: Any | None = None,
) -> IngestionTick:
    """
    Normalize raw ingestion output into a canonical IngestionTick.

    Rules:
        - timestamp is ALWAYS provided by ingestion controller
        - data_ts defaults to timestamp if source timestamp is missing
        - no mutation, no enrichment, no inference
    """
    arrival_ts = _coerce_epoch_ms(timestamp)
    event_ts = _coerce_epoch_ms(data_ts) if data_ts is not None else arrival_ts

    return IngestionTick(
        timestamp=arrival_ts,
        data_ts=event_ts,
        domain=_normalize_domain(domain),
        symbol=str(symbol),
        payload=payload,
        source_id=_normalize_source_id(source_id),
    )
