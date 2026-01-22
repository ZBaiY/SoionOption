from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import json
from typing import Protocol, Mapping, Any, runtime_checkable, Optional


@runtime_checkable
class Snapshot(Protocol):
    """
    Immutable runtime snapshot contract.

    A Snapshot represents a frozen view of domain data at engine clock `timestamp` (epoch ms),
    derived from underlying data timestamp `data_ts` (epoch ms).

    Invariants:
    - timestamp >= data_ts (anti-lookahead)
    - latency == timestamp - data_ts (milliseconds)
    - to_dict() returns pure-python serializable objects
    - schema_version is used for tolerant evolution of snapshot fields
    - market encodes timezone/calendar/session semantics (no implicit 24/7 assumptions)
    """

    # --- timing ---
    # timestamp: int      # engine clock timestamp (epoch ms)
    symbol: str         # associated symbol
    market: MarketSpec  # market/session/calendar semantics for this snapshot

    data_ts: int        # data-origin timestamp (epoch ms)
    # latency: int        # timestamp - data_ts (ms)
    # --- identity ---
    domain: str        # "ohlcv" | "orderbook" | "option_chain" | "iv_surface" | ...
    schema_version: int  # for forward/backward compatibility

    def to_dict(self) -> Mapping[str, Any]:
        """
        Return a pure-python, serialization-safe representation.

        Must NOT return pandas / numpy objects.
        Must NOT expose internal mutable references.
        """
        ...

    def get_attr(self, key: str) -> Any:
        if not hasattr(self, key):
            raise AttributeError(f"{type(self).__name__} has no attribute {key!r}")
        return getattr(self, key)

@runtime_checkable
class MarketSpec(Protocol):
    """Market / instrument environment contract.

    This is the minimal set of market semantics needed to make data quality,
    sessions, and calendar-aware gap logic explicit (instead of crypto-only
    implicit assumptions).

    Notes:
      - Keep this small and stable; extend via optional fields or schema_versioned dicts.
      - For equities/futures/options, richer specs (corporate actions, rolls, chains)
        should live in dedicated domain contracts, referenced from snapshots.
    """

    venue: str                 # e.g. "binance", "nasdaq", "cme"
    asset_class: str           # e.g. "crypto", "equity", "future", "option"
    timezone: str              # IANA tz, e.g. "UTC", "America/New_York"
    calendar: str              # calendar identifier, e.g. "24x7", "XNYS"
    session: str               # session regime, e.g. "regular", "extended", "24x7"

    # Optional knobs (do NOT rely on these being present everywhere)
    currency: Optional[str]    # quote currency if known
    schema_version: int        # for tolerant evolution

    def to_dict(self) -> Mapping[str, Any]:
        ...


class GapType(str, Enum):
    EXPECTED_CLOSED = "expected_closed"
    HALT = "halt"
    DATA_MISSING = "data_missing"
    NO_ACTIVITY = "no_activity"


@dataclass(frozen=True)
class MarketInfo(MarketSpec):
    venue: str
    asset_class: str
    timezone: str
    calendar: str
    session: str
    currency: Optional[str] = None
    schema_version: int = 2
    status: Optional[str] = None
    gap_type: Optional[str] = None

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "venue": self.venue,
            "asset_class": self.asset_class,
            "timezone": self.timezone,
            "calendar": self.calendar,
            "session": self.session,
            "currency": self.currency,
            "schema_version": self.schema_version,
            "status": self.status,
            "gap_type": self.gap_type,
        }


def _coerce_gap_type(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, GapType):
        return value.value
    return str(value)


_DEFAULT_GAP_FEATURES: dict[str, Any] = {
    "gap_threshold_multiplier": 1.5,
    "status_aliases": {
        "expected_closed": ["closed", "outside_market", "out_of_market", "off_market"],
        "halt": ["halt", "halted", "paused"],
        "no_activity": ["no_activity", "inactive", "no_trade", "empty"],
    },
}


def load_gap_features() -> dict[str, Any]:
    path = Path(__file__).with_name("gap_features.json")
    if not path.exists():
        return dict(_DEFAULT_GAP_FEATURES)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return dict(_DEFAULT_GAP_FEATURES)
    if not isinstance(data, dict):
        return dict(_DEFAULT_GAP_FEATURES)
    merged = dict(_DEFAULT_GAP_FEATURES)
    merged.update(data)
    return merged


def status_to_gap_type(status: Any) -> Optional[str]:
    if status is None:
        return None
    status_s = str(status).strip().lower()
    cfg = load_gap_features()
    aliases = cfg.get("status_aliases", {})
    if isinstance(aliases, dict):
        for gap_key, names in aliases.items():
            if isinstance(names, (list, tuple)) and status_s in {str(n).lower() for n in names}:
                return str(gap_key)
    return None


def classify_gap(
    *,
    status: Any | None,
    last_ts: int | None,
    data_ts: int | None,
    expected_interval_ms: int | None,
    min_gap_ms: int | None = None,
) -> Optional[str]:
    if last_ts is None or data_ts is None:
        return None
    if data_ts <= last_ts:
        return None

    cfg = load_gap_features()
    multiplier = cfg.get("gap_threshold_multiplier", 1.0)
    try:
        multiplier_f = float(multiplier)
    except Exception:
        multiplier_f = 1.0

    threshold = min_gap_ms
    if threshold is None and expected_interval_ms is not None:
        threshold = int(expected_interval_ms * multiplier_f)

    if threshold is not None and (data_ts - last_ts) < int(threshold):
        return None

    status_gap = status_to_gap_type(status)
    if status_gap is not None:
        return status_gap

    if threshold is not None:
        return GapType.DATA_MISSING.value
    return None

def ensure_market_spec(
    value: Any,
    *,
    default_venue: str = "unknown",
    default_asset_class: str = "unknown",
    default_timezone: str = "UTC",
    default_calendar: str = "24x7",
    default_session: str = "24x7",
    default_currency: Optional[str] = None,
    default_schema_version: int = 2,
) -> MarketInfo:
    if isinstance(value, MarketInfo):
        return value

    data: dict[str, Any] = {}
    if value is not None:
        if hasattr(value, "to_dict"):
            try:
                data = dict(value.to_dict())  # type: ignore[call-arg]
            except Exception:
                data = {}
        elif isinstance(value, dict):
            data = dict(value)

    venue = str(data.get("venue", default_venue))
    asset_class = str(data.get("asset_class", default_asset_class))
    timezone = str(data.get("timezone", default_timezone))
    calendar = str(data.get("calendar", default_calendar))
    session = str(data.get("session", default_session))
    currency = data.get("currency", default_currency)
    schema_version_any = data.get("schema_version", default_schema_version)
    try:
        schema_version = int(schema_version_any)
    except Exception:
        schema_version = default_schema_version

    status = data.get("status")
    gap_type = _coerce_gap_type(data.get("gap_type"))

    return MarketInfo(
        venue=venue,
        asset_class=asset_class,
        timezone=timezone,
        calendar=calendar,
        session=session,
        currency=currency,
        schema_version=schema_version,
        status=str(status) if status is not None else None,
        gap_type=gap_type,
    )


def merge_market_spec(
    base: MarketSpec | None,
    override: Any = None,
    *,
    status: Any | None = None,
    gap_type: Any | None = None,
) -> MarketInfo:
    base_data: dict[str, Any] = {}
    if base is not None and hasattr(base, "to_dict"):
        try:
            base_data = dict(base.to_dict())  # type: ignore[call-arg]
        except Exception:
            base_data = {}

    override_data: dict[str, Any] = {}
    if override is not None:
        if hasattr(override, "to_dict"):
            try:
                override_data = dict(override.to_dict())  # type: ignore[call-arg]
            except Exception:
                override_data = {}
        elif isinstance(override, dict):
            override_data = dict(override)

    data = {**base_data, **override_data}
    if status is not None:
        data["status"] = status
    if gap_type is not None:
        data["gap_type"] = gap_type

    return ensure_market_spec(
        data,
        default_venue=str(base_data.get("venue", "unknown")),
        default_asset_class=str(base_data.get("asset_class", "unknown")),
        default_timezone=str(base_data.get("timezone", "UTC")),
        default_calendar=str(base_data.get("calendar", "24x7")),
        default_session=str(base_data.get("session", "24x7")),
        default_currency=base_data.get("currency"),
        default_schema_version=int(base_data.get("schema_version", 2)),
    )
