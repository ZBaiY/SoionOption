from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import json
from typing import Any, Mapping


@dataclass(frozen=True)
class MarketCalendar:
    calendar: str
    timezone: str
    session: str
    open_days: list[int] | None = None
    open_time: str | None = None
    close_time: str | None = None


def _load_calendar_config() -> dict[str, Any]:
    path = Path(__file__).with_name("calendar.json")
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _parse_hhmm(value: str) -> tuple[int, int]:
    parts = value.split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid time format: {value!r}")
    return int(parts[0]), int(parts[1])


def _resolve_calendar(calendar_name: str, config: dict[str, Any]) -> MarketCalendar:
    calendars = config.get("calendars") or {}
    if not isinstance(calendars, dict):
        calendars = {}
    entry = calendars.get(calendar_name) or {}
    if not isinstance(entry, dict):
        entry = {}
    return MarketCalendar(
        calendar=calendar_name,
        timezone=str(entry.get("timezone", "UTC")),
        session=str(entry.get("session", "regular")),
        open_days=entry.get("open_days"),
        open_time=entry.get("open_time"),
        close_time=entry.get("close_time"),
    )


def _is_market_open(ts_ms: int, cal: MarketCalendar) -> bool:
    if cal.calendar == "24x7":
        return True
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    if cal.open_days is not None and dt.weekday() not in set(int(d) for d in cal.open_days):
        return False
    if cal.open_time and cal.close_time:
        open_h, open_m = _parse_hhmm(cal.open_time)
        close_h, close_m = _parse_hhmm(cal.close_time)
        open_minutes = open_h * 60 + open_m
        close_minutes = close_h * 60 + close_m
        now_minutes = dt.hour * 60 + dt.minute
        return open_minutes <= now_minutes < close_minutes
    return True


def resolve_market_spec(
    *,
    symbol: str,
    venue: str | None,
    asset_class: str | None,
    currency: str | None,
    event_ts: int | None,
    calendar: str | None = None,
    session: str | None = None,
    timezone_name: str | None = None,
) -> tuple[dict[str, Any], str | None]:
    cfg = _load_calendar_config()

    symbols = cfg.get("symbols") or {}
    symbol_cfg = symbols.get(symbol) if isinstance(symbols, dict) else None
    if not isinstance(symbol_cfg, dict):
        symbol_cfg = {}

    venues = cfg.get("venues") or {}
    venue_cfg = venues.get(venue) if isinstance(venues, dict) else None
    if not isinstance(venue_cfg, dict):
        venue_cfg = {}

    asset_classes = cfg.get("asset_classes") or {}
    asset_cfg = asset_classes.get(asset_class) if isinstance(asset_classes, dict) else None
    if not isinstance(asset_cfg, dict):
        asset_cfg = {}

    cal_name = (
        calendar
        or symbol_cfg.get("calendar")
        or venue_cfg.get("calendar")
        or asset_cfg.get("calendar")
        or "24x7"
    )
    session_name = (
        session
        or symbol_cfg.get("session")
        or venue_cfg.get("session")
        or asset_cfg.get("session")
        or "24x7"
    )
    tz_name = (
        timezone_name
        or symbol_cfg.get("timezone")
        or venue_cfg.get("timezone")
        or asset_cfg.get("timezone")
        or "UTC"
    )

    cal = _resolve_calendar(str(cal_name), cfg)
    cal = MarketCalendar(
        calendar=cal.calendar,
        timezone=tz_name,
        session=session_name,
        open_days=cal.open_days,
        open_time=cal.open_time,
        close_time=cal.close_time,
    )

    status = None
    if event_ts is not None:
        status = "open" if _is_market_open(int(event_ts), cal) else "closed"

    market = {
        "venue": str(venue or symbol_cfg.get("venue") or "unknown"),
        "asset_class": str(asset_class or venue_cfg.get("asset_class") or asset_cfg.get("asset_class") or "unknown"),
        "timezone": cal.timezone,
        "calendar": cal.calendar,
        "session": cal.session,
        "currency": currency,
        "schema_version": 2,
    }
    return market, status


def annotate_payload_market(
    payload: Mapping[str, Any],
    *,
    symbol: str,
    venue: str | None,
    asset_class: str | None,
    currency: str | None,
    event_ts: int | None,
    calendar: str | None = None,
    session: str | None = None,
    timezone_name: str | None = None,
) -> dict[str, Any]:
    out = dict(payload)
    market, status = resolve_market_spec(
        symbol=symbol,
        venue=venue,
        asset_class=asset_class,
        currency=currency,
        event_ts=event_ts,
        calendar=calendar,
        session=session,
        timezone_name=timezone_name,
    )
    if status is not None:
        market = dict(market)
        market["status"] = status
    out["market"] = market
    return out
