from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

import datetime as dt
import re

import pandas as pd

from quant_engine.data.contracts.snapshot import Snapshot, MarketInfo, MarketSpec, ensure_market_spec
from quant_engine.utils.num import to_float


def to_ms_int(x: Any) -> int:
    """Coerce a timestamp-like value to epoch milliseconds as int."""
    return int(to_float(x))


# --- Deribit helpers (temporary fallback) ---
# Instrument example: BTC-28JUN24-60000-C
_DERIBIT_OPT_RE = re.compile(
    r"^(?P<underlying>[A-Z]+)-(?P<date>\d{2}[A-Z]{3}\d{2})-(?P<strike>[0-9.]+)-(?P<type>[CP])$"
)


def _parse_deribit_expiry_ts_ms(instrument_name: str) -> int:
    """Best-effort Deribit expiry timestamp (epoch ms, UTC).

    Prefer exchange-provided `expiration_timestamp` when available.
    This is only a fallback.
    """
    m = _DERIBIT_OPT_RE.match(instrument_name)
    if not m:
        raise ValueError(f"Unrecognized Deribit instrument_name: {instrument_name}")
    expiry_date = dt.datetime.strptime(m.group("date"), "%d%b%y").date()
    expiry_dt = dt.datetime(
        expiry_date.year,
        expiry_date.month,
        expiry_date.day,
        8,
        0,
        0,
        0,
        tzinfo=dt.timezone.utc,
    )
    return int(expiry_dt.timestamp() * 1000)


def _parse_deribit_cp(instrument_name: str) -> str | None:
    m = _DERIBIT_OPT_RE.match(instrument_name)
    if not m:
        return None
    t = m.group("type")
    return "C" if t == "C" else ("P" if t == "P" else None)


def _coerce_cp(x: Any) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    u = s.upper()
    if u in {"C", "CALL"}:
        return "C"
    if u in {"P", "PUT"}:
        return "P"
    if u.startswith("CALL"):
        return "C"
    if u.startswith("PUT"):
        return "P"
    return None


# Any IV-ish column is treated as venue-fetched and kept only in aux.
_IV_KEY_RE = re.compile(r"^(iv)$|(_iv$)|(^iv_)|(_iv_)", re.IGNORECASE)


def _is_iv_col(c: str) -> bool:
    return bool(_IV_KEY_RE.search(c))


_GREEK_COLS = {
    "delta",
    "gamma",
    "vega",
    "theta",
    "rho",
    "vanna",
    "vomma",
    "volga",
    "charm",
    "speed",
    "zomma",
    "color",
}


# Minimal, IV-surface-relevant columns we keep in the main frame.
# Everything else goes into `aux` per row.
_SURFACE_KEEP_COLS = {
    "instrument_name",
    "expiry_ts",
    "strike",
    "cp",
    # optional: if you later join quotes/marks into the same frame
    "bid",
    "ask",
    "mid",
    "mark",
    "mark_price",
    "index_price",
    "underlying_price",
    "forward_price",
    "oi",
    "open_interest",
    "volume",
    # aux holder
    "aux",
}


@dataclass(frozen=True)
class OptionChainSnapshot(Snapshot):
    """Immutable option chain snapshot.

    Schema v2:
      - the chain payload is stored as a pandas DataFrame (`frame`).
      - the frame is *lean*: only IV-surface relevant columns are kept.
      - all other incoming columns are moved into a per-row dict column `aux`.
      - any fetched IV/greeks columns are treated as non-canonical and moved into aux as `*_fetch`.

    Canonical IV lives in iv_handler; greeks are computed in features.
    """

    data_ts: int
    symbol: str
    market: MarketSpec
    domain: str
    schema_version: int

    # normalized chain table
    frame: pd.DataFrame

    @staticmethod
    def _normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["instrument_name", "expiry_ts", "strike", "cp", "aux"])

        x = df.copy()

        # Map Deribit instrument metadata: expiration_timestamp -> expiry_ts
        if "expiry_ts" not in x.columns and "expiration_timestamp" in x.columns:
            x["expiry_ts"] = x["expiration_timestamp"]

        # Coerce expiry_ts (ms-int)
        if "expiry_ts" in x.columns:
            x["expiry_ts"] = pd.to_numeric(x["expiry_ts"], errors="coerce").fillna(0).astype("int64")

        # Coerce strike
        if "strike" in x.columns:
            x["strike"] = pd.to_numeric(x["strike"], errors="coerce")

        # Derive cp: option_type -> cp, else instrument_name fallback
        if "cp" not in x.columns:
            if "option_type" in x.columns:
                x["cp"] = x["option_type"].map(_coerce_cp)
            else:
                x["cp"] = None

        if "instrument_name" in x.columns:
            miss = x["cp"].isna() | (x["cp"].astype("string") == "")
            if bool(miss.any()):
                x.loc[miss, "cp"] = x.loc[miss, "instrument_name"].map(
                    lambda s: _parse_deribit_cp(str(s)) if s is not None else None
                )

        # Final fallback expiry_ts from instrument_name if still missing/zero
        if "expiry_ts" in x.columns and "instrument_name" in x.columns:
            miss_exp = (x["expiry_ts"].isna()) | (x["expiry_ts"].astype("int64") <= 0)
            if bool(miss_exp.any()):
                def _fallback_exp(v: Any) -> int | None:
                    try:
                        return _parse_deribit_expiry_ts_ms(str(v))
                    except Exception:
                        return None

                x.loc[miss_exp, "expiry_ts"] = x.loc[miss_exp, "instrument_name"].map(_fallback_exp)
                x["expiry_ts"] = pd.to_numeric(x["expiry_ts"], errors="coerce").fillna(0).astype("int64")

        # Ensure aux exists
        if "aux" not in x.columns:
            x["aux"] = [{} for _ in range(len(x))]
        else:
            # normalize aux values to dict
            x["aux"] = x["aux"].map(lambda v: dict(v) if isinstance(v, dict) else {})

        # Move fetched IV/greeks columns into aux as *_fetch (non-canonical)
        cols = list(x.columns)
        for c in cols:
            if not isinstance(c, str):
                continue
            lc = c.lower()
            if lc == "aux":
                continue
            if lc.endswith("_fetch") or _is_iv_col(c) or lc in _GREEK_COLS:
                def _move(v: Any, col: str = c) -> None:
                    # handled below via apply
                    return None

                # vectorized-ish: apply per row
                x["aux"] = x.apply(
                    lambda row, col=c: {**(row["aux"] if isinstance(row["aux"], dict) else {}), f"{col}_fetch" if not str(col).lower().endswith("_fetch") else str(col): row[col]},
                    axis=1,
                )
                x = x.drop(columns=[c])

        # Now move all non-surface columns into aux
        keep = set(_SURFACE_KEEP_COLS)
        for c in list(x.columns):
            if c in keep:
                continue
            # move into aux then drop
            x["aux"] = x.apply(
                lambda row, col=c: {**(row["aux"] if isinstance(row["aux"], dict) else {}), str(col): row[col]},
                axis=1,
            )
            x = x.drop(columns=[c])

        # Ensure required columns exist (even if None)
        for c in ("instrument_name", "expiry_ts", "strike", "cp"):
            if c not in x.columns:
                x[c] = None

        # Stable order
        order = [c for c in ("instrument_name", "expiry_ts", "strike", "cp", "aux") if c in x.columns]
        rest = [c for c in x.columns if c not in order]
        x = x[order + rest]

        # Sort for deterministic downstream processing
        try:
            x = x.sort_values(["expiry_ts", "strike", "cp", "instrument_name"], kind="stable")
        except Exception:
            pass

        return x.reset_index(drop=True)

    @classmethod
    def from_chain_aligned(
        cls,
        *,
        data_ts: int,
        symbol: str,
        market: MarketSpec | None = None,
        chain: pd.DataFrame,
        schema_version: int = 2,
        domain: str = "option_chain",
    ) -> "OptionChainSnapshot":
        dts = to_ms_int(data_ts)

        if not isinstance(chain, pd.DataFrame):
            raise TypeError("OptionChainSnapshot.from_chain_aligned expects `chain` as a pandas DataFrame")

        frame = cls._normalize_frame(chain)

        return cls(
            data_ts=dts,
            symbol=symbol,
            market=ensure_market_spec(market),
            domain=domain,
            schema_version=int(schema_version),
            frame=frame,
        )

    @classmethod
    def from_chain(
        cls,
        ts: int,
        chain: pd.DataFrame,
        symbol: str,
        market: MarketSpec | None = None,
    ) -> "OptionChainSnapshot":
        return cls.from_chain_aligned(
            data_ts=ts,
            symbol=symbol,
            market=market,
            chain=chain,
        )

    def to_dict(self) -> Dict[str, Any]:
        assert isinstance(self.market, MarketInfo)
        return {
            "data_ts": self.data_ts,
            "symbol": self.symbol,
            "market": self.market.to_dict(),
            "domain": self.domain,
            "schema_version": self.schema_version,
            # store as records for JSON-compat
            "frame": self.frame,
        }
    def get_attr(self, key: str) -> Any:
        if not hasattr(self, key):
            raise AttributeError(f"{type(self).__name__} has no attribute {key!r}")
        return getattr(self, key)
