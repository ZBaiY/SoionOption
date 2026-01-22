from __future__ import annotations

from typing import Any, Mapping

import pandas as pd
from ingestion.contracts.normalize import Normalizer
from ingestion.contracts.tick import IngestionTick, _coerce_epoch_ms, normalize_tick


def _now_ms() -> int:
    import time

    return int(time.time() * 1000.0)


def _coerce_cp(x: Any) -> str | None:
    if x is None:
        return None
    s = str(x).strip().upper()
    if s in {"C", "CALL"}:
        return "C"
    if s in {"P", "PUT"}:
        return "P"
    return None


def _cp_from_instrument_name(name: str) -> str | None:
    parts = str(name).split("-")
    if parts:
        tail = parts[-1].upper()
        if tail == "C":
            return "C"
        if tail == "P":
            return "P"
    return None


def _asset_from_symbol(symbol: str) -> str:
    if symbol.endswith("USDT"):
        return symbol[: -len("USDT")]
    return symbol


class DeribitOptionChainNormalizer(Normalizer):
    """Normalize Deribit option instruments into the OptionChainDataHandler v2 payload.

    Output payload:
      {"data_ts": int, "frame": pd.DataFrame}
    """

    def __init__(self, *, symbol: str):
        self.symbol = str(symbol)
        self.asset_symbol = _asset_from_symbol(self.symbol)

    def normalize(self, *, raw: Mapping[str, Any]) -> IngestionTick:
        data_ts, df = self._coerce_raw(raw)
        if df is None or df.empty:
            payload = {"data_ts": int(data_ts), "frame": pd.DataFrame()}
            return normalize_tick(
                timestamp=int(data_ts),
                data_ts=int(data_ts),
                domain="option_chain",
                symbol=self.asset_symbol,
                payload=payload,
                source_id=getattr(self, "source_id", None),
            )

        x = df.copy()

        if "instrument_name" not in x.columns:
            raise ValueError("Option chain payload missing instrument_name")

        if "expiry_ts" not in x.columns:
            if "expiration_timestamp" in x.columns:
                x["expiry_ts"] = x["expiration_timestamp"].map(_coerce_epoch_ms)
            else:
                x["expiry_ts"] = 0

        x["expiry_ts"] = pd.to_numeric(x["expiry_ts"], errors="coerce").fillna(0).astype("int64")
        if "strike" in x.columns:
            x["strike"] = pd.to_numeric(x["strike"], errors="coerce")
        else:
            x["strike"] = float("nan")

        if "cp" not in x.columns:
            if "option_type" in x.columns:
                x["cp"] = x["option_type"].map(_coerce_cp)
            else:
                x["cp"] = None

        miss = x["cp"].isna() | (x["cp"].astype("string") == "")
        if bool(miss.any()):
            x.loc[miss, "cp"] = x.loc[miss, "instrument_name"].map(_cp_from_instrument_name)

        core_cols = {"instrument_name", "expiry_ts", "strike", "cp"}
        aux_cols = [c for c in x.columns if c not in core_cols and c != "data_ts"]
        aux = x[aux_cols].to_dict(orient="records") if aux_cols else [{} for _ in range(len(x))]

        frame = pd.DataFrame(
            {
                "instrument_name": x["instrument_name"].astype("string"),
                "expiry_ts": x["expiry_ts"].astype("int64"),
                "strike": x["strike"].astype("float64"),
                "cp": x["cp"].astype("string"),
                "aux": aux,
            }
        )

        frame = frame.sort_values(["expiry_ts", "strike", "cp", "instrument_name"], kind="stable").reset_index(drop=True)

        payload = {"data_ts": int(data_ts), "frame": frame}
        return normalize_tick(
            timestamp=int(data_ts),
            data_ts=int(data_ts),
            domain="option_chain",
            symbol=self.asset_symbol,
            payload=payload,
            source_id=getattr(self, "source_id", None),
        )

    def _coerce_raw(self, raw: Any) -> tuple[int, pd.DataFrame]:
        if isinstance(raw, pd.DataFrame):
            df = raw.copy()
            if "data_ts" in df.columns:
                ts = int(df["data_ts"].dropna().iloc[0]) if not df["data_ts"].dropna().empty else _now_ms()
                df = df.drop(columns=["data_ts"], errors="ignore")
                return ts, df
            return _now_ms(), df

        if isinstance(raw, Mapping):
            d = {str(k): v for k, v in raw.items()}
            ts_any = d.get("data_ts") or d.get("timestamp")
            ts = _coerce_epoch_ms(ts_any) if ts_any is not None else _now_ms()
            frame_any = d.get("frame") or d.get("records") or d.get("raw")
            if isinstance(frame_any, pd.DataFrame):
                return ts, frame_any.copy()
            if isinstance(frame_any, list):
                return ts, pd.DataFrame(frame_any)

        if isinstance(raw, list):
            return _now_ms(), pd.DataFrame(raw)

        raise ValueError("Unsupported option chain raw payload type")


GenericOptionChainNormalizer = DeribitOptionChainNormalizer
