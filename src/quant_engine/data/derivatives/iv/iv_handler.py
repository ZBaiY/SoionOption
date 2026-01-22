from __future__ import annotations

import json
import math
from collections import deque
from typing import Any, Deque, Optional

import numpy as np
import pandas as pd
from quant_engine.utils.logger import get_logger, log_debug, log_info, log_warn, log_exception, log_throttle, throttle_key

from quant_engine.data.contracts.protocol_realtime import RealTimeDataHandler, to_interval_ms
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.derivatives.option_chain.snapshot import OptionChainSnapshot
from quant_engine.data.derivatives.iv.snapshot import IVSurfaceSnapshot
from quant_engine.data.contracts.snapshot import MarketSpec, ensure_market_spec, merge_market_spec
from quant_engine.data.derivatives.iv.surface import (
    _atm_and_skew,
    _smile_curve,
    _pick_nearest_expiry,
    _coerce_ts,
    _get_num,
    _get_int,
    _get_str,
    _first_float,
)
from ingestion.contracts.tick import IngestionTick
from quant_engine.runtime.modes import EngineMode
from quant_engine.utils.cleaned_path_resolver import resolve_cleaned_paths, base_asset_from_symbol, resolve_domain_symbol_keys
from quant_engine.utils.paths import resolve_data_root

class IVSurfaceDataHandler(RealTimeDataHandler):
    """Runtime IV surface handler (derived layer, mode-agnostic).

    Contract / protocol shadow:
      - kwargs-driven __init__ (loader/builder passes handler config via **cfg)
      - bootstrap(end_ts, lookback) present (no-op by default; IO-free)
      - warmup_to(ts) establishes anti-lookahead anchor
      - get_snapshot(ts=None) / window(ts=None,n) are timestamp-aligned
      - last_timestamp() supported

    Semantics:
      - This handler is a *derived* layer from an underlying OptionChainDataHandler.
      - It does not touch exchange APIs; it only converts OptionChainSnapshot -> IVSurfaceSnapshot.
      - It derives IV inputs from OptionChainSnapshot.frame (DataFrame) and per-row aux dict keys ("*_fetch").
      - A real SABR/SSVI calibrator can be plugged later without changing this API.

    Config (Strategy.DATA.*.iv_surface):
      - source: routing/metadata (default: "deribit")
      - interval: required cadence (e.g. "1m", "5m")
      - bootstrap.lookback: convenience horizon for Engine.bootstrap()
      - cache.max_bars: in-memory cache depth (IVSurfaceSnapshot)
      - expiry: optional expiry selector (future)
      - model_name: optional label (e.g. "SSVI", "SABR", "CHAIN_DERIVED")

    NOTE: `chain_handler` must be provided via kwargs at construction time by the builder.
    """

    # --- declared attributes (protocol/typing shadow) ---
    symbol: str
    chain_handler: OptionChainDataHandler
    interval: str
    bootstrap_cfg: dict[str, Any]
    cache_cfg: dict[str, Any]
    expiry: str | None
    model_name: str
    interval_ms: int
    asset: str

    _snapshots: Deque[IVSurfaceSnapshot]
    market: MarketSpec
    _anchor_ts: int | None
    _logger: Any
    _engine_mode: EngineMode | None
    _data_root: Any
    _backfill_worker: Any | None
    _backfill_emit: Any | None

    def __init__(self, symbol: str, **kwargs: Any):
        self.symbol = symbol
        self._logger = get_logger(__name__)

        # required: chain_handler
        ch = kwargs.get("chain_handler") or kwargs.get("option_chain_handler")
        if not isinstance(ch, OptionChainDataHandler):
            raise ValueError("IVSurfaceDataHandler requires 'chain_handler' (OptionChainDataHandler) in kwargs")
        self.chain_handler = ch
        self.source_id = kwargs.get("source_id") or getattr(self.chain_handler, "source_id", None)

        # required: interval
        ri = kwargs.get("interval")
        if not isinstance(ri, str) or not ri:
            raise ValueError("IV surface handler requires non-empty 'interval' (e.g. '5m')")
        self.interval = ri
        ri_ms = to_interval_ms(self.interval)
        if ri_ms is None:
            raise ValueError(f"Invalid interval format: {self.interval}")
        self.interval_ms = int(ri_ms)
        self.asset = base_asset_from_symbol(symbol)

        # optional model/expiry
        expiry = kwargs.get("expiry")
        if expiry is not None and (not isinstance(expiry, str) or not expiry):
            raise ValueError("IV surface 'expiry' must be a non-empty string if provided")
        self.expiry = expiry

        model_name = kwargs.get("model_name", kwargs.get("model", "CHAIN_DERIVED"))
        if not isinstance(model_name, str) or not model_name:
            raise ValueError("IV surface 'model_name' must be a non-empty string")
        self.model_name = model_name

        # optional nested configs
        bootstrap = kwargs.get("bootstrap") or {}
        if not isinstance(bootstrap, dict):
            raise TypeError("IV surface 'bootstrap' must be a dict")
        self.bootstrap_cfg = dict(bootstrap)

        cache = kwargs.get("cache") or {}
        if not isinstance(cache, dict):
            raise TypeError("IV surface 'cache' must be a dict")
        self.cache_cfg = dict(cache)

        max_bars = self.cache_cfg.get("max_bars")
        if max_bars is None:
            max_bars = kwargs.get("window", 1000)
        max_bars_i = int(max_bars)
        if max_bars_i <= 0:
            raise ValueError("IV surface cache.max_bars must be > 0")

        self._snapshots = deque(maxlen=max_bars_i)
        self._anchor_ts = None
        base_market = getattr(self.chain_handler, "market", None)
        self.market = ensure_market_spec(
            kwargs.get("market") or base_market,
            default_venue=str(kwargs.get("venue", kwargs.get("source", "unknown"))),
            default_asset_class=str(kwargs.get("asset_class", "option")),
            default_timezone=str(kwargs.get("timezone", "UTC")),
            default_calendar=str(kwargs.get("calendar", "24x7")),
            default_session=str(kwargs.get("session", "24x7")),
            default_currency=kwargs.get("currency"),
        )
        self.display_symbol, self._symbol_aliases = resolve_domain_symbol_keys(
            "iv_surface",
            self.symbol,
            self.asset,
            getattr(self.market, "currency", None),
        )

        log_debug(
            self._logger,
            "IVSurfaceDataHandler initialized",
            symbol=self.display_symbol, instrument_symbol=self.symbol,
            interval=self.interval,
            max_bars=max_bars_i,
            bootstrap=self.bootstrap_cfg,
            model_name=self.model_name,
            expiry=self.expiry,
        )
        self._engine_mode = _coerce_engine_mode(kwargs.get("mode"))
        self._data_root = resolve_data_root(
            __file__,
            levels_up=5,
            data_root=kwargs.get("data_root") or kwargs.get("cleaned_root"),
        )
        # External backfill is intentionally unsupported for iv_surface.
        self._backfill_worker = None
        self._backfill_emit = None
        self._backfill_skip_logged = False


    def bootstrap(self, *, anchor_ts: int | None = None, lookback: Any | None = None) -> None:
        if lookback is None:
            lookback = self.bootstrap_cfg.get("lookback")
        # Bootstrap only reads local storage.
        log_debug(
            self._logger,
            "IVSurfaceDataHandler.bootstrap (no-op)",
            symbol=self.display_symbol, instrument_symbol=self.symbol,
            anchor_ts=anchor_ts,
            lookback=lookback,
        )
        if anchor_ts is None:
            return
        self._bootstrap_from_files(anchor_ts=int(anchor_ts), lookback=lookback)

    # align_to(ts) defines the maximum visible engine-time for all read APIs.
    def align_to(self, ts: int) -> None:
        self._anchor_ts = int(ts)
        log_debug(self._logger, "IVSurfaceDataHandler.align_to", symbol=self.display_symbol, instrument_symbol=self.symbol, anchor_ts=self._anchor_ts)

    def load_history(
        self,
        *,
        start_ts: int | None = None,
        end_ts: int | None = None,
    ) -> None:
        log_debug(
            self._logger,
            "IVSurfaceDataHandler.load_history (no-op)",
            symbol=self.display_symbol, instrument_symbol=self.symbol,
            start_ts=start_ts,
            end_ts=end_ts,
        )

    def warmup_to(self, ts: int) -> None:
        self.align_to(ts)

    # ------------------------------------------------------------------
    # Derived update (called by engine/driver when appropriate)
    # ------------------------------------------------------------------

    def on_new_tick(self, tick: IngestionTick) -> None:
        """
        Ingest a derived-tick trigger for IV surface generation.

        Semantics:
          - This handler is *derived* from OptionChainDataHandler.
          - `bar` is treated as a trigger carrying an engine timestamp only.
          - No raw market data is ingested here.
          - Snapshot derivation pulls from chain_handler.get_snapshot(ts).
          - Visibility is enforced exclusively via align_to(ts).
        """
        if tick.domain != "iv_surface" or tick.symbol not in self._symbol_aliases:
            return
        expected_source = getattr(self, "source_id", None)
        tick_source = getattr(tick, "source_id", None)
        if expected_source is not None and tick_source != expected_source:
            return
        ts = _coerce_ts(tick.data_ts)
        if ts is None:
            return
        snap = self._derive_from_chain(ts)
        if snap is not None:
            self._snapshots.append(snap)

    def _maybe_backfill(self, *, target_ts: int) -> None:
        if not self._should_backfill():
            return
        self._backfill_to_target(target_ts=int(target_ts))

    # ------------------------------------------------------------------
    # Backfill helpers (realtime/mock only)
    # ------------------------------------------------------------------

    def _should_backfill(self) -> bool:
        # iv_surface is derived from option_chain; do not external-backfill.
        return False

    def _bootstrap_from_files(self, *, anchor_ts: int, lookback: Any | None) -> None:
        bars = _coerce_lookback_bars(lookback, self.interval_ms, getattr(self._snapshots, "maxlen", None))
        if bars is None or bars <= 0:
            return
        start_ts = int(anchor_ts) - (int(bars) - 1) * int(self.interval_ms)
        end_ts = int(anchor_ts)
        log_info(
            self._logger,
            "iv_surface.bootstrap.start",
            symbol=self.display_symbol, instrument_symbol=self.symbol,
            asset=self.asset,
            start_ts=start_ts,
            end_ts=end_ts,
            bars=int(bars),
        )
        prev_anchor = self._anchor_ts
        if prev_anchor is None:
            self._anchor_ts = int(anchor_ts)
        try:
            loaded = self._load_from_files(start_ts=start_ts, end_ts=end_ts)
            log_info(
                self._logger,
                "iv_surface.bootstrap.done",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                asset=self.asset,
                loaded_count=int(loaded),
                cache_size=len(self._snapshots),
            )
        except Exception as exc:
            log_warn(
                self._logger,
                "iv_surface.bootstrap.error",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                asset=self.asset,
                err_type=type(exc).__name__,
                err=str(exc),
            )
        finally:
            if prev_anchor is None:
                self._anchor_ts = prev_anchor

    def _backfill_to_target(self, *, target_ts: int) -> None:
        last_ts = self.last_timestamp()
        if last_ts is None:
            lookback = self.bootstrap_cfg.get("lookback") if self.bootstrap_cfg else None
            bars = _coerce_lookback_bars(lookback, self.interval_ms, getattr(self._snapshots, "maxlen", None))
            if bars is None or bars <= 0:
                throttle_id = throttle_key("iv_surface.backfill.no_lookback", type(self).__name__, self.symbol, self.interval_ms)
                if log_throttle(throttle_id, 60.0):
                    log_warn(
                        self._logger,
                        "iv_surface.backfill.no_lookback",
                        symbol=self.display_symbol, instrument_symbol=self.symbol,
                        asset=self.asset,
                        target_ts=int(target_ts),
                    )
                return
            start_ts = int(target_ts) - (int(bars) - 1) * int(self.interval_ms)
            end_ts = int(target_ts)
            log_warn(
                self._logger,
                "iv_surface.backfill.cold_start",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                asset=self.asset,
                target_ts=int(target_ts),
                interval_ms=int(self.interval_ms),
                start_ts=start_ts,
                end_ts=end_ts,
            )
            try:
                loaded = self._backfill_from_source(start_ts=start_ts, end_ts=end_ts, target_ts=target_ts)
                log_info(
                    self._logger,
                    "iv_surface.backfill.done",
                    symbol=self.display_symbol, instrument_symbol=self.symbol,
                    asset=self.asset,
                    loaded_count=int(loaded),
                    cache_size=len(self._snapshots),
                )
            except Exception as exc:
                log_exception(
                    self._logger,
                    "iv_surface.backfill.error",
                    symbol=self.display_symbol, instrument_symbol=self.symbol,
                    asset=self.asset,
                    err=str(exc),
                )
            return
        # Gap check is a guard: no downstream updates until data is continuous.
        gap_threshold = int(target_ts) - int(self.interval_ms)
        if int(last_ts) >= gap_threshold:
            return
        start_ts = int(last_ts) + int(self.interval_ms)
        end_ts = int(target_ts)
        log_warn(
            self._logger,
            "iv_surface.gap_detected",
            symbol=self.display_symbol, instrument_symbol=self.symbol,
            asset=self.asset,
            last_ts=int(last_ts),
            target_ts=int(target_ts),
            interval_ms=int(self.interval_ms),
            start_ts=start_ts,
            end_ts=end_ts,
        )
        try:
            loaded = self._backfill_from_source(start_ts=start_ts, end_ts=end_ts, target_ts=target_ts)
            log_info(
                self._logger,
                "iv_surface.backfill.done",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                asset=self.asset,
                loaded_count=int(loaded),
                cache_size=len(self._snapshots),
            )
        except Exception as exc:
            log_exception(
                self._logger,
                "iv_surface.backfill.error",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                asset=self.asset,
                err=str(exc),
            )

    def _backfill_from_source(self, *, start_ts: int, end_ts: int, target_ts: int) -> int:
        if not self._backfill_skip_logged:
            backfill_required = bool(self.bootstrap_cfg.get("backfill_required")) if isinstance(self.bootstrap_cfg, dict) else False
            log_fn = log_warn if backfill_required else log_info
            log_fn(
                self._logger,
                "iv_surface.backfill.skipped",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                asset=self.asset,
                start_ts=int(start_ts),
                end_ts=int(end_ts),
                reason="no_external_source",
            )
            self._backfill_skip_logged = True
        return 0

    def set_external_source(self, worker: Any | None, *, emit: Any | None = None) -> None:
        """IV surfaces are derived; external backfill is intentionally ignored."""
        self._backfill_worker = None
        self._backfill_emit = None

    def _load_from_files(self, *, start_ts: int, end_ts: int) -> int:
        stage = "cleaned"
        if self._engine_mode == EngineMode.SAMPLE:
            stage = "sample"
        paths = resolve_cleaned_paths(
            data_root=self._data_root,
            stage=stage,
            domain="iv_surface",
            asset=self.asset,
            interval=self.interval,
            start_ts=int(start_ts),
            end_ts=int(end_ts),
        )
        if not paths:
            return 0
        count = 0
        last_ts = self.last_timestamp()
        for row in _iter_parquet_rows(paths, start_ts=int(start_ts), end_ts=int(end_ts)):
            ts = row.get("data_ts")
            if ts is None:
                continue
            data_ts = int(ts)
            if last_ts is not None and data_ts <= int(last_ts):
                continue
            snap = _snapshot_from_row(
                row,
                symbol=self.symbol, 
                market=self.market,
                default_expiry=self.expiry,
                default_model=self.model_name,
            )
            if snap is None:
                continue
            self._snapshots.append(snap)
            last_ts = data_ts
            count += 1
        return count
    # Derive IV surface strictly from visible option-chain state at ts.
    def _derive_from_chain(self, ts: int) -> IVSurfaceSnapshot | None:
        chain_snap: OptionChainSnapshot | None = self.chain_handler.get_snapshot(ts)
        if chain_snap is None:
            return None

        # OptionChainSnapshot is data-time aligned; prefer its data_ts when present.
        surface_ts = int(getattr(chain_snap, "data_ts", getattr(chain_snap, "timestamp", ts)))

        frame = getattr(chain_snap, "frame", None)
        if frame is None or len(frame) == 0:
            return None

        # --- required columns (best-effort) ---
        strike = _get_num(frame, "strike")
        expiry_ts = _get_int(frame, "expiry_ts")
        cp = _get_str(frame, "cp")

        # underlying / index price for moneyness
        spot = _first_float(_get_num(frame, "price_index"))
        if spot is None:
            spot = _first_float(_get_num(frame, "index_price"))

        # IV series: prefer fetched IV keys in aux (or columns if present)
        iv = _get_num(frame, "mark_iv_fetch")
        if iv is None:
            iv = _get_num(frame, "iv_fetch")
        if iv is None:
            bid_iv = _get_num(frame, "bid_iv_fetch")
            ask_iv = _get_num(frame, "ask_iv_fetch")
            if bid_iv is not None and ask_iv is not None:
                iv = (bid_iv + ask_iv) / 2.0

        if iv is None or strike is None or expiry_ts is None:
            return None

        # Choose a reference expiry (nearest future expiry in the snapshot).
        exp_key = _pick_nearest_expiry(surface_ts, expiry_ts)
        if exp_key is None:
            return None

        mask = (expiry_ts == exp_key)
        k = strike[mask]
        v = iv[mask]
        cpc = cp[mask] if cp is not None else None

        # Drop invalid rows
        ok = (k.notna()) & (v.notna()) & (k > 0) & (v > 0)
        if cpc is not None:
            ok = ok & cpc.notna()
        k = k[ok]
        v = v[ok]
        cpc = cpc[ok] if cpc is not None else None

        if len(k) < 3:
            atm_iv = float(v.iloc[-1]) if len(v) else 0.0
            skew = 0.0
            curve = {}
        else:
            atm_iv, skew = _atm_and_skew(spot=spot, strike=k, iv=v, cp=cpc)
            curve = _smile_curve(spot=spot, strike=k, iv=v)

        # Enforce timestamp = engine ts, data_ts = surface_ts
        market = getattr(chain_snap, "market", None)
        market = merge_market_spec(self.market, market)

        # Surface payload: keep richer info for debugging / later calibration.
        surface = {
            "expiry_ts": int(exp_key),
            "spot": float(spot) if spot is not None else None,
            "smile": curve,
        }

        return IVSurfaceSnapshot.from_surface_aligned(
            timestamp=ts,
            data_ts=surface_ts,
            atm_iv=float(atm_iv),
            skew=float(skew),
            curve=dict(curve),
            surface=surface,
            symbol=self.symbol, 
            market=market,
            expiry=self.expiry,
            model=self.model_name,
        )

    # ------------------------------------------------------------------
    # v4 timestamp-aligned API
    # ------------------------------------------------------------------

    def last_timestamp(self) -> int | None:
        if not self._snapshots:
            return None
        last_ts = int(self._snapshots[-1].data_ts)
        if self._anchor_ts is not None:
            return min(last_ts, int(self._anchor_ts))
        return last_ts

    def get_snapshot(self, ts: int | None = None) -> Optional[IVSurfaceSnapshot]:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return None

        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        for s in reversed(self._snapshots):
            if int(s.data_ts) <= t:
                return s
        return None

    def window(self, ts: int | None = None, n: int = 1) -> list[IVSurfaceSnapshot]:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return []

        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        out: list[IVSurfaceSnapshot] = []
        for s in reversed(self._snapshots):
            if int(s.data_ts) <= t:
                out.append(s)
                if len(out) >= int(n):
                    break
        out.reverse()
        return out


def _coerce_lookback_ms(lookback: Any, interval_ms: int | None) -> int | None:
    if lookback is None:
        return None
    if isinstance(lookback, dict):
        window_ms = lookback.get("window_ms")
        if window_ms is not None:
            return int(window_ms)
        bars = lookback.get("bars")
        if bars is not None and interval_ms is not None:
            return int(float(bars) * int(interval_ms))
        return None
    if isinstance(lookback, (int, float)):
        if interval_ms is not None:
            return int(float(lookback) * int(interval_ms))
        return int(float(lookback))
    if isinstance(lookback, str):
        ms = to_interval_ms(lookback)
        return int(ms) if ms is not None else None
    return None


def _coerce_lookback_bars(lookback: Any, interval_ms: int | None, max_bars: int | None) -> int | None:
    if interval_ms is None or interval_ms <= 0:
        return None
    window_ms = _coerce_lookback_ms(lookback, interval_ms)
    if window_ms is None:
        return None
    bars = max(1, int(math.ceil(int(window_ms) / int(interval_ms))))
    if max_bars is not None:
        bars = min(bars, int(max_bars))
    return bars


def _coerce_engine_mode(mode: Any) -> EngineMode | None:
    if isinstance(mode, EngineMode):
        return mode
    if isinstance(mode, str):
        try:
            return EngineMode(mode)
        except Exception:
            return None
    return None


def _coerce_dict(x: Any) -> dict[str, Any]:
    if isinstance(x, dict):
        return {str(k): v for k, v in x.items()}
    if isinstance(x, str):
        try:
            val = json.loads(x)
            if isinstance(val, dict):
                return {str(k): v for k, v in val.items()}
        except Exception:
            return {}
    return {}


def _snapshot_from_row(
    row: dict[str, Any],
    *,
    symbol: str,
    market: MarketSpec,
    default_expiry: str | None,
    default_model: str | None,
) -> IVSurfaceSnapshot | None:
    ts_any = row.get("data_ts") or row.get("timestamp")
    if ts_any is None:
        return None
    data_ts = int(ts_any)
    curve = row.get("curve")
    surface = row.get("surface")
    expiry = row.get("expiry") or row.get("expiry_code") or default_expiry
    model = row.get("model") or default_model
    atm_iv = row.get("atm_iv")
    skew = row.get("skew")
    if atm_iv is None or skew is None:
        return None
    return IVSurfaceSnapshot.from_surface_aligned(
        timestamp=data_ts,
        data_ts=data_ts,
        symbol=symbol,
        market=market,
        atm_iv=atm_iv,
        skew=skew,
        curve=_coerce_dict(curve),
        surface=_coerce_dict(surface),
        expiry=str(expiry) if expiry is not None else None,
        model=str(model) if model is not None else None,
    )


def _iter_parquet_rows(paths, *, start_ts: int, end_ts: int):
    for fp in paths:
        if not fp.exists():
            continue
        df = pd.read_parquet(fp)
        if df is None or df.empty:
            continue
        if "data_ts" not in df.columns:
            if "timestamp" in df.columns:
                df["data_ts"] = df["timestamp"]
            else:
                continue
        df["data_ts"] = df["data_ts"].astype("int64", copy=False)
        if start_ts is not None:
            df = df[df["data_ts"] >= int(start_ts)]
        if end_ts is not None:
            df = df[df["data_ts"] <= int(end_ts)]
        if df.empty:
            continue
        df = df.sort_values("data_ts", kind="mergesort")
        for rec in df.to_dict(orient="records"):
            yield {str(k): v for k, v in rec.items()}
