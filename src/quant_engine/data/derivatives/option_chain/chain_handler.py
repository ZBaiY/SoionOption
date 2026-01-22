from __future__ import annotations

import math
from typing import Any, Mapping, cast

import pandas as pd
import time

from quant_engine.utils.logger import get_logger, log_debug, log_info, log_warn, log_exception, log_throttle, throttle_key
from ingestion.contracts.tick import IngestionTick, _coerce_epoch_ms
from quant_engine.data.contracts.protocol_realtime import RealTimeDataHandler, to_interval_ms
from quant_engine.data.contracts.snapshot import (
    MarketSpec,
    ensure_market_spec,
    merge_market_spec,
    classify_gap,
)
from quant_engine.runtime.modes import EngineMode
from quant_engine.utils.cleaned_path_resolver import resolve_cleaned_paths, base_asset_from_symbol, resolve_domain_symbol_keys
from quant_engine.utils.paths import resolve_data_root
from ingestion.option_chain.source import OptionChainFileSource
from .cache import (
    OptionChainCache,
    OptionChainSimpleCache,
    OptionChainExpiryCache,
    OptionChainTermBucketedCache,
)
from .snapshot import OptionChainSnapshot


class OptionChainDataHandler(RealTimeDataHandler):
    """Runtime Option-Chain handler (mode-agnostic).

    Domain semantics:
      - Stores immutable OptionChainSnapshot (observation-time facts)
      - Observation/event time is snapshot timestamp (epoch ms) -> snapshot.data_ts
      - No system time / latency stored in snapshots

    Read semantics (anti-lookahead):
      - align_to(ts) sets an anchor; all reads clamp to min(ts, anchor)
      - cache ordering is by data_ts (observation time)

    Polling cadence is external to this handler; it does not own IO.

    Config mapping (Strategy.DATA.*.option_chain):
      - source: origin identifier (e.g., "DERIBIT") (kept for metadata/routing)
      - cache.maxlen: global snapshot cache maxlen (note: each snapshot can be large)
      - cache.per_expiry_maxlen: per-expiry bucket maxlen (stores snapshot refs)
      - columns: dataframe view columns for `chain_df()`
    """

    symbol: str
    asset: str
    source: str
    interval: str | None
    interval_ms: int | None
    columns: list[str] | None
    market: MarketSpec
    gap_min_gap_ms: int | None
    bootstrap_cfg: dict[str, Any] | None

    cache_cfg: dict[str, Any]
    cache: OptionChainCache

    _anchor_ts: int | None
    _logger: Any
    _backfill_worker: Any | None
    _backfill_emit: Any | None
    _engine_mode: EngineMode | None
    _data_root: Any

    def __init__(self, symbol: str, **kwargs: Any):
        self.symbol = symbol
        self.source = str(kwargs.get("source") or "DERIBIT")

        interval = kwargs.get("interval")
        if interval is not None and (not isinstance(interval, str) or not interval):
            raise ValueError("option_chain 'interval' must be a non-empty string if provided")
        self.interval = interval
        interval_ms = to_interval_ms(self.interval) if self.interval is not None else None
        self.interval_ms = int(interval_ms) if interval_ms is not None else None

        asset_any = kwargs.get("asset") or kwargs.get("currency") or kwargs.get("underlying")
        self.asset = base_asset_from_symbol(str(asset_any)) if asset_any is not None else base_asset_from_symbol(symbol)

        self._engine_mode = _coerce_engine_mode(kwargs.get("mode"))
        self._data_root = resolve_data_root(
            __file__,
            levels_up=5,
            data_root=kwargs.get("data_root") or kwargs.get("cleaned_root"),
        )
        self.source_id = _resolve_source_id(
            source_id=kwargs.get("source_id"),
            mode=self._engine_mode,
            data_root=self._data_root,
            source=self.source,
        )

        cache = kwargs.get("cache") or {}
        if not isinstance(cache, dict):
            raise TypeError("option_chain 'cache' must be a dict")
        self.cache_cfg = dict(cache)
        self.bootstrap_cfg = kwargs.get("bootstrap") or None

        maxlen = int(self.cache_cfg.get("maxlen", kwargs.get("maxlen", 512)))
        if maxlen <= 0:
            raise ValueError("option_chain cache.maxlen must be > 0")

        # cache kind: simple | expiry | term
        kind = str(self.cache_cfg.get("kind") or self.cache_cfg.get("type") or "expiry").lower()

        if kind in {"simple", "deque"}:

            self.cache = OptionChainSimpleCache(maxlen=maxlen)
            per_expiry_maxlen = None
            per_term_maxlen = None
            term_bucket_ms = None
        elif kind in {"term", "term_bucket", "bucketed"}:
            per_term_maxlen = int(self.cache_cfg.get("per_term_maxlen", kwargs.get("per_term_maxlen", 256)))
            term_bucket_ms = int(self.cache_cfg.get("term_bucket_ms", kwargs.get("term_bucket_ms", 86_400_000)))
            if per_term_maxlen <= 0:
                raise ValueError("option_chain cache.per_term_maxlen must be > 0")
            if term_bucket_ms <= 0:
                raise ValueError("option_chain cache.term_bucket_ms must be > 0")

            # optional expiry index inside term cache
            enable_expiry_index = bool(self.cache_cfg.get("enable_expiry_index", True))
            per_expiry_maxlen = self.cache_cfg.get("per_expiry_maxlen", kwargs.get("per_expiry_maxlen"))
            per_expiry_maxlen_i = int(per_expiry_maxlen) if per_expiry_maxlen is not None else None

            self.cache = OptionChainTermBucketedCache(
                maxlen=maxlen,
                per_term_maxlen=per_term_maxlen,
                term_bucket_ms=term_bucket_ms,
                per_expiry_maxlen=per_expiry_maxlen_i,
                enable_expiry_index=enable_expiry_index,
            )
        else:
            # default: expiry-indexed cache
            per_expiry_maxlen = int(self.cache_cfg.get("per_expiry_maxlen", kwargs.get("per_expiry_maxlen", 256)))
            if per_expiry_maxlen <= 0:
                raise ValueError("option_chain cache.per_expiry_maxlen must be > 0")
            per_term_maxlen = None
            term_bucket_ms = None
            self.cache = OptionChainExpiryCache(maxlen=maxlen, per_expiry_maxlen=per_expiry_maxlen)

        # dataframe view columns only (storage keeps full record dicts)
        self.columns = kwargs.get(
            "columns",
            [
                "data_ts",
                "instrument_name",
                "expiry_ts",
                "strike",
                "cp",
                "bid",
                "ask",
                "mark",
                "index_price",
                # fetched IVs are under aux with *_fetch suffix (schema v2)
                "mark_iv_fetch",
                "bid_iv_fetch",
                "ask_iv_fetch",
                "iv_fetch",
                # optional carry-through
                "oi",
                "volume",
            ],
        )

        self.market = ensure_market_spec(
            kwargs.get("market"),
            default_venue=str(kwargs.get("venue", kwargs.get("source", self.source))),
            default_asset_class=str(kwargs.get("asset_class", "option")),
            default_timezone=str(kwargs.get("timezone", "UTC")),
            default_calendar=str(kwargs.get("calendar", "24x7")),
            default_session=str(kwargs.get("session", "24x7")),
            default_currency=kwargs.get("currency"),
        )
        self.display_symbol, self._symbol_aliases = resolve_domain_symbol_keys(
            "option_chain",
            self.symbol,
            self.asset,
            getattr(self.market, "currency", None),
        )

        gap_cfg = kwargs.get("gap") or {}
        if not isinstance(gap_cfg, dict):
            raise TypeError("option_chain 'gap' must be a dict")
        min_gap_ms = gap_cfg.get("min_gap_ms")
        self.gap_min_gap_ms = int(min_gap_ms) if min_gap_ms is not None else None

        # Backfill worker is wired by runtime/apps; do not initialize here.
        self._backfill_worker = None
        self._backfill_emit = None
        self._anchor_ts = None
        self._logger = get_logger(__name__)

        log_debug(
            self._logger,
            "OptionChainDataHandler initialized",
            symbol=self.display_symbol,
            instrument_symbol=self.symbol,
            source=self.source,
            cache_kind=kind,
            maxlen=maxlen,
            per_expiry_maxlen=per_expiry_maxlen,
            per_term_maxlen=per_term_maxlen,
            term_bucket_ms=term_bucket_ms,
        )

    def set_external_source(self, worker: Any | None, *, emit: Any | None = None) -> None:
        """Attach ingestion worker for backfill (wired by runtime/apps)."""
        self._backfill_worker = worker
        self._backfill_emit = emit

    # ------------------------------------------------------------------
    # Lifecycle (realtime/mock)
    # ------------------------------------------------------------------

    def align_to(self, ts: int) -> None:
        """Set observation-time anchor (anti-lookahead)."""
        self._anchor_ts = int(ts)
        log_debug(
            self._logger,
            "OptionChainDataHandler align_to",
            symbol=self.display_symbol,
            instrument_symbol=self.symbol,
            anchor_ts=self._anchor_ts,
        )

    def bootstrap(self, *, anchor_ts: int | None = None, lookback: Any | None = None) -> None:
        # IO-free by default. Bootstrap only reads local storage.
        log_debug(
            self._logger,
            "OptionChainDataHandler.bootstrap (no-op)",
            symbol=self.display_symbol,
            instrument_symbol=self.symbol,
            anchor_ts=anchor_ts,
            lookback=lookback,
        )
        if anchor_ts is None:
            return
        self._bootstrap_from_files(anchor_ts=int(anchor_ts), lookback=lookback)

    def load_history(
        self,
        *,
        start_ts: int | None = None,
        end_ts: int | None = None,
    ) -> None:
        log_debug(
            self._logger,
            "OptionChainDataHandler.load_history (no-op)",
            symbol=self.display_symbol,
            instrument_symbol=self.symbol,
            start_ts=start_ts,
            end_ts=end_ts,
        )

    def warmup_to(self, ts: int) -> None:
        self.align_to(ts)

    # ------------------------------------------------------------------
    # Streaming tick API
    # ------------------------------------------------------------------

    def on_new_tick(self, tick: IngestionTick) -> None:
        """Ingest one snapshot tick.

        Accepted payloads (tick.payload):
          - OptionChainSnapshot
          - Mapping[str, Any] with keys {data_ts, records} (and optional metadata)
          - Mapping[str, Any] representing a *single record* (will be treated as a one-record snapshot)
          - Sequence[Mapping[str, Any]] / pandas.DataFrame as records (requires explicit data_ts)

        Notes:
          - schema_version is defaulted to 2 in snapshot builder.
          - fetched IV fields (iv/mark_iv/bid_iv/ask_iv) are moved into record["aux"] as *_fetch.
        """
        if tick.domain != "option_chain" or tick.symbol not in self._symbol_aliases:
            return
        expected_source = getattr(self, "source_id", None)
        tick_source = getattr(tick, "source_id", None)
        if expected_source is not None and tick_source != expected_source:
            return
        payload = dict(tick.payload)
        if "data_ts" not in payload:
            payload["data_ts"] = int(tick.data_ts)
        snap = _build_snapshot_from_payload(payload, symbol=self.display_symbol, market=self.market)
        if snap is None:
            return

        # update market gap classification (best-effort)
        last = self.cache.last()
        last_ts = int(last.data_ts) if last is not None else None
        self._set_gap_market(snap, last_ts=last_ts)

        self.cache.push(snap)
        log_debug(
            self._logger,
            "OptionChainDataHandler.on_new_tick",
            symbol=self.display_symbol,
            instrument_symbol=self.symbol,
            data_ts=int(snap.data_ts),
            n_rows=int(len(snap.frame)),
        )

    # ------------------------------------------------------------------
    # Unified access (timestamp-aligned)
    # ------------------------------------------------------------------

    def last_timestamp(self) -> int | None:
        last = self.cache.last()
        if last is None:
            return None
        ts = int(last.data_ts)
        if self._anchor_ts is not None:
            return min(ts, int(self._anchor_ts))
        return ts

    def get_snapshot(self, ts: int | None = None) -> OptionChainSnapshot | None:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return None
        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        return self.cache.get_at_or_before(t)

    def window(self, ts: int | None = None, n: int = 1) -> list[OptionChainSnapshot]:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return []
        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        return list(self.cache.get_n_before(t, int(n)))

    # ------------------------------------------------------------------
    # Expiry helpers (optional)
    # ------------------------------------------------------------------

    def expiries(self) -> list[int]:
        if hasattr(self.cache, "expiries"):
            try:
                return list(self.cache.expiries())  # type: ignore[attr-defined]
            except Exception:
                return []
        return []

    def get_snapshot_for_expiry(self, *, expiry_ts: int, ts: int | None = None) -> OptionChainSnapshot | None:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return None
        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        if not hasattr(self.cache, "get_at_or_before_for_expiry"):
            return None
        return self.cache.get_at_or_before_for_expiry(int(expiry_ts), t)  # type: ignore[attr-defined]

    # ------------------------------------------------------------------
    # Term helpers (optional; requires term-bucketed cache)
    # ------------------------------------------------------------------

    def term_buckets(self) -> list[int]:
        if hasattr(self.cache, "term_buckets"):
            try:
                return list(self.cache.term_buckets())  # type: ignore[attr-defined]
            except Exception:
                return []
        return []

    def get_snapshot_for_term(self, *, term_key_ms: int, ts: int | None = None) -> OptionChainSnapshot | None:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return None
        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        if not hasattr(self.cache, "get_at_or_before_for_term"):
            return None
        return self.cache.get_at_or_before_for_term(int(term_key_ms), t)  # type: ignore[attr-defined]

    def window_for_term(self, *, term_key_ms: int, n: int = 1) -> list[OptionChainSnapshot]:
        ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
        if ts is None:
            return []
        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        if not hasattr(self.cache, "get_n_before_for_term"):
            return []
        return list(self.cache.get_n_before_for_term(int(term_key_ms), t, int(n)))  # type: ignore[attr-defined]

    # ------------------------------------------------------------------
    # Views
    # ------------------------------------------------------------------

    def chain_df(self, ts: int | None = None, *, columns: list[str] | None = None) -> pd.DataFrame:
        """Return a DataFrame view of the chain at time ts.

        `OptionChainSnapshot` stores a normalized `frame` with an `aux` dict per row.
        This view can surface aux keys as columns on demand.
        """
        snap = self.get_snapshot(ts)
        if snap is None:
            return pd.DataFrame()

        base = snap.frame
        if base is None or len(base) == 0:
            return pd.DataFrame()

        cols = self.columns if columns is None else columns
        if cols is None:
            out = base.copy()
            out.insert(0, "data_ts", int(snap.data_ts))
            return out

        out = pd.DataFrame({"data_ts": [int(snap.data_ts)] * len(base)})
        aux_series = base["aux"] if "aux" in base.columns else None

        for c in cols:
            if c == "data_ts":
                out[c] = int(snap.data_ts)
                continue
            if c in base.columns:
                out[c] = base[c]
                continue
            if aux_series is not None:
                out[c] = aux_series.map(lambda d, key=c: d.get(key) if isinstance(d, dict) else None)
            else:
                out[c] = None

        return out

    # ------------------------------------------------------------------
    # Legacy / misc
    # ------------------------------------------------------------------

    def reset(self) -> None:
        log_info(
            self._logger,
            "OptionChainDataHandler reset requested",
            symbol=self.display_symbol,
            instrument_symbol=self.symbol,
        )
        self.cache.clear()

    def _set_gap_market(self, snap: OptionChainSnapshot, *, last_ts: int | None) -> None:
        # We do not mutate snapshot.market (snap is frozen). Gap classification is best-effort
        # and must live in handler-level market spec.
        #
        # If upstream provides market override/status, merge it into handler market.
        payload: dict[str, Any] = {}
        # currently no standardized market override for option_chain payloads.
        data_ts = int(snap.data_ts)
        gap_type = classify_gap(
            status=None,
            last_ts=last_ts,
            data_ts=data_ts,
            expected_interval_ms=None,
            min_gap_ms=self.gap_min_gap_ms,
        )
        self.market = merge_market_spec(self.market, payload.get("market"), status=None, gap_type=gap_type)

    def _maybe_backfill(self, *, target_ts: int) -> None:
        if not self._should_backfill():
            return
        self._backfill_to_target(target_ts=int(target_ts))

    # ------------------------------------------------------------------
    # Backfill helpers (realtime/mock only)
    # ------------------------------------------------------------------

    def _should_backfill(self) -> bool:
        return self._engine_mode in (EngineMode.REALTIME, EngineMode.MOCK)

    def _bootstrap_from_files(self, *, anchor_ts: int, lookback: Any | None) -> None:
        bars = _coerce_lookback_bars(lookback, self.interval_ms, getattr(self.cache, "maxlen", None))
        if bars is None or bars <= 0 or self.interval_ms is None:
            return
        start_ts = int(anchor_ts) - (int(bars) - 1) * int(self.interval_ms)
        end_ts = int(anchor_ts)
        log_info(
            self._logger,
            "option_chain.bootstrap.start",
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
                "option_chain.bootstrap.done",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                asset=self.asset,
                loaded_count=int(loaded),
                cache_size=len(getattr(self.cache, "buffer", [])),
            )
        except Exception as exc:
            log_warn(
                self._logger,
                "option_chain.bootstrap.error",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                asset=self.asset,
                err_type=type(exc).__name__,
                err=str(exc),
            )
        finally:
            if prev_anchor is None:
                self._anchor_ts = prev_anchor

    def _backfill_to_target(self, *, target_ts: int) -> None:
        if self.interval_ms is None or self.interval_ms <= 0:
            return
        last_ts = self.last_timestamp()
        if last_ts is None:
            lookback = self.bootstrap_cfg.get("lookback") if self.bootstrap_cfg else None
            bars = _coerce_lookback_bars(lookback, self.interval_ms, getattr(self.cache, "maxlen", None))
            if bars is None or bars <= 0:
                throttle_id = throttle_key("option_chain.backfill.no_lookback", type(self).__name__, self.symbol, self.interval_ms)
                if log_throttle(throttle_id, 60.0):
                    log_warn(
                        self._logger,
                        "option_chain.backfill.no_lookback",
                        symbol=self.display_symbol, instrument_symbol=self.symbol,
                        asset=self.asset,
                        target_ts=int(target_ts),
                    )
                return
            start_ts = int(target_ts) - (int(bars) - 1) * int(self.interval_ms)
            end_ts = int(target_ts)
            log_warn(
                self._logger,
                "option_chain.backfill.cold_start",
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
                    "option_chain.backfill.done",
                    symbol=self.display_symbol, instrument_symbol=self.symbol,
                    asset=self.asset,
                    loaded_count=int(loaded),
                    cache_size=len(getattr(self.cache, "buffer", [])),
                )
            except Exception as exc:
                log_exception(
                    self._logger,
                    "option_chain.backfill.error",
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
            "option_chain.gap_detected",
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
                "option_chain.backfill.done",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                asset=self.asset,
                loaded_count=int(loaded),
                cache_size=len(getattr(self.cache, "buffer", [])),
            )
            post_last = self.last_timestamp()
            if post_last is None or int(post_last) < (int(target_ts) - int(self.interval_ms)):
                log_warn(
                    self._logger,
                    "option_chain.backfill.incomplete",
                    symbol=self.display_symbol, instrument_symbol=self.symbol,
                    asset=self.asset,
                    last_ts=int(post_last) if post_last is not None else None,
                    target_ts=int(target_ts),
                    interval_ms=int(self.interval_ms),
                )
        except Exception as exc:
            log_exception(
                self._logger,
                "option_chain.backfill.error",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                asset=self.asset,
                err=str(exc),
            )

    def _load_from_files(self, *, start_ts: int, end_ts: int) -> int:
        stage = "cleaned"
        if self._engine_mode == EngineMode.SAMPLE:
            stage = "sample"
        paths = resolve_cleaned_paths(
            data_root=self._data_root,
            stage=stage,
            domain="option_chain",
            asset=self.asset,
            interval=self.interval,
            start_ts=int(start_ts),
            end_ts=int(end_ts),
        )
        if not paths:
            return 0
        source = OptionChainFileSource(
            root=f"{stage}/option_chain",
            asset=self.asset,
            interval=self.interval,
            start_ts=int(start_ts),
            end_ts=int(end_ts),
            paths=paths,
        )
        last_ts: int | None = None
        last_snap = self.cache.last()
        if last_snap is not None:
            last_ts = int(last_snap.data_ts)
        count = 0
        for payload in source:
            ts = _infer_data_ts(payload)
            if last_ts is not None and int(ts) <= int(last_ts):
                continue
            self.on_new_tick(_tick_from_payload(payload, symbol=self.display_symbol, source_id=getattr(self, "source_id", None)))
            last_ts = int(ts)
            count += 1
        return count

    def _backfill_from_source(self, *, start_ts: int, end_ts: int, target_ts: int) -> int:
        worker = self._backfill_worker
        if worker is None:
            throttle_id = throttle_key("option_chain.backfill.no_worker", type(self).__name__, self.symbol, self.interval_ms)
            if log_throttle(throttle_id, 60.0):
                log_debug(
                    self._logger,
                    "option_chain.backfill.no_worker",
                    symbol=self.display_symbol, instrument_symbol=self.symbol,
                    asset=self.asset,
                    start_ts=int(start_ts),
                    end_ts=int(end_ts),
                )
            return 0
        backfill = getattr(worker, "backfill", None)
        if not callable(backfill):
            throttle_id = throttle_key("option_chain.backfill.no_worker_method", type(self).__name__, self.symbol, self.interval_ms)
            if log_throttle(throttle_id, 60.0):
                log_debug(
                    self._logger,
                    "option_chain.backfill.no_worker_method",
                    symbol=self.display_symbol, instrument_symbol=self.symbol,
                    worker_type=type(worker).__name__,
                )
            return 0
        emit = self._backfill_emit or self.on_new_tick
        return int(
            cast(int, backfill(
                start_ts=int(start_ts),
                end_ts=int(end_ts),
                anchor_ts=int(target_ts),
                emit=emit,
            ))
        )


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------


def _now_ms() -> int:
    return int(time.time() * 1000)


def _tick_from_payload(payload: Mapping[str, Any], *, symbol: str, source_id: str | None = None) -> IngestionTick:
    data_ts = _infer_data_ts(payload)
    return IngestionTick(
        timestamp=int(data_ts),
        data_ts=int(data_ts),
        domain="option_chain",
        symbol=symbol,
        payload=payload,
        source_id=source_id,
    )


def _infer_data_ts(payload: Mapping[str, Any]) -> int:
    ts_any = payload.get("data_ts") or payload.get("timestamp")
    if ts_any is None:
        raise ValueError("Option chain payload missing data_ts/timestamp for backfill")
    return _coerce_epoch_ms(ts_any)


def _persist_option_chain_payload(persist, payload: Any, target_ts: int) -> None:
    if isinstance(payload, Mapping):
        data_ts_any = payload.get("data_ts") or payload.get("timestamp") or target_ts
        records = payload.get("records") or payload.get("chain") or payload.get("frame") or []
        if isinstance(records, pd.DataFrame):
            df = records
        else:
            df = pd.DataFrame(records)
        persist(df=df, data_ts=int(data_ts_any))
        return
    if isinstance(payload, pd.DataFrame):
        persist(df=payload, data_ts=int(target_ts))
        return
    try:
        df = pd.DataFrame(payload)
    except Exception:
        df = pd.DataFrame([])
    persist(df=df, data_ts=int(target_ts))


def _build_snapshot_from_payload(payload: Mapping[str, Any], *, symbol: str, market: MarketSpec) -> OptionChainSnapshot | None:
    d = {str(k): v for k, v in payload.items()}

    ts_any = d.get("data_ts") or d.get("timestamp")
    ts = int(ts_any) if ts_any is not None else _now_ms()

    chain = d.get("chain")
    if chain is None:
        chain = d.get("frame")
    if chain is None:
        chain = d.get("records")

    if isinstance(chain, list):
        chain = pd.DataFrame(chain)

    if not isinstance(chain, pd.DataFrame):
        return None

    try:
        return OptionChainSnapshot.from_chain_aligned(
            data_ts=ts,
            chain=chain,
            symbol=symbol,
            market=market,
            schema_version=int(d.get("schema_version") or 2),
        )
    except Exception:
        return None


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


def _resolve_source_id(
    *,
    source_id: Any | None,
    mode: EngineMode | None,
    data_root: Any | None,
    source: Any | None,
) -> str | None:
    if source_id is not None:
        return str(source_id)
    if mode == EngineMode.BACKTEST and data_root is not None:
        return str(data_root)
    if source is not None:
        return str(source)
    return None
