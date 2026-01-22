from __future__ import annotations

from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.strategy.engine import StrategyEngine
from quant_engine.strategy.loader import StrategyLoader
from quant_engine.strategy.config import NormalizedStrategyCfg
from quant_engine.strategy.registry import get_strategy

from ingestion.ohlcv.worker import OHLCVWorker
from ingestion.ohlcv.source import OHLCVFileSource
from ingestion.ohlcv.normalize import BinanceOHLCVNormalizer
from ingestion.orderbook.worker import OrderbookWorker
from ingestion.orderbook.source import OrderbookFileSource
from ingestion.orderbook.normalize import BinanceOrderbookNormalizer
from ingestion.option_chain.worker import OptionChainWorker
from ingestion.option_chain.source import OptionChainFileSource
from ingestion.option_chain.normalize import DeribitOptionChainNormalizer
from ingestion.sentiment.worker import SentimentWorker
from ingestion.sentiment.source import SentimentFileSource
from ingestion.sentiment.normalize import SentimentNormalizer
from ingestion.contracts.tick import _to_interval_ms
from quant_engine.utils.cleaned_path_resolver import (
    base_asset_from_symbol,
    resolve_cleaned_paths,
    symbol_from_base_asset,
)
from quant_engine.utils.paths import data_root_from_file

from pathlib import Path
import copy
from typing import Any


def _index_domain_cfgs(cfg: Any) -> dict[str, dict[str, dict[str, Any]]]:
    """Index normalized strategy cfg blocks by domain and symbol."""
    out: dict[str, dict[str, dict[str, Any]]] = {}
    if isinstance(cfg, dict):
        data = cfg.get("data")
        primary_symbol = cfg.get("symbol")
    else:
        data = getattr(cfg, "data", None)
        primary_symbol = getattr(cfg, "symbol", None)
    if not isinstance(data, dict):
        return out

    primary = data.get("primary")
    if isinstance(primary_symbol, str) and primary_symbol and isinstance(primary, dict):
        for domain, block in primary.items():
            if isinstance(block, dict):
                out.setdefault(domain, {})[primary_symbol] = block

    secondary = data.get("secondary")
    if isinstance(secondary, dict):
        for symbol, block in secondary.items():
            if not isinstance(symbol, str) or not symbol:
                continue
            if not isinstance(block, dict):
                continue
            for domain, domain_block in block.items():
                if isinstance(domain_block, dict):
                    out.setdefault(domain, {})[symbol] = domain_block

    return out


def _inject_data_root(data_spec: dict[str, Any], data_root: Path) -> dict[str, Any]:
    """Attach data_root to handler configs without overriding explicit roots."""
    out = copy.deepcopy(data_spec)

    def _inject_section(section: dict[str, Any]) -> None:
        for _, block in section.items():
            if not isinstance(block, dict):
                continue
            if "data_root" not in block and "cleaned_root" not in block:
                block["data_root"] = str(data_root)

    primary = out.get("primary")
    if isinstance(primary, dict):
        _inject_section(primary)

    secondary = out.get("secondary")
    if isinstance(secondary, dict):
        for sec_block in secondary.values():
            if isinstance(sec_block, dict):
                _inject_section(sec_block)

    return out


def _build_backtest_ingestion_plan(
    engine: StrategyEngine,
    *,
    data_root: Path,
    start_ts: int,
    end_ts: int,
    require_local_data: bool,
    domain_cfgs: dict[str, dict[str, dict[str, Any]]] | None = None,
) -> list[dict[str, Any]]:
    plan: list[dict[str, Any]] = []

    def _cfg_for(domain: str, symbol: str) -> dict[str, Any]:
        if not domain_cfgs:
            return {}
        domain_map = domain_cfgs.get(domain, {})
        if symbol in domain_map:
            return domain_map[symbol]
        base = base_asset_from_symbol(symbol)
        if base in domain_map:
            return domain_map[base]
        full = symbol_from_base_asset(symbol)
        if full in domain_map:
            return domain_map[full]
        return {}

    def _interval_for(domain: str, symbol: str, handler: Any | None = None) -> str | None:
        if handler is not None:
            interval = getattr(handler, "interval", None)
            if isinstance(interval, str) and interval:
                return interval
        cfg = _cfg_for(domain, symbol)
        interval = cfg.get("interval")
        return interval if isinstance(interval, str) and interval else None

    def _symbol_for_paths(symbol: str) -> str:
        return symbol_from_base_asset(symbol)

    def _asset_for(domain: str, symbol: str, handler: Any | None = None) -> str:
        cfg = _cfg_for(domain, symbol)
        for key in ("asset", "currency", "underlying", "symbol"):
            val = cfg.get(key)
            if val:
                return base_asset_from_symbol(str(val))
        market = getattr(handler, "market", None) if handler is not None else None
        currency = getattr(market, "currency", None)
        if currency:
            return base_asset_from_symbol(str(currency))
        return base_asset_from_symbol(symbol)

    def _provider_for(symbol: str, handler: Any | None = None) -> str:
        cfg = _cfg_for("sentiment", symbol)
        for key in ("provider", "source", "venue"):
            val = cfg.get(key)
            if val:
                return str(val)
        market = getattr(handler, "market", None) if handler is not None else None
        venue = getattr(market, "venue", None)
        if venue and str(venue).lower() != "unknown":
            return str(venue)
        return symbol

    # -------------------------
    # OHLCV ingestion
    # -------------------------
    for symbol, handler in engine.ohlcv_handlers.items():
        interval = _interval_for("ohlcv", symbol, handler)
        if not interval:
            raise ValueError(f"Missing OHLCV interval for symbol {symbol!r}")
        interval_ms = _to_interval_ms(interval)
        if interval_ms is None:
            raise ValueError(f"Invalid OHLCV interval {interval!r} for symbol {symbol!r}")
        root = data_root / "cleaned" / "ohlcv"
        symbol_for_paths = _symbol_for_paths(symbol)
        paths = resolve_cleaned_paths(
            data_root=data_root,
            domain="ohlcv",
            symbol=symbol_for_paths,
            interval=interval,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        existing_paths = [p for p in paths if p.exists()]
        has_local_data = True if not require_local_data else bool(existing_paths)

        def _build_worker_ohlcv(
            *,
            symbol: str = symbol,
            interval: str = interval,
            interval_ms: int = int(interval_ms),
            root: Path = root,
            paths: list[Path] = existing_paths,
            start_ts: int = start_ts,
            end_ts: int = end_ts,
        ):
            source = OHLCVFileSource(
                root=root,
                symbol=symbol,
                interval=interval,
                start_ts=start_ts,
                end_ts=end_ts,
                paths=paths,
            )
            normalizer = BinanceOHLCVNormalizer(symbol=symbol)
            return OHLCVWorker(
                source=source,
                normalizer=normalizer,
                symbol=symbol,
                interval=interval,
                interval_ms=interval_ms,
                poll_interval=None,  # backtest: do not throttle
            )

        plan.append(
            {
                "domain": "ohlcv",
                "symbol": symbol,
                "source_type": "OHLCVFileSource",
                "root": str(root),
                "interval": interval,
                "interval_ms": int(interval_ms),
                "has_local_data": has_local_data,
                "paths": existing_paths,
                "build_worker": _build_worker_ohlcv,
                "start_ts": start_ts,
                "end_ts": end_ts,
            }
        )
    # -------------------------
    # Orderbook ingestion
    # -------------------------
    for symbol, handler in engine.orderbook_handlers.items():
        root = data_root / "cleaned" / "orderbook"
        interval = _interval_for("orderbook", symbol, handler)
        interval_ms = _to_interval_ms(interval) if interval else None
        if interval and interval_ms is None:
            raise ValueError(f"Invalid orderbook interval {interval!r} for symbol {symbol!r}")
        symbol_for_paths = _symbol_for_paths(symbol)
        paths = resolve_cleaned_paths(
            data_root=data_root,
            domain="orderbook",
            symbol=symbol_for_paths,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        existing_paths = [p for p in paths if p.exists()]
        has_local_data = True if not require_local_data else bool(existing_paths)

        def _build_worker_orderbook(
            *,
            symbol: str = symbol,
            interval: str | None = interval,
            interval_ms: int | None = int(interval_ms) if interval_ms is not None else None,
            root: Path = root,
            paths: list[Path] = existing_paths,
            start_ts: int = start_ts,
            end_ts: int = end_ts,
        ):
            source = OrderbookFileSource(
                root=root,
                symbol=symbol,
                start_ts=start_ts,
                end_ts=end_ts,
                paths=paths,
            )
            normalizer = BinanceOrderbookNormalizer(symbol=symbol)
            return OrderbookWorker(
                source=source,
                normalizer=normalizer,
                symbol=symbol,
                interval=interval,
                interval_ms=interval_ms,
                poll_interval=None,  # backtest: do not throttle
            )

        plan.append(
            {
                "domain": "orderbook",
                "symbol": symbol,
                "source_type": "OrderbookFileSource",
                "root": str(root),
                "interval": interval,
                "interval_ms": int(interval_ms) if interval_ms is not None else None,
                "has_local_data": has_local_data,
                "paths": existing_paths,
                "build_worker": _build_worker_orderbook,
                "start_ts": start_ts,
                "end_ts": end_ts,
            }
        )
    

    # -------------------------
    # Option chain ingestion
    # -------------------------
    for symbol, handler in engine.option_chain_handlers.items():
        root = data_root / "cleaned" / "option_chain"
        interval = _interval_for("option_chain", symbol, handler)
        if not interval:
            raise ValueError(f"Missing option_chain interval for symbol {symbol!r}")
        interval_ms = _to_interval_ms(interval)
        if interval_ms is None:
            raise ValueError(f"Invalid option_chain interval {interval!r} for symbol {symbol!r}")
        asset = _asset_for("option_chain", symbol, handler)
        paths = resolve_cleaned_paths(
            data_root=data_root,
            domain="option_chain",
            asset=asset,
            interval=interval,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        existing_paths = [p for p in paths if p.exists()]
        has_local_data = True if not require_local_data else bool(existing_paths)

        def _build_worker_option_chain(
            *,
            symbol: str = symbol,
            asset: str = asset,
            interval: str = interval,
            interval_ms: int = int(interval_ms),
            root: Path = root,
            paths: list[Path] = existing_paths,
            start_ts: int = start_ts,
            end_ts: int = end_ts,
        ):
            source = OptionChainFileSource(
                root=root,
                asset=asset,
                interval=interval,
                start_ts=start_ts,
                end_ts=end_ts,
                paths=paths,
            )
            normalizer = DeribitOptionChainNormalizer(symbol=symbol)
            return OptionChainWorker(
                source=source,
                normalizer=normalizer,
                symbol=symbol,
                interval=interval,
                interval_ms=interval_ms,
                poll_interval=None,  # backtest: do not throttle
            )

        plan.append(
            {
                "domain": "option_chain",
                "symbol": symbol,
                "asset": asset,
                "source_type": "OptionChainFileSource",
                "root": str(root),
                "interval": interval,
                "interval_ms": int(interval_ms),
                "has_local_data": has_local_data,
                "paths": existing_paths,
                "build_worker": _build_worker_option_chain,
                "start_ts": start_ts,
                "end_ts": end_ts,
            }
        )

    # -------------------------
    # Sentiment ingestion
    # -------------------------
    for symbol, handler in engine.sentiment_handlers.items():
        root = data_root / "cleaned" / "sentiment"
        interval = _interval_for("sentiment", symbol, handler)
        interval_ms = _to_interval_ms(interval) if interval else None
        if interval and interval_ms is None:
            raise ValueError(f"Invalid sentiment interval {interval!r} for symbol {symbol!r}")
        provider = _provider_for(symbol, handler)
        paths = resolve_cleaned_paths(
            data_root=data_root,
            domain="sentiment",
            provider=provider,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        existing_paths = [p for p in paths if p.exists()]
        has_local_data = True if not require_local_data else bool(existing_paths)

        def _build_worker_sentiment(
            *,
            symbol: str = symbol,
            provider: str = provider,
            interval: str | None = interval,
            interval_ms: int | None = int(interval_ms) if interval_ms is not None else None,
            root: Path = root,
            paths: list[Path] = existing_paths,
            start_ts: int = start_ts,
            end_ts: int = end_ts,
        ):
            source = SentimentFileSource(
                root=root,
                provider=provider,
                start_ts=start_ts,
                end_ts=end_ts,
                paths=paths,
            )
            normalizer = SentimentNormalizer(symbol=symbol, provider=provider)
            return SentimentWorker(
                source=source,
                normalizer=normalizer,
                interval=interval,
                interval_ms=interval_ms,
                poll_interval=None,  # backtest: do not throttle
            )

        plan.append(
            {
                "domain": "sentiment",
                "symbol": symbol,
                "source_type": "SentimentFileSource",
                "root": str(root),
                "interval": interval,
                "interval_ms": int(interval_ms) if interval_ms is not None else None,
                "has_local_data": has_local_data,
                "paths": existing_paths,
                "build_worker": _build_worker_sentiment,
                "start_ts": start_ts,
                "end_ts": end_ts,
            }
        )

    return plan


def build_backtest_engine(
    *,
    strategy_name: str = "EXAMPLE",
    bind_symbols: dict[str, str] | None = None,
    overrides: dict | None = None,
    start_ts: int = 1622505600000,
    end_ts: int = 1622592000000,
    data_root: Path | None = None,
    require_local_data: bool = True,
    engine_spec: EngineSpec | None = None,
) -> tuple[StrategyEngine, dict[str, int], list[dict[str, Any]]]:
    StrategyCls = get_strategy(strategy_name)
    
    if data_root is None:
        data_root = data_root_from_file(__file__, levels_up=4)
    
    cfg = StrategyCls.standardize(overrides=overrides or {}, symbols=bind_symbols)
    cfg_for_loader: Any = cfg
    cfg_for_domains: Any = cfg
    if data_root is not None:
        cfg_dict = cfg.to_dict() if isinstance(cfg, NormalizedStrategyCfg) else dict(cfg)
        data_spec = cfg_dict.get("data")
        if isinstance(data_spec, dict):
            cfg_dict["data"] = _inject_data_root(data_spec, data_root)
        cfg_for_loader = cfg_dict
        cfg_for_domains = cfg_dict

    mode = engine_spec.mode if engine_spec is not None else EngineMode.BACKTEST
    engine = StrategyLoader.from_config(
        strategy=cfg_for_loader,
        mode=mode,
        overrides={},
    )
    driver_cfg = {"start_ts": int(start_ts), "end_ts": int(end_ts)}
    domain_cfgs = _index_domain_cfgs(cfg_for_domains)
    ingestion_plan = _build_backtest_ingestion_plan(
        engine,
        data_root=data_root,
        start_ts=int(start_ts),
        end_ts=int(end_ts),
        require_local_data=require_local_data,
        domain_cfgs=domain_cfgs,
    )
    return engine, driver_cfg, ingestion_plan
