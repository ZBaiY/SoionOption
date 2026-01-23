#!/usr/bin/env python3
"""
Data Cache Initialization Script

Initializes handlers and ingestion workers to load data correctly into cache.
Supports both file-based (backtest/sample) and REST-based (realtime) ingestion.

Usage:
    python scripts/init_data_cache.py --symbol BTC --interval 5m --mode sample
    python scripts/init_data_cache.py --symbol BTC --interval 1h --mode cleaned
    python scripts/init_data_cache.py --symbol BTC --interval 1m --mode realtime --poll-count 5
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Add src to path for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.option_chain.source import OptionChainFileSource, DeribitOptionChainRESTSource
from ingestion.option_chain.normalize import DeribitOptionChainNormalizer
from ingestion.contracts.tick import IngestionTick
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.derivatives.iv.iv_handler import IVSurfaceDataHandler
from quant_engine.utils.logger import get_logger, log_info, log_debug, log_warn


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging for the script."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    return get_logger(__name__)


def now_ms() -> int:
    """Current timestamp in milliseconds."""
    return int(time.time() * 1000)


def create_option_chain_handler(
    symbol: str,
    interval: str,
    mode: str,
    cache_kind: str = "expiry",
    maxlen: int = 512,
    per_expiry_maxlen: int = 256,
    source_id: str | None = None,
) -> OptionChainDataHandler:
    """
    Create and configure an OptionChainDataHandler.

    Args:
        symbol: Asset symbol (e.g., 'BTC')
        interval: Data interval (e.g., '5m', '1h')
        mode: Engine mode ('sample', 'cleaned', 'realtime', 'backtest')
        cache_kind: Cache strategy ('simple', 'expiry', 'term')
        maxlen: Global cache size
        per_expiry_maxlen: Per-expiry bucket size (for expiry/term caches)
        source_id: Source identifier for tick filtering (None accepts all ticks)

    Returns:
        Configured OptionChainDataHandler instance
    """
    cache_cfg = {
        "kind": cache_kind,
        "maxlen": maxlen,
        "per_expiry_maxlen": per_expiry_maxlen,
    }

    if cache_kind == "term":
        cache_cfg["per_term_maxlen"] = per_expiry_maxlen
        cache_cfg["term_bucket_ms"] = 86_400_000  # 1 day buckets
        cache_cfg["enable_expiry_index"] = True

    bootstrap_cfg = {
        "lookback": {"bars": min(100, maxlen)},
    }

    handler = OptionChainDataHandler(
        symbol=symbol,
        source="DERIBIT",
        interval=interval,
        mode=mode.upper() if mode != "cleaned" else "BACKTEST",
        cache=cache_cfg,
        bootstrap=bootstrap_cfg,
        asset=symbol,
    )

    # Override source_id after creation if specified
    # Setting to None disables source filtering (accepts all ticks)
    if source_id is None:
        handler.source_id = None

    return handler


def create_iv_surface_handler(
    symbol: str,
    interval: str,
    chain_handler: OptionChainDataHandler,
    mode: str,
    max_bars: int = 1000,
) -> IVSurfaceDataHandler:
    """
    Create and configure an IVSurfaceDataHandler.

    The IV surface handler is derived from the option chain handler and
    computes IV surfaces from chain snapshots.

    Args:
        symbol: Asset symbol
        interval: Data interval
        chain_handler: Parent OptionChainDataHandler
        mode: Engine mode
        max_bars: Maximum bars in cache

    Returns:
        Configured IVSurfaceDataHandler instance
    """
    handler = IVSurfaceDataHandler(
        symbol=symbol,
        chain_handler=chain_handler,
        interval=interval,
        mode=mode.upper() if mode != "cleaned" else "BACKTEST",
        model_name="CHAIN_DERIVED",
        cache={"max_bars": max_bars},
        bootstrap={"lookback": {"bars": min(50, max_bars)}},
    )

    return handler


def create_file_source(
    asset: str,
    interval: str,
    mode: str,
    start_ts: int | None = None,
    end_ts: int | None = None,
) -> OptionChainFileSource:
    """
    Create a file-based ingestion source.

    Args:
        asset: Asset symbol
        interval: Data interval
        mode: Data stage ('sample', 'cleaned', 'raw')
        start_ts: Start timestamp (epoch ms)
        end_ts: End timestamp (epoch ms)

    Returns:
        Configured OptionChainFileSource instance
    """
    root_map = {
        "sample": "sample/option_chain",
        "cleaned": "cleaned/option_chain",
        "raw": "raw/option_chain",
    }
    root = root_map.get(mode, "cleaned/option_chain")

    source = OptionChainFileSource(
        root=root,
        asset=asset,
        interval=interval,
        start_ts=start_ts,
        end_ts=end_ts,
    )

    return source


def create_rest_source(
    currency: str,
    interval: str,
    stop_event=None,
) -> DeribitOptionChainRESTSource:
    """
    Create a REST-based ingestion source for Deribit.

    Args:
        currency: Currency (e.g., 'BTC', 'ETH')
        interval: Polling interval
        stop_event: Optional threading.Event to signal stop

    Returns:
        Configured DeribitOptionChainRESTSource instance
    """
    source = DeribitOptionChainRESTSource(
        currency=currency,
        interval=interval,
        kind="option",
        expired=False,
        stop_event=stop_event,
    )

    return source


def load_from_files(
    handler: OptionChainDataHandler,
    source: OptionChainFileSource,
    normalizer: DeribitOptionChainNormalizer,
    logger: logging.Logger,
    anchor_ts: int | None = None,
) -> int:
    """
    Load data from file source into handler cache.

    Args:
        handler: Target OptionChainDataHandler
        source: File source to read from
        normalizer: Normalizer for raw payloads
        logger: Logger instance
        anchor_ts: Optional anchor timestamp for anti-lookahead

    Returns:
        Number of snapshots loaded
    """
    count = 0

    if anchor_ts is not None:
        handler.align_to(anchor_ts)

    for raw_payload in source:
        try:
            tick = normalizer.normalize(raw=raw_payload)
            handler.on_new_tick(tick)
            count += 1

            if count % 100 == 0:
                log_debug(
                    logger,
                    "Loading progress",
                    count=count,
                    data_ts=tick.data_ts,
                )
        except Exception as e:
            log_warn(
                logger,
                "Failed to process payload",
                error=str(e),
            )

    return count


def load_from_rest(
    handler: OptionChainDataHandler,
    source: DeribitOptionChainRESTSource,
    normalizer: DeribitOptionChainNormalizer,
    logger: logging.Logger,
    max_polls: int = 10,
) -> int:
    """
    Load data from REST source into handler cache.

    Args:
        handler: Target OptionChainDataHandler
        source: REST source to poll
        normalizer: Normalizer for raw payloads
        logger: Logger instance
        max_polls: Maximum number of polls

    Returns:
        Number of snapshots loaded
    """
    import threading

    count = 0
    stop_event = threading.Event()

    try:
        for raw_payload in source:
            try:
                tick = normalizer.normalize(raw=raw_payload)
                handler.on_new_tick(tick)
                count += 1

                log_info(
                    logger,
                    "Received REST snapshot",
                    count=count,
                    data_ts=tick.data_ts,
                    records=len(raw_payload.get("records", [])),
                )

                if count >= max_polls:
                    log_info(logger, "Reached max poll count", max_polls=max_polls)
                    break

            except Exception as e:
                log_warn(
                    logger,
                    "Failed to process REST payload",
                    error=str(e),
                )
    except KeyboardInterrupt:
        log_info(logger, "Interrupted by user")
    finally:
        stop_event.set()

    return count


def derive_iv_surfaces(
    chain_handler: OptionChainDataHandler,
    iv_handler: IVSurfaceDataHandler,
    logger: logging.Logger,
) -> int:
    """
    Derive IV surfaces from loaded option chain data.

    Args:
        chain_handler: Source option chain handler
        iv_handler: Target IV surface handler
        logger: Logger instance

    Returns:
        Number of IV surfaces derived
    """
    count = 0

    # Get all timestamps from chain cache
    chain_window = chain_handler.window(n=chain_handler.cache.maxlen)

    for snap in chain_window:
        ts = int(snap.data_ts)

        # Create a trigger tick for IV derivation
        tick = IngestionTick(
            timestamp=ts,
            data_ts=ts,
            domain="iv_surface",
            symbol=iv_handler.symbol,
            payload={"trigger": True},
            source_id=iv_handler.source_id,
        )

        iv_handler.on_new_tick(tick)
        count += 1

    return count


def print_cache_summary(
    handler: OptionChainDataHandler,
    iv_handler: IVSurfaceDataHandler | None,
    logger: logging.Logger,
) -> None:
    """Print summary of loaded cache state."""
    print("\n" + "=" * 60)
    print("CACHE SUMMARY")
    print("=" * 60)

    # Option chain summary
    print(f"\nOption Chain Handler ({handler.symbol}):")
    print(f"  Cache size: {len(handler.cache.buffer)}")
    print(f"  Last timestamp: {handler.last_timestamp()}")

    if hasattr(handler.cache, "expiries"):
        expiries = handler.expiries()
        print(f"  Expiries tracked: {len(expiries)}")
        if expiries:
            print(f"  Sample expiries: {expiries[:5]}")

    if hasattr(handler.cache, "term_buckets"):
        buckets = handler.term_buckets()
        print(f"  Term buckets: {len(buckets)}")
        if buckets:
            print(f"  Sample buckets (days): {[b // 86_400_000 for b in buckets[:5]]}")

    # Get latest snapshot
    snap = handler.get_snapshot()
    if snap is not None:
        print(f"\n  Latest snapshot:")
        print(f"    data_ts: {snap.data_ts}")
        print(f"    records: {len(snap.frame) if snap.frame is not None else 0}")

        if snap.frame is not None and len(snap.frame) > 0:
            expiries_in_snap = snap.frame["expiry_ts"].unique()
            print(f"    expiries in snapshot: {len(expiries_in_snap)}")

    # IV surface summary
    if iv_handler is not None:
        print(f"\nIV Surface Handler ({iv_handler.symbol}):")
        print(f"  Cache size: {len(iv_handler._snapshots)}")
        print(f"  Last timestamp: {iv_handler.last_timestamp()}")

        iv_snap = iv_handler.get_snapshot()
        if iv_snap is not None:
            print(f"\n  Latest IV surface:")
            print(f"    data_ts: {iv_snap.data_ts}")
            print(f"    ATM IV: {iv_snap.atm_iv:.4f}")
            print(f"    Skew: {iv_snap.skew:.4f}")
            if iv_snap.curve:
                print(f"    Smile points: {len(iv_snap.curve)}")

    print("\n" + "=" * 60)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Initialize handlers and ingestion workers to load data into cache",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--symbol",
        default="BTC",
        help="Asset symbol (default: BTC)",
    )
    parser.add_argument(
        "--interval",
        default="5m",
        help="Data interval (default: 5m)",
    )
    parser.add_argument(
        "--mode",
        default="cleaned",
        choices=["sample", "cleaned", "raw", "realtime"],
        help="Data mode/stage (default: cleaned)",
    )
    parser.add_argument(
        "--cache-kind",
        default="expiry",
        choices=["simple", "expiry", "term"],
        help="Cache strategy (default: expiry)",
    )
    parser.add_argument(
        "--maxlen",
        type=int,
        default=512,
        help="Cache max length (default: 512)",
    )
    parser.add_argument(
        "--poll-count",
        type=int,
        default=5,
        help="Number of REST polls for realtime mode (default: 5)",
    )
    parser.add_argument(
        "--derive-iv",
        action="store_true",
        help="Also derive IV surfaces from chain data",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date for file loading (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date for file loading (YYYY-MM-DD)",
    )

    args = parser.parse_args()
    logger = setup_logging(args.verbose)

    log_info(
        logger,
        "Initializing data cache",
        symbol=args.symbol,
        interval=args.interval,
        mode=args.mode,
        cache_kind=args.cache_kind,
    )

    # Parse optional date range
    start_ts = None
    end_ts = None
    if args.start_date:
        start_ts = int(datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    if args.end_date:
        end_ts = int(datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)

    # Create handlers
    # For file-based modes, use source_id=None to accept all ticks
    # For realtime mode, we'd normally use source_id="DERIBIT" but the normalizer
    # doesn't set source_id, so we use None for compatibility
    source_id = None

    log_info(logger, "Creating OptionChainDataHandler")
    chain_handler = create_option_chain_handler(
        symbol=args.symbol,
        interval=args.interval,
        mode=args.mode,
        cache_kind=args.cache_kind,
        maxlen=args.maxlen,
        source_id=source_id,
    )

    iv_handler = None
    if args.derive_iv:
        log_info(logger, "Creating IVSurfaceDataHandler")
        iv_handler = create_iv_surface_handler(
            symbol=args.symbol,
            interval=args.interval,
            chain_handler=chain_handler,
            mode=args.mode,
        )

    # Create normalizer
    normalizer = DeribitOptionChainNormalizer(symbol=args.symbol)

    # Load data based on mode
    if args.mode == "realtime":
        log_info(logger, "Starting REST-based ingestion from Deribit")
        source = create_rest_source(
            currency=args.symbol,
            interval=args.interval,
        )
        loaded = load_from_rest(
            handler=chain_handler,
            source=source,
            normalizer=normalizer,
            logger=logger,
            max_polls=args.poll_count,
        )
    else:
        log_info(logger, "Starting file-based ingestion", stage=args.mode)
        source = create_file_source(
            asset=args.symbol,
            interval=args.interval,
            mode=args.mode,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        loaded = load_from_files(
            handler=chain_handler,
            source=source,
            normalizer=normalizer,
            logger=logger,
            anchor_ts=end_ts or now_ms(),
        )

    log_info(logger, "Data loading complete", snapshots_loaded=loaded)

    # Derive IV surfaces if requested
    if iv_handler is not None and loaded > 0:
        log_info(logger, "Deriving IV surfaces")
        iv_count = derive_iv_surfaces(chain_handler, iv_handler, logger)
        log_info(logger, "IV derivation complete", surfaces_derived=iv_count)

    # Print summary
    print_cache_summary(chain_handler, iv_handler, logger)

    return 0


if __name__ == "__main__":
    sys.exit(main())
