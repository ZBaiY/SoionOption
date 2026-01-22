#!/usr/bin/env python3
# DEMO-ONLY MODULE
# Intended final location: TBD (demo runner; will be removed or replaced by SoionLab CLI entrypoint)
"""
SoionOption Demo Entrypoint

Minimal runner that demonstrates the scaffolding pipeline.
Writes artifacts and logs to artifacts/ directory.

Usage:
    python -m apps.run_option_qc --run_id DEMO1 --dataset sample --symbol BTC --interval 5m --mode qc
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="SoionOption Demo - Option QC Pipeline Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--run_id",
        required=True,
        help="Unique run identifier (e.g., DEMO1)",
    )
    parser.add_argument(
        "--dataset",
        default="sample",
        choices=["sample", "cleaned", "raw"],
        help="Data stage to use (default: sample)",
    )
    parser.add_argument(
        "--symbol",
        default="BTC",
        help="Symbol/asset to process (default: BTC)",
    )
    parser.add_argument(
        "--interval",
        default="5m",
        help="Data interval (default: 5m)",
    )
    parser.add_argument(
        "--mode",
        default="qc",
        choices=["qc", "mc", "regime", "full"],
        help="Pipeline mode (default: qc)",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to config JSON file (optional)",
    )
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=None,
        help="Artifacts output directory (default: artifacts/)",
    )

    args = parser.parse_args()

    # Resolve project root and artifacts directory
    project_root = Path(__file__).resolve().parent.parent.parent
    artifacts_dir = args.artifacts_dir or (project_root / "artifacts")
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    # Initialize logging
    import logging
    from quant_engine.utils.logger import get_logger, log_info, log_exception

    # Setup basic file logging to artifacts
    log_file = artifacts_dir / f"{args.run_id}.log"
    file_handler = logging.FileHandler(str(log_file), encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    ))
    logging.getLogger().addHandler(file_handler)
    logging.getLogger().setLevel(logging.INFO)

    logger = get_logger(__name__)
    log_info(
        logger,
        "run_option_qc started",
        run_id=args.run_id,
        dataset=args.dataset,
        symbol=args.symbol,
        interval=args.interval,
        mode=args.mode,
    )

    # Load config if provided
    config: dict = {}
    if args.config and args.config.exists():
        with open(args.config) as f:
            config = json.load(f)
        log_info(logger, "Loaded config", path=str(args.config))

    try:
        # Import and run the demo pipeline
        from soion_option_demo.pipeline import DemoPipeline

        pipeline = DemoPipeline(
            run_id=args.run_id,
            symbol=args.symbol,
            interval=args.interval,
            mode=args.mode,
            config=config,
        )

        result = pipeline.run(artifacts_dir=artifacts_dir)

        # Write final status
        status = {
            "run_id": args.run_id,
            "status": "success",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "result": result,
        }
        status_path = artifacts_dir / f"{args.run_id}_status.json"
        with open(status_path, "w") as f:
            json.dump(status, f, indent=2)

        log_info(
            logger,
            "run_option_qc completed successfully",
            run_id=args.run_id,
            status_path=str(status_path),
        )

        print(f"[OK] Run {args.run_id} completed. Artifacts in: {artifacts_dir}")
        return 0

    except Exception as e:
        log_exception(logger, "run_option_qc failed", error=str(e))

        # Write error status
        status = {
            "run_id": args.run_id,
            "status": "error",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": str(e),
        }
        status_path = artifacts_dir / f"{args.run_id}_status.json"
        with open(status_path, "w") as f:
            json.dump(status, f, indent=2)

        print(f"[ERROR] Run {args.run_id} failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
