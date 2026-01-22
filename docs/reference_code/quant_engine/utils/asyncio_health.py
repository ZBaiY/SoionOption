from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from quant_engine.utils.asyncio import create_task_named, get_to_thread_stats
from quant_engine.utils.logger import get_logger, log_info, log_warn

_DEFAULT_PERIOD_S = 2.0
_DEFAULT_WARN_LAG_MS = 100.0


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


async def _heartbeat_loop(
    *,
    loop: asyncio.AbstractEventLoop,
    logger,
    period_s: float,
    warn_lag_ms: float,
) -> None:
    try:
        while True:
            t0 = time.monotonic()
            await asyncio.sleep(period_s)
            lag_ms = max(0.0, (time.monotonic() - t0 - period_s) * 1000.0)
            payload: dict[str, Any] = {
                "loop_lag_ms": float(lag_ms),
                "tasks_total": len(asyncio.all_tasks(loop)),
                **get_to_thread_stats(loop),
            }
            if lag_ms >= warn_lag_ms:
                log_warn(logger, "asyncio.health", **payload)
            else:
                log_info(logger, "asyncio.health", **payload)
    except asyncio.CancelledError:
        return


def start_asyncio_heartbeat(
    *,
    logger=None,
    period_s: float = _DEFAULT_PERIOD_S,
    warn_lag_ms: float = _DEFAULT_WARN_LAG_MS,
    enabled: bool | None = None,
) -> asyncio.Task[None] | None:
    if enabled is None:
        enabled = _env_bool("ASYNCIO_HEARTBEAT_ENABLED", False)
    if not enabled:
        return None

    if logger is None:
        logger = get_logger("quant_engine.asyncio")

    loop = asyncio.get_running_loop()
    return create_task_named(
        _heartbeat_loop(
            loop=loop,
            logger=logger,
            period_s=float(period_s),
            warn_lag_ms=float(warn_lag_ms),
        ),
        name="runtime.asyncio_heartbeat",
        logger=get_logger(__name__),
        context={"task": "asyncio_heartbeat"},
    )
