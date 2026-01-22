from __future__ import annotations

from abc import ABC, abstractmethod
from typing import AsyncIterator, Iterable, List
import asyncio
import threading
import traceback

from quant_engine.exceptions.core import FatalError
from quant_engine.runtime.lifecycle import LifecycleGuard, RuntimePhase
from quant_engine.runtime.modes import EngineSpec
from quant_engine.runtime.snapshot import EngineSnapshot
from quant_engine.strategy.engine import StrategyEngine
from quant_engine.utils.asyncio import cancel_tasks, set_loop_exception_handler
from quant_engine.utils.asyncio_health import start_asyncio_heartbeat
from quant_engine.utils.guards import format_exc, join_threads
from quant_engine.utils.logger import get_logger, log_error, log_exception


class BaseDriver(ABC):
    """
    Base class for all runtime drivers.

    Responsibilities:
      - Own runtime lifecycle ordering.
      - Own time progression only (no ingestion).
      - Never own strategy logic.
    """

    def __init__(
        self,
        *,
        engine: StrategyEngine,
        spec: EngineSpec,
        stop_event: threading.Event | None = None,
        shutdown_threads: list[threading.Thread] | None = None,
    ):
        self.engine = engine
        self.spec = spec
        self.guard = LifecycleGuard()
        self._stop_event = stop_event or threading.Event()
        self._shutdown_threads = shutdown_threads or []
        self._alerted = False
        self._logger = get_logger(self.__class__.__name__)
        self._background_tasks: list[asyncio.Task[object]] = []
        try:
            setattr(self.engine, "stop_event", self._stop_event)
        except Exception as exc:
            log_exception(
                self._logger,
                "runtime.stop_event_bind_failed",
                engine_type=type(self.engine).__name__,
                err_type=type(exc).__name__,
                err=str(exc),
            )

        # Runtime-owned snapshots (optional)
        self._snapshots: List[EngineSnapshot] = []

    @property
    def snapshots(self) -> List[EngineSnapshot]:
        """
        Collected EngineSnapshot objects produced during runtime.

        Semantics:
          - Owned by Driver.
          - Empty if snapshots are not collected.
          - Intended for backtest / debug / artifact layers.
        """
        return self._snapshots

    @property
    def stop_event(self) -> threading.Event:
        return self._stop_event

    # -------------------------------------------------
    # Hooks for subclasses
    # -------------------------------------------------

    @abstractmethod
    async def iter_timestamps(self) -> AsyncIterator[int]:
        """
        Yield engine-time timestamps (epoch ms int) in strictly increasing order.

        Must be implemented as an async generator.
        """
        raise NotImplementedError

    # -------------------------------------------------
    # Canonical runtime loop
    # -------------------------------------------------

    async def run(self) -> None:
        """
        Execute the canonical v4 runtime loop.

        PRELOAD → WARMUP → STEP* → FINISH
        """
        raise NotImplementedError

    def _shutdown_components(self) -> None:
        self._stop_event.set()

        for obj in self._iter_shutdown_objects():
            for method in ("close", "shutdown", "stop"):
                fn = getattr(obj, method, None)
                if callable(fn):
                    try:
                        fn()
                    except Exception as exc:
                        log_exception(
                            self._logger,
                            "runtime.shutdown_error",
                            target=type(obj).__name__,
                            method=method,
                            err_type=type(exc).__name__,
                            err=str(exc),
                        )

        if self._shutdown_threads:
            join_threads(self._shutdown_threads, timeout_s=2.0)

    def _install_loop_exception_handler(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        set_loop_exception_handler(
            loop,
            logger=self._logger,
            context={"driver": self.__class__.__name__},
            stop_event=self._stop_event,
        )

    def _start_asyncio_heartbeat(self) -> None:
        task = start_asyncio_heartbeat()
        if task is not None:
            self._background_tasks.append(task)

    async def _cancel_background_tasks(self) -> None:
        if not self._background_tasks:
            return
        await cancel_tasks(
            self._background_tasks,
            logger=self._logger,
            context={"driver": self.__class__.__name__},
        )
        self._background_tasks.clear()

    def _iter_shutdown_objects(self) -> Iterable[object]:
        if hasattr(self.engine, "iter_shutdown_objects"):
            yield from self.engine.iter_shutdown_objects()
            return
        yield self.engine

    def _collect_gap_labels(self, *, target_ts: int) -> list[str]:
        labels: list[str] = []
        domain_handlers = {
            "ohlcv": getattr(self.engine, "ohlcv_handlers", {}),
            "orderbook": getattr(self.engine, "orderbook_handlers", {}),
            "option_chain": getattr(self.engine, "option_chain_handlers", {}),
            "iv_surface": getattr(self.engine, "iv_surface_handlers", {}),
            "sentiment": getattr(self.engine, "sentiment_handlers", {}),
            "trades": getattr(self.engine, "trades_handlers", {}),
            "option_trades": getattr(self.engine, "option_trades_handlers", {}),
        }
        for domain, handlers in domain_handlers.items():
            if not handlers:
                continue
            for sym, h in handlers.items():
                should_backfill = getattr(h, "_should_backfill", None)
                if callable(should_backfill) and not should_backfill():
                    continue
                interval_ms = getattr(h, "interval_ms", None)
                if not isinstance(interval_ms, int) or interval_ms <= 0:
                    continue
                last_ts = h.last_timestamp() if hasattr(h, "last_timestamp") else None
                if last_ts is None:
                    labels.append(f"{domain}:{sym}")
                    continue
                if int(last_ts) < int(target_ts) - int(interval_ms):
                    labels.append(f"{domain}:{sym}")
        return labels

    def _catch_up_features(self, *, from_ts: int, to_ts: int) -> None:
        interval_ms = getattr(self.spec, "interval_ms", None)
        if not isinstance(interval_ms, int) or interval_ms <= 0:
            return
        ts = int(from_ts)
        while True:
            ts = int(self.spec.advance(ts))
            if ts > int(to_ts):
                break
            self.engine.feature_extractor.update(timestamp=int(ts))

    def _catch_up_once(self, *, from_ts: int, to_ts: int) -> list[str]:
        self.engine.align_to(int(to_ts))
        gaps = self._collect_gap_labels(target_ts=int(to_ts))
        self._catch_up_features(from_ts=int(from_ts), to_ts=int(to_ts))
        return gaps

    def _alert_once(self, exc: BaseException) -> None:
        if self._alerted:
            return
        self._alerted = True
        summary = format_exc(exc)
        stack = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        log_error(
            self._logger,
            "runtime.fatal_error",
            err_type=type(exc).__name__,
            err=summary,
            stack=stack,
        )

    def _handle_fatal(self, exc: BaseException) -> None:
        self._shutdown_components()
        self._alert_once(exc)
        if isinstance(exc, FatalError):
            raise
        raise FatalError(str(exc)) from exc
