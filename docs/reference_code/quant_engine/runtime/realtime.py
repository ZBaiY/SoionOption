from __future__ import annotations

import time
import asyncio
import threading
from quant_engine.runtime.driver import BaseDriver
from quant_engine.runtime.lifecycle import RuntimePhase
from quant_engine.runtime.modes import EngineSpec
from quant_engine.runtime.snapshot import EngineSnapshot
from collections.abc import AsyncIterator
from quant_engine.strategy.engine import StrategyEngine
from quant_engine.utils.asyncio import create_task_named, loop_lag_monitor, to_thread_limited
from quant_engine.utils.logger import log_error

LOOP_LAG_INTERVAL_S = 1.0
LOOP_LAG_WARN_S = 0.2

class RealtimeDriver(BaseDriver):
    """
    Realtime trading driver (v4).

    Semantics:
      - Engine-time advances according to EngineSpec.advance().
      - Runtime loop is open-ended.
      - Ingestion is external to runtime (apps / wiring layer).
    """

    def __init__(
        self,
        *,
        engine: StrategyEngine,
        spec: EngineSpec,
        stop_event: threading.Event | None = None,
    ):
        super().__init__(engine=engine, spec=spec, stop_event=stop_event)

    # -------------------------------------------------
    # Time progression
    # -------------------------------------------------

    async def iter_timestamps(self) -> AsyncIterator[int]:
        """Yield engine-time timestamps (epoch ms int) in strictly increasing order.

        Realtime semantics:
          - Driver owns the strategy observation clock.
          - We pace steps to wall-clock using sleep.
          - If the process is delayed (e.g. long step), we do not busy-loop.
        """
        override = getattr(self, "_start_ts_override", None)
        current_ts = int(override) if override is not None else (
            int(self.spec.timestamp) if self.spec.timestamp is not None else int(time.time() * 1000)
        )
        if override is not None:
            self._start_ts_override = None

        while True:
            yield current_ts

            next_ts = int(self.spec.advance(current_ts))

            # Pace to wall-clock.
            now = int(time.time() * 1000)
            sleep_ms = next_ts - now
            if sleep_ms > 0:
                await asyncio.sleep(sleep_ms / 1000.0)
            else:
                # We're late; yield control to let ingestion tasks run,
                # but do not spin.
                await asyncio.sleep(0)

            current_ts = next_ts
    
    async def run(self) -> None:
        anchor_ts = int(self.spec.timestamp) if self.spec.timestamp is not None else int(time.time() * 1000)
        self._install_loop_exception_handler()
        self._start_asyncio_heartbeat()
        self._background_tasks.append(
            create_task_named(
                loop_lag_monitor(
                    interval_s=LOOP_LAG_INTERVAL_S,
                    warn_after_s=LOOP_LAG_WARN_S,
                    logger=self._logger,
                    context={"driver": self.__class__.__name__},
                ),
                name="runtime.loop_lag_monitor",
                logger=self._logger,
                context={"driver": self.__class__.__name__},
                stop_event=self.stop_event,
            )
        )
        try:
            self.guard.enter(RuntimePhase.PRELOAD)
            # Intentional sync: keep engine single-threaded until thread-safe preload exists.
            self.engine.bootstrap(anchor_ts=anchor_ts)

            # -------- warmup --------
            self.guard.enter(RuntimePhase.WARMUP)
            # Intentional sync: warmup mutates engine state; avoid cross-thread access.
            self.engine.warmup_features(anchor_ts=anchor_ts)

            # -------- catch-up (realtime/mock only) --------
            last_ts = int(anchor_ts)
            max_rounds = 3
            for round_idx in range(max_rounds):
                now_ts = int(self.spec.timestamp) if self.spec.timestamp is not None else int(time.time() * 1000)
                if now_ts <= last_ts:
                    break
                gaps = self._catch_up_once(from_ts=last_ts, to_ts=now_ts)
                last_ts = int(now_ts)
                if gaps and round_idx == max_rounds - 1:
                    log_error(
                        self._logger,
                        "runtime.catchup.gaps_remaining",
                        driver=self.__class__.__name__,
                        missing=gaps,
                        target_ts=int(now_ts),
                    )
                    self.stop_event.set()
                    break

            if last_ts != int(anchor_ts):
                self._start_ts_override = int(last_ts)

            # -------- main loop --------
            async for ts in self.iter_timestamps():
                if self.stop_event.is_set():
                    break
                self.guard.enter(RuntimePhase.INGEST)
                self.guard.enter(RuntimePhase.STEP)

                await to_thread_limited(
                    self.engine.align_to,
                    ts,
                    logger=self._logger,
                    context={"driver": self.__class__.__name__},
                    op="align_to",
                )
                # Intentional sync: step must remain single-threaded to preserve engine invariants.
                result = self.engine.step(ts=ts)

                # Yield to the event loop so background ingestion tasks can run
                # even if step() is CPU-heavy.
                await asyncio.sleep(0)

                if not isinstance(result, EngineSnapshot):
                    raise TypeError(f"engine.step() must return EngineSnapshot, got {type(result).__name__}")
                self._snapshots.append(result)
        except asyncio.CancelledError:
            self._shutdown_components()
            raise
        except Exception as exc:
            self._handle_fatal(exc)
        finally:
            await self._cancel_background_tasks()

        # -------- finish --------
        self.guard.enter(RuntimePhase.FINISH)
