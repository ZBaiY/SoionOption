from __future__ import annotations

from typing import Iterable, AsyncIterator, cast
import asyncio
import threading

from quant_engine.runtime.driver import BaseDriver
from ingestion.contracts.tick import IngestionTick
from quant_engine.runtime.lifecycle import RuntimePhase
from quant_engine.runtime.modes import EngineSpec
from quant_engine.runtime.snapshot import EngineSnapshot
from quant_engine.contracts.engine import StrategyEngineProto
from quant_engine.strategy.engine import StrategyEngine
from quant_engine.utils.asyncio import to_thread_limited
from quant_engine.utils.logger import log_error


class MockDriver(BaseDriver):
    """
    Synthetic runtime driver (v4).

    Intended for:
      - unit / integration tests
      - debugging runtime semantics
      - synthetic or adversarial tick streams

    Guarantees:
      - Deterministic time progression
      - Fully reproducible execution
    """

    def __init__(
        self,
        *,
        engine: StrategyEngine,
        spec: EngineSpec,
        timestamps: Iterable[int],
        ticks: Iterable[IngestionTick],
        stop_event: threading.Event | None = None,
    ):
        super().__init__(engine=engine, spec=spec, stop_event=stop_event)
        self._timestamps = [int(t) for t in timestamps]
        self._ticks = sorted(ticks, key=lambda t: t.data_ts)
        self._idx = 0

    # -------------------------------------------------
    # Time progression
    # -------------------------------------------------

    async def iter_timestamps(self) -> AsyncIterator[int]:
        """Yield pre-defined engine-time timestamps (epoch ms int)."""
        start_ts = getattr(self, "_start_ts_override", None)
        for timestamp in self._timestamps:
            if start_ts is not None and int(timestamp) < int(start_ts):
                continue
            yield int(timestamp)

    # -------------------------------------------------
    # Tick ingestion
    # -------------------------------------------------

    def iter_ticks(self, *, until_ts: int) -> Iterable[IngestionTick]:
        """
        Yield all ticks with data_ts <= until_ts (epoch ms int).
        """
        while self._idx < len(self._ticks):
            tick = self._ticks[self._idx]
            if int(tick.data_ts) <= int(until_ts):
                self._idx += 1
                yield tick
            else:
                break

    async def run(self) -> None:
        anchor_ts = self._timestamps[0] if self._timestamps else None
        self._install_loop_exception_handler()
        self._start_asyncio_heartbeat()
        try:
            self.guard.enter(RuntimePhase.PRELOAD)
            self.engine.bootstrap(anchor_ts=anchor_ts)

            self.guard.enter(RuntimePhase.WARMUP)
            self.engine.warmup_features(anchor_ts=anchor_ts)

            last_ts = int(anchor_ts) if anchor_ts is not None else None
            if last_ts is not None:
                max_rounds = 3
                for round_idx in range(max_rounds):
                    now_ts = getattr(self, "_catchup_now_ts", None)
                    if now_ts is None:
                        now_ts = last_ts
                    now_ts = int(now_ts)
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
                assert anchor_ts is not None
                if last_ts != int(anchor_ts):
                    self._start_ts_override = int(last_ts)
                    while self._idx < len(self._ticks):
                        if int(self._ticks[self._idx].data_ts) <= int(last_ts):
                            self._idx += 1
                        else:
                            break

            async for ts in self.iter_timestamps():
                if self.stop_event.is_set():
                    break
                self.guard.enter(RuntimePhase.INGEST)
                await to_thread_limited(
                    self.engine.align_to,
                    ts,
                    logger=self._logger,
                    context={"driver": self.__class__.__name__},
                    op="align_to",
                )
                for tick in self.iter_ticks(until_ts=ts):
                    self.engine.ingest_tick(tick)
                self.guard.enter(RuntimePhase.STEP)
                result = self.engine.step(ts=ts)
                await asyncio.sleep(0)
                if isinstance(result, EngineSnapshot):
                    self._snapshots.append(result)
                elif isinstance(result, dict):
                    snap = self.engine.get_snapshot() if hasattr(self.engine, "get_snapshot") else None
                    if snap is not None:
                        self._snapshots.append(snap)
            self.guard.enter(RuntimePhase.FINISH)
        except asyncio.CancelledError:
            self._shutdown_components()
            raise
        except Exception as exc:
            self._handle_fatal(exc)
        finally:
            await self._cancel_background_tasks()
