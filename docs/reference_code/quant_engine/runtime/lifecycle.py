

from __future__ import annotations

from enum import Enum, auto


class RuntimePhase(Enum):
    """
    Canonical runtime lifecycle phases.

    This enum encodes the *only* legal execution order of the runtime.
    It is used purely for guards, assertions, and debugging.
    """

    PRELOAD = auto()   # data bootstrap / backfill
    WARMUP = auto()    # feature / model state initialization
    INGEST = auto()    # tick ingestion (may interleave with STEP)
    STEP = auto()      # strategy evaluation at engine timestamp (epoch ms int)
    FINISH = auto()    # termination / teardown


class LifecycleGuard:
    """
    Lightweight lifecycle guard enforcing legal phase transitions.

    Intended usage:
        guard = LifecycleGuard()
        guard.enter(RuntimePhase.PRELOAD)
        guard.enter(RuntimePhase.WARMUP)
        guard.enter(RuntimePhase.INGEST)
        guard.enter(RuntimePhase.STEP)
    """

    _ALLOWED_TRANSITIONS = {
        RuntimePhase.PRELOAD: {RuntimePhase.WARMUP},
        RuntimePhase.WARMUP: {RuntimePhase.INGEST},
        RuntimePhase.INGEST: {RuntimePhase.INGEST, RuntimePhase.STEP},
        RuntimePhase.STEP: {RuntimePhase.INGEST, RuntimePhase.STEP, RuntimePhase.FINISH},
        RuntimePhase.FINISH: set(),
    }

    def __init__(self) -> None:
        self._phase: RuntimePhase | None = None

    @property
    def phase(self) -> RuntimePhase | None:
        return self._phase

    def enter(self, next_phase: RuntimePhase) -> None:
        """
        Transition into `next_phase`, asserting legality.

        Raises:
            RuntimeError if the transition is not allowed.
        """
        if self._phase is None:
            # First entry must be PRELOAD
            if next_phase is not RuntimePhase.PRELOAD:
                raise RuntimeError(
                    f"Invalid initial lifecycle phase: {next_phase}"
                )
            self._phase = next_phase
            return

        allowed = self._ALLOWED_TRANSITIONS.get(self._phase, set())
        if next_phase not in allowed:
            raise RuntimeError(
                f"Illegal lifecycle transition: {self._phase} → {next_phase}"
            )

        self._phase = next_phase