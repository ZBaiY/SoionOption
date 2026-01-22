from __future__ import annotations

from dataclasses import dataclass

from quant_engine.runtime.modes import EngineMode

SCHEMA_VERSION = 2


@dataclass(frozen=True)
class RuntimeContext:
    """
    Immutable runtime context.

    Semantics:
      - Represents the current engine-time execution context.
      - Owned by runtime / driver.
      - May be passed to lower layers for read-only access.
    """

    timestamp: int
    mode: EngineMode
