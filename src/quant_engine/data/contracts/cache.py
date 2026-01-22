from __future__ import annotations

from typing import Protocol, Iterable, TypeVar, runtime_checkable

from collections import deque

from quant_engine.data.contracts.snapshot import Snapshot

SnapT = TypeVar("SnapT", bound=Snapshot)

@runtime_checkable
class SnapshotCache(Protocol[SnapT]):
    """
    Snapshot cache contract.

    Responsibilities:
    - Store immutable Snapshot objects in a bounded structure
    - Provide O(1) append and efficient time-based lookup
    - Remain completely agnostic to engine mode (backtest / realtime / mock)

    Non-responsibilities:
    - No timestamp validation (anti-lookahead is handler/engine concern)
    - No alignment, resampling, or semantic interpretation
    """
    maxlen: int
    buffer: deque[SnapT]

    def push(self, snapshot: SnapT) -> None:
        """
        Append a snapshot to the cache.
        Eviction policy (ring / LRU / etc.) is implementation-defined.
        """
        ...

    def last(self) -> SnapT | None:
        """
        Return the most recent snapshot, or None if empty.
        """
        ...

    def window(self, n: int) -> Iterable[SnapT]:
        """
        Return the last n snapshots in insertion order.
        """
        ...

    def get_at_or_before(self, timestamp: int) -> SnapT | None:
        """
        Return the latest snapshot with snapshot.timestamp <= timestamp.
        """
        ...
    def get_n_before(self, timestamp: int, n: int) -> Iterable[SnapT]:
        """
        Return up to n snapshots with snapshot.timestamp <= timestamp,
        in reverse chronological order.
        """
        ...

    def has_ts(self, ts: int) -> bool:
        """
        Return True if the cache contains any bar with timestamp <= ts (epoch-ms int).
        """
        ...

    def clear(self) -> None:
        """
        Clear all cached snapshots.
        """
        ...
    
