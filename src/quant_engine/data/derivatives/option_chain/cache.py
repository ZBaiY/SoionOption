from __future__ import annotations

"""Option-chain snapshot caches.

Design goals:
- Main ordering is ALWAYS by snapshot event-time (snapshot.data_ts).
- Expiry is an indexing key only; it must never become the primary sort key.
- Term / DTE bucketing is query-time (term := expiry_ts - ts), not an ingest-time index.

This module intentionally stores references to snapshots in secondary indices
(e.g. per-expiry buckets) to avoid duplicating large record payloads.
"""

from collections import deque
from typing import Generic, Iterable, TypeAlias, TypeVar

from quant_engine.data.contracts.cache import SnapshotCache
from quant_engine.data.derivatives.option_chain.snapshot import OptionChainSnapshot


SnapT = TypeVar("SnapT", bound=OptionChainSnapshot)


def _snap_ts(s: object) -> int:
    ts = getattr(s, "data_ts", None)
    if ts is None:
        ts = getattr(s, "timestamp", None)
    if ts is None:
        raise AttributeError("Snapshot object must expose data_ts (preferred) or timestamp")
    return int(ts)


def _snap_expiries_ms(s: object) -> set[int]:
    """Extract unique expiry_ts values from snapshot frame (best-effort)."""
    frame = getattr(s, "frame", None)
    if frame is None:
        return set()
    try:
        if hasattr(frame, "columns") and "expiry_ts" in frame.columns:  # type: ignore[attr-defined]
            xs = frame["expiry_ts"].dropna().unique()  # type: ignore[index]
            return {int(v) for v in xs if v is not None}
    except Exception:
        return set()
    return set()


def _term_key_ms(*, snap_ts: int, expiry_ts: int, term_bucket_ms: int) -> int:
    """Bucket key for term-structure queries.

    term := expiry_ts - snap_ts (ms)
    key  := floor(term / term_bucket_ms) * term_bucket_ms
    """
    tb = int(term_bucket_ms)
    if tb <= 0:
        raise ValueError("term_bucket_ms must be > 0")
    term = int(expiry_ts) - int(snap_ts)
    if term < 0:
        term = 0
    return (int(term) // tb) * tb


class DequeSnapshotCache(Generic[SnapT], SnapshotCache[SnapT]):
    """Deque-backed cache ordered by snapshot timestamp (append order).

    push(): O(1)
    get_at_or_before(): O(N) reverse scan (OK for moderate N)
    """

    def __init__(self, maxlen: int = 512):
        self.maxlen = int(maxlen)
        if self.maxlen <= 0:
            raise ValueError("maxlen must be > 0")
        self.buffer: deque[SnapT] = deque(maxlen=self.maxlen)

    def push(self, s: SnapT) -> None:
        self.buffer.append(s)

    def last(self) -> SnapT | None:
        return self.buffer[-1] if self.buffer else None

    def window(self, n: int) -> Iterable[SnapT]:
        if not self.buffer:
            return []
        k = int(n)
        return list(self.buffer)[-k:]

    def get_at_or_before(self, timestamp: int) -> SnapT | None:
        t = int(timestamp)
        for s in reversed(self.buffer):
            if _snap_ts(s) <= t:
                return s
        return None

    def get_n_before(self, timestamp: int, n: int) -> Iterable[SnapT]:
        t = int(timestamp)
        k = int(n)
        out: list[SnapT] = []
        for s in reversed(self.buffer):
            if _snap_ts(s) <= t:
                out.append(s)
                if len(out) == k:
                    break
        out.reverse()
        return out

    def has_ts(self, ts: int) -> bool:
        return self.get_at_or_before(int(ts)) is not None

    def clear(self) -> None:
        self.buffer.clear()


class OptionChainExpiryIndexedCache(SnapshotCache[OptionChainSnapshot]):
    """Option-chain cache with an expiry index.

    Indices:
      1) main: global time-ordered cache (snapshots ordered by data_ts)
      2) by_expiry: snapshots indexed by expiry_ts (references only)

    Notes:
    - by_expiry stores references to snapshots; it does not copy snapshot records.
    - If a snapshot contains many expiries, it will be referenced in many buckets.
      This is intentional and cheap (pointer references), and avoids materializing
      per-expiry slices at ingest-time.
    """

    def __init__(
        self,
        *,
        maxlen: int = 512,
        per_expiry_maxlen: int = 256,
    ):
        self.maxlen = int(maxlen)
        self._per_expiry_maxlen = int(per_expiry_maxlen)
        if self.maxlen <= 0:
            raise ValueError("maxlen must be > 0")
        if self._per_expiry_maxlen <= 0:
            raise ValueError("per_expiry_maxlen must be > 0")

        self.main: DequeSnapshotCache[OptionChainSnapshot] = DequeSnapshotCache(maxlen=self.maxlen)
        self.by_expiry: dict[int, DequeSnapshotCache[OptionChainSnapshot]] = {}

        # Protocol attribute: iterable buffer points to global cache.
        self.buffer = self.main.buffer

    def _bucket_expiry(self, expiry_ts: int) -> DequeSnapshotCache[OptionChainSnapshot]:
        k = int(expiry_ts)
        b = self.by_expiry.get(k)
        if b is None:
            b = DequeSnapshotCache(maxlen=self._per_expiry_maxlen)
            self.by_expiry[k] = b
        return b

    # --- SnapshotCache interface (delegates to main) ---

    def push(self, s: OptionChainSnapshot) -> None:
        self.main.push(s)

        # expiry index: store snapshot reference in each expiry bucket present.
        for ex in _snap_expiries_ms(s):
            self._bucket_expiry(ex).push(s)

    def last(self) -> OptionChainSnapshot | None:
        return self.main.last()

    def window(self, n: int) -> Iterable[OptionChainSnapshot]:
        return self.main.window(n)

    def get_at_or_before(self, timestamp: int) -> OptionChainSnapshot | None:
        return self.main.get_at_or_before(timestamp)

    def get_n_before(self, timestamp: int, n: int) -> Iterable[OptionChainSnapshot]:
        return self.main.get_n_before(timestamp, n)

    def has_ts(self, ts: int) -> bool:
        return self.main.has_ts(ts)

    def clear(self) -> None:
        self.main.clear()
        for b in self.by_expiry.values():
            b.clear()
        self.by_expiry.clear()

    # --- Expiry helpers ---

    def expiries(self) -> list[int]:
        return sorted(self.by_expiry.keys())

    def last_for_expiry(self, expiry_ts: int) -> OptionChainSnapshot | None:
        b = self.by_expiry.get(int(expiry_ts))
        return b.last() if b is not None else None

    def get_at_or_before_for_expiry(self, expiry_ts: int, timestamp: int) -> OptionChainSnapshot | None:
        b = self.by_expiry.get(int(expiry_ts))
        return b.get_at_or_before(timestamp) if b is not None else None

    def window_for_expiry(self, expiry_ts: int, n: int) -> Iterable[OptionChainSnapshot]:
        b = self.by_expiry.get(int(expiry_ts))
        return b.window(n) if b is not None else []

    def get_n_before_for_expiry(self, expiry_ts: int, timestamp: int, n: int) -> Iterable[OptionChainSnapshot]:
        b = self.by_expiry.get(int(expiry_ts))
        return b.get_n_before(timestamp, n) if b is not None else []


class OptionChainTermBucketedCache(SnapshotCache[OptionChainSnapshot]):
    """Option-chain cache with term-bucket (DTE-like) index.

    Indices:
      1) main: global time-ordered cache (snapshots ordered by data_ts)
      2) by_term: snapshots indexed by term bucket key (references only)
         term := expiry_ts - snapshot.data_ts (ms)
         key  := floor(term / term_bucket_ms) * term_bucket_ms
      3) by_expiry: optional per-expiry buckets (references only)

    Notes:
    - A single chain snapshot usually contains many expiries.
      Therefore one snapshot will be referenced in many term buckets.
    - This is pointer-cheap and avoids per-expiry slicing at ingest time.
    - Term bucketing is a convenience index; it must never become a primary sort key.
    """

    def __init__(
        self,
        *,
        maxlen: int = 512,
        per_term_maxlen: int = 256,
        term_bucket_ms: int = 86_400_000,
        per_expiry_maxlen: int | None = None,
        enable_expiry_index: bool = True,
    ):
        self.maxlen = int(maxlen)
        self.term_bucket_ms = int(term_bucket_ms)
        if self.maxlen <= 0:
            raise ValueError("maxlen must be > 0")
        if self.term_bucket_ms <= 0:
            raise ValueError("term_bucket_ms must be > 0")

        self._per_term_maxlen = int(per_term_maxlen)
        if self._per_term_maxlen <= 0:
            raise ValueError("per_term_maxlen must be > 0")

        self._enable_expiry_index = bool(enable_expiry_index)
        self._per_expiry_maxlen = (
            int(per_expiry_maxlen)
            if per_expiry_maxlen is not None
            else int(self._per_term_maxlen)
        )
        if self._enable_expiry_index and self._per_expiry_maxlen <= 0:
            raise ValueError("per_expiry_maxlen must be > 0")

        self.main: DequeSnapshotCache[OptionChainSnapshot] = DequeSnapshotCache(maxlen=self.maxlen)

        # Indices (references only)
        self.by_term: dict[int, DequeSnapshotCache[OptionChainSnapshot]] = {}
        self.by_expiry: dict[int, DequeSnapshotCache[OptionChainSnapshot]] = {}

        # Protocol attribute: iterable buffer points to global cache.
        self.buffer = self.main.buffer

    def _bucket_term(self, term_key: int) -> DequeSnapshotCache[OptionChainSnapshot]:
        k = int(term_key)
        b = self.by_term.get(k)
        if b is None:
            b = DequeSnapshotCache(maxlen=self._per_term_maxlen)
            self.by_term[k] = b
        return b

    def _bucket_expiry(self, expiry_ts: int) -> DequeSnapshotCache[OptionChainSnapshot]:
        k = int(expiry_ts)
        b = self.by_expiry.get(k)
        if b is None:
            b = DequeSnapshotCache(maxlen=self._per_expiry_maxlen)
            self.by_expiry[k] = b
        return b

    # --- SnapshotCache interface (delegates to main) ---

    def push(self, s: OptionChainSnapshot) -> None:
        self.main.push(s)

        snap_ts = _snap_ts(s)
        expiries = _snap_expiries_ms(s)
        if not expiries:
            return

        for ex in expiries:
            tk = _term_key_ms(snap_ts=snap_ts, expiry_ts=int(ex), term_bucket_ms=self.term_bucket_ms)
            self._bucket_term(tk).push(s)
            if self._enable_expiry_index:
                self._bucket_expiry(int(ex)).push(s)

    def last(self) -> OptionChainSnapshot | None:
        return self.main.last()

    def window(self, n: int) -> Iterable[OptionChainSnapshot]:
        return self.main.window(n)

    def get_at_or_before(self, timestamp: int) -> OptionChainSnapshot | None:
        return self.main.get_at_or_before(timestamp)

    def get_n_before(self, timestamp: int, n: int) -> Iterable[OptionChainSnapshot]:
        return self.main.get_n_before(timestamp, n)

    def has_ts(self, ts: int) -> bool:
        return self.main.has_ts(ts)

    def clear(self) -> None:
        self.main.clear()
        for b in self.by_term.values():
            b.clear()
        self.by_term.clear()
        for b in self.by_expiry.values():
            b.clear()
        self.by_expiry.clear()

    # --- Term helpers ---

    def term_buckets(self) -> list[int]:
        return sorted(self.by_term.keys())

    def last_for_term(self, term_key_ms: int) -> OptionChainSnapshot | None:
        b = self.by_term.get(int(term_key_ms))
        return b.last() if b is not None else None

    def get_at_or_before_for_term(self, term_key_ms: int, timestamp: int) -> OptionChainSnapshot | None:
        b = self.by_term.get(int(term_key_ms))
        return b.get_at_or_before(timestamp) if b is not None else None

    def window_for_term(self, term_key_ms: int, n: int) -> Iterable[OptionChainSnapshot]:
        b = self.by_term.get(int(term_key_ms))
        return b.window(n) if b is not None else []

    def get_n_before_for_term(self, term_key_ms: int, timestamp: int, n: int) -> Iterable[OptionChainSnapshot]:
        b = self.by_term.get(int(term_key_ms))
        return b.get_n_before(timestamp, n) if b is not None else []

    # --- Expiry helpers (optional) ---

    def expiries(self) -> list[int]:
        return sorted(self.by_expiry.keys())

    def last_for_expiry(self, expiry_ts: int) -> OptionChainSnapshot | None:
        b = self.by_expiry.get(int(expiry_ts))
        return b.last() if b is not None else None

    def get_at_or_before_for_expiry(self, expiry_ts: int, timestamp: int) -> OptionChainSnapshot | None:
        b = self.by_expiry.get(int(expiry_ts))
        return b.get_at_or_before(timestamp) if b is not None else None

    def window_for_expiry(self, expiry_ts: int, n: int) -> Iterable[OptionChainSnapshot]:
        b = self.by_expiry.get(int(expiry_ts))
        return b.window(n) if b is not None else []

    def get_n_before_for_expiry(self, expiry_ts: int, timestamp: int, n: int) -> Iterable[OptionChainSnapshot]:
        b = self.by_expiry.get(int(expiry_ts))
        return b.get_n_before(timestamp, n) if b is not None else []


# Public aliases (constructors)
OptionChainSimpleCache = DequeSnapshotCache
OptionChainExpiryCache = OptionChainExpiryIndexedCache
OptionChainTermBucketedCache = OptionChainTermBucketedCache

# Type alias for handler annotations (interface type)
OptionChainCache: TypeAlias = SnapshotCache[OptionChainSnapshot]

# Default concrete cache (optional)
OptionChainDefaultCache = OptionChainExpiryIndexedCache