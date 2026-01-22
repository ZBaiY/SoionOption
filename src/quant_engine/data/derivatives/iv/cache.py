from __future__ import annotations

from collections import deque
from typing import Generic, Iterable, TypeAlias, TypeVar

from quant_engine.data.contracts.cache import SnapshotCache

from .snapshot import IVSurfaceSnapshot


SnapT = TypeVar("SnapT", bound=IVSurfaceSnapshot)


def _snap_ts(s: object) -> int:
    """Engine-aligned timestamp accessor for cache ordering/queries."""
    ts = getattr(s, "timestamp", None)
    if ts is None:
        ts = getattr(s, "data_ts", None)
    if ts is None:
        raise AttributeError("Snapshot must expose timestamp (preferred) or data_ts")
    return int(ts)


def _data_ts(s: object) -> int:
    """Data/event time for term computations."""
    ts = getattr(s, "data_ts", None)
    if ts is None:
        # fall back to engine ts if needed
        ts = getattr(s, "timestamp", None)
    if ts is None:
        raise AttributeError("Snapshot must expose data_ts or timestamp")
    return int(ts)


def _expiry_ts_from_surface(s: object) -> int | None:
    """Try to extract expiry_ts (epoch ms) from snapshot.surface dict."""
    surface = getattr(s, "surface", None)
    if isinstance(surface, dict):
        v = surface.get("expiry_ts")
        if v is not None:
            try:
                return int(v)
            except Exception:
                return None
    return None


def _term_key_ms(s: object, *, term_bucket_ms: int) -> int | None:
    """Bucket key for term-structure queries.

    term := expiry_ts - data_ts (ms)
    key  := floor(term / term_bucket_ms) * term_bucket_ms
    """
    tb = int(term_bucket_ms)
    if tb <= 0:
        raise ValueError("term_bucket_ms must be > 0")

    ex = _expiry_ts_from_surface(s)
    if ex is None:
        return None

    term = int(ex) - _data_ts(s)
    if term < 0:
        term = 0
    return (term // tb) * tb


class DequeIVSnapshotCache(Generic[SnapT], SnapshotCache[SnapT]):
    """Simple deque-backed cache ordered by snapshot timestamp (append order).

    push(): O(1)
    get_at_or_before(): O(N) reverse scan
    """

    def __init__(self, maxlen: int = 2048):
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


class IVSurfaceTermBucketedCache(SnapshotCache[IVSurfaceSnapshot]):
    """Multi-index cache for IV surface snapshots.

    Indices:
      1) main: global time-ordered cache (engine alignment / replay / windowing)
      2) by_term: per-term-bucket caches, where term := expiry_ts - data_ts
         (term structure / DTE-bucket features)
      3) by_expiry: per-expiry caches (optional)

    IMPORTANT:
    - main ordering is ALWAYS by snapshot.timestamp (engine time).
    - expiry/term are indexing/bucketing only; never the sort key.
    """

    def __init__(
        self,
        *,
        maxlen: int = 2048,
        per_term_maxlen: int = 512,
        term_bucket_ms: int = 86_400_000,
        enable_expiry_index: bool = True,
        per_expiry_maxlen: int | None = None,
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
        self._per_expiry_maxlen = int(per_expiry_maxlen) if per_expiry_maxlen is not None else int(self._per_term_maxlen)
        if self._per_expiry_maxlen <= 0:
            raise ValueError("per_expiry_maxlen must be > 0")

        self.main: DequeIVSnapshotCache[IVSurfaceSnapshot] = DequeIVSnapshotCache(maxlen=self.maxlen)
        self.by_term: dict[int, DequeIVSnapshotCache[IVSurfaceSnapshot]] = {}
        self.by_expiry: dict[int, DequeIVSnapshotCache[IVSurfaceSnapshot]] = {}

        # Protocol attribute (iterable buffer) points to the global time cache.
        self.buffer = self.main.buffer

    def _bucket_term(self, key: int) -> DequeIVSnapshotCache[IVSurfaceSnapshot]:
        k = int(key)
        b = self.by_term.get(k)
        if b is None:
            b = DequeIVSnapshotCache(maxlen=self._per_term_maxlen)
            self.by_term[k] = b
        return b

    def _bucket_expiry(self, expiry_ts: int) -> DequeIVSnapshotCache[IVSurfaceSnapshot]:
        k = int(expiry_ts)
        b = self.by_expiry.get(k)
        if b is None:
            b = DequeIVSnapshotCache(maxlen=self._per_expiry_maxlen)
            self.by_expiry[k] = b
        return b

    # --- SnapshotCache interface (delegates to main) ---

    def push(self, s: IVSurfaceSnapshot) -> None:
        self.main.push(s)

        tk = _term_key_ms(s, term_bucket_ms=self.term_bucket_ms)
        if tk is not None:
            self._bucket_term(tk).push(s)

        if self._enable_expiry_index:
            ex = _expiry_ts_from_surface(s)
            if ex is not None:
                self._bucket_expiry(ex).push(s)

    def last(self) -> IVSurfaceSnapshot | None:
        return self.main.last()

    def window(self, n: int) -> Iterable[IVSurfaceSnapshot]:
        return self.main.window(n)

    def get_at_or_before(self, timestamp: int) -> IVSurfaceSnapshot | None:
        return self.main.get_at_or_before(timestamp)

    def get_n_before(self, timestamp: int, n: int) -> Iterable[IVSurfaceSnapshot]:
        return self.main.get_n_before(timestamp, n)

    def has_ts(self, ts: int) -> bool:
        return self.get_at_or_before(int(ts)) is not None

    def clear(self) -> None:
        self.main.clear()
        for b in self.by_term.values():
            b.clear()
        self.by_term.clear()
        for b in self.by_expiry.values():
            b.clear()
        self.by_expiry.clear()

    # --- Extra helpers (term buckets) ---

    def term_buckets(self) -> list[int]:
        return sorted(self.by_term.keys())

    def last_for_term(self, term_key_ms: int) -> IVSurfaceSnapshot | None:
        b = self.by_term.get(int(term_key_ms))
        return b.last() if b is not None else None

    def get_at_or_before_for_term(self, term_key_ms: int, timestamp: int) -> IVSurfaceSnapshot | None:
        b = self.by_term.get(int(term_key_ms))
        return b.get_at_or_before(timestamp) if b is not None else None

    def get_n_before_for_term(self, term_key_ms: int, timestamp: int, n: int) -> Iterable[IVSurfaceSnapshot]:
        b = self.by_term.get(int(term_key_ms))
        return b.get_n_before(timestamp, n) if b is not None else []

    # --- Extra helpers (per-expiry) ---

    def expiry_list(self) -> list[int]:
        return sorted(self.by_expiry.keys())

    def last_for_expiry(self, expiry_ts: int) -> IVSurfaceSnapshot | None:
        b = self.by_expiry.get(int(expiry_ts))
        return b.last() if b is not None else None

    def get_at_or_before_for_expiry(self, expiry_ts: int, timestamp: int) -> IVSurfaceSnapshot | None:
        b = self.by_expiry.get(int(expiry_ts))
        return b.get_at_or_before(timestamp) if b is not None else None

    def get_n_before_for_expiry(self, expiry_ts: int, timestamp: int, n: int) -> Iterable[IVSurfaceSnapshot]:
        b = self.by_expiry.get(int(expiry_ts))
        return b.get_n_before(timestamp, n) if b is not None else []


# ---- public aliases ----

IVSurfaceCache: TypeAlias = SnapshotCache[IVSurfaceSnapshot]

# constructor-style aliases
IVSurfaceSimpleCache = DequeIVSnapshotCache
IVSurfaceBucketedCache = IVSurfaceTermBucketedCache

# default implementation
IVSurfaceDefaultCache = IVSurfaceTermBucketedCache
