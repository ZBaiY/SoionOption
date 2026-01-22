from __future__ import annotations

import traceback
from threading import Thread
from typing import Iterable, Any

from quant_engine.exceptions.core import FatalError


def ensure_epoch_ms(x: Any) -> int:
    if x is None:
        raise ValueError("timestamp cannot be None")
    if isinstance(x, bool):
        raise ValueError("invalid timestamp type: bool")
    if isinstance(x, (int, float)):
        v = float(x)
    else:
        try:
            v = float(x)
        except Exception as e:
            raise ValueError(f"invalid timestamp: {x!r}") from e
    if v < 10_000_000_000:
        return int(round(v * 1000.0))
    return int(round(v))


def assert_monotonic(ts: int, last_ts: int | None, *, label: str) -> int:
    if last_ts is not None and int(ts) < int(last_ts):
        raise FatalError(f"{label} timestamp went backwards: {ts} < {last_ts}")
    return int(ts)


def format_exc(e: BaseException) -> str:
    return "".join(traceback.format_exception_only(type(e), e)).strip()


def join_threads(threads: Iterable[Thread], timeout_s: float) -> None:
    for t in threads:
        if t is None:
            continue
        t.join(timeout=timeout_s)


def assert_no_lookahead(query_ts: int, snap_ts: int, *, label: str) -> None:
    if int(snap_ts) > int(query_ts):
        raise FatalError(f"{label} snapshot_ts {snap_ts} > query_ts {query_ts}")


def assert_schema_subset(cols: set[str], expected: set[str], *, label: str) -> None:
    extra = cols - expected
    if extra:
        raise FatalError(f"{label} schema drift: unexpected columns {sorted(extra)}")
