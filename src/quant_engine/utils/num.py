from __future__ import annotations

from typing import Any


def visible_end_ts(ts: int, interval_ms: int) -> int:
    return (ts // interval_ms) * interval_ms - 1


def to_float(x: Any) -> float:
    """
    Normalize numeric-like objects to Python float.

    Accepts:
    - Python int / float
    - numpy scalar (via float())
    - pandas scalar (via float())
    """
    if x is None:
        raise TypeError("Cannot convert None to float")

    # Reject complex explicitly (float(complex) is invalid but be explicit)
    if isinstance(x, complex):
        raise TypeError(f"Cannot convert complex to float: {x!r}")

    try:
        return float(x)  # numpy / pandas scalars land here
    except Exception as e:
        raise TypeError(f"Not convertible to float: {type(x).__name__}: {x!r}") from e
