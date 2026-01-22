import time
from contextlib import contextmanager
from quant_engine.utils.logger import get_logger, log_debug


@contextmanager
def timed_block(name: str):
    """Profile execution time of a code block."""
    logger = get_logger()

    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = (time.perf_counter() - start) * 1000  # ms
        log_debug(logger, f"[TIMER] {name}", elapsed_ms=round(elapsed, 3))
