from __future__ import annotations

import asyncio
import inspect
import threading
import time
from collections.abc import AsyncIterator, Iterable, Iterator, Mapping
from typing import Any, TypeVar
from weakref import WeakKeyDictionary

from quant_engine.utils.logger import get_logger, log_debug, log_exception, log_info, log_warn

_T = TypeVar("_T")
_STOP_SENTINEL = object()
_TO_THREAD_MAX_INFLIGHT = 8
_TO_THREAD_LOG_EVERY = 128
_TO_THREAD_WAIT_WARN_MS = 50
_TO_THREAD_WAIT_SLOW_MS = 20
_TO_THREAD_EXEC_SLOW_MS = 50
_CLOSE_TIMEOUT_S = 2.0


class _ToThreadLimiter:
    def __init__(self, max_inflight: int) -> None:
        self._sem = asyncio.Semaphore(max_inflight)
        self._inflight = 0
        self._waiting = 0
        self._count = 0
        self._max_inflight = int(max_inflight)

    @property
    def inflight(self) -> int:
        return int(self._inflight)

    @property
    def waiting(self) -> int:
        return int(self._waiting)

    @property
    def max_inflight(self) -> int:
        return int(self._max_inflight)

    async def run(
        self,
        fn,
        *args: Any,
        logger,
        context: dict[str, Any] | None,
        op: str,
        timeout_s: float | None = None,
        **kwargs: Any,
    ) -> Any:
        self._waiting += 1
        wait_start = time.monotonic()
        try:
            await self._sem.acquire()
        except Exception:
            self._waiting -= 1
            raise
        self._waiting -= 1
        wait_ms = int((time.monotonic() - wait_start) * 1000.0)
        self._inflight += 1
        try:
            start = time.monotonic()
            if timeout_s is None:
                result = await asyncio.to_thread(fn, *args, **kwargs)
            else:
                result = await asyncio.wait_for(
                    asyncio.to_thread(fn, *args, **kwargs), timeout=timeout_s
                )
            dur_ms = int((time.monotonic() - start) * 1000.0)
            self._count += 1
            if wait_ms >= _TO_THREAD_WAIT_SLOW_MS:
                asyncio_logger = get_logger("quant_engine.asyncio")
                log_info(
                    asyncio_logger,
                    "asyncio.to_thread.wait_slow",
                    op=op,
                    wait_ms=wait_ms,
                    inflight=self._inflight,
                    waiting=self._waiting,
                    count=self._count,
                    timeout_s=timeout_s,
                    **(context or {}),
                )
            if dur_ms >= _TO_THREAD_EXEC_SLOW_MS:
                asyncio_logger = get_logger("quant_engine.asyncio")
                log_info(
                    asyncio_logger,
                    "asyncio.to_thread.exec_slow",
                    op=op,
                    fn_ms=dur_ms,
                    inflight=self._inflight,
                    waiting=self._waiting,
                    count=self._count,
                    **(context or {}),
                )
            if logger is not None and (
                wait_ms >= _TO_THREAD_WAIT_WARN_MS or (self._count % _TO_THREAD_LOG_EVERY) == 0
            ):
                log_debug(
                    logger,
                    "asyncio.to_thread",
                    op=op,
                    wait_ms=wait_ms,
                    duration_ms=dur_ms,
                    inflight=self._inflight,
                    waiting=self._waiting,
                    max_inflight=_TO_THREAD_MAX_INFLIGHT,
                    **(context or {}),
                )
            return result
        finally:
            self._inflight -= 1
            self._sem.release()


_TO_THREAD_LIMITERS: WeakKeyDictionary[asyncio.AbstractEventLoop, _ToThreadLimiter] = WeakKeyDictionary()


def _get_to_thread_limiter() -> _ToThreadLimiter:
    loop = asyncio.get_running_loop()
    limiter = _TO_THREAD_LIMITERS.get(loop)
    if limiter is None:
        limiter = _ToThreadLimiter(_TO_THREAD_MAX_INFLIGHT)
        _TO_THREAD_LIMITERS[loop] = limiter
    return limiter


_QUEUE_PUT_COUNTER = 0


async def queue_put_timed(
    q: Any,
    item: Any,
    *,
    logger,
    op: str,
    context: dict[str, Any] | None = None,
    slow_ms: int = 5,
    sample_every: int = 200,
) -> None:
    global _QUEUE_PUT_COUNTER
    _QUEUE_PUT_COUNTER += 1
    start = time.monotonic()
    await q.put(item)
    wait_ms = int((time.monotonic() - start) * 1000.0)
    sample = False
    if sample_every > 0 and (_QUEUE_PUT_COUNTER % sample_every) == 0:
        sample = True
    if wait_ms < slow_ms and not sample:
        return
    if logger is None:
        logger = get_logger("quant_engine.asyncio")
    qsize = q.qsize() if hasattr(q, "qsize") else None
    maxsize = getattr(q, "maxsize", None)
    item_ts = None
    tick = item
    if isinstance(item, tuple) and item:
        item_ts = item[0]
        tick = item[-1]
    item_domain = getattr(tick, "domain", None)
    item_symbol = getattr(tick, "symbol", None)
    log_info(
        logger,
        "asyncio.queue.put",
        op=op,
        wait_ms=wait_ms,
        qsize=qsize,
        maxsize=maxsize,
        item_ts=item_ts,
        item_domain=item_domain,
        item_symbol=item_symbol,
        sample=sample,
        **(context or {}),
    )


def get_to_thread_stats(loop: asyncio.AbstractEventLoop) -> dict[str, int]:
    limiter = _TO_THREAD_LIMITERS.get(loop)
    if limiter is None:
        return {"to_thread_inflight": 0, "to_thread_waiting": 0}
    return {
        "to_thread_inflight": limiter.inflight,
        "to_thread_waiting": limiter.waiting,
    }


# Use to_thread for sync I/O / occasional heavy warmup; avoid per-tick CPU heavy work (use batching or ProcessPool).
async def to_thread_limited(
    fn,
    *args: Any,
    logger,
    context: dict[str, Any] | None = None,
    op: str,
    timeout_s: float | None = None,
    **kwargs: Any,
) -> Any:
    limiter = _get_to_thread_limiter()
    return await limiter.run(
        fn,
        *args,
        logger=logger,
        context=context,
        op=op,
        timeout_s=timeout_s,
        **kwargs,
    )


def _set_stop_event(stop_event: threading.Event | asyncio.Event | None) -> None:
    if stop_event is None:
        return
    if isinstance(stop_event, threading.Event):
        stop_event.set()
        return
    if isinstance(stop_event, asyncio.Event):
        stop_event.set()
        return
    try:
        stop_event.set()  # type: ignore[call-arg]
    except Exception:
        return


def create_task_named(
    coro: Any,
    name: str,
    *,
    logger,
    context: dict[str, Any] | None = None,
    stop_event: threading.Event | asyncio.Event | None = None,
) -> asyncio.Task[Any]:
    task = asyncio.create_task(coro, name=name)
    base_context = dict(context or {})
    base_context.setdefault("task_name", name)

    def _done_callback(t: asyncio.Task[Any]) -> None:
        if t.cancelled():
            return
        try:
            exc = t.exception()
        except asyncio.CancelledError:
            return
        if exc is None:
            return
        ctx = dict(base_context)
        ctx.setdefault("task_name", t.get_name())
        ctx["err_type"] = type(exc).__name__
        ctx["err"] = str(exc)
        try:
            raise exc
        except Exception:
            if logger is not None:
                log_exception(logger, "asyncio.task_failed", **ctx)
        _set_stop_event(stop_event)

    task.add_done_callback(_done_callback)
    return task


def set_loop_exception_handler(
    loop: asyncio.AbstractEventLoop,
    *,
    logger,
    context: dict[str, Any] | None = None,
    stop_event: threading.Event | asyncio.Event | None = None,
) -> None:
    base_context = dict(context or {})

    def _handler(_loop: asyncio.AbstractEventLoop, ctx: dict[str, Any]) -> None:
        extra = dict(base_context)
        extra["message"] = ctx.get("message")
        task = ctx.get("task") or ctx.get("future")
        if task is not None:
            get_name = getattr(task, "get_name", None)
            if callable(get_name):
                extra["task_name"] = get_name()
        exc = ctx.get("exception")
        if exc is not None:
            extra["err_type"] = type(exc).__name__
            extra["err"] = str(exc)
            try:
                raise exc
            except Exception:
                if logger is not None:
                    log_exception(logger, "asyncio.loop_exception", **extra)
        else:
            if logger is not None:
                log_warn(logger, "asyncio.loop_exception", **extra)
        _set_stop_event(stop_event)

    loop.set_exception_handler(_handler)


async def loop_lag_monitor(
    *,
    interval_s: float,
    warn_after_s: float,
    logger,
    context: dict[str, Any] | None = None,
) -> None:
    last = time.monotonic()
    base_context = dict(context or {})
    interval_ms = int(interval_s * 1000.0)
    warn_after_ms = int(warn_after_s * 1000.0)

    while True:
        await asyncio.sleep(interval_s)
        now = time.monotonic()
        drift = now - last - interval_s
        last = now
        if drift <= warn_after_s:
            continue
        if logger is None:
            continue
        log_warn(
            logger,
            "runtime.loop_lag",
            drift_ms=int(drift * 1000.0),
            interval_ms=interval_ms,
            warn_after_ms=warn_after_ms,
            **base_context,
        )


async def cancel_tasks(
    tasks: list[asyncio.Task[Any]],
    *,
    logger,
    context: dict[str, Any] | None = None,
    timeout_s: float = 2.0,
) -> None:
    if not tasks:
        return
    for task in tasks:
        task.cancel()
    try:
        await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=timeout_s,
        )
    except asyncio.TimeoutError:
        if logger is not None:
            log_warn(
                logger,
                "asyncio.task_cancel_timeout",
                pending=sum(1 for t in tasks if not t.done()),
                timeout_s=float(timeout_s),
                **(context or {}),
            )


def _close_source(
    source: Any,
    iterator: Any,
    *,
    logger,
    context: dict[str, Any] | None = None,
) -> None:
    seen = set()
    for obj in (iterator, source):
        if obj is None:
            continue
        oid = id(obj)
        if oid in seen:
            continue
        seen.add(oid)
        for method in ("close", "shutdown", "stop"):
            fn = getattr(obj, method, None)
            if callable(fn):
                try:
                    fn()
                except Exception as exc:
                    if logger is None:
                        continue
                    log_exception(
                        logger,
                        "ingestion.source_close_error",
                        err_type=type(exc).__name__,
                        err=str(exc),
                        **(context or {}),
                    )


def source_kind(source: Any) -> str:
    if callable(getattr(source, "__aiter__", None)):
        return "async"
    if callable(getattr(source, "fetch", None)):
        return "fetch"
    if callable(getattr(source, "__iter__", None)):
        return "iter"
    return "unknown"


async def iter_source(
    source: Any,
    *,
    logger,
    context: dict[str, Any] | None = None,
    poll_interval_s: float | None = None,
    log_exceptions: bool = False,
) -> AsyncIterator[Any]:
    kind = source_kind(source)
    if kind == "async":
        try:
            async for item in source:  
                yield item
        finally:
            await _close_source_async(source, None, logger=logger, context=context)
        return
    if kind == "fetch":
        async for item in _iter_fetch_source(
            source,
            logger=logger,
            context=context,
            poll_interval_s=poll_interval_s,
            log_exceptions=log_exceptions,
        ):
            yield item
        return
    if kind == "iter":
        async for item in iter_sync_source(
            source,
            logger=logger,
            context=context,
            log_exceptions=log_exceptions,
        ):
            yield item
        return
    raise TypeError(f"source must be async iterable, iterable, or fetch()-based; got {type(source)!r}")


def _next_or_sentinel(iterator: Iterator[_T]) -> _T | object:
    try:
        return next(iterator)
    except StopIteration:
        return _STOP_SENTINEL


async def _iter_fetch_source(
    source: Any,
    *,
    logger,
    context: dict[str, Any] | None = None,
    poll_interval_s: float | None = None,
    log_exceptions: bool = False,
) -> AsyncIterator[Any]:
    fetch = getattr(source, "fetch", None)
    if not callable(fetch):
        raise TypeError(f"fetch source missing callable fetch(): {type(source)!r}")

    while True:
        try:
            if inspect.iscoroutinefunction(fetch):
                batch = await fetch()
            else:
                timeout_s = getattr(source, "timeout", None)
                if timeout_s is None:
                    timeout_s = getattr(source, "_timeout", None)
                batch = await to_thread_limited(
                    fetch,
                    logger=logger,
                    context=context,
                    op="fetch",
                    timeout_s=float(timeout_s) if timeout_s is not None else None,
                )
            if inspect.isawaitable(batch):
                batch = await batch
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if log_exceptions and logger is not None:
                log_exception(
                    logger,
                    "ingestion.fetch_error",
                    err_type=type(exc).__name__,
                    err=str(exc),
                    **(context or {}),
                )
            raise

        items: list[Any]
        if batch is None:
            items = []
        elif isinstance(batch, Mapping):
            items = [batch]
        elif isinstance(batch, Iterable):
            items = list(batch)
        else:
            raise TypeError(f"fetch() must return Iterable[Raw], got {type(batch)!r}")

        for item in items:
            yield item

        if poll_interval_s is None:
            return
        await asyncio.sleep(max(0.0, float(poll_interval_s)))



async def _close_source_async(
    source: Any,
    iterator: Any,
    *,
    logger,
    context: dict[str, Any] | None = None,
) -> None:
    # Close/shutdown/stop can be sync or async; keep event loop unblocked.
    seen: set[int] = set()
    for obj in (iterator, source):
        if obj is None:
            continue
        oid = id(obj)
        if oid in seen:
            continue
        seen.add(oid)

        for method in ("aclose", "close", "shutdown", "stop"):
            fn = getattr(obj, method, None)
            if not callable(fn):
                continue
            try:
                if inspect.iscoroutinefunction(fn):
                    await asyncio.wait_for(fn(), timeout=_CLOSE_TIMEOUT_S)  # type: ignore[misc]
                else:
                    await to_thread_limited(
                        fn,
                        logger=logger,
                        context=context,
                        op="close_source",
                        timeout_s=_CLOSE_TIMEOUT_S,
                    )
            except asyncio.TimeoutError:
                if logger is None:
                    continue
                log_warn(
                    logger,
                    "ingestion.source_close_timeout",
                    timeout_s=_CLOSE_TIMEOUT_S,
                    **(context or {}),
                )
            except Exception as exc:
                if logger is None:
                    continue
                log_exception(
                    logger,
                    "ingestion.source_close_error",
                    err_type=type(exc).__name__,
                    err=str(exc),
                    **(context or {}),
                )
            break  # only call one close-like method per object


async def iter_sync_source(
    source: Iterable[_T],
    *,
    logger,
    context: dict[str, Any] | None = None,
    log_exceptions: bool = False,
) -> AsyncIterator[_T]:
    iterator: Iterator[_T] = iter(source)

    if inspect.isawaitable(iterator):
        # Should never happen for real sync iterables; indicates misclassification/bug.
        raise TypeError(
            "iter(source) returned awaitable; source must be a sync iterable "
            f"(got {type(iterator)!r} from {type(source)!r})"
        )

    try:
        while True:
            item = await to_thread_limited(
                _next_or_sentinel,
                iterator,
                logger=logger,
                context=context,
                op="sync_iter_next",
            )
            if item is _STOP_SENTINEL:
                return

            # Strict contract: sync iterable must yield concrete items, not awaitables.
            if inspect.isawaitable(item):
                raise TypeError(
                    "sync iterable yielded awaitable item; source must yield raw payloads "
                    f"(got {type(item)!r} from {type(source)!r})"
                )

            yield item  # type: ignore[misc]
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        if log_exceptions and logger is not None:
            log_exception(
                logger,
                "ingestion.sync_iter_error",
                err_type=type(exc).__name__,
                err=str(exc),
                **(context or {}),
            )
        raise
    finally:
        await _close_source_async(source, iterator, logger=logger, context=context)
