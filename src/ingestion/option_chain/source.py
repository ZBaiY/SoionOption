from __future__ import annotations

import time
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Any, AsyncIterable, AsyncIterator, Iterator, Mapping, Iterable
import pandas as pd

import os
import threading
import requests
import pyarrow as pa
import pyarrow.parquet as pq
from ingestion.contracts.source import Source, AsyncSource, Raw
from ingestion.contracts.tick import _coerce_epoch_ms, _guard_interval_ms, _to_interval_ms

from quant_engine.utils.paths import data_root_from_file, resolve_under_root
from quant_engine.utils.logger import get_logger, log_warn, log_debug, log_exception

DATA_ROOT = data_root_from_file(__file__, levels_up=3)
_LOG = get_logger(__name__)
_LOCK_WARN_S = 0.2
_WRITE_LOG_EVERY = 100

records: list[Mapping[str, Any]] = []

class OptionChainWriteError(RuntimeError):
    """Raised for non-recoverable option-chain parquet write failures."""

def _now_ms() -> int:
    return int(time.time() * 1000.0)


def _date_path(root: Path, *, interval: str, asset: str, data_ts: int) -> Path:
    dt = datetime.fromtimestamp(int(data_ts) / 1000.0, tz=timezone.utc)
    year = dt.strftime("%Y")
    ymd = dt.strftime("%Y_%m_%d")
    return root / asset / interval / year / f"{ymd}.parquet"


def _get_lock(path: Path) -> threading.Lock:
    with DeribitOptionChainRESTSource._global_lock:
        lock = DeribitOptionChainRESTSource._global_locks.get(path)
        if lock is None:
            lock = threading.Lock()
            DeribitOptionChainRESTSource._global_locks[path] = lock
        return lock


def _get_writer_and_schema(
    path: Path,
    df: pd.DataFrame,
    used_paths: set[Path],
) -> tuple[pq.ParquetWriter, pa.Schema]:
    with DeribitOptionChainRESTSource._global_lock:
        writer = DeribitOptionChainRESTSource._global_writers.get(path)
        if writer is not None:
            if path not in used_paths:
                DeribitOptionChainRESTSource._global_refs[path] = DeribitOptionChainRESTSource._global_refs.get(path, 0) + 1
                used_paths.add(path)
            return writer, DeribitOptionChainRESTSource._global_schemas[path]

    if path.exists():
        return _bootstrap_existing(path, df, used_paths)

    table = pa.Table.from_pandas(df, preserve_index=False)
    writer = pq.ParquetWriter(path, table.schema)
    with DeribitOptionChainRESTSource._global_lock:
        DeribitOptionChainRESTSource._global_writers[path] = writer
        DeribitOptionChainRESTSource._global_schemas[path] = table.schema
        DeribitOptionChainRESTSource._global_refs[path] = DeribitOptionChainRESTSource._global_refs.get(path, 0) + 1
    used_paths.add(path)
    return writer, table.schema


def _bootstrap_existing(
    path: Path,
    df: pd.DataFrame,
    used_paths: set[Path],
) -> tuple[pq.ParquetWriter, pa.Schema]:
    bak_path = path.with_suffix(".parquet.bak")
    os.replace(path, bak_path)
    try:
        table = pq.read_table(bak_path)
        schema = table.schema
        with DeribitOptionChainRESTSource._global_lock:
            cached_schema = DeribitOptionChainRESTSource._global_schemas.get(path)
        if cached_schema is not None:
            schema = cached_schema
            table = _align_table_to_schema(table, schema, bak_path)
        writer = pq.ParquetWriter(path, schema)
        writer.write_table(table)
        with DeribitOptionChainRESTSource._global_lock:
            DeribitOptionChainRESTSource._global_writers[path] = writer
            DeribitOptionChainRESTSource._global_schemas[path] = schema
            DeribitOptionChainRESTSource._global_refs[path] = DeribitOptionChainRESTSource._global_refs.get(path, 0) + 1
        used_paths.add(path)
        os.remove(bak_path)
        return writer, schema
    except Exception as exc:
        raise OptionChainWriteError(
            f"Failed to bootstrap parquet writer for {path}; "
            f"backup retained at {bak_path}: {exc}"
        )


def _align_to_schema(df: pd.DataFrame, schema: pa.Schema, path: Path) -> pd.DataFrame:
    cols = list(schema.names)
    extra = [c for c in df.columns if c not in cols]
    if extra:
        raise ValueError(
            f"Option chain schema drift for {path}: unexpected columns {extra}"
        )
    missing = [c for c in cols if c not in df.columns]
    if missing:
        df = df.copy()
        for c in missing:
            df[c] = None
    return df[cols]


def _align_table_to_schema(table: pa.Table, schema: pa.Schema, path: Path) -> pa.Table:
    cols = list(schema.names)
    extra = [c for c in table.column_names if c not in cols]
    if extra:
        raise ValueError(
            f"Option chain schema drift for {path}: unexpected columns {extra}"
        )
    missing = [c for c in cols if c not in table.column_names]
    if missing:
        for c in missing:
            table = table.append_column(c, pa.nulls(table.num_rows))
    return table.select(cols)


def _write_raw_snapshot(
    *,
    root: Path,
    asset: str,
    interval: str,
    df: pd.DataFrame,
    data_ts: int,
    used_paths: set[Path],
    write_counter: list[int] | None = None,
) -> None:
    path = _date_path(root, interval=interval, asset=asset, data_ts=data_ts)
    path.parent.mkdir(parents=True, exist_ok=True)
    if "data_ts" not in df.columns:
        df = df.copy()
        df["data_ts"] = int(data_ts)

    lock = _get_lock(path)
    write_start = time.monotonic()
    start = time.monotonic()
    lock.acquire()
    waited_s = time.monotonic() - start
    if waited_s > _LOCK_WARN_S:
        log_debug(
            _LOG,
            "ingestion.lock_wait",
            component="option_chain",
            path=str(path),
            wait_ms=int(waited_s * 1000.0),
        )
    try:
        writer, schema = _get_writer_and_schema(path, df, used_paths)
        aligned = _align_to_schema(df, schema, path)
        table = pa.Table.from_pandas(aligned, schema=schema, preserve_index=False)
        writer.write_table(table)
        if write_counter is not None:
            write_counter[0] += 1
            write_count = write_counter[0]
        else:
            write_count = None
        if write_count is not None and write_count % _WRITE_LOG_EVERY == 0:
            log_debug(
                _LOG,
                "ingestion.write_sample",
                component="option_chain",
                path=str(path),
                rows=int(len(df)),
                write_ms=int((time.monotonic() - write_start) * 1000),
                write_seq=write_count,
            )
    except Exception as exc:
        raise OptionChainWriteError(str(exc)) from exc
    finally:
        lock.release()

class OptionChainFileSource(Source):
    """
    Option chain source backed by local parquet snapshots.

    Layout:
        data/raw/option_chain/<ASSET>/<INTERVAL>/<YYYY>/<YYYY>_<MM>_<DD>.parquet.
    """

    def __init__(
        self,
        *,
        root: str | Path,
        asset: str,
        interval: str | None = None,
        start_ts: int | None = None,
        end_ts: int | None = None,
        paths: Iterable[Path] | None = None,
    ):
        self._root = resolve_under_root(DATA_ROOT, root, strip_prefix="data")
        self._asset = str(asset)
        self._interval = str(interval) if interval is not None else None
        self._start_ts = int(start_ts) if start_ts is not None else None
        self._end_ts = int(end_ts) if end_ts is not None else None
        self._path = self._root / self._asset
        if self._interval is not None:
            self._path = self._path / self._interval
        if paths is not None:
            resolved_paths: list[Path] = []
            for p in paths:
                path = Path(p)
                if not path.is_absolute():
                    path = self._root / path
                resolved_paths.append(path)
            self._paths: list[Path] | None = resolved_paths
        else:
            self._paths = None

    def __iter__(self) -> Iterator[Raw]:
        try:
            import pandas as pd
        except ImportError as e:
            raise RuntimeError("pandas is required for OptionChainFileSource parquet loading") from e

        if self._paths is not None:
            files: list[Path] = [p for p in self._paths if p.exists()]
        elif self._start_ts is not None or self._end_ts is not None:
            start_date = datetime.fromtimestamp((self._start_ts or 0) / 1000.0, tz=timezone.utc).date()
            end_date = datetime.fromtimestamp((self._end_ts or _now_ms()) / 1000.0, tz=timezone.utc).date()

            dated_files: list[tuple[date, Path]] = []
            for year_dir in sorted(self._path.glob("[0-9][0-9][0-9][0-9]")):
                if not year_dir.is_dir():
                    continue
                year_name = year_dir.name
                if not year_name.isdigit():
                    continue
                year = int(year_name)
                if year < start_date.year or year > end_date.year:
                    continue

                for fp in year_dir.glob("*.parquet"):
                    stem = fp.stem
                    parts = stem.split("_")
                    if len(parts) != 3 or not all(p.isdigit() for p in parts):
                        continue
                    try:
                        d = datetime(int(parts[0]), int(parts[1]), int(parts[2]), tzinfo=timezone.utc).date()
                    except Exception:
                        continue
                    if start_date <= d <= end_date:
                        dated_files.append((d, fp))

            dated_files.sort(key=lambda t: (t[0], str(t[1])))
            files: list[Path] = [fp for _, fp in dated_files]
        else:
            files: list[Path] = sorted(self._path.rglob("*.parquet"))
        if not files:
            return

        for fp in files:
            df = pd.read_parquet(fp)
            if df is None or df.empty:
                continue
            if "data_ts" not in df.columns:
                raise ValueError(f"Parquet file {fp} missing data_ts column")

            df["data_ts"] = df["data_ts"].map(_coerce_epoch_ms)
            if self._start_ts is not None:
                df = df[df["data_ts"] >= int(self._start_ts)]
            if self._end_ts is not None:
                df = df[df["data_ts"] <= int(self._end_ts)]
            if df.empty:
                continue
            df = df.sort_values(["data_ts"], kind="stable")
            df["data_ts"] = df["data_ts"].astype("int64", copy=False)
            for ts, sub in df.groupby("data_ts", sort=True):
                snap = sub.reset_index(drop=True)
                # Source contract: yield a Mapping[str, Any]
                assert isinstance(ts, (int, float)), f"data_ts must be int/float, got {type(ts)!r}"
                records = snap.to_dict(orient="records")
                yield {"data_ts": int(ts), "records": records}

class DeribitOptionChainRESTSource(Source):
    """Deribit option-chain source using REST polling.

    Fetches instrument metadata (kind=option, expired=false) and writes raw
    snapshots under data/raw/option_chain/<ASSET>/<INTERVAL>/<YYYY>/<YYYY>_<MM>_<DD>.parquet.
    """
    _global_lock = threading.Lock()
    _global_writers: dict[Path, pq.ParquetWriter] = {}
    _global_schemas: dict[Path, pa.Schema] = {}
    _global_locks: dict[Path, threading.Lock] = {}
    _global_refs: dict[Path, int] = {}

    def __init__(
        self,
        *,
        currency: str,
        interval: str | None = None,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
        base_url: str = "https://www.deribit.com",
        timeout: float = 10.0,
        root: str | Path = DATA_ROOT / "raw" / "option_chain",
        kind: str = "option",
        expired: bool = False,
        max_retries: int = 5,
        backoff_s: float = 1.0,
        backoff_max_s: float = 30.0,
        stop_event: threading.Event | None = None,
    ):
        
        self._currency = str(currency)
        self._base_url = base_url.rstrip("/")
        self._timeout = float(timeout)
        self.interval = interval if interval is not None else "1m"
        self._kind = str(kind)
        self._expired = bool(expired)
        self._root = resolve_under_root(DATA_ROOT, root, strip_prefix="data")
        self._root.mkdir(parents=True, exist_ok=True)
        self._max_retries = int(max_retries)
        self._backoff_s = float(backoff_s)
        self._backoff_max_s = float(backoff_max_s)
        self._stop_event = stop_event
        self._used_paths: set[Path] = set()
        self._write_count = 0

        interval_ms = _to_interval_ms(self.interval)
        if interval_ms is None:
            raise ValueError(f"Invalid interval format: {self.interval!r}")

        if poll_interval_ms is not None:
            poll_ms = int(poll_interval_ms)
        elif poll_interval is not None:
            poll_ms = int(round(float(poll_interval) * 1000.0))
        else:
            poll_ms = int(interval_ms)

        if poll_ms != int(interval_ms):
            log_warn(
                _LOG,
                "ingestion.poll_interval_override",
                domain="option_chain",
                interval=self.interval,
                interval_ms=int(interval_ms),
                poll_interval_ms=int(poll_ms),
            )
            poll_ms = int(interval_ms)

        self._poll_interval_ms = poll_ms
        _guard_interval_ms(self.interval, self._poll_interval_ms)

        if self._poll_interval_ms <= 0:
            raise ValueError(f"poll interval must be > 0ms, got {self._poll_interval_ms}")

    def __iter__(self) -> Iterator[Raw]:
        try:
            while True:
                if self._stop_event is not None and self._stop_event.is_set():
                    break
                for snap in self.fetch():
                    yield snap
                if self._sleep_or_stop(self._poll_interval_ms / 1000.0):
                    return
        finally:
            self._close_writers()

    def _fetch(self) -> list[dict]:
        url = f"{self._base_url}/api/v2/public/get_instruments"
        params = {
            "currency": self._currency,
            "kind": self._kind,
            "expired": str(self._expired).lower(),
        }
        r = requests.get(url, params=params, timeout=self._timeout)
        r.raise_for_status()
        payload = r.json()
        result = payload.get("result")
        if not isinstance(result, list):
            raise RuntimeError(f"Unexpected Deribit response: {type(result)!r}")
        return result

    def fetch(self) -> list[Raw]:
        if self._stop_event is not None and self._stop_event.is_set():
            return []
        try:
            import pandas as pd
        except ImportError as e:
            raise RuntimeError("pandas is required for DeribitOptionChainRESTSource parquet writing") from e

        data_ts = _now_ms()
        backoff = self._backoff_s
        for _ in range(self._max_retries):
            try:
                rows = self._fetch()
                df = pd.DataFrame(rows or [])
                records: list[dict[str, Any]] = []
                if not df.empty:
                    df["data_ts"] = int(data_ts)
                    self._write_raw_snapshot(df=df, data_ts=int(data_ts))
                    records = [{str(k): v for k, v in rec.items()} for rec in df.to_dict(orient="records")]
                # Source contract: return Mapping[str, Any] items
                return [{"data_ts": int(data_ts), "records": records}]
            except Exception as exc:
                log_warn(
                    _LOG,
                    "option_chain.fetch_or_write_error",
                    err_type=type(exc).__name__,
                    err=str(exc),
                )
                if isinstance(exc, OptionChainWriteError):
                    raise
                if self._sleep_or_stop(min(backoff, self._backoff_max_s)):
                    return []
                backoff = min(backoff * 2.0, self._backoff_max_s)
        return []

    def _write_raw_snapshot(self, *, df, data_ts: int) -> None:
        write_counter = [self._write_count]
        _write_raw_snapshot(
            root=self._root,
            asset=self._currency,
            interval=self.interval,
            df=df,
            data_ts=int(data_ts),
            used_paths=self._used_paths,
            write_counter=write_counter,
        )
        self._write_count = write_counter[0]

    def _sleep_or_stop(self, seconds: float) -> bool:
        if self._stop_event is None:
            time.sleep(seconds)
            return False
        return self._stop_event.wait(seconds)

    def _get_lock(self, path: Path) -> threading.Lock:
        with self._global_lock:
            lock = self._global_locks.get(path)
            if lock is None:
                lock = threading.Lock()
                self._global_locks[path] = lock
            return lock

    def _get_writer_and_schema(
        self,
        path: Path,
        df: pd.DataFrame,
    ) -> tuple[pq.ParquetWriter, pa.Schema]:
        with self._global_lock:
            writer = self._global_writers.get(path)
            if writer is not None:
                if path not in self._used_paths:
                    self._global_refs[path] = self._global_refs.get(path, 0) + 1
                    self._used_paths.add(path)
                return writer, self._global_schemas[path]

        if path.exists():
            return self._bootstrap_existing(path, df)

        table = pa.Table.from_pandas(df, preserve_index=False)
        writer = pq.ParquetWriter(path, table.schema)
        with self._global_lock:
            self._global_writers[path] = writer
            self._global_schemas[path] = table.schema
            self._global_refs[path] = self._global_refs.get(path, 0) + 1
        self._used_paths.add(path)
        return writer, table.schema

    def _bootstrap_existing(
        self,
        path: Path,
        df: pd.DataFrame,
    ) -> tuple[pq.ParquetWriter, pa.Schema]:
        bak_path = path.with_suffix(".parquet.bak")
        os.replace(path, bak_path)
        try:
            table = pq.read_table(bak_path)
            schema = table.schema
            with self._global_lock:
                cached_schema = self._global_schemas.get(path)
            if cached_schema is not None:
                schema = cached_schema
                table = self._align_table_to_schema(table, schema, bak_path)
            writer = pq.ParquetWriter(path, schema)
            writer.write_table(table)
            with self._global_lock:
                self._global_writers[path] = writer
                self._global_schemas[path] = schema
                self._global_refs[path] = self._global_refs.get(path, 0) + 1
            self._used_paths.add(path)
            os.remove(bak_path)
            return writer, schema
        except Exception as exc:
            raise OptionChainWriteError(
                f"Failed to bootstrap parquet writer for {path}; "
                f"backup retained at {bak_path}: {exc}"
            )

    def _align_to_schema(self, df: pd.DataFrame, schema: pa.Schema, path: Path) -> pd.DataFrame:
        cols = list(schema.names)
        extra = [c for c in df.columns if c not in cols]
        if extra:
            raise ValueError(
                f"Option chain schema drift for {path}: unexpected columns {extra}"
            )
        missing = [c for c in cols if c not in df.columns]
        if missing:
            df = df.copy()
            for c in missing:
                df[c] = None
        return df[cols]

    def _align_table_to_schema(self, table: pa.Table, schema: pa.Schema, path: Path) -> pa.Table:
        cols = list(schema.names)
        extra = [c for c in table.column_names if c not in cols]
        if extra:
            raise ValueError(
                f"Option chain schema drift for {path}: unexpected columns {extra}"
            )
        missing = [c for c in cols if c not in table.column_names]
        if missing:
            for c in missing:
                table = table.append_column(c, pa.nulls(table.num_rows))
        return table.select(cols)

    def _close_writers(self) -> None:
        with self._global_lock:
            to_close: list[tuple[Path, pq.ParquetWriter]] = []
            for path in list(self._used_paths):
                ref = self._global_refs.get(path, 0) - 1
                if ref <= 0:
                    writer = self._global_writers.pop(path, None)
                    if writer is not None:
                        to_close.append((path, writer))
                    self._global_refs.pop(path, None)
                    self._global_schemas.pop(path, None)
                    self._global_locks.pop(path, None)
                else:
                    self._global_refs[path] = ref
            self._used_paths.clear()
        for _, writer in to_close:
            try:
                writer.close()
            except Exception as exc:
                log_exception(
                    _LOG,
                    "ingestion.writer_close_error",
                    component="option_chain",
                    err_type=type(exc).__name__,
                    err=str(exc),
                )


class OptionChainStreamSource(AsyncSource):
    """
    Option chain source backed by an async stream.
    """

    def __init__(self, stream: AsyncIterable[Raw] | None = None):
        self._stream = stream

    def __aiter__(self) -> AsyncIterator[Raw]:
        async def _gen():
            assert self._stream is not None, "stream must be provided for OptionChainStreamSource"
            async for msg in self._stream:
                yield msg

        return _gen()
