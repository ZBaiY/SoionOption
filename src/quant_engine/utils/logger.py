import dataclasses
import hashlib
import json
import logging
import logging.config
import os
import time
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
from logging import Logger
from pathlib import Path
from typing import Any, Optional, TypedDict, Literal, cast

import numpy as np
import pandas as pd

_DEFAULT_LEVEL = logging.INFO
_DEBUG_ENABLED = False
_DEBUG_MODULES: set[str] = set()
_CONFIGURED = False
_RUN_ID: str | None = None
_MODE: str | None = None
_LOG_THROTTLE_STATE: dict[str, float] = {}

# ---------------------------------------------------------------------
# Canonical log categories (semantic contract)
# ---------------------------------------------------------------------

CATEGORY_DATA_INTEGRITY = "data_integrity"
CATEGORY_DECISION = "decision_trace"
CATEGORY_EXECUTION = "execution_discrepancy"
CATEGORY_PORTFOLIO = "portfolio_accounting"
CATEGORY_HEARTBEAT = "health_heartbeat"
CATEGORY_STEP_TRACE = "step_trace"
CATEGORY_TRACE_META = "trace_meta"

TRACE_SCHEMA_VERSION = "trace_v2"
TRACE_HEADER_EVENT = "trace.header"

_TRACE_HEADER_WRITTEN = False


class ExecutionOutcome(TypedDict, total=False):
    execution_decision: Literal["ACCEPTED", "CLAMPED", "REJECTED"]
    reject_reason: str
    projected_target: float | None
    realizable_target: float | None
    symbol: str | None
    side: str | None
    fill_ts: int | None


class StepTracePayload(TypedDict, total=False):
    strategy: str | None
    symbol: str | None
    intent_id: str
    features: Any
    models: Any
    portfolio: Any
    primary_snapshots: Any
    market_snapshots: Any
    decision_score: Any
    target_position: Any
    fills: Any
    snapshot: Any
    guardrails: Any
    execution_outcomes: list[ExecutionOutcome]


@dataclass(frozen=True, slots=True)
class ExecutionConstraints:
    fractional: bool
    min_lot: float | None
    min_notional: float | None
    rounding_policy: str


@dataclass(frozen=True, slots=True)
class TraceHeader:
    schema_version: str
    run_id: str
    engine_mode: str
    engine_git_sha: str
    config_hash: str
    strategy_name: str
    interval: str
    execution_constraints: ExecutionConstraints
    start_ts_ms: int | None = None
    start_ts: str | None = None

def _merge_profile(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(merged.get(k), dict):
            merged[k] = _merge_profile(merged[k], v)
        else:
            merged[k] = v
    return merged


def _build_dict_config(profile: dict[str, Any], *, run_id: str | None, mode: str | None) -> dict[str, Any]:
    level_name = str(profile.get("level", "INFO")).upper()

    format_cfg = profile.get("format", {}) if isinstance(profile.get("format"), dict) else {}
    use_json = bool(format_cfg.get("json", True))
    formatter_name = "json" if use_json else "standard"

    handlers_cfg = profile.get("handlers", {}) if isinstance(profile.get("handlers"), dict) else {}
    console_cfg = handlers_cfg.get("console", {}) if isinstance(handlers_cfg.get("console"), dict) else {}
    file_cfg = handlers_cfg.get("file", {}) if isinstance(handlers_cfg.get("file"), dict) else {}
    trace_cfg = handlers_cfg.get("trace", {}) if isinstance(handlers_cfg.get("trace"), dict) else {}
    asyncio_cfg = handlers_cfg.get("asyncio", {}) if isinstance(handlers_cfg.get("asyncio"), dict) else {}

    handlers: dict[str, Any] = {}
    root_handlers: list[str] = []
    loggers_cfg: dict[str, Any] = {}

    if bool(console_cfg.get("enabled", True)):
        handlers["console"] = {
            "class": "logging.StreamHandler",
            "level": str(console_cfg.get("level", level_name)).upper(),
            "formatter": formatter_name,
            "filters": ["context", "trace_exclude"],
            "stream": "ext://sys.stdout",
        }
        root_handlers.append("console")

    if bool(file_cfg.get("enabled", False)):
        path_template = str(file_cfg.get("path", "artifacts/runs/{run_id}/logs/{mode}.jsonl"))
        path = Path(path_template.format(
            run_id=run_id or "run",
            mode=mode or "default",
        ))
        path.parent.mkdir(parents=True, exist_ok=True)
        handlers["file"] = {
            "class": "logging.FileHandler",
            "level": str(file_cfg.get("level", level_name)).upper(),
            "formatter": formatter_name,
            "filters": ["context", "trace_exclude"],
            "filename": str(path),
            "encoding": "utf-8",
        }
        root_handlers.append("file")

    trace_enabled = trace_cfg.get("enabled")
    if trace_enabled is None:
        trace_enabled = bool(file_cfg.get("enabled", False))
    if bool(trace_enabled):
        trace_path_template = str(
            trace_cfg.get("path", "artifacts/runs/{run_id}/logs/{mode}/trace.jsonl")
            # trace_cfg.get("path", "artifacts/runs/{run_id}/logs/trace.jsonl")
        )
        trace_path = Path(trace_path_template.format(
            run_id=run_id or "run",
            mode=mode or "default",
        ))
        trace_path.parent.mkdir(parents=True, exist_ok=True)
        handlers["trace"] = {
            "class": "logging.FileHandler",
            "level": str(trace_cfg.get("level", level_name)).upper(),
            "formatter": "trace_json",
            "filters": ["context", "trace_only"],
            "filename": str(trace_path),
            "encoding": "utf-8",
            "mode": "a",
            "delay": True,
        }
        root_handlers.append("trace")

    asyncio_enabled = asyncio_cfg.get("enabled")
    if asyncio_enabled is None:
        asyncio_enabled = bool(file_cfg.get("enabled", False))
    if bool(asyncio_enabled) and bool(file_cfg.get("enabled", False)):
        asyncio_path_template = str(
            asyncio_cfg.get("path", "artifacts/runs/{run_id}/logs/asyncio.jsonl")
        )
        asyncio_path = Path(asyncio_path_template.format(
            run_id=run_id or "run",
            mode=mode or "default",
        ))
        asyncio_path.parent.mkdir(parents=True, exist_ok=True)
        handlers["asyncio_file"] = {
            "class": "logging.FileHandler",
            "level": str(asyncio_cfg.get("level", level_name)).upper(),
            "formatter": formatter_name,
            "filters": ["context", "trace_exclude"],
            "filename": str(asyncio_path),
            "encoding": "utf-8",
        }

    loggers_cfg["quant_engine.asyncio"] = {
        "level": str(asyncio_cfg.get("level", level_name)).upper(),
        "handlers": ["asyncio_file"] if "asyncio_file" in handlers else [],
        "propagate": False,
    }

    return {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {
            "context": {"()": "quant_engine.utils.logger.ContextFilter"},
            "trace_only": {
                "()": "quant_engine.utils.logger.CategoryFilter",
                "include": [CATEGORY_STEP_TRACE, CATEGORY_TRACE_META],
            },
            "trace_exclude": {
                "()": "quant_engine.utils.logger.CategoryFilter",
                "exclude": [CATEGORY_STEP_TRACE, CATEGORY_TRACE_META],
            },
        },
        "formatters": {
            "json": {"()": "quant_engine.utils.logger.JsonFormatter"},
            "trace_json": {"()": "quant_engine.utils.logger.TraceJsonFormatter"},
            "standard": {
                "()": "quant_engine.utils.logger.UtcFormatter",
                "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
                "datefmt": "%Y-%m-%dT%H:%M:%S",
            },
        },
        "handlers": handlers,
        "loggers": loggers_cfg,
        "root": {
            "level": level_name,
            "handlers": root_handlers,
        },
    }


def init_logging(
    config_path: str = "configs/logging.json",
    *,
    run_id: str | None = None,
    mode: str | None = None,
) -> None:
    global _DEFAULT_LEVEL, _DEBUG_ENABLED, _DEBUG_MODULES, _CONFIGURED, _RUN_ID, _MODE, _TRACE_HEADER_WRITTEN

    path = Path(config_path)
    if not path.is_absolute():
        path = Path.cwd() / path
    with path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    profiles = cfg.get("profiles", {})
    if not isinstance(profiles, dict):
        raise TypeError("logging.json 'profiles' must be a dict")

    profile_name = str(mode or cfg.get("active_profile") or "default")
    if profile_name not in profiles:
        raise KeyError(f"logging profile not found: {profile_name}")

    base_profile = profiles.get("default", {})
    if not isinstance(base_profile, dict):
        base_profile = {}

    profile = _merge_profile(base_profile, profiles.get(profile_name, {}))

    level_name = str(profile.get("level", "INFO")).upper()
    _DEFAULT_LEVEL = getattr(logging, level_name, logging.INFO)

    debug_cfg = profile.get("debug", {})
    if not isinstance(debug_cfg, dict):
        debug_cfg = {}
    _DEBUG_ENABLED = bool(debug_cfg.get("enabled", False))
    _DEBUG_MODULES = {str(x) for x in debug_cfg.get("modules", [])}

    _RUN_ID = run_id
    _MODE = mode or profile_name
    _TRACE_HEADER_WRITTEN = False

    dict_cfg = _build_dict_config(profile, run_id=run_id, mode=mode or profile_name)
    logging.config.dictConfig(dict_cfg)

    _CONFIGURED = True
    get_logger.cache_clear()
    for logger in logging.root.manager.loggerDict.values():
        if isinstance(logger, logging.Logger) and logger.level != logging.NOTSET:
            logger.setLevel(logging.NOTSET)


class ContextFilter(logging.Filter):
    """
    Guarantees LogRecord has a `context` attribute.
    This makes dynamic LogRecord extension explicit and type-safe.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        # Ensure `context` exists.
        ctx = getattr(record, "context", None)

        # If not configured yet, do not mutate records beyond ensuring the attribute exists.
        if not _CONFIGURED:
            if not hasattr(record, "context"):
                setattr(record, "context", None)
            return True

        # Normalize context to a dict so we can inject base fields.
        if ctx is None:
            ctx = {}
            setattr(record, "context", ctx)
        elif not isinstance(ctx, dict):
            ctx = {"_context": safe_jsonable(ctx)}
            setattr(record, "context", ctx)

        # Inject base context fields if missing.
        if _RUN_ID is not None and "run_id" not in ctx:
            ctx["run_id"] = _RUN_ID
        if _MODE is not None and "mode" not in ctx:
            ctx["mode"] = _MODE

        return True
    
class CategoryFilter(logging.Filter):
    def __init__(
        self,
        *,
        include: list[str] | None = None,
        exclude: list[str] | None = None,
    ) -> None:
        super().__init__()
        self._include = {str(x) for x in include or []}
        self._exclude = {str(x) for x in exclude or []}

    def filter(self, record: logging.LogRecord) -> bool:
        ctx = getattr(record, "context", None)
        category = None
        if isinstance(ctx, dict):
            category = ctx.get("category")
        if self._include and category not in self._include:
            return False
        if self._exclude and category in self._exclude:
            return False
        return True

class JsonFormatter(logging.Formatter):
    """Structured JSON formatter for deterministic, parseable logs."""

    def format(self, record: logging.LogRecord) -> str:
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        payload: dict[str, Any] = {
            # human-readable UTC time with millisecond precision
            "ts": dt.isoformat(timespec="milliseconds"),
            # numeric epoch milliseconds for machine joins/replay
            "ts_ms": int(record.created * 1000),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
            # backward-compatible aliases (can be removed later)
            "module": record.name,
            "msg": record.getMessage(),
        }

        context = cast(Optional[dict[str, Any]], getattr(record, "context", None))

        if isinstance(context, dict) and context:
            # Lift category to top-level and avoid duplicating it inside context.
            if "category" in context:
                payload["category"] = safe_jsonable(context["category"])
                context = dict(context)
                context.pop("category", None)
            if payload.get("category") == CATEGORY_STEP_TRACE:
                log_ts = payload.get("ts")
                log_ts_ms = payload.get("ts_ms")
                step_ts = context.pop("step_ts", None)
                if step_ts is not None:
                    payload["log_ts"] = log_ts
                    payload["log_ts_ms"] = log_ts_ms
                    payload["ts"] = safe_jsonable(step_ts)
                    payload["ts_ms"] = safe_jsonable(step_ts)
                for key in ("run_id", "mode", "strategy", "symbol"):
                    if key in context:
                        payload[key] = safe_jsonable(context.pop(key))
            payload["context"] = safe_jsonable(context)

        try:
            return json.dumps(payload, ensure_ascii=False)
        except Exception as exc:
            fallback = {
                "ts": payload.get("ts"),
                "ts_ms": payload.get("ts_ms"),
                "level": payload.get("level"),
                "logger": payload.get("logger"),
                "event": payload.get("event"),
                "module": payload.get("module"),
                "msg": payload.get("msg"),
                "context": repr(payload.get("context")),
                "format_error": repr(exc),
            }
            return json.dumps(fallback, ensure_ascii=False)


class UtcFormatter(logging.Formatter):
    converter = time.gmtime


class TraceJsonFormatter(logging.Formatter):
    """Trace JSONL formatter with minimal per-event noise (trace_v2)."""

    def format(self, record: logging.LogRecord) -> str:
        context = cast(Optional[dict[str, Any]], getattr(record, "context", None))
        ctx: dict[str, Any] = dict(context) if isinstance(context, dict) else {}

        event = record.getMessage()
        ts_ms = int(record.created * 1000)
        step_ts = ctx.pop("step_ts", None)
        if step_ts is not None:
            try:
                ts_ms = int(step_ts)
            except Exception:
                ts_ms = int(record.created * 1000)

        run_id = ctx.pop("run_id", _RUN_ID)

        payload: dict[str, Any] = {
            "ts_ms": ts_ms,
            "event": event,
        }
        if run_id is not None:
            payload["run_id"] = run_id
        if record.levelname:
            payload["level"] = record.levelname

        if event == TRACE_HEADER_EVENT:
            if "schema_version" not in ctx:
                ctx["schema_version"] = TRACE_SCHEMA_VERSION
            category = ctx.get("category")
            if category is not None:
                payload["category"] = category
            payload["logger"] = record.name
            payload["module"] = record.name
            payload["msg"] = record.getMessage()
            payload.update(ctx)
        else:
            ctx.pop("category", None)
            payload.update(ctx)

        try:
            return json.dumps(safe_jsonable(payload), ensure_ascii=False)
        except Exception as exc:
            fallback = {
                "ts_ms": payload.get("ts_ms"),
                "event": payload.get("event"),
                "run_id": payload.get("run_id"),
                "level": payload.get("level"),
                "format_error": repr(exc),
            }
            return json.dumps(fallback, ensure_ascii=False)



@lru_cache(None)
def get_logger(name: str = "quant_engine", level: int = logging.INFO) -> Logger:
    logger = logging.getLogger(name)
    if not _CONFIGURED:
        logger.setLevel(logging.NOTSET)
    return logger


def safe_jsonable(x: Any) -> Any:
    if x is None or isinstance(x, (str, int, float, bool)):
        return x
    if isinstance(x, datetime):
        if x.tzinfo is None:
            return x.replace(tzinfo=timezone.utc).isoformat()
        return x.astimezone(timezone.utc).isoformat()
    if isinstance(x, Path):
        return str(x)
    if isinstance(x, Enum):
        value = safe_jsonable(getattr(x, "value", None))
        return value if value is not None else str(x)
    if dataclasses.is_dataclass(x) and not isinstance(x, type):
        try:
            return safe_jsonable(asdict(cast(Any, x)))
        except Exception:
            return repr(x)
    if dataclasses.is_dataclass(x) and isinstance(x, type):
        # x is a dataclass class, not an instance
        return f"{x.__module__}.{x.__qualname__}"
        
    if isinstance(x, Mapping):
        out: dict[str, Any] = {}
        for k, v in x.items():
            try:
                key = safe_jsonable(k)
                if not isinstance(key, str):
                    key = repr(key)
            except Exception:
                key = repr(k)
            out[key] = safe_jsonable(v)
        return out
    if isinstance(x, (list, tuple, set)):
        return [safe_jsonable(v) for v in x]
    try:
        return str(x)
    except Exception:
        return repr(x)

def _sanitize_context(context: dict[str, Any]) -> dict[str, Any]:
    cleaned = safe_jsonable(context)
    if isinstance(cleaned, dict):
        return cleaned
    return {"_context": cleaned}

def _stable_json_dumps(value: Any) -> str:
    return json.dumps(
        to_jsonable(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def compute_config_hash(cfg: Any) -> str:
    payload = _stable_json_dumps(cfg)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_intent_id(
    *,
    ts_ms: int,
    strategy: str | None,
    symbol: str | None,
    decision_inputs: dict[str, Any],
) -> str:
    payload = {
        "ts_ms": int(ts_ms),
        "strategy": strategy,
        "symbol": symbol,
        "decision_inputs": decision_inputs,
    }
    digest = hashlib.sha256(_stable_json_dumps(payload).encode("utf-8")).hexdigest()
    return digest


def _git_sha_from_root(root: Path) -> str | None:
    head = root / ".git" / "HEAD"
    if not head.exists():
        return None
    try:
        head_text = head.read_text(encoding="utf-8").strip()
    except Exception:
        return None
    if head_text.startswith("ref:"):
        ref_path = head_text.split(":", 1)[1].strip()
        ref_file = root / ".git" / ref_path
        try:
            return ref_file.read_text(encoding="utf-8").strip()
        except Exception:
            return None
    return head_text or None


def get_engine_git_sha() -> str:
    try:
        here = Path(__file__).resolve()
    except Exception:
        return "unknown"
    for parent in [here] + list(here.parents):
        sha = _git_sha_from_root(parent)
        if sha:
            return sha
    return "unknown"


def build_execution_constraints(portfolio: Any) -> ExecutionConstraints:
    step_size = getattr(portfolio, "step_size", None)
    min_notional = getattr(portfolio, "min_notional", None)
    fractional = False
    if step_size is not None:
        try:
            fractional = float(step_size) < 1.0
        except Exception:
            fractional = False
    rounding_policy = "integer_floor" if not fractional else "step_floor"
    try:
        min_lot = float(step_size) if step_size is not None else None
    except Exception:
        min_lot = None
    try:
        min_notional_val = float(min_notional) if min_notional is not None else None
    except Exception:
        min_notional_val = None
    return ExecutionConstraints(
        fractional=bool(fractional),
        min_lot=min_lot,
        min_notional=min_notional_val,
        rounding_policy=rounding_policy,
    )


def build_trace_header(
    *,
    run_id: str,
    engine_mode: str,
    config_hash: str,
    strategy_name: str,
    interval: str,
    execution_constraints: ExecutionConstraints,
    start_ts_ms: int | None = None,
    start_ts: str | None = None,
    engine_git_sha: str | None = None,
) -> TraceHeader:
    if engine_git_sha is None:
        engine_git_sha = get_engine_git_sha()
    if start_ts is None and start_ts_ms is not None:
        try:
            start_ts = datetime.fromtimestamp(int(start_ts_ms) / 1000, tz=timezone.utc).isoformat()
        except Exception:
            start_ts = None
    return TraceHeader(
        schema_version=TRACE_SCHEMA_VERSION,
        run_id=run_id,
        engine_mode=str(engine_mode).upper(),
        engine_git_sha=engine_git_sha or "unknown",
        config_hash=config_hash or "unknown",
        strategy_name=str(strategy_name) if strategy_name else "unknown",
        interval=str(interval) if interval else "unknown",
        execution_constraints=execution_constraints,
        start_ts_ms=start_ts_ms,
        start_ts=start_ts,
    )


def log_trace_header(logger: Logger, header: TraceHeader) -> None:
    global _TRACE_HEADER_WRITTEN
    if _TRACE_HEADER_WRITTEN:
        return
    payload = asdict(header)
    payload["category"] = CATEGORY_TRACE_META
    logger.info(TRACE_HEADER_EVENT, extra={"context": _sanitize_context(payload)})
    _TRACE_HEADER_WRITTEN = True


def _debug_module_matches(logger_name: str, module: str) -> bool:
    module = module.strip()
    if not module:
        return False
    if logger_name == module or logger_name.startswith(module + "."):
        return True
    parts = logger_name.split(".")
    return module in parts


def log_debug(logger: Logger, msg: str, **context):
    if not _DEBUG_ENABLED:
        return
    if _DEBUG_MODULES:
        if not any(_debug_module_matches(logger.name, m) for m in _DEBUG_MODULES):
            return
    logger.debug(msg, extra={"context": _sanitize_context(context)})


def log_info(logger: Logger, msg: str, **context):
    logger.info(msg, extra={"context": _sanitize_context(context)})


def log_warn(logger: Logger, msg: str, **context):
    logger.warning(msg, extra={"context": _sanitize_context(context)})


def log_error(logger: Logger, msg: str, **context):
    logger.error(msg, extra={"context": _sanitize_context(context)})


def log_exception(logger: Logger, msg: str, **context):
    logger.exception(msg, extra={"context": _sanitize_context(context)})

def throttle_key(*parts: object) -> str:
    normalized = ("<none>" if p is None else str(p) for p in parts)
    return ":".join(normalized)

def log_throttle(key: str, every_s: float, *, now_s: float | None = None) -> bool:
    if every_s is None or every_s <= 0:
        return True
    now = time.monotonic() if now_s is None else float(now_s)
    last = _LOG_THROTTLE_STATE.get(key)
    if last is None or (now - last) >= float(every_s):
        _LOG_THROTTLE_STATE[key] = now
        return True
    return False

# ---------------------------------------------------------------------
# Domain-specific logging helpers (semantic, not infrastructural)
# ---------------------------------------------------------------------

def log_data_integrity(logger: Logger, msg: str, **context):
    """
    Realtime data health: gaps, missing timestamps, late arrivals.
    Expected context: data_ts, expected_ts, gap_seconds, symbol, source
    """
    context["category"] = CATEGORY_DATA_INTEGRITY
    logger.warning(msg, extra={"context": _sanitize_context(context)})


def log_portfolio(logger: Logger, msg: str, **context):
    """
    Portfolio / accounting / audit logs.
    Expected context: cash, positions, realized_pnl, unrealized_pnl, equity
    """
    context["category"] = CATEGORY_PORTFOLIO
    logger.info(msg, extra={"context": _sanitize_context(context)})


def log_decision(logger: Logger, msg: str, **context):
    """
    Decision trace logs (CRITICAL for explainability).
    Expected context: model_scores, features_hash, regime, signal, weights
    """
    context["category"] = CATEGORY_DECISION
    logger.info(msg, extra={"context": _sanitize_context(context)})


def log_execution(logger: Logger, msg: str, **context):
    """
    Execution discrepancies, slippage, partial fills, rejections.
    Expected context: order_id, expected_price, fill_price, slippage_bps
    """
    context["category"] = CATEGORY_EXECUTION
    logger.warning(msg, extra={"context": _sanitize_context(context)})


def log_heartbeat(logger: Logger, msg: str, **context):
    """
    System health / liveness logs.
    Expected context: cycle_ms, last_data_ts, backlog, component
    """
    context["category"] = CATEGORY_HEARTBEAT
    logger.info(msg, extra={"context": _sanitize_context(context)})


def _short_repr(x: Any, max_len: int) -> str:
    try:
        rep = repr(x)
    except Exception:
        rep = "<unrepr>"
    if max_len <= 0 or len(rep) <= max_len:
        return rep
    return rep[: max(0, max_len - 3)] + "..."


def _is_pandas_df(x: Any) -> bool:
    return isinstance(x, pd.DataFrame)


def _is_pandas_series(x: Any) -> bool:
    return isinstance(x, pd.Series)


def _is_numpy(x: Any) -> bool:
    return isinstance(x, np.ndarray)

def _summarize_dataframe(df: Any, *, max_rows: int = 2, max_cols: int = 12, max_cell_str: int = 128) -> dict[str, Any]:
    cols = list(df.columns)
    truncated_cols = len(cols) > max_cols
    cols = cols[:max_cols]

    dtypes = {str(c): str(df[c].dtype) for c in cols}

    preview: dict[str, list[Any]] = {}
    head_df = df.loc[:, cols].head(max_rows)
    for c in cols:
        # per-cell truncation
        vals = head_df[c].tolist()
        preview[str(c)] = [
            v if isinstance(v, (int, float, bool)) or v is None
            else (v[: max_cell_str - 3] + "..." if isinstance(v, str) and len(v) > max_cell_str else v)
            for v in vals
        ]

    return {
        "__type__": "DataFrame",
        "shape": [int(df.shape[0]), int(df.shape[1])],
        "columns": [str(c) for c in cols],
        "columns_truncated": truncated_cols,
        "dtypes": dtypes,
        "preview": preview,  # << replaces huge "head"
    }

def _summarize_series(s: Any, *, max_items: int = 5) -> dict[str, Any]:
    try:
        n = int(len(s))
        head = s.head(min(max_items, n)).tolist()
        return {
            "__type__": "Series",
            "len": n,
            "dtype": str(s.dtype),
            "head": head,
            "name": getattr(s, "name", None),
        }
    except Exception:
        return {"__type__": "Series", "repr": _short_repr(s, 512)}


def _summarize_numpy(arr: Any, *, max_items: int = 5) -> dict[str, Any]:
    try:
        flat = arr.ravel()
        n = int(flat.shape[0])
        head = flat[: min(max_items, n)].tolist()
        return {
            "__type__": "ndarray",
            "shape": [int(x) for x in arr.shape],
            "dtype": str(arr.dtype),
            "head": head,
        }
    except Exception:
        return {"__type__": "ndarray", "repr": _short_repr(arr, 512)}


def to_jsonable(
    x: Any,
    *,
    max_depth: int = 6,
    max_items: int = 256,
    max_str: int = 4096,
    _depth: int = 0,
    _seen: set[int] | None = None,
) -> Any:
    if _seen is None:
        _seen = set()

    # cycle guard (containers + dataclasses + common heavy objects)
    oid = id(x)
    if isinstance(x, (Mapping, list, tuple, set)) or dataclasses.is_dataclass(x) or _is_pandas_df(x) or _is_pandas_series(x):
        if oid in _seen:
            return {"__cycle__": True, "repr": _short_repr(x, max_str)}
        _seen.add(oid)

    if x is None or isinstance(x, (str, int, float, bool)):
        if isinstance(x, str) and len(x) > max_str:
            return x[: max(0, max_str - 3)] + "..."
        return x

    if _depth >= max_depth:
        # at depth limit, still try to summarize known heavy types
        if _is_pandas_df(x):
            return _summarize_dataframe(x)
        if _is_pandas_series(x):
            return _summarize_series(x)
        if _is_numpy(x):
            return _summarize_numpy(x)
        return {"__truncated__": True, "reason": "max_depth", "repr": _short_repr(x, max_str)}

    if isinstance(x, datetime):
        if x.tzinfo is None:
            return x.replace(tzinfo=timezone.utc).isoformat()
        return x.astimezone(timezone.utc).isoformat()

    if isinstance(x, Path):
        return str(x)

    if isinstance(x, Enum):
        value = getattr(x, "value", None)
        return to_jsonable(value, max_depth=max_depth, max_items=max_items, max_str=max_str, _depth=_depth + 1, _seen=_seen)

    # pandas / numpy: never dump full payload, always summarize
    if _is_pandas_df(x):
        summary = _summarize_dataframe(x)
        return to_jsonable(summary, max_depth=max_depth, max_items=max_items, max_str=max_str, _depth=_depth + 1, _seen=_seen)
    if _is_pandas_series(x):
        summary = _summarize_series(x)
        return to_jsonable(summary, max_depth=max_depth, max_items=max_items, max_str=max_str, _depth=_depth + 1, _seen=_seen)
    if _is_numpy(x):
        summary = _summarize_numpy(x)
        return to_jsonable(summary, max_depth=max_depth, max_items=max_items, max_str=max_str, _depth=_depth + 1, _seen=_seen)

    # dataclass instance
    if dataclasses.is_dataclass(x) and not isinstance(x, type):
        try:
            return to_jsonable(asdict(cast(Any, x)), max_depth=max_depth, max_items=max_items, max_str=max_str, _depth=_depth + 1, _seen=_seen)
        except Exception:
            return _short_repr(x, max_str)

    # dataclass type
    if dataclasses.is_dataclass(x) and isinstance(x, type):
        return f"{x.__module__}.{x.__qualname__}"

    # objects with to_dict
    to_dict = getattr(x, "to_dict", None)
    if callable(to_dict):
        try:
            return to_jsonable(to_dict(), max_depth=max_depth, max_items=max_items, max_str=max_str, _depth=_depth + 1, _seen=_seen)
        except Exception:
            return _short_repr(x, max_str)

    # Mapping
    if isinstance(x, Mapping):
        items = list(x.items())
        if len(items) > max_items:
            kept: dict[str, Any] = {}
            for k, v in items[:max_items]:
                key = k if isinstance(k, str) else _short_repr(k, max_str)
                kept[str(key)] = to_jsonable(v, max_depth=max_depth, max_items=max_items, max_str=max_str, _depth=_depth + 1, _seen=_seen)
            return {"__truncated__": True, "kept": kept, "dropped": len(items) - max_items}

        out: dict[str, Any] = {}
        for k, v in items:
            key = k if isinstance(k, str) else _short_repr(k, max_str)
            out[str(key)] = to_jsonable(v, max_depth=max_depth, max_items=max_items, max_str=max_str, _depth=_depth + 1, _seen=_seen)
        return out

    # Iterables
    if isinstance(x, (list, tuple, set)):
        seq = list(cast(Iterable[Any], x))
        if len(seq) > max_items:
            kept_list = [to_jsonable(v, max_depth=max_depth, max_items=max_items, max_str=max_str, _depth=_depth + 1, _seen=_seen) for v in seq[:max_items]]
            return {"__truncated__": True, "kept": kept_list, "dropped": len(seq) - max_items}
        return [to_jsonable(v, max_depth=max_depth, max_items=max_items, max_str=max_str, _depth=_depth + 1, _seen=_seen) for v in seq]

    return _short_repr(x, max_str)


def _trace_full_market() -> bool:
    raw = os.getenv("QUANT_TRACE_FULL_MARKET", "")
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}

def _pick(d: Mapping[str, Any], keys: Iterable[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k in keys:
        if k in d:
            out[k] = d.get(k)
    return out


def _summarize_market_spec(market: Any) -> dict[str, Any] | None:
    # market can be dict or MarketSpec-like (dataclass/to_dict)
    if market is None:
        return None

    m = market
    if dataclasses.is_dataclass(m) and not isinstance(m, type):
        try:
            m = asdict(cast(Any, m))
        except Exception:
            return {"repr": _short_repr(market, 256)}
    to_dict = getattr(market, "to_dict", None)
    if callable(to_dict):
        try:
            m = to_dict()
        except Exception:
            return {"repr": _short_repr(market, 256)}

    if not isinstance(m, Mapping):
        return {"repr": _short_repr(market, 256)}

    # whitelist only what you said you want stable
    keep = _pick(m, ("venue", "session", "asset_class", "timezone", "calendar", "schema_version", "currency"))
    # hard-reduce to two fields if you want:
    keep = _pick(keep, ("venue", "session"))
    return {k: to_jsonable(v, max_depth=2, max_items=32, max_str=256) for k, v in keep.items()}


def _summarize_snapshot(snapshot: Any) -> Any:
    if snapshot is None:
        return None

    snap = snapshot
    if dataclasses.is_dataclass(snapshot) and not isinstance(snapshot, type):
        try:
            snap = asdict(cast(Any, snapshot))
        except Exception:
            return {"repr": _short_repr(snapshot, 512)}
    to_dict = getattr(snapshot, "to_dict", None)
    if callable(to_dict):
        try:
            snap = to_dict()
        except Exception:
            return {"repr": _short_repr(snapshot, 512)}

    if isinstance(snap, Mapping):
        out: dict[str, Any] = {}

        # identity fields (stable, small)
        # ident = _pick(snap, ("symbol", "domain", "schema_version", "data_ts", "timestamp"))
        ident = _pick(snap, ("data_ts"))
        for k, v in ident.items():
            out[k] = to_jsonable(v, max_depth=2, max_items=32, max_str=256)

        # market spec (whitelisted)
        if "market" in snap:
            ms = _summarize_market_spec(snap.get("market"))
            if ms is not None:
                out["market"] = ms

        # small numeric subset (excluding identity keys)
        numeric: dict[str, Any] = {}
        for k, v in snap.items():
            if k in ident or k in ("market", "frame"):
                continue
            if isinstance(v, (int, float)):
                numeric[str(k)] = v
                if len(numeric) >= 12:
                    break
        if numeric:
            out["numeric"] = numeric

        # heavy field
        if "frame" in snap:
            out["frame"] = to_jsonable(snap["frame"])  # your df summarizer must be tight

        return out

    return {"repr": _short_repr(snapshot, 512)}


def _summarize_market_snapshots(market_snapshots: Any) -> Any:
    # Full mode: still uses to_jsonable which will summarize DataFrames anyway.
    if _trace_full_market():
        return to_jsonable(market_snapshots)

    if market_snapshots is None:
        return None
    if not isinstance(market_snapshots, Mapping):
        return _summarize_snapshot(market_snapshots)

    out: dict[str, Any] = {}
    for domain, snaps in market_snapshots.items():
        if isinstance(snaps, Mapping):
            sym_out: dict[str, Any] = {}
            for symbol, snap in snaps.items():
                sym_out[str(symbol)] = _summarize_snapshot(snap)
            out[str(domain)] = sym_out
        else:
            out[str(domain)] = _summarize_snapshot(snaps)
    return out


def log_step_trace(
    logger: Any,  # keep Logger type if you have it
    *,
    step_ts: int,
    strategy: str | None = None,
    symbol: str | None = None,
    features: Any | None = None,
    models: Any | None = None,
    portfolio: Any | None = None,
    primary_snapshots: Any | None = None,
    market_snapshots: Any | None = None,
    decision_score: Any | None = None,
    target_position: Any | None = None,
    fills: Any | None = None,
    snapshot: Any | None = None,
    guardrails: Any | None = None,
    execution_outcomes: list[dict[str, Any]] | None = None,
    **meta: Any,
) -> None:
    payload: StepTracePayload = {}

    if strategy is not None:
        payload["strategy"] = str(strategy)
    if symbol is not None:
        payload["symbol"] = str(symbol)
    normalized_outcomes: list[ExecutionOutcome] = []
    if execution_outcomes is not None:
        for entry in execution_outcomes or []:
            if isinstance(entry, dict):
                normalized_outcomes.append(cast(ExecutionOutcome, entry))

    decision_inputs = {
        "features": to_jsonable(features),
        "models": to_jsonable(models),
        "primary_snapshots": to_jsonable(primary_snapshots),
        "market_snapshots": _summarize_market_snapshots(market_snapshots),
        "decision_score": to_jsonable(decision_score),
    }
    payload["intent_id"] = compute_intent_id(
        ts_ms=int(step_ts),
        strategy=strategy,
        symbol=symbol,
        decision_inputs=decision_inputs,
    )
    payload["features"] = decision_inputs["features"]
    payload["models"] = decision_inputs["models"]
    payload["portfolio"] = to_jsonable(portfolio)
    payload["primary_snapshots"] = decision_inputs["primary_snapshots"]
    payload["market_snapshots"] = decision_inputs["market_snapshots"]
    payload["decision_score"] = to_jsonable(decision_score)
    payload["target_position"] = to_jsonable(target_position)
    payload["fills"] = to_jsonable(fills)
    payload["snapshot"] = to_jsonable(snapshot)
    payload["guardrails"] = to_jsonable(guardrails)
    if normalized_outcomes:
        payload["execution_outcomes"] = normalized_outcomes

    context: dict[str, Any] = {}
    context.update(meta)
    context["category"] = CATEGORY_STEP_TRACE
    context["step_ts"] = int(step_ts)
    context.update(payload)

    logger.info("engine.step.trace", extra={"context": _sanitize_context(context)})
