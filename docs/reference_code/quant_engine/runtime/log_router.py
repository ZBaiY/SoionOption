import logging
import json
import traceback
from pathlib import Path
from typing import Any

from quant_engine.utils.paths import artifacts_root_from_file, resolve_under_root

ARTIFACTS_ROOT = artifacts_root_from_file(__file__, levels_up=3)


# ---------------------------------------------------------------------
# Base class: artifact-grade log sink
# ---------------------------------------------------------------------

class ArtifactFileHandler(logging.Handler):
    """
    Base class for artifact logs.
    Subclasses decide which category they accept.
    """

    category: str  # must be overridden

    def __init__(self, path: Path):
        super().__init__()
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, record: logging.LogRecord) -> None:
        context = getattr(record, "context", None)
        if not isinstance(context, dict):
            return

        if context.get("category") != self.category:
            return

        exc: str | None = None
        if record.exc_info:
            try:
                exc = "".join(traceback.format_exception(*record.exc_info))
            except Exception:
                exc = "<exc_info>"

        event: dict[str, Any] = {
            "timestamp": int(record.created * 1000),   # epoch ms int for replay
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "context": context,
        }
        if exc is not None:
            event["exc"] = exc

        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")


# ---------------------------------------------------------------------
# Concrete handlers
# ---------------------------------------------------------------------

class DecisionFileHandler(ArtifactFileHandler):
    """
    Stores decision_trace events.
    """
    category = "decision_trace"

    def __init__(self, run_id: str, base_dir: str = str(ARTIFACTS_ROOT / "runs")):
        base = resolve_under_root(ARTIFACTS_ROOT, base_dir, strip_prefix="artifacts")
        path = (
            base
            / run_id
            / "decisions"
            / "decisions.jsonl"
        )
        super().__init__(path)


class ExecutionFileHandler(ArtifactFileHandler):
    """
    Stores execution_discrepancy events.
    """
    category = "execution_discrepancy"

    def __init__(self, run_id: str, base_dir: str = str(ARTIFACTS_ROOT / "runs")):
        base = resolve_under_root(ARTIFACTS_ROOT, base_dir, strip_prefix="artifacts")
        path = (
            base
            / run_id
            / "execution"
            / "execution.jsonl"
        )
        super().__init__(path)


class DataRepairFileHandler(ArtifactFileHandler):
    """
    Stores data integrity repair / backfill events.
    """
    category = "data_integrity"

    def __init__(self, run_id: str, base_dir: str = str(ARTIFACTS_ROOT / "runs")):
        base = resolve_under_root(ARTIFACTS_ROOT, base_dir, strip_prefix="artifacts")
        path = (
            base
            / run_id
            / "data_repairs"
            / "repairs.jsonl"
        )
        super().__init__(path)


# ---------------------------------------------------------------------
# Wiring helper (optional but clean)
# ---------------------------------------------------------------------

def attach_artifact_handlers(
    logger: logging.Logger,
    *,
    run_id: str,
    decisions: bool = True,
    execution: bool = True,
    data_repairs: bool = False,
    base_dir: str = str(ARTIFACTS_ROOT / "runs"),
) -> None:
    """
    Attach artifact handlers to a logger at runtime.
    Call once during engine bootstrap.
    """

    # Avoid attaching duplicates
    existing = {(type(h), getattr(h, "category", None), getattr(h, "path", None)) for h in logger.handlers}

    if decisions:
        h = DecisionFileHandler(run_id, base_dir=base_dir)
        key = (type(h), getattr(h, "category", None), getattr(h, "path", None))
        if key not in existing:
            logger.addHandler(h)
            existing.add(key)

    if execution:
        h = ExecutionFileHandler(run_id, base_dir=base_dir)
        key = (type(h), getattr(h, "category", None), getattr(h, "path", None))
        if key not in existing:
            logger.addHandler(h)
            existing.add(key)

    if data_repairs:
        h = DataRepairFileHandler(run_id, base_dir=base_dir)
        key = (type(h), getattr(h, "category", None), getattr(h, "path", None))
        if key not in existing:
            logger.addHandler(h)
            existing.add(key)


# ---------------------------------------------------------------------
# Emit helper (preferred API for callers)
# ---------------------------------------------------------------------

def log_artifact(
    logger: logging.Logger,
    *,
    category: str,
    msg: str,
    level: int = logging.INFO,
    **payload: Any,
) -> None:
    """Emit an artifact-grade event.

    ArtifactFileHandler routes events by `context['category']`.
    """
    context: dict[str, Any] = {"category": category, **payload}
    logger.log(level, msg, extra={"context": context})
