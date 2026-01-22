from __future__ import annotations
from pathlib import Path
import subprocess
import sys

def test_logging_smoke(tmp_path: Path):
    # run logging_smoke.py against a temp copy of artifacts folder
    root = Path(__file__).resolve().parents[1]
    run_id = "pytest_run"
    # ensure artifacts dir in temp
    (tmp_path / "artifacts").mkdir(parents=True, exist_ok=True)

    script = root / "scripts" / "logging_smoke.py"
    assert script.exists()

    subprocess.check_call([sys.executable, str(script), "--run_id", run_id, "--root", str(tmp_path)])

    logs = tmp_path / "artifacts" / run_id / "logs"
    assert (logs / "default.jsonl").exists()
    assert (logs / "trace.jsonl").exists()
    assert (logs / "asyncio.jsonl").exists()

    assert (logs / "default.jsonl").read_text(encoding="utf-8").strip()
    assert (logs / "trace.jsonl").read_text(encoding="utf-8").strip()
    assert (logs / "asyncio.jsonl").read_text(encoding="utf-8").strip()
