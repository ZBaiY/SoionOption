from __future__ import annotations
import argparse
from pathlib import Path
import subprocess
import sys

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run_id", required=True)
    ap.add_argument("--root", default=".")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    script = root / "scripts" / "logging_smoke.py"
    if not script.exists():
        print(f"missing {script}", file=sys.stderr)
        return 2

    cmd = [sys.executable, str(script), "--run_id", args.run_id, "--root", str(root)]
    return subprocess.call(cmd)

if __name__ == "__main__":
    raise SystemExit(main())
