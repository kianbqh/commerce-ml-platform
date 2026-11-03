"""Manually generate a static snapshot; does not start a server or open a browser."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from m4.dashboard import write_dashboard


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", required=True, help="Existing M4 run evidence folder; may be absent before a run")
    parser.add_argument("--out", required=True, help="New output folder; existing snapshots are never overwritten")
    args = parser.parse_args()
    snapshot = write_dashboard(args.run_root, args.out)
    print(Path(args.out).resolve() / "index.html")
    print("result status:", snapshot["run"]["status"])
