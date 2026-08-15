#!/usr/bin/env python3
"""CLI: compute watch history stats from local sync.db."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# repo root on path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from anime_sync.stats import write_watch_stats
from anime_sync.storage import db, ensure_loaded


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Watch history stats from sync.db")
    ap.add_argument("--json-out", default="watch_history_stats.json")
    ap.add_argument("--md-out", default="watch_history_stats.md")
    args = ap.parse_args(argv)
    ensure_loaded()
    write_watch_stats(db.get("entries") or {}, json_path=args.json_out, md_path=args.md_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
