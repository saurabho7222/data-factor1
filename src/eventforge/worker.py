"""Long-running projection worker process with graceful polling semantics."""

from __future__ import annotations

import argparse
import os
import time
from collections.abc import Sequence
from pathlib import Path

from .processing import Worker
from .storage import Database

DEFAULT_DB = Path(".local/eventforge.db")


def run_once(database: Database, *, max_attempts: int = 3) -> bool:
    database.initialize()
    return Worker(database, max_attempts=max_attempts).process_one()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="eventforge-worker", description="EventForge projection worker")
    parser.add_argument("--db", type=Path, default=Path(os.environ.get("EVENTFORGE_DB", DEFAULT_DB)))
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--poll-interval", type=float, default=0.25)
    parser.add_argument("--max-attempts", type=int, default=3)
    args = parser.parse_args(argv)
    if args.poll_interval < 0.01 or args.poll_interval > 60:
        parser.error("--poll-interval must be between 0.01 and 60 seconds")

    database = Database(args.db)
    database.initialize()
    worker = Worker(database, max_attempts=args.max_attempts)
    if args.once:
        worker.process_one()
        return 0

    try:
        while True:
            if not worker.process_one():
                time.sleep(args.poll_interval)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
