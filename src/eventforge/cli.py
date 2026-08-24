"""Operational CLI for local administration and deterministic batch actions."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from .processing import Worker
from .replay import enqueue_replay
from .schemas import ReplayRequest
from .storage import Database

DEFAULT_DB = Path(".local/eventforge.db")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="eventforge", description="EventForge operator CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command in ("init", "status"):
        sub = subparsers.add_parser(command)
        sub.add_argument("--db", type=Path, default=DEFAULT_DB)

    replay = subparsers.add_parser("replay")
    replay.add_argument("--db", type=Path, default=DEFAULT_DB)
    replay.add_argument("--tenant", required=True)
    replay.add_argument("--event-type")
    replay.add_argument("--start-date")
    replay.add_argument("--end-date")
    replay.add_argument("--limit", type=int, default=1000)

    drain = subparsers.add_parser("drain")
    drain.add_argument("--db", type=Path, default=DEFAULT_DB)
    drain.add_argument("--max-jobs", type=int, default=1000)
    drain.add_argument("--max-attempts", type=int, default=3)
    return parser


def _database(path: Path) -> Database:
    database = Database(path)
    database.initialize()
    return database


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    database = _database(args.db)

    if args.command == "init":
        print(json.dumps({"status": "initialized", "database": str(args.db)}, sort_keys=True))
        return 0
    if args.command == "status":
        print(
            json.dumps(
                {"healthy": database.healthcheck(), "database": str(args.db), "counts": database.counts()},
                sort_keys=True,
            )
        )
        return 0
    if args.command == "replay":
        request = ReplayRequest(
            tenant_id=args.tenant,
            event_type=args.event_type,
            start_date=args.start_date,
            end_date=args.end_date,
            limit=args.limit,
        )
        queued = enqueue_replay(database, request)
        print(json.dumps({"queued": queued, "tenant_id": request.tenant_id}, sort_keys=True))
        return 0
    if args.command == "drain":
        if args.max_jobs < 1 or args.max_jobs > 100_000:
            raise SystemExit("--max-jobs must be between 1 and 100000")
        worker = Worker(database, max_attempts=args.max_attempts)
        processed = 0
        while processed < args.max_jobs and worker.process_one():
            processed += 1
        print(json.dumps({"processed_jobs": processed}, sort_keys=True))
        return 0
    raise AssertionError(f"unhandled command {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
