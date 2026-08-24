from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from eventforge.cli import main
from eventforge.models import JobRecord
from eventforge.processing import Worker
from eventforge.schemas import EventIn
from eventforge.storage import Database
from eventforge.worker import run_once


def test_init_and_status_commands_emit_machine_readable_json(tmp_path: Path, capsys: object) -> None:
    path = tmp_path / "events.db"
    assert main(["init", "--db", str(path)]) == 0
    init_payload = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]
    assert init_payload["status"] == "initialized"

    assert main(["status", "--db", str(path)]) == 0
    status_payload = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]
    assert status_payload["healthy"] is True
    assert status_payload["counts"] == {"events": 0, "jobs": 0, "projections": 0}


def test_drain_command_processes_queued_jobs(tmp_path: Path, capsys: object) -> None:
    path = tmp_path / "events.db"
    database = Database(path)
    database.initialize()
    database.ingest(
        EventIn(
            tenant_id="acme",
            source="sdk.python",
            event_type="user.active",
            idempotency_key="cli-drain-0001",
            occurred_at=datetime.now(UTC),
            payload={},
        )
    )
    assert main(["drain", "--db", str(path), "--max-jobs", "10"]) == 0
    payload = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]
    assert payload == {"processed_jobs": 1}
    assert database.counts()["projections"] == 1


def test_replay_command_queues_reprojection(tmp_path: Path, capsys: object) -> None:
    path = tmp_path / "events.db"
    database = Database(path)
    database.initialize()
    database.ingest(
        EventIn(
            tenant_id="acme",
            source="sdk.python",
            event_type="user.active",
            idempotency_key="cli-replay-0001",
            occurred_at=datetime.now(UTC),
            payload={},
        )
    )
    assert main(["replay", "--db", str(path), "--tenant", "acme", "--limit", "10"]) == 0
    payload = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]
    assert payload == {"queued": 1, "tenant_id": "acme"}


def test_worker_once_processes_one_job_and_empty_queue_is_safe(tmp_path: Path) -> None:
    path = tmp_path / "events.db"
    database = Database(path)
    database.initialize()
    assert run_once(database) is False
    database.ingest(
        EventIn(
            tenant_id="acme",
            source="sdk.python",
            event_type="user.active",
            idempotency_key="worker-once-001",
            occurred_at=datetime.now(UTC),
            payload={},
        )
    )
    assert run_once(database) is True
    assert database.counts()["projections"] == 1


def test_dead_letter_cli_lists_requeues_and_recovers_failed_job(tmp_path: Path, capsys: object) -> None:
    path = tmp_path / "events.db"
    database = Database(path)
    database.initialize()
    database.ingest(
        EventIn(
            tenant_id="acme",
            source="cli.test",
            event_type="projection.failed",
            idempotency_key="cli-deadletter-01",
            occurred_at=datetime.now(UTC),
            payload={},
        )
    )

    def failing_projector(_database: Database, _job: JobRecord, _now: datetime) -> None:
        raise RuntimeError("forced failure")

    assert Worker(database, max_attempts=1, projector=failing_projector).process_one() is True
    assert main(["dead-letters", "--db", str(path), "--tenant", "acme", "--limit", "10"]) == 0
    dead_letters = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]
    assert len(dead_letters) == 1
    assert dead_letters[0]["last_error"] == "RuntimeError"

    assert main(["retry-failed", "--db", str(path), "--tenant", "acme", "--limit", "10"]) == 0
    retry_payload = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]
    assert retry_payload == {"requeued": 1, "tenant_id": "acme"}
    assert Worker(database).process_one() is True
    assert database.counts()["projections"] == 1
