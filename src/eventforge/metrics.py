"""Thread-safe in-process counters exported in Prometheus text format."""

from __future__ import annotations

from collections import Counter
from threading import Lock


class Metrics:
    def __init__(self) -> None:
        self._values: Counter[str] = Counter()
        self._lock = Lock()

    def increment(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._values[name] += amount

    def value(self, name: str) -> int:
        with self._lock:
            return int(self._values[name])

    def render(self) -> str:
        names = (
            "http_requests_total",
            "events_accepted_total",
            "events_duplicate_total",
            "replay_jobs_queued_total",
            "application_errors_total",
        )
        with self._lock:
            lines = [f"eventforge_{name} {int(self._values[name])}" for name in names]
        return "\n".join(lines) + "\n"
