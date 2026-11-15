"""Runtime settings, read from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


@dataclass(frozen=True)
class Settings:
    redis_url: str = os.environ.get("TASKQ_REDIS_URL", "redis://127.0.0.1:6379/0")
    database_url: str = os.environ.get(
        "TASKQ_DATABASE_URL", "postgresql://postgres@127.0.0.1:5432/taskq"
    )
    namespace: str = os.environ.get("TASKQ_NAMESPACE", "taskq")
    # How long a worker may hold a job without sending a heartbeat.
    lease_seconds: float = _float("TASKQ_LEASE_SECONDS", 30.0)
    heartbeat_seconds: float = _float("TASKQ_HEARTBEAT_SECONDS", 10.0)
    # Retry delay = min(cap, base * 2**(attempt-1)), with full jitter.
    backoff_base_seconds: float = _float("TASKQ_BACKOFF_BASE_SECONDS", 1.0)
    backoff_cap_seconds: float = _float("TASKQ_BACKOFF_CAP_SECONDS", 300.0)
    default_max_attempts: int = _int("TASKQ_MAX_ATTEMPTS", 5)
    # Maintenance loop: promote delayed jobs, reap expired leases, reconcile.
    maintenance_seconds: float = _float("TASKQ_MAINTENANCE_SECONDS", 1.0)
    reconcile_grace_seconds: float = _float("TASKQ_RECONCILE_GRACE_SECONDS", 15.0)
    reconcile_every_seconds: float = _float("TASKQ_RECONCILE_EVERY_SECONDS", 5.0)


def load() -> Settings:
    return Settings()
