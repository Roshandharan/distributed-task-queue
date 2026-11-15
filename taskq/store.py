"""PostgreSQL job store: the source of truth for job state.

Every state change is a single guarded UPDATE. The ``attempts`` counter is a
fencing token: a worker may only finish the attempt it started, so a worker
that lost its lease (paused, partitioned, or presumed dead) cannot overwrite
the result of the attempt that replaced it.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from importlib import resources
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool


@dataclass
class Job:
    id: str
    queue: str
    task: str
    payload: dict[str, Any]
    priority: int
    status: str
    attempts: int
    max_attempts: int
    run_at: datetime
    created_at: datetime
    lease_expires_at: datetime | None = None
    worker_id: str | None = None
    idempotency_key: str | None = None
    result: Any = None
    last_error: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def created_ms(self) -> int:
        return int(self.created_at.timestamp() * 1000)

    @property
    def run_at_ms(self) -> int:
        return int(self.run_at.timestamp() * 1000)

    def to_dict(self) -> dict[str, Any]:
        out = dict(self.__dict__)
        for key, value in out.items():
            if isinstance(value, datetime):
                out[key] = value.isoformat()
        return out


def _job(row: dict[str, Any]) -> Job:
    row = dict(row)
    row["id"] = str(row["id"])
    return Job(**row)


class Store:
    def __init__(self, dsn: str, pool_size: int = 8) -> None:
        self.pool = ConnectionPool(
            dsn,
            min_size=1,
            max_size=pool_size,
            kwargs={"autocommit": True, "row_factory": dict_row},
            open=True,
        )

    def close(self) -> None:
        self.pool.close()

    def migrate(self) -> None:
        sql = resources.files("taskq").joinpath("schema.sql").read_text()
        # Workers and API pods all migrate on start; the advisory lock makes
        # concurrent CREATE TABLE IF NOT EXISTS calls safe.
        with self.pool.connection() as conn, conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(727475001)")
            conn.execute(sql)

    # -- enqueue -----------------------------------------------------------
    def insert(
        self,
        task: str,
        payload: dict[str, Any],
        *,
        queue: str,
        priority: int,
        max_attempts: int,
        delay_seconds: float = 0.0,
        idempotency_key: str | None = None,
    ) -> tuple[Job, bool]:
        """Insert a job. Returns (job, created); created is False when the
        idempotency key already existed and the earlier job is returned."""
        with self.pool.connection() as conn:
            row = conn.execute(
                """
                INSERT INTO jobs (id, queue, task, payload, priority, max_attempts,
                                  run_at, idempotency_key)
                VALUES (%s, %s, %s, %s, %s, %s,
                        now() + make_interval(secs => %s), %s)
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING *
                """,
                (
                    uuid.uuid4(),
                    queue,
                    task,
                    Jsonb(payload),
                    priority,
                    max_attempts,
                    delay_seconds,
                    idempotency_key,
                ),
            ).fetchone()
            if row is not None:
                return _job(row), True
            row = conn.execute(
                "SELECT * FROM jobs WHERE idempotency_key = %s", (idempotency_key,)
            ).fetchone()
            return _job(row), False

    def insert_many(
        self,
        task: str,
        payloads: list[dict[str, Any]],
        *,
        queue: str,
        priority: int,
        max_attempts: int,
    ) -> list[Job]:
        ids = [uuid.uuid4() for _ in payloads]
        with self.pool.connection() as conn:
            rows = conn.execute(
                """
                INSERT INTO jobs (id, queue, task, payload, priority, max_attempts)
                SELECT u.id, %s, %s, u.payload::jsonb, %s, %s
                FROM unnest(%s::uuid[], %s::text[]) AS u(id, payload)
                RETURNING *
                """,
                (queue, task, priority, max_attempts, ids, [json.dumps(p) for p in payloads]),
            ).fetchall()
        return [_job(r) for r in rows]

    # -- worker transitions --------------------------------------------------
    def start(self, job_id: str, worker_id: str, lease_seconds: float) -> Job | None:
        """queued -> running. Returns None if the job is not claimable (already
        taken, finished, or not due yet), in which case the worker drops it."""
        with self.pool.connection() as conn:
            row = conn.execute(
                """
                UPDATE jobs
                   SET status = 'running', attempts = attempts + 1, worker_id = %s,
                       lease_expires_at = now() + make_interval(secs => %s),
                       started_at = now(), updated_at = now()
                 WHERE id = %s AND status = 'queued' AND run_at <= now()
                RETURNING *
                """,
                (worker_id, lease_seconds, job_id),
            ).fetchone()
        return _job(row) if row else None

    def heartbeat(self, job_id: str, attempt: int, lease_seconds: float) -> bool:
        """Extend the lease. False means this attempt no longer owns the job."""
        with self.pool.connection() as conn:
            cur = conn.execute(
                """
                UPDATE jobs
                   SET lease_expires_at = now() + make_interval(secs => %s), updated_at = now()
                 WHERE id = %s AND status = 'running' AND attempts = %s
                """,
                (lease_seconds, job_id, attempt),
            )
            return cur.rowcount == 1

    def succeed(self, job_id: str, attempt: int, result: Any) -> bool:
        with self.pool.connection() as conn:
            cur = conn.execute(
                """
                UPDATE jobs
                   SET status = 'succeeded', result = %s, lease_expires_at = NULL,
                       last_error = NULL, finished_at = now(), updated_at = now()
                 WHERE id = %s AND status = 'running' AND attempts = %s
                """,
                (Jsonb(result), job_id, attempt),
            )
            return cur.rowcount == 1

    def fail(
        self,
        job_id: str,
        attempt: int,
        error: str,
        retry_in_seconds: float,
        worker_id: str | None = None,
    ) -> Job | None:
        """running -> queued (retry later) or -> dead (attempts exhausted)."""
        with self.pool.connection() as conn, conn.transaction():
            row = conn.execute(
                """
                UPDATE jobs
                   SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
                       run_at = CASE WHEN attempts >= max_attempts THEN run_at
                                     ELSE now() + make_interval(secs => %s) END,
                       finished_at = CASE WHEN attempts >= max_attempts THEN now() END,
                       last_error = %s, lease_expires_at = NULL, worker_id = NULL,
                       updated_at = now()
                 WHERE id = %s AND status = 'running' AND attempts = %s
                RETURNING *
                """,
                (retry_in_seconds, error[:4000], job_id, attempt),
            ).fetchone()
            if row is None:
                return None
            event = "dead" if row["status"] == "dead" else "retry"
            conn.execute(
                "INSERT INTO job_events (job_id, event, attempt, worker_id, detail) "
                "VALUES (%s, %s, %s, %s, %s)",
                (job_id, event, attempt, worker_id, error[:4000]),
            )
        return _job(row)

    # -- maintenance ---------------------------------------------------------
    def reap_expired(self, limit: int = 500) -> list[Job]:
        """Recover jobs whose worker stopped heartbeating (crash, kill, partition).

        The interrupted attempt still counts, so a job that keeps killing its
        worker ends up in the dead-letter queue instead of looping forever."""
        with self.pool.connection() as conn, conn.transaction():
            rows = conn.execute(
                """
                UPDATE jobs
                   SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
                       finished_at = CASE WHEN attempts >= max_attempts THEN now() END,
                       last_error = 'lease expired: worker stopped heartbeating',
                       lease_expires_at = NULL, run_at = now(), updated_at = now()
                 WHERE id IN (SELECT id FROM jobs
                               WHERE status = 'running' AND lease_expires_at < now()
                               ORDER BY lease_expires_at
                               LIMIT %s FOR UPDATE SKIP LOCKED)
                RETURNING *
                """,
                (limit,),
            ).fetchall()
            if rows:
                conn.cursor().executemany(
                    "INSERT INTO job_events (job_id, event, attempt, worker_id, detail) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    [
                        (
                            r["id"],
                            "dead" if r["status"] == "dead" else "lease_expired",
                            r["attempts"],
                            r["worker_id"],
                            "lease expired: worker stopped heartbeating",
                        )
                        for r in rows
                    ],
                )
        return [_job(r) for r in rows]

    def queued_page(self, after_id: str | None, grace_seconds: float, limit: int) -> list[Job]:
        """Queued jobs untouched for ``grace_seconds``, in id order (keyset paging).
        The reconciler re-adds these to Redis in case the dispatch entry was lost."""
        with self.pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM jobs
                 WHERE status = 'queued'
                   AND updated_at < now() - make_interval(secs => %s)
                   AND (%s::uuid IS NULL OR id > %s::uuid)
                 ORDER BY id LIMIT %s
                """,
                (grace_seconds, after_id, after_id, limit),
            ).fetchall()
        return [_job(r) for r in rows]

    # -- reads / dead-letter queue -------------------------------------------
    def get(self, job_id: str) -> Job | None:
        with self.pool.connection() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id = %s", (job_id,)).fetchone()
        return _job(row) if row else None

    def events(self, job_id: str) -> list[dict[str, Any]]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                "SELECT event, attempt, worker_id, detail, at FROM job_events "
                "WHERE job_id = %s ORDER BY id",
                (job_id,),
            ).fetchall()
        return [{**r, "at": r["at"].isoformat()} for r in rows]

    def dead(self, queue: str | None = None, limit: int = 100) -> list[Job]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM jobs
                 WHERE status = 'dead' AND (%s::text IS NULL OR queue = %s)
                 ORDER BY finished_at DESC LIMIT %s
                """,
                (queue, queue, limit),
            ).fetchall()
        return [_job(r) for r in rows]

    def requeue_dead(self, job_id: str) -> Job | None:
        """dead -> queued with a fresh attempt budget."""
        with self.pool.connection() as conn, conn.transaction():
            row = conn.execute(
                """
                UPDATE jobs
                   SET status = 'queued', attempts = 0, run_at = now(), finished_at = NULL,
                       updated_at = now()
                 WHERE id = %s AND status = 'dead'
                RETURNING *
                """,
                (job_id,),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                "INSERT INTO job_events (job_id, event, attempt, detail) "
                "VALUES (%s, 'requeued', 0, 'requeued from dead-letter queue')",
                (job_id,),
            )
        return _job(row)

    def counts(self) -> dict[str, dict[str, int]]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                "SELECT queue, status, count(*) AS n FROM jobs GROUP BY queue, status"
            ).fetchall()
        out: dict[str, dict[str, int]] = {}
        for r in rows:
            out.setdefault(r["queue"], {})[r["status"]] = int(r["n"])
        return out

    def queues(self) -> list[str]:
        with self.pool.connection() as conn:
            rows = conn.execute("SELECT DISTINCT queue FROM jobs").fetchall()
        return [r["queue"] for r in rows]

    def truncate(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("TRUNCATE jobs, job_events")
