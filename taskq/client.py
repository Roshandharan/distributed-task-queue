"""Producer-side API: enqueue jobs and inspect them."""

from __future__ import annotations

from typing import Any

from taskq.broker import Broker, now_ms
from taskq.config import Settings, load
from taskq.store import Job, Store


class Queue:
    def __init__(
        self,
        settings: Settings | None = None,
        *,
        store: Store | None = None,
        broker: Broker | None = None,
    ) -> None:
        self.settings = settings or load()
        self.store = store or Store(self.settings.database_url)
        self.broker = broker or Broker(self.settings.redis_url, self.settings.namespace)

    def enqueue(
        self,
        task: str,
        payload: dict[str, Any] | None = None,
        *,
        queue: str = "default",
        priority: int = 5,
        delay_seconds: float = 0.0,
        max_attempts: int | None = None,
        idempotency_key: str | None = None,
    ) -> Job:
        """Persist the job in PostgreSQL, then publish it to Redis.

        If the process dies between the two steps the job is still safe: the
        reconciler finds queued rows that are missing from Redis and re-adds them.
        """
        if not 0 <= priority <= 9:
            raise ValueError("priority must be between 0 (highest) and 9 (lowest)")
        job, created = self.store.insert(
            task,
            payload or {},
            queue=queue,
            priority=priority,
            max_attempts=max_attempts or self.settings.default_max_attempts,
            delay_seconds=delay_seconds,
            idempotency_key=idempotency_key,
        )
        if created:
            self.publish(job)
        return job

    def enqueue_many(
        self,
        task: str,
        payloads: list[dict[str, Any]],
        *,
        queue: str = "default",
        priority: int = 5,
        max_attempts: int | None = None,
    ) -> list[Job]:
        jobs = self.store.insert_many(
            task,
            payloads,
            queue=queue,
            priority=priority,
            max_attempts=max_attempts or self.settings.default_max_attempts,
        )
        self.broker.push_many_ready(queue, [(j.id, j.priority, j.created_ms) for j in jobs])
        return jobs

    def publish(self, job: Job) -> None:
        """Put a queued job into the right Redis set (idempotent)."""
        if job.run_at_ms <= now_ms():
            self.broker.push_ready(job.queue, job.id, job.priority, job.created_ms)
        else:
            self.broker.push_delayed(job.queue, job.id, job.priority, job.created_ms, job.run_at_ms)

    def get(self, job_id: str) -> Job | None:
        return self.store.get(job_id)

    def dead_letters(self, queue: str | None = None, limit: int = 100) -> list[Job]:
        return self.store.dead(queue, limit)

    def requeue_dead(self, job_id: str) -> Job | None:
        job = self.store.requeue_dead(job_id)
        if job is not None:
            self.publish(job)
        return job

    def stats(self) -> dict[str, Any]:
        counts = self.store.counts()
        return {
            queue: {**statuses, "redis": self.broker.depth(queue)}
            for queue, statuses in counts.items()
        }

    def close(self) -> None:
        self.store.close()
