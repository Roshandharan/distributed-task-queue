"""Background upkeep that every worker runs.

Three jobs, all safe to run from many workers at once:

1. promote   move delayed jobs (retries, scheduled jobs) to ready when due.
2. reap      requeue or dead-letter jobs whose worker stopped heartbeating.
3. reconcile re-publish queued jobs that are missing from Redis (a producer
             or worker died between its PostgreSQL write and its Redis write,
             or Redis lost its data).
"""

from __future__ import annotations

import logging
import time

from taskq.client import Queue

log = logging.getLogger("taskq.maintenance")


class Maintainer:
    def __init__(self, queue: Queue, queues: list[str], owner: str) -> None:
        self.q = queue
        self.queues = queues
        self.owner = owner
        self._cursor: str | None = None
        self._last_reconcile = 0.0

    def tick(self) -> dict[str, int]:
        s = self.q.settings
        done = {"promoted": 0, "reaped": 0, "dead": 0, "reconciled": 0}
        for name in self.queues:
            done["promoted"] += self.q.broker.promote_due(name)

        # One worker at a time does the PostgreSQL sweeps.
        if not self.q.broker.acquire_lock("maintenance", s.maintenance_seconds * 0.9, self.owner):
            return done

        for job in self.q.store.reap_expired():
            if job.status == "queued":
                self.q.publish(job)
                done["reaped"] += 1
            else:
                done["dead"] += 1
        if done["reaped"] or done["dead"]:
            log.warning("reaped %s expired leases, %s dead-lettered", done["reaped"], done["dead"])

        if time.monotonic() - self._last_reconcile >= s.reconcile_every_seconds:
            self._last_reconcile = time.monotonic()
            done["reconciled"] = self.reconcile()
        return done

    def reconcile(self, page_size: int = 1000) -> int:
        """Walk one page of long-queued jobs and re-publish them (ZADD NX, so
        jobs already in Redis are untouched)."""
        page = self.q.store.queued_page(
            self._cursor, self.q.settings.reconcile_grace_seconds, page_size
        )
        for job in page:
            self.q.publish(job)
        self._cursor = page[-1].id if len(page) == page_size else None
        return len(page)
