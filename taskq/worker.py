"""Worker process: claims jobs, runs them, and reports the outcome.

Delivery is at-least-once. A job runs again if its worker dies or loses its
lease, so task functions should be idempotent.
"""

from __future__ import annotations

import logging
import os
import random
import signal
import socket
import threading
import time
import traceback
import uuid
from typing import Any

from taskq import registry
from taskq.client import Queue
from taskq.config import Settings, load
from taskq.maintenance import Maintainer
from taskq.store import Job, Store

log = logging.getLogger("taskq.worker")


def backoff_seconds(attempt: int, base: float, cap: float) -> float:
    """Exponential backoff with full jitter: uniform(0, min(cap, base * 2**(attempt-1)))."""
    return random.uniform(0, min(cap, base * (2 ** (attempt - 1))))


class Worker:
    def __init__(
        self,
        queues: list[str] | None = None,
        *,
        concurrency: int = 4,
        settings: Settings | None = None,
        queue: Queue | None = None,
    ) -> None:
        self.settings = settings or load()
        self.queues = queues or ["default"]
        self.concurrency = concurrency
        self.id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.q = queue or Queue(
            self.settings,
            store=Store(self.settings.database_url, pool_size=concurrency + 3),
        )
        self.maintainer = Maintainer(self.q, self.queues, self.id)
        self._stop = threading.Event()
        self._inflight: dict[str, int] = {}  # job id -> attempt number
        self._inflight_lock = threading.Lock()
        self.stats = {"succeeded": 0, "failed": 0, "dead": 0, "lost_lease": 0, "skipped": 0}

    # -- lifecycle -----------------------------------------------------------
    def run(self, install_signals: bool = True) -> None:
        if install_signals:
            signal.signal(signal.SIGTERM, lambda *_: self.stop())
            signal.signal(signal.SIGINT, lambda *_: self.stop())
        log.info(
            "worker %s started: queues=%s concurrency=%s", self.id, self.queues, self.concurrency
        )
        threads = [
            threading.Thread(target=self._consume, name=f"consumer-{i}", daemon=True)
            for i in range(self.concurrency)
        ]
        threads.append(threading.Thread(target=self._heartbeat_loop, name="heartbeat", daemon=True))
        threads.append(
            threading.Thread(target=self._maintenance_loop, name="maintenance", daemon=True)
        )
        for t in threads:
            t.start()
        while not self._stop.is_set():
            time.sleep(0.1)
        # Graceful shutdown: stop claiming, let in-flight jobs finish.
        for t in threads[: self.concurrency]:
            t.join()
        log.info("worker %s stopped: %s", self.id, self.stats)
        self.q.close()

    def stop(self) -> None:
        self._stop.set()

    # -- loops ---------------------------------------------------------------
    def _consume(self) -> None:
        while not self._stop.is_set():
            try:
                popped = self.q.broker.pop_ready(self.queues, timeout=1)
                if popped is None:
                    continue
                _queue, job_id = popped
                job = self.q.store.start(job_id, self.id, self.settings.lease_seconds)
                if job is None:
                    # Finished, already running elsewhere, or not due: nothing to do.
                    self.stats["skipped"] += 1
                    continue
                self._execute(job)
            except Exception:
                log.exception("consumer error; backing off")
                time.sleep(0.5)

    def _execute(self, job: Job) -> None:
        with self._inflight_lock:
            self._inflight[job.id] = job.attempts
        try:
            result: Any = registry.get(job.task)(**job.payload)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=5)}"
            self._on_failure(job, error)
        else:
            if self.q.store.succeed(job.id, job.attempts, result):
                self.stats["succeeded"] += 1
            else:
                # Our lease expired and another attempt owns the job now.
                self.stats["lost_lease"] += 1
                log.warning(
                    "job %s attempt %s finished after losing its lease", job.id, job.attempts
                )
        finally:
            with self._inflight_lock:
                self._inflight.pop(job.id, None)

    def _on_failure(self, job: Job, error: str) -> None:
        s = self.settings
        delay = backoff_seconds(job.attempts, s.backoff_base_seconds, s.backoff_cap_seconds)
        updated = self.q.store.fail(job.id, job.attempts, error, delay, self.id)
        if updated is None:
            self.stats["lost_lease"] += 1
        elif updated.status == "dead":
            self.stats["dead"] += 1
            log.error("job %s dead-lettered after %s attempts", job.id, job.attempts)
        else:
            self.stats["failed"] += 1
            self.q.publish(updated)

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self.settings.heartbeat_seconds):
            with self._inflight_lock:
                inflight = list(self._inflight.items())
            for job_id, attempt in inflight:
                try:
                    self.q.store.heartbeat(job_id, attempt, self.settings.lease_seconds)
                except Exception:
                    log.exception("heartbeat failed for %s", job_id)

    def _maintenance_loop(self) -> None:
        while not self._stop.wait(self.settings.maintenance_seconds):
            try:
                self.maintainer.tick()
            except Exception:
                log.exception("maintenance pass failed")
