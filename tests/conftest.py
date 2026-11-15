from __future__ import annotations

import dataclasses
import threading
import time
from collections.abc import Callable, Iterator

import pytest

from taskq.broker import Broker
from taskq.client import Queue
from taskq.config import load
from taskq.store import Store
from taskq.worker import Worker


@pytest.fixture(scope="session")
def base_settings():
    return dataclasses.replace(
        load(),
        namespace="taskq-test",
        lease_seconds=5.0,
        heartbeat_seconds=0.2,
        backoff_base_seconds=0.05,
        backoff_cap_seconds=0.2,
        maintenance_seconds=0.1,
        reconcile_grace_seconds=0.0,
        reconcile_every_seconds=0.2,
    )


@pytest.fixture(scope="session")
def store(base_settings) -> Iterator[Store]:
    s = Store(base_settings.database_url, pool_size=20)
    s.migrate()
    yield s
    s.close()


@pytest.fixture
def q(base_settings, store) -> Queue:
    broker = Broker(base_settings.redis_url, base_settings.namespace)
    broker.flush_namespace()
    store.truncate()
    return Queue(base_settings, store=store, broker=broker)


class Runner:
    """Runs workers in background threads for the duration of a test."""

    def __init__(self, q: Queue) -> None:
        self.q = q
        self.workers: list[Worker] = []
        self.threads: list[threading.Thread] = []

    def start(self, concurrency: int = 2, queues: list[str] | None = None, settings=None) -> Worker:
        queue = (
            self.q
            if settings is None
            else Queue(settings, store=self.q.store, broker=self.q.broker)
        )
        w = Worker(queues, concurrency=concurrency, settings=queue.settings, queue=queue)
        w.q.close = lambda: None  # the shared pool outlives each worker
        t = threading.Thread(target=w.run, kwargs={"install_signals": False}, daemon=True)
        t.start()
        self.workers.append(w)
        self.threads.append(t)
        return w

    def stop(self) -> None:
        for w in self.workers:
            w.stop()
        for t in self.threads:
            t.join(timeout=10)


@pytest.fixture
def runner(q) -> Iterator[Runner]:
    r = Runner(q)
    yield r
    r.stop()


@pytest.fixture
def wait_for() -> Callable[..., None]:
    def _wait(predicate: Callable[[], bool], timeout: float = 10.0, what: str = "") -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.02)
        raise AssertionError(f"timed out waiting for {what or predicate}")

    return _wait
