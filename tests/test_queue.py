"""End-to-end tests against real Redis and PostgreSQL."""

from __future__ import annotations

import dataclasses
import threading
import time
from collections import Counter

from taskq import task
from taskq.maintenance import Maintainer
from taskq.worker import backoff_seconds

calls: Counter[str] = Counter()
order: list[str] = []
lock = threading.Lock()


@task("t_add")
def t_add(a: int, b: int) -> int:
    return a + b


@task("t_record")
def t_record(name: str) -> None:
    with lock:
        order.append(name)
        calls[name] += 1


@task("t_fail_twice")
def t_fail_twice(key: str) -> str:
    with lock:
        calls[key] += 1
        n = calls[key]
    if n <= 2:
        raise RuntimeError(f"transient failure {n}")
    return "done"


@task("t_always_fails")
def t_always_fails() -> None:
    raise ValueError("permanent failure")


@task("t_sleep")
def t_sleep(seconds: float, name: str = "") -> str:
    time.sleep(seconds)
    with lock:
        calls[name] += 1
    return "slept"


def status(q, job_id):
    return q.get(job_id).status


def test_job_runs_and_stores_result(q, runner, wait_for):
    job = q.enqueue("t_add", {"a": 2, "b": 3})
    runner.start()
    wait_for(lambda: status(q, job.id) == "succeeded", what="job to succeed")
    done = q.get(job.id)
    assert done.result == 5
    assert done.attempts == 1


def test_idempotency_key_deduplicates(q, runner, wait_for):
    calls.clear()
    first = q.enqueue("t_record", {"name": "once"}, idempotency_key="order-42")
    second = q.enqueue("t_record", {"name": "once"}, idempotency_key="order-42")
    assert first.id == second.id
    runner.start()
    wait_for(lambda: status(q, first.id) == "succeeded")
    time.sleep(0.3)
    assert calls["once"] == 1


def test_higher_priority_runs_first_and_fifo_within_priority(q, runner, wait_for):
    order.clear()
    for name in ["low-1", "low-2"]:
        q.enqueue("t_record", {"name": name}, priority=9)
        time.sleep(0.002)
    for name in ["mid-1", "mid-2"]:
        q.enqueue("t_record", {"name": name}, priority=5)
        time.sleep(0.002)
    q.enqueue("t_record", {"name": "urgent"}, priority=0)
    runner.start(concurrency=1)
    wait_for(lambda: len(order) == 5)
    assert order == ["urgent", "mid-1", "mid-2", "low-1", "low-2"]


def test_failed_job_retries_with_backoff_then_succeeds(q, runner, wait_for):
    calls.clear()
    job = q.enqueue("t_fail_twice", {"key": "retry-me"}, max_attempts=5)
    runner.start()
    wait_for(lambda: status(q, job.id) == "succeeded")
    done = q.get(job.id)
    assert done.attempts == 3
    assert [e["event"] for e in q.store.events(job.id)] == ["retry", "retry"]


def test_exhausted_job_goes_to_dead_letter_queue_and_can_be_requeued(q, runner, wait_for):
    job = q.enqueue("t_always_fails", max_attempts=3)
    runner.start()
    wait_for(lambda: status(q, job.id) == "dead")
    dead = q.dead_letters()
    assert [d.id for d in dead] == [job.id]
    assert dead[0].attempts == 3
    assert "permanent failure" in dead[0].last_error
    assert [e["event"] for e in q.store.events(job.id)] == ["retry", "retry", "dead"]

    requeued = q.requeue_dead(job.id)
    assert requeued.status == "queued" and requeued.attempts == 0
    wait_for(lambda: status(q, job.id) == "dead")  # it fails again, as expected
    assert q.get(job.id).attempts == 3


def test_job_from_crashed_worker_is_recovered(q, runner, wait_for):
    """A worker claims a job and dies without finishing it or heartbeating."""
    job = q.enqueue("t_add", {"a": 1, "b": 1})
    assert q.broker.pop_ready(["default"], timeout=1) == ("default", job.id)
    claimed = q.store.start(job.id, "crashed-worker", lease_seconds=0.3)
    assert claimed.status == "running"

    runner.start()  # a healthy worker; its maintenance loop reaps the lease
    wait_for(lambda: status(q, job.id) == "succeeded")
    done = q.get(job.id)
    assert done.attempts == 2
    assert done.result == 2
    assert [e["event"] for e in q.store.events(job.id)] == ["lease_expired"]


def test_stale_worker_cannot_overwrite_newer_attempt(q):
    """Fencing: once a lease is lost, the old attempt's result is rejected."""
    job = q.enqueue("t_add", {"a": 1, "b": 1})
    q.broker.pop_ready(["default"], timeout=1)
    first = q.store.start(job.id, "worker-a", lease_seconds=0.1)
    time.sleep(0.2)
    assert [j.id for j in q.store.reap_expired()] == [job.id]
    second = q.store.start(job.id, "worker-b", lease_seconds=5)
    assert (first.attempts, second.attempts) == (1, 2)

    assert q.store.succeed(job.id, first.attempts, "stale") is False
    assert q.store.heartbeat(job.id, first.attempts, 5) is False
    assert q.store.fail(job.id, first.attempts, "stale", 0) is None
    assert q.store.succeed(job.id, second.attempts, "fresh") is True
    assert q.get(job.id).result == "fresh"


def test_job_that_keeps_killing_workers_is_dead_lettered(q):
    job = q.enqueue("t_add", {"a": 1, "b": 1}, max_attempts=2)
    maintainer = Maintainer(q, ["default"], "test")
    for _ in range(2):
        assert q.broker.pop_ready(["default"], timeout=1) is not None
        q.store.start(job.id, "doomed", lease_seconds=0.05)
        time.sleep(0.15)
        maintainer.tick()
    assert status(q, job.id) == "dead"
    assert q.broker.depth("default") == {"ready": 0, "delayed": 0}


def test_queue_rebuilds_after_redis_loses_all_data(q, runner, wait_for):
    jobs = [q.enqueue("t_add", {"a": i, "b": i}) for i in range(50)]
    q.enqueue("t_add", {"a": 0, "b": 0}, delay_seconds=0.3)
    q.broker.flush_namespace()
    assert q.broker.depth("default") == {"ready": 0, "delayed": 0}

    runner.start()  # the reconciler re-publishes from PostgreSQL
    wait_for(lambda: q.store.counts()["default"].get("succeeded") == 51, timeout=15)
    assert all(q.get(j.id).attempts == 1 for j in jobs)


def test_delayed_job_waits_until_due(q, runner, wait_for):
    job = q.enqueue("t_add", {"a": 1, "b": 2}, delay_seconds=0.6)
    assert q.broker.depth("default") == {"ready": 0, "delayed": 1}
    runner.start()
    time.sleep(0.3)
    assert status(q, job.id) == "queued"
    wait_for(lambda: status(q, job.id) == "succeeded")
    waited = (q.get(job.id).started_at - q.get(job.id).created_at).total_seconds()
    assert waited >= 0.55


def test_heartbeat_keeps_a_long_job_from_being_reaped(q, runner, wait_for, base_settings):
    calls.clear()
    short_lease = dataclasses.replace(base_settings, lease_seconds=0.4, heartbeat_seconds=0.1)
    job = q.enqueue("t_sleep", {"seconds": 1.2, "name": "long"})
    runner.start(settings=short_lease)
    wait_for(lambda: status(q, job.id) == "succeeded")
    assert q.get(job.id).attempts == 1
    assert calls["long"] == 1


def test_graceful_shutdown_finishes_in_flight_job(q, runner, wait_for):
    job = q.enqueue("t_sleep", {"seconds": 0.5, "name": "inflight"})
    worker = runner.start(concurrency=1)
    wait_for(lambda: status(q, job.id) == "running")
    worker.stop()
    runner.threads[0].join(timeout=5)
    assert status(q, job.id) == "succeeded"


def test_many_workers_never_run_a_job_twice(q, runner, wait_for):
    calls.clear()
    names = [f"job-{i}" for i in range(300)]
    q.enqueue_many("t_record", [{"name": n} for n in names])
    for _ in range(3):
        runner.start(concurrency=4)
    wait_for(lambda: q.store.counts()["default"].get("succeeded") == 300, timeout=20)
    assert all(calls[n] == 1 for n in names)


def test_backoff_grows_exponentially_and_is_capped():
    for attempt, ceiling in [(1, 1), (2, 2), (3, 4), (4, 8), (10, 60)]:
        samples = [backoff_seconds(attempt, base=1, cap=60) for _ in range(200)]
        assert 0 <= min(samples) and max(samples) <= ceiling
