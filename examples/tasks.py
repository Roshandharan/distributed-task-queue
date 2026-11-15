"""Example tasks. Run a worker with: python -m taskq worker --tasks examples.tasks"""

from __future__ import annotations

import random
import time

from taskq import task


@task("add")
def add(a: int, b: int) -> int:
    return a + b


@task("sleep")
def sleep(seconds: float = 0.1) -> dict[str, float]:
    time.sleep(seconds)
    return {"slept": seconds}


@task("flaky")
def flaky(fail_rate: float = 0.5) -> str:
    """Fails some of the time, to show retries with backoff."""
    if random.random() < fail_rate:
        raise RuntimeError("transient failure")
    return "ok"


@task("always_fails")
def always_fails() -> None:
    """Ends up in the dead-letter queue."""
    raise ValueError("this job can never succeed")


@task("noop")
def noop() -> None:
    return None
