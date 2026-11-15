"""Redis dispatch layer.

Redis holds only ordering, never the job itself:

* ``{ns}:{queue}:ready``   sorted set of job ids that can run now.
  score = priority * 1e13 + created_at_ms, so lower priority numbers run first
  and jobs of equal priority run in FIFO order.
* ``{ns}:{queue}:delayed`` sorted set of ``"{priority}:{created_ms}:{job_id}"``
  scored by the time (ms) the job becomes runnable (retries with backoff, or
  jobs scheduled for later).

If Redis loses data, the reconciler rebuilds both sets from PostgreSQL.
"""

from __future__ import annotations

import time

import redis

PRIORITY_WEIGHT = 10**13  # > any millisecond timestamp for the next ~250 years

# Move every due member of the delayed set into the ready set, atomically.
_PROMOTE_LUA = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, member in ipairs(due) do
  local prio, created, id = string.match(member, '^(%d+):(%d+):(.+)$')
  redis.call('ZREM', KEYS[1], member)
  redis.call('ZADD', KEYS[2], 'NX', tonumber(prio) * 1e13 + tonumber(created), id)
end
return #due
"""


def now_ms() -> int:
    return int(time.time() * 1000)


def ready_score(priority: int, created_ms: int) -> float:
    return priority * PRIORITY_WEIGHT + created_ms


class Broker:
    def __init__(self, url: str, namespace: str = "taskq") -> None:
        self.redis = redis.Redis.from_url(url, decode_responses=True)
        self.ns = namespace
        self._promote = self.redis.register_script(_PROMOTE_LUA)

    def ready_key(self, queue: str) -> str:
        return f"{self.ns}:{queue}:ready"

    def delayed_key(self, queue: str) -> str:
        return f"{self.ns}:{queue}:delayed"

    # -- producers ---------------------------------------------------------
    def push_ready(self, queue: str, job_id: str, priority: int, created_ms: int) -> None:
        self.redis.zadd(self.ready_key(queue), {job_id: ready_score(priority, created_ms)}, nx=True)

    def push_delayed(
        self, queue: str, job_id: str, priority: int, created_ms: int, run_at_ms: int
    ) -> None:
        member = f"{priority}:{created_ms}:{job_id}"
        self.redis.zadd(self.delayed_key(queue), {member: run_at_ms}, nx=True)

    def push_many_ready(self, queue: str, jobs: list[tuple[str, int, int]]) -> None:
        """jobs: (job_id, priority, created_ms)."""
        if not jobs:
            return
        pipe = self.redis.pipeline(transaction=False)
        key = self.ready_key(queue)
        for start in range(0, len(jobs), 5000):
            chunk = jobs[start : start + 5000]
            pipe.zadd(key, {j: ready_score(p, c) for j, p, c in chunk}, nx=True)
        pipe.execute()

    # -- consumers ---------------------------------------------------------
    def pop_ready(self, queues: list[str], timeout: float) -> tuple[str, str] | None:
        """Block until a job is ready on any queue. Returns (queue, job_id).

        BZPOPMIN is atomic, so two workers can never receive the same member.
        """
        keys = [self.ready_key(q) for q in queues]
        popped = self.redis.bzpopmin(keys, timeout=timeout)
        if popped is None:
            return None
        key, job_id, _score = popped
        queue = key[len(self.ns) + 1 : -len(":ready")]
        return queue, job_id

    def promote_due(self, queue: str, batch: int = 1000) -> int:
        return int(
            self._promote(
                keys=[self.delayed_key(queue), self.ready_key(queue)],
                args=[now_ms(), batch],
            )
        )

    # -- maintenance -------------------------------------------------------
    def acquire_lock(self, name: str, ttl_seconds: float, owner: str) -> bool:
        """Short-lived lock so only one worker runs a maintenance pass at a time."""
        return bool(
            self.redis.set(f"{self.ns}:lock:{name}", owner, nx=True, px=int(ttl_seconds * 1000))
        )

    def depth(self, queue: str) -> dict[str, int]:
        pipe = self.redis.pipeline(transaction=False)
        pipe.zcard(self.ready_key(queue))
        pipe.zcard(self.delayed_key(queue))
        ready, delayed = pipe.execute()
        return {"ready": int(ready), "delayed": int(delayed)}

    def ping(self) -> bool:
        return bool(self.redis.ping())

    def flush_namespace(self) -> None:
        """Delete every key in this namespace (tests and local resets only)."""
        for key in self.redis.scan_iter(f"{self.ns}:*"):
            self.redis.delete(key)
