# taskq: a distributed task queue built from primitives

A job queue written from scratch in Python on **Redis** and **PostgreSQL**, with no Celery,
RQ, or other queue library underneath. It has priority queues, retries with exponential
backoff, a dead-letter queue, and worker leases, and it is built so that **no job is lost
when a worker crashes**.

[![CI](https://github.com/Roshandharan/distributed-task-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Roshandharan/distributed-task-queue/actions/workflows/ci.yml)

## Measured results

**Crash test** (`make chaos`): 6 worker processes run 5,000 jobs while the test harness
kills a random worker with `SIGKILL` every 0.4 seconds and wipes Redis once mid-run.

| | |
|---|---|
| Workers killed | 33 |
| Redis wiped mid-run | yes |
| Jobs lost | **0 of 5,000** |
| Jobs dead-lettered | 0 |
| Jobs recovered after their worker was killed | 104 |
| Jobs that ran twice (at-least-once delivery) | 3 |

**Throughput** (`make bench`): 20,000 no-op jobs, 4 worker processes with 8 threads each, with
Redis, PostgreSQL, and the workers all sharing one 2-vCPU container.

| | |
|---|---|
| Processing | about 630 jobs/second |
| Enqueue (batched) | about 39,000 jobs/second |
| Claim-to-done latency | p50 2.4 ms, p99 18 ms |

These are single runs on a small shared machine and vary a little from run to run.
Processing is CPU-bound in that setup: every job costs two durable PostgreSQL writes, and
the database, Redis, and workers compete for the same two cores.

## Design

```mermaid
flowchart LR
    P[Producer / HTTP API] -->|1. INSERT job| PG[(PostgreSQL<br/>source of truth)]
    P -->|2. ZADD job id| R[(Redis<br/>ready + delayed sets)]
    R -->|BZPOPMIN| W[Workers]
    W -->|claim, heartbeat,<br/>succeed, fail| PG
    W -->|retry with backoff| R
    M[Maintenance loop<br/>in every worker] -->|reap expired leases<br/>reconcile lost entries| PG
    M -->|promote due jobs<br/>re-publish| R
```

**PostgreSQL is the source of truth; Redis is only the dispatch order.** Every job is a row
in `jobs`. Redis holds two sorted sets per queue that contain job ids and nothing else, so
Redis can lose all of its data and the queue rebuilds itself.

| Concern | How it works |
|---|---|
| **Priority** | The ready set is scored `priority * 1e13 + created_ms`. Priority 0 runs before priority 9, and equal priorities run first-in, first-out. |
| **No double claim** | Workers block on `BZPOPMIN`, which is atomic, then move the row `queued -> running` with a guarded `UPDATE`. Only one worker can win. |
| **Crashed workers** | A running job holds a lease that its worker extends with heartbeats. If the heartbeats stop, a maintenance pass requeues the job. |
| **Zombie workers** | The attempt number is a fencing token. A worker that lost its lease cannot overwrite the result of the attempt that replaced it. |
| **Retries** | A failed job goes to the delayed set and returns after `uniform(0, min(cap, base * 2^(attempt-1)))`: exponential backoff with full jitter. |
| **Dead-letter queue** | After `max_attempts` a job becomes `dead`, with its last error and a per-attempt history. Dead jobs can be listed and requeued. |
| **Poison jobs** | An attempt that kills its worker still counts, so a job that keeps crashing workers is dead-lettered and cannot loop forever. |
| **Lost dispatch entries** | A reconciler pages through long-queued rows and re-publishes them with `ZADD NX`. This covers a crash between the PostgreSQL write and the Redis write, and a full Redis wipe. |
| **Duplicate submissions** | An optional idempotency key is enforced by a unique index. |
| **Graceful shutdown** | On `SIGTERM` a worker stops claiming and finishes its in-flight jobs. |

### Delivery guarantee

Delivery is **at-least-once**. If a worker dies after doing the work but before recording
success, the job runs again, so task functions should be idempotent. The crash test above
measured 3 such repeats in 5,000 jobs under 33 worker kills.

### Job lifecycle

```
queued --claim--> running --ok--> succeeded
   ^                 |
   |                 +--error, attempts left--> queued (after backoff)
   |                 +--lease expired---------> queued
   |                 +--attempts exhausted----> dead
   +------------------ requeue ----------------- dead
```

## Run it

Needs Python 3.11+, Redis, and PostgreSQL.

```bash
pip install -e ".[dev]"
export TASKQ_REDIS_URL=redis://localhost:6379/0
export TASKQ_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/taskq

python -m taskq migrate
python -m taskq worker --tasks examples.tasks --concurrency 4 &
python -m taskq enqueue add --payload '{"a": 2, "b": 3}' --priority 0
python -m taskq stats
```

Or the whole stack with Docker:

```bash
docker compose up --build --scale worker=3
curl -X POST localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"task": "add", "payload": {"a": 2, "b": 3}, "priority": 0}'
```

### Define a task

```python
from taskq import Queue, task


@task("send_email")
def send_email(to: str, subject: str) -> None: ...


q = Queue()
q.enqueue(
    "send_email",
    {"to": "a@example.com", "subject": "hi"},
    priority=2,
    max_attempts=5,
    idempotency_key="welcome-a@example.com",
)
q.enqueue("send_email", {"to": "b@example.com", "subject": "later"}, delay_seconds=3600)
```

### HTTP API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/jobs` | Enqueue a job |
| `GET` | `/jobs/{id}` | Status, result, and attempt history |
| `GET` | `/dead` | List the dead-letter queue |
| `POST` | `/dead/{id}/requeue` | Requeue a dead job |
| `GET` | `/stats` | Counts per queue and status |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/healthz` | Health check |

## Kubernetes

`k8s/` holds manifests for the API, workers, Redis, and PostgreSQL, with a
HorizontalPodAutoscaler and a PodDisruptionBudget for the workers.

```bash
docker build -t taskq:latest .
kind create cluster && kind load docker-image taskq:latest
kubectl apply -k k8s
bash scripts/k8s_smoke.sh    # enqueues 200 jobs, force-deletes a worker pod, expects all to succeed
```

Redis runs without persistence on purpose: a restarted Redis comes back empty and the
reconciler refills it from PostgreSQL.

## Tests

```bash
make test    # 15 tests against real Redis and PostgreSQL
make chaos   # the crash test above
make bench   # the throughput benchmark above
```

The tests cover priority order, idempotency keys, retries, the dead-letter queue, recovery
from a crashed worker, fencing of stale workers, poison jobs, rebuilding after Redis data
loss, delayed jobs, heartbeats on long jobs, graceful shutdown, and 12 concurrent consumers
running 300 jobs exactly once each.

CI runs the lint, the tests, and the crash test on every push, then builds the image,
deploys it to a `kind` cluster, and runs the Kubernetes smoke test.

## Limits

- **At-least-once, not exactly-once.** Tasks need to be idempotent.
- **One PostgreSQL primary.** Throughput is bounded by its write rate; there is no sharding.
- **No cron-style schedules, rate limits, or task chaining.**
- **Single Redis instance.** Redis Cluster is not supported, since each queue's keys are used
  together in one Lua script.
- **The autoscaler scales on CPU.** Scaling on queue depth would need KEDA or a custom
  metrics adapter reading `/metrics`.

## Layout

```
taskq/
  broker.py       Redis: ready and delayed sorted sets, atomic promotion script
  store.py        PostgreSQL: guarded state transitions, fencing, dead-letter queue
  client.py       enqueue and inspect jobs
  worker.py       consumer threads, heartbeats, backoff, graceful shutdown
  maintenance.py  promote, reap expired leases, reconcile
  api.py          FastAPI service
scripts/          crash test, benchmark, Kubernetes smoke test
k8s/              Kubernetes manifests
tests/            pytest suite
```
