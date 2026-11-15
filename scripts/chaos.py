"""Crash test: kill workers with SIGKILL while they process jobs.

    python -m scripts.chaos --jobs 5000 --workers 6 --kill-every 0.4

Passes only if every job ends up succeeded: none lost, none dead-lettered.
Because delivery is at-least-once, a job whose worker was killed after doing
its work but before recording success runs again; the report counts those.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import subprocess
import sys
import time

from taskq.client import Queue

ENV = {
    "TASKQ_LEASE_SECONDS": "2",
    "TASKQ_HEARTBEAT_SECONDS": "0.5",
    "TASKQ_MAINTENANCE_SECONDS": "0.25",
    "TASKQ_RECONCILE_GRACE_SECONDS": "2",
    "TASKQ_RECONCILE_EVERY_SECONDS": "1",
    "TASKQ_BACKOFF_BASE_SECONDS": "0.05",
    "TASKQ_NAMESPACE": "taskq-chaos",
}


def spawn(concurrency: int) -> subprocess.Popen:
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "taskq",
            "worker",
            "--queues",
            "chaos",
            "--tasks",
            "scripts.chaos_tasks",
            "--concurrency",
            str(concurrency),
        ],
        env={**os.environ, **ENV},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jobs", type=int, default=5000)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--kill-every", type=float, default=0.4, help="seconds between SIGKILLs")
    ap.add_argument(
        "--flush-redis",
        action="store_true",
        help="also wipe Redis once, mid-run, to test rebuilding from PostgreSQL",
    )
    ap.add_argument("--timeout", type=float, default=600)
    args = ap.parse_args()

    os.environ.update(ENV)
    q = Queue()
    q.store.migrate()
    with q.store.pool.connection() as conn:
        conn.execute("DELETE FROM jobs WHERE queue = 'chaos'")
        conn.execute("DROP TABLE IF EXISTS chaos_effects")
        conn.execute("CREATE TABLE chaos_effects (n INT, pid INT, at TIMESTAMPTZ DEFAULT now())")
    q.broker.flush_namespace()

    q.enqueue_many(
        "chaos_work", [{"n": i} for i in range(args.jobs)], queue="chaos", max_attempts=25
    )
    workers = [spawn(args.concurrency) for _ in range(args.workers)]
    started = time.monotonic()
    kills = 0
    flushed = False
    next_kill = started + 2.0  # let the workers boot first

    try:
        while True:
            counts = q.store.counts().get("chaos", {})
            done = counts.get("succeeded", 0)
            if done == args.jobs or counts.get("dead"):
                break
            if time.monotonic() - started > args.timeout:
                break
            if time.monotonic() >= next_kill:
                victim = random.randrange(len(workers))
                workers[victim].send_signal(signal.SIGKILL)
                workers[victim].wait()
                workers[victim] = spawn(args.concurrency)
                kills += 1
                next_kill = time.monotonic() + args.kill_every
            if args.flush_redis and not flushed and done > args.jobs // 3:
                q.broker.flush_namespace()
                flushed = True
            time.sleep(0.05)
    finally:
        elapsed = time.monotonic() - started
        for w in workers:
            w.send_signal(signal.SIGTERM)
        for w in workers:
            try:
                w.wait(timeout=10)
            except subprocess.TimeoutExpired:
                w.kill()

    counts = q.store.counts().get("chaos", {})
    with q.store.pool.connection() as conn:
        executions = conn.execute("SELECT count(*) AS n FROM chaos_effects").fetchone()["n"]
        distinct = conn.execute("SELECT count(DISTINCT n) AS n FROM chaos_effects").fetchone()["n"]
        max_attempts = conn.execute(
            "SELECT max(attempts) AS n FROM jobs WHERE queue = 'chaos'"
        ).fetchone()["n"]
        retried = conn.execute(
            "SELECT count(*) AS n FROM jobs WHERE queue = 'chaos' AND attempts > 1"
        ).fetchone()["n"]
    report = {
        "jobs": args.jobs,
        "workers": args.workers,
        "threads_per_worker": args.concurrency,
        "workers_killed": kills,
        "redis_flushed_mid_run": flushed,
        "succeeded": counts.get("succeeded", 0),
        "lost": args.jobs - counts.get("succeeded", 0),
        "dead_lettered": counts.get("dead", 0),
        "jobs_never_executed": args.jobs - distinct,
        "jobs_recovered_after_a_crash": retried,
        "duplicate_executions": executions - distinct,
        "max_attempts_for_one_job": max_attempts,
        "seconds": round(elapsed, 1),
    }
    print(json.dumps(report, indent=2))
    q.close()
    ok = report["lost"] == 0 and report["dead_lettered"] == 0 and report["jobs_never_executed"] == 0
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
