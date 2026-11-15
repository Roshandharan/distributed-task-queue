"""Throughput benchmark with no-op jobs.

python -m scripts.bench --jobs 20000 --workers 4 --concurrency 8
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time

from taskq.client import Queue


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jobs", type=int, default=20000)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--concurrency", type=int, default=8)
    args = ap.parse_args()

    os.environ["TASKQ_NAMESPACE"] = "taskq-bench"
    q = Queue()
    q.store.migrate()
    with q.store.pool.connection() as conn:
        conn.execute("DELETE FROM jobs WHERE queue = 'bench'")
    q.broker.flush_namespace()

    t0 = time.monotonic()
    for start in range(0, args.jobs, 5000):
        q.enqueue_many("noop", [{} for _ in range(min(5000, args.jobs - start))], queue="bench")
    enqueue_s = time.monotonic() - t0

    procs = [
        subprocess.Popen(
            [
                sys.executable,
                "-m",
                "taskq",
                "worker",
                "--queues",
                "bench",
                "--tasks",
                "examples.tasks",
                "--concurrency",
                str(args.concurrency),
            ],
            env=dict(os.environ),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        for _ in range(args.workers)
    ]
    # Start the clock when the first job finishes, so process start-up is excluded.
    first = last = None
    while True:
        done = q.store.counts().get("bench", {}).get("succeeded", 0)
        now = time.monotonic()
        if done and first is None:
            first, first_done = now, done
        if done == args.jobs:
            last = now
            break
        time.sleep(0.1)
    for p in procs:
        p.send_signal(signal.SIGTERM)
    for p in procs:
        p.wait(timeout=10)

    with q.store.pool.connection() as conn:
        row = conn.execute(
            """
            SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY finished_at - started_at) AS p50,
                   percentile_cont(0.99) WITHIN GROUP (ORDER BY finished_at - started_at) AS p99
              FROM jobs WHERE queue = 'bench'
            """
        ).fetchone()
    print(
        json.dumps(
            {
                "jobs": args.jobs,
                "workers": args.workers,
                "threads_per_worker": args.concurrency,
                "cpus": os.cpu_count(),
                "enqueue_jobs_per_second": round(args.jobs / enqueue_s),
                "process_jobs_per_second": round((args.jobs - first_done) / (last - first)),
                "claim_to_done_p50_ms": round(row["p50"].total_seconds() * 1000, 2),
                "claim_to_done_p99_ms": round(row["p99"].total_seconds() * 1000, 2),
            },
            indent=2,
        )
    )
    q.close()


if __name__ == "__main__":
    main()
