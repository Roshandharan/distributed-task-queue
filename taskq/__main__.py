"""Command line: python -m taskq <command>."""

from __future__ import annotations

import argparse
import json
import logging

from taskq import registry
from taskq.client import Queue
from taskq.worker import Worker


def main() -> None:
    parser = argparse.ArgumentParser(prog="taskq")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("migrate", help="create the PostgreSQL tables")

    w = sub.add_parser("worker", help="run a worker")
    w.add_argument("--queues", default="default", help="comma-separated, highest priority first")
    w.add_argument("--concurrency", type=int, default=4)
    w.add_argument("--tasks", default="examples.tasks", help="comma-separated modules to import")

    a = sub.add_parser("api", help="run the HTTP API")
    a.add_argument("--host", default="0.0.0.0")
    a.add_argument("--port", type=int, default=8000)

    e = sub.add_parser("enqueue", help="enqueue one job")
    e.add_argument("task")
    e.add_argument("--payload", default="{}")
    e.add_argument("--queue", default="default")
    e.add_argument("--priority", type=int, default=5)
    e.add_argument("--delay", type=float, default=0.0)

    sub.add_parser("stats", help="print queue counts")
    sub.add_parser("dead", help="list the dead-letter queue")
    r = sub.add_parser("requeue", help="requeue a dead job")
    r.add_argument("job_id")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if args.command == "api":
        import uvicorn

        uvicorn.run("taskq.api:app", host=args.host, port=args.port)
        return

    q = Queue()
    if args.command == "migrate":
        q.store.migrate()
        print("schema ready")
    elif args.command == "worker":
        q.close()
        registry.load_modules([m for m in args.tasks.split(",") if m])
        worker = Worker(args.queues.split(","), concurrency=args.concurrency)
        worker.q.store.migrate()
        worker.run()
        return
    elif args.command == "enqueue":
        job = q.enqueue(
            args.task,
            json.loads(args.payload),
            queue=args.queue,
            priority=args.priority,
            delay_seconds=args.delay,
        )
        print(job.id)
    elif args.command == "stats":
        print(json.dumps(q.stats(), indent=2))
    elif args.command == "dead":
        for job in q.dead_letters():
            print(job.id, job.task, job.attempts, (job.last_error or "").splitlines()[0])
    elif args.command == "requeue":
        job = q.requeue_dead(args.job_id)
        print("requeued" if job else "no dead job with that id")
    q.close()


if __name__ == "__main__":
    main()
