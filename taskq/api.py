"""HTTP API for producers and operators."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from taskq.client import Queue


class EnqueueRequest(BaseModel):
    task: str
    payload: dict[str, Any] = Field(default_factory=dict)
    queue: str = "default"
    priority: int = Field(default=5, ge=0, le=9)
    delay_seconds: float = Field(default=0.0, ge=0)
    max_attempts: int | None = Field(default=None, ge=1)
    idempotency_key: str | None = None


def create_app(queue: Queue | None = None) -> FastAPI:
    state: dict[str, Queue] = {}

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        state["q"] = queue or Queue()
        state["q"].store.migrate()
        yield
        if queue is None:
            state["q"].close()

    app = FastAPI(title="taskq", version="0.1.0", lifespan=lifespan)

    @app.get("/healthz")
    def healthz() -> dict[str, bool]:
        q = state["q"]
        q.store.counts()
        return {"ok": q.broker.ping()}

    @app.post("/jobs", status_code=202)
    def enqueue(req: EnqueueRequest) -> dict[str, Any]:
        job = state["q"].enqueue(
            req.task,
            req.payload,
            queue=req.queue,
            priority=req.priority,
            delay_seconds=req.delay_seconds,
            max_attempts=req.max_attempts,
            idempotency_key=req.idempotency_key,
        )
        return job.to_dict()

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str) -> dict[str, Any]:
        job = state["q"].get(job_id)
        if job is None:
            raise HTTPException(404, "job not found")
        return {**job.to_dict(), "events": state["q"].store.events(job_id)}

    @app.get("/dead")
    def dead(queue: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return [j.to_dict() for j in state["q"].dead_letters(queue, limit)]

    @app.post("/dead/{job_id}/requeue")
    def requeue(job_id: str) -> dict[str, Any]:
        job = state["q"].requeue_dead(job_id)
        if job is None:
            raise HTTPException(404, "no dead job with that id")
        return job.to_dict()

    @app.get("/stats")
    def stats() -> dict[str, Any]:
        return state["q"].stats()

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics() -> str:
        """Prometheus text format; queue depth here is what the autoscaler reads."""
        lines = [
            "# HELP taskq_jobs Jobs by queue and status.",
            "# TYPE taskq_jobs gauge",
        ]
        depth_lines = [
            "# HELP taskq_redis_depth Jobs waiting in Redis by queue and set.",
            "# TYPE taskq_redis_depth gauge",
        ]
        for name, data in state["q"].stats().items():
            for status, n in data.items():
                if status == "redis":
                    for kind, depth in n.items():
                        depth_lines.append(
                            f'taskq_redis_depth{{queue="{name}",set="{kind}"}} {depth}'
                        )
                else:
                    lines.append(f'taskq_jobs{{queue="{name}",status="{status}"}} {n}')
        return "\n".join(lines + depth_lines) + "\n"

    return app


app = create_app()
