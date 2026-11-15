from __future__ import annotations

from fastapi.testclient import TestClient

from taskq import task
from taskq.api import create_app


@task("t_api_fail")
def t_api_fail() -> None:
    raise RuntimeError("nope")


def test_api_enqueue_inspect_and_dead_letter_flow(q, runner, wait_for):
    with TestClient(create_app(q)) as client:
        assert client.get("/healthz").json() == {"ok": True}

        r = client.post("/jobs", json={"task": "t_add", "payload": {"a": 4, "b": 5}})
        assert r.status_code == 202
        ok_id = r.json()["id"]
        bad_id = client.post("/jobs", json={"task": "t_api_fail", "max_attempts": 1}).json()["id"]
        assert client.post("/jobs", json={"task": "t_add", "priority": 12}).status_code == 422

        runner.start()
        wait_for(lambda: client.get(f"/jobs/{ok_id}").json()["status"] == "succeeded")
        wait_for(lambda: client.get(f"/jobs/{bad_id}").json()["status"] == "dead")

        assert client.get(f"/jobs/{ok_id}").json()["result"] == 9
        assert [d["id"] for d in client.get("/dead").json()] == [bad_id]
        assert client.get(f"/jobs/{bad_id}").json()["events"][0]["event"] == "dead"
        assert client.post(f"/dead/{bad_id}/requeue").status_code == 200
        assert client.post(f"/dead/{ok_id}/requeue").status_code == 404
        assert client.get("/jobs/00000000-0000-0000-0000-000000000000").status_code == 404

        stats = client.get("/stats").json()
        assert stats["default"]["succeeded"] == 1
        assert 'taskq_jobs{queue="default",status="succeeded"} 1' in client.get("/metrics").text
