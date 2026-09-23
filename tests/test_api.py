"""HTTP API 集成测试：提交幂等、查询、取消冲突码。"""

import logging

from fastapi.testclient import TestClient

from app.api import create_app
from app.clock import FakeClock
from app.service import OrchestrationService

logger = logging.getLogger("test.api")


def _client() -> tuple[TestClient, OrchestrationService]:
    svc = OrchestrationService(max_concurrency=2, clock=FakeClock())
    return TestClient(create_app(svc)), svc


def test_submit_get_cancel_flow() -> None:
    client, _ = _client()
    payload = {"idempotency_key": "http-1", "priority": 2, "dependencies": []}
    logger.info("输入 POST /tasks payload=%s", payload)

    r1 = client.post("/tasks", json=payload)
    assert r1.status_code == 201
    task = r1.json()
    assert task["state"] == "PENDING"

    r2 = client.post("/tasks", json=payload)
    assert r2.status_code == 201
    assert r2.json()["id"] == task["id"]
    assert r2.json()["submit_count"] == 2
    logger.info("重复提交收敛到同一 id=%s submit_count=2", task["id"])

    rg = client.get(f"/tasks/{task['id']}")
    assert rg.status_code == 200

    rc = client.post(f"/tasks/{task['id']}/cancel")
    assert rc.status_code == 200
    assert rc.json()["state"] == "CANCELLED"

    rc2 = client.post(f"/tasks/{task['id']}/cancel")
    assert rc2.status_code == 409
    assert rc2.json()["reason"] == "already_cancelled"
    logger.info("重复取消 -> 409 reason=%s", rc2.json()["reason"])


def test_get_unknown_returns_404() -> None:
    client, _ = _client()
    r = client.get("/tasks/ghost")
    assert r.status_code == 404
    assert r.json()["error"] == "task_not_found"
