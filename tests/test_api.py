"""FastAPI HTTP 层集成测试：状态码、错误码可区分、幂等响应。"""
from __future__ import annotations

import pytest

from fastapi.testclient import TestClient

from orchestration.api import create_app
from orchestration.clock import FakeClock
from orchestration.executor import LocalExecutor
from orchestration.service import OrchestrationService


@pytest.fixture
def client():
    clock = FakeClock()
    executor = LocalExecutor(max_workers=4)
    executor.register("echo", lambda p, ctx: {"echo": p})
    executor.register("boom", lambda p, ctx: (_ for _ in ()).throw(RuntimeError("x")))
    service = OrchestrationService(clock, executor, max_concurrency=2, lease_timeout=10.0)
    app = create_app(service)
    with TestClient(app) as c:
        yield c, service, clock
    executor.shutdown()


def test_submit_idempotent_via_http(client):
    c, service, _ = client
    body = {"handler": "echo", "payload": {"x": 1}, "idempotency_key": "K"}
    r1 = c.post("/tasks", json=body)
    r2 = c.post("/tasks", json=body)
    assert r1.status_code == 201 and r1.json()["created"] is True
    assert r2.status_code == 201 and r2.json()["created"] is False
    assert r1.json()["task"]["id"] == r2.json()["task"]["id"]


def test_get_missing_returns_404_with_code(client):
    c, _, _ = client
    r = c.get("/tasks/nope")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "task_not_found"


def test_cancel_completed_returns_409_distinct_code(client):
    import time

    c, service, _ = client
    r = c.post("/tasks", json={"handler": "echo"})
    tid = r.json()["task"]["id"]
    for _ in range(100):
        c.post("/scheduler/tick")
        if c.get(f"/tasks/{tid}").json()["task"]["state"] == "COMPLETED":
            break
        time.sleep(0.01)
    r = c.post(f"/tasks/{tid}/cancel", json={"reason": "x"})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "task_completed"


def test_unknown_dependency_returns_400(client):
    c, _, _ = client
    r = c.post("/tasks", json={"handler": "echo", "dependencies": ["ghost"]})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "unknown_dependency"


def test_tick_returns_decisions(client):
    c, _, _ = client
    c.post("/tasks", json={"handler": "echo", "priority": 1})
    r = c.post("/scheduler/tick")
    assert r.status_code == 200
    data = r.json()
    assert data["max_concurrency"] == 2
    assert any(d["dispatched"] for d in data["decisions"])
    assert data["running"] >= 1


def test_cancel_flow_and_propagation_http(client):
    c, service, _ = client
    parent = c.post("/tasks", json={"handler": "echo"}).json()["task"]["id"]
    child = c.post(
        "/tasks", json={"handler": "echo", "dependencies": [parent]}
    ).json()["task"]["id"]
    r = c.post(f"/tasks/{parent}/cancel", json={"reason": "cleanup"})
    assert r.status_code == 200
    child_state = c.get(f"/tasks/{child}").json()["task"]
    assert child_state["state"] == "BLOCKED"
    assert child_state["terminal"]["reason"] == "upstream_cancelled"

    # 重复取消得到可区分错误码
    again = c.post(f"/tasks/{parent}/cancel", json={})
    assert again.status_code == 409
    assert again.json()["error"]["code"] == "task_cancelled"
