"""HTTP API（FastAPI）。

默认共享一个进程内 OrchestrationService；也可通过 create_app(service=...) 注入
带 FakeClock 的实例用于测试，保证结果可复现。
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.errors import InvalidStateTransitionError, TaskNotFoundError
from app.models import Task, TaskSpec, TaskState
from app.service import OrchestrationService


class SubmitBody(BaseModel):
    idempotency_key: str
    payload: str = ""
    priority: int = 0
    dependencies: list[str] = Field(default_factory=list)
    max_retries: int = 0
    lease_timeout_ms: int = 5000
    group: str = "default"
    retry_delay_ms: int = 0
    retry_backoff_multiplier: float = 1.0


class GroupLimitBody(BaseModel):
    limit: int = Field(ge=1)


def task_to_dto(task: Task) -> dict[str, Any]:
    return {
        "id": task.id,
        "idempotency_key": task.idempotency_key,
        "state": task.state.value,
        "priority": task.priority,
        "group": task.group,
        "dependencies": sorted(task.dependencies),
        "attempts": task.attempts,
        "max_retries": task.max_retries,
        "submit_count": task.submit_count,
        "failure_reason": task.failure_reason,
        "failure_detail": task.failure_detail,
        "next_retry_at_ms": task.next_retry_at_ms,
        "lease_owner": task.lease_owner,
        "lease_expire_at_ms": task.lease_expire_at_ms,
        "created_at_ms": task.created_at_ms,
        "updated_at_ms": task.updated_at_ms,
        "events": list(task.events),
    }


def create_app(service: OrchestrationService | None = None) -> FastAPI:
    svc = service or OrchestrationService(max_concurrency=4)
    app = FastAPI(title="local-task-orchestration", version="0.1.0")

    @app.exception_handler(TaskNotFoundError)
    async def _not_found(_request: Any, exc: TaskNotFoundError) -> JSONResponse:
        return JSONResponse(
            status_code=404,
            content={"error": "task_not_found", "task_id": exc.task_id},
        )

    @app.exception_handler(InvalidStateTransitionError)
    async def _invalid_transition(
        _request: Any, exc: InvalidStateTransitionError
    ) -> JSONResponse:
        # 重复取消/终态取消返回 409；原因码可区分（already_cancelled 等）。
        return JSONResponse(
            status_code=409,
            content={
                "error": "invalid_state_transition",
                "reason": exc.reason,
                "task_id": exc.task_id,
                "from_state": exc.from_state,
            },
        )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/tasks", status_code=201)
    def submit_task(body: SubmitBody) -> dict[str, Any]:
        spec = TaskSpec(
            idempotency_key=body.idempotency_key,
            payload=body.payload,
            priority=body.priority,
            dependencies=frozenset(body.dependencies),
            max_retries=body.max_retries,
            lease_timeout_ms=body.lease_timeout_ms,
            group=body.group,
            retry_delay_ms=body.retry_delay_ms,
            retry_backoff_multiplier=body.retry_backoff_multiplier,
        )
        task = svc.submit(spec)
        return task_to_dto(task)

    @app.put("/groups/{group}/concurrency")
    def set_group_concurrency(group: str, body: GroupLimitBody) -> dict[str, Any]:
        svc.set_group_limit(group, body.limit)
        return {"group": group, "limit": body.limit}

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str) -> dict[str, Any]:
        return task_to_dto(svc.get(task_id))

    @app.post("/tasks/{task_id}/cancel")
    def cancel_task(task_id: str) -> dict[str, Any]:
        return task_to_dto(svc.cancel(task_id))

    @app.post("/schedule")
    def schedule() -> dict[str, Any]:
        return {"dispatched": svc.schedule_once()}

    @app.post("/tasks/{task_id}/heartbeat")
    def heartbeat(task_id: str, owner: str) -> dict[str, Any]:
        ok = svc.heartbeat(task_id, owner)
        return {"renewed": ok}

    @app.post("/tasks/{task_id}/succeed")
    def succeed(task_id: str, owner: str) -> Response:
        svc.succeed(task_id, owner)
        return Response(status_code=204)

    @app.post("/tasks/{task_id}/fail")
    def fail(task_id: str, owner: str, detail: str = "") -> Response:
        svc.fail(task_id, owner, detail)
        return Response(status_code=204)

    return app
