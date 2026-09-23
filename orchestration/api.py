"""FastAPI HTTP 适配层。

端点：

* ``POST /tasks``                 提交任务（幂等键收敛）
* ``GET  /tasks/{task_id}``       查询任务
* ``POST /tasks/{task_id}/cancel`` 取消任务
* ``POST /tasks/{task_id}/heartbeat`` 执行者心跳续租
* ``POST /tasks/{task_id}/complete``  执行者上报成功
* ``POST /tasks/{task_id}/fail``      执行者上报失败
* ``POST /scheduler/tick``        手动触发一轮调度（返回判定依据）
* ``POST /leases/reclaim``        手动回收超时租约
* ``GET  /tasks``                 列出全部任务
"""
from __future__ import annotations

import threading
from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel, Field

from .errors import ErrorCode, OrchestrationError
from .models import TaskSpec
from .service import OrchestrationService

_NOT_FOUND_CODES = {ErrorCode.TASK_NOT_FOUND}
_CONFLICT_CODES = {
    ErrorCode.IDEMPOTENCY_CONFLICT,
    ErrorCode.ALREADY_TERMINAL,
    ErrorCode.TASK_COMPLETED,
    ErrorCode.TASK_CANCELLED,
    ErrorCode.TASK_BLOCKED,
    ErrorCode.TASK_RUNNING,
    ErrorCode.ILLEGAL_STATE_TRANSITION,
    ErrorCode.LEASE_EXPIRED,
    ErrorCode.LEASE_HEARTBEAT_NOT_RUNNING,
    ErrorCode.LEASE_TOKEN_MISMATCH,
}
_BAD_REQUEST_CODES = {
    ErrorCode.UNKNOWN_DEPENDENCY,
    ErrorCode.DEPENDENCY_CYCLE,
    ErrorCode.INVALID_PAYLOAD,
    ErrorCode.RETRIES_EXHAUSTED,
    ErrorCode.HANDLER_NOT_FOUND,
}


class SubmitRequest(BaseModel):
    handler: str
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None
    dependencies: list[str] = Field(default_factory=list)
    priority: int = 0
    max_retries: int = 3


class CancelRequest(BaseModel):
    reason: str = "user requested"


class HeartbeatRequest(BaseModel):
    lease_token: str


class CompleteRequest(BaseModel):
    lease_token: str
    result: Any = None


class FailRequest(BaseModel):
    lease_token: str
    error: str


def _error_response(exc: OrchestrationError):
    from fastapi.responses import JSONResponse

    if exc.code in _NOT_FOUND_CODES:
        status = 404
    elif exc.code in _BAD_REQUEST_CODES:
        status = 400
    else:
        status = 409
    return JSONResponse(status_code=status, content={"error": exc.to_dict()})


def create_app(service: OrchestrationService) -> FastAPI:
    app = FastAPI(title="Local Task Orchestration Service", version="0.1.0")

    @app.exception_handler(OrchestrationError)
    async def _handle_orch_error(_request, exc: OrchestrationError):
        return _error_response(exc)

    @app.post("/tasks", status_code=201)
    def submit_task(req: SubmitRequest):
        spec = TaskSpec(
            handler=req.handler,
            payload=req.payload,
            idempotency_key=req.idempotency_key,
            dependencies=tuple(req.dependencies),
            priority=req.priority,
            max_retries=req.max_retries,
        )
        result = service.submit(spec)
        return {"task": result.task.to_dict(), "created": result.created}

    @app.get("/tasks")
    def list_tasks():
        return {"tasks": [t.to_dict() for t in service.store.all_tasks()]}

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str):
        return {"task": service.get(task_id).to_dict()}

    @app.post("/tasks/{task_id}/cancel")
    def cancel_task(task_id: str, req: CancelRequest):
        task = service.cancel(task_id, reason=req.reason)
        return {"task": task.to_dict()}

    @app.post("/tasks/{task_id}/heartbeat")
    def heartbeat_task(task_id: str, req: HeartbeatRequest):
        service.heartbeat(task_id, req.lease_token)
        task = service.get(task_id)
        return {
            "task_id": task_id,
            "lease_expire_at": task.lease_expire_at,
            "last_heartbeat_at": task.last_heartbeat_at,
        }

    @app.post("/tasks/{task_id}/complete")
    def complete_task(task_id: str, req: CompleteRequest):
        service.report_success(task_id, req.lease_token, req.result)
        return {"task": service.get(task_id).to_dict()}

    @app.post("/tasks/{task_id}/fail")
    def fail_task(task_id: str, req: FailRequest):
        service.report_failure(task_id, req.lease_token, req.error)
        return {"task": service.get(task_id).to_dict()}

    @app.post("/scheduler/tick")
    def scheduler_tick():
        decisions = service.schedule_tick()
        return {
            "running": service.running_count(),
            "max_concurrency": service.max_concurrency,
            "decisions": [d.to_dict() for d in decisions],
        }

    @app.post("/leases/reclaim")
    def reclaim_leases():
        reclaimed = service.reclaim_expired_leases()
        return {"reclaimed": reclaimed}

    return app


def build_default_service() -> OrchestrationService:
    """供 ``main.py`` 使用：真实时钟 + 本地线程池执行器。"""
    from .clock import SystemClock
    from .executor import LocalExecutor

    executor = LocalExecutor()
    service = OrchestrationService(
        clock=SystemClock(),
        executor=executor,
        max_concurrency=4,
        lease_timeout=30.0,
    )
    return service


def start_background_daemon(
    service: OrchestrationService, interval: float = 1.0
) -> threading.Event:
    """启动后台守护线程：周期性调度与租约回收。返回停止事件。"""

    stop = threading.Event()

    def _loop() -> None:
        while not stop.wait(interval):
            try:
                service.schedule_tick()
                service.reclaim_expired_leases()
            except Exception:  # pragma: no cover - 守护线程兜底
                import logging

                logging.getLogger("orchestration").exception("background daemon error")

    thread = threading.Thread(target=_loop, name="orch-daemon", daemon=True)
    thread.start()
    return stop
