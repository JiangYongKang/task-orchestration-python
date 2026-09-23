"""执行器抽象：拿到执行权的任务在这里运行，并受租约保护。

处理函数通过 :class:`ExecutionContext` 与编排服务交互：

* ``heartbeat()`` 续租；
* ``is_cancelled()`` 协作式感知取消（当前状态机不取消运行中的任务，保留接口）。

执行结果通过回调异步回报给 :class:`~orchestration.service.OrchestrationService`，
回报时必须携带派发时发放的 ``lease_token``，过期/被回收的尝试结算将被拒绝
（fencing token），因此重试不会造成重复结算。
"""
from __future__ import annotations

import logging
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Callable

from .models import Task

logger = logging.getLogger("orchestration.executor")

# 处理函数签名：(payload, context) -> 结果（任意可序列化对象）
TaskHandler = Callable[[dict, "ExecutionContext"], object]


@dataclass
class ExecutionContext:
    task_id: str
    attempt_no: int
    lease_token: str
    is_cancelled: Callable[[], bool]
    heartbeat: Callable[[], None]


class LocalExecutor:
    """使用线程池在本地执行任务。

    回调由编排服务注入：

    ``on_success(task_id, lease_token, result)``
    ``on_failure(task_id, lease_token, error)``
    """

    def __init__(
        self,
        handlers: dict[str, TaskHandler] | None = None,
        max_workers: int = 32,
    ) -> None:
        self._handlers: dict[str, TaskHandler] = dict(handlers or {})
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="task")
        self.on_success: Callable[[str, str, object], None] | None = None
        self.on_failure: Callable[[str, str, str], None] | None = None

    def register(self, name: str, handler: TaskHandler) -> None:
        self._handlers[name] = handler

    def has_handler(self, name: str) -> bool:
        return name in self._handlers

    def submit(self, task: Task, lease_token: str, attempt_no: int) -> None:
        """把已获得执行权的任务投入线程池异步执行。"""
        handler = self._handlers.get(task.handler)
        if handler is None:
            # 理论上调度前已校验；防御性处理
            if self.on_failure is not None:
                self.on_failure(
                    task.id, lease_token, f"handler {task.handler!r} not registered"
                )
            return
        ctx = ExecutionContext(
            task_id=task.id,
            attempt_no=attempt_no,
            lease_token=lease_token,
            is_cancelled=lambda: False,
            heartbeat=lambda: self._safe_heartbeat(task.id, lease_token),
        )
        logger.info(
            "dispatch task=%s handler=%s attempt=%d lease=%s payload=%r",
            task.id, task.handler, attempt_no, lease_token, task.payload,
        )
        self._pool.submit(self._run, task.id, handler, ctx, task.payload)

    # ---- internals ----

    def _safe_heartbeat(self, task_id: str, lease_token: str) -> None:
        callback = getattr(self, "_heartbeat_callback", None)
        if callback is not None:
            callback(task_id, lease_token)

    def set_heartbeat_callback(self, callback: Callable[[str, str], None]) -> None:
        self._heartbeat_callback = callback

    def _run(
        self,
        task_id: str,
        handler: TaskHandler,
        ctx: ExecutionContext,
        payload: dict,
    ) -> None:
        try:
            result = handler(payload, ctx)
        except Exception as exc:  # 业务异常 => 失败回报
            logger.warning(
                "task=%s attempt=%d raised: %s\n%s",
                task_id, ctx.attempt_no, exc, traceback.format_exc(),
            )
            if self.on_failure is not None:
                self.on_failure(task_id, ctx.lease_token, f"{type(exc).__name__}: {exc}")
        else:
            logger.info("task=%s attempt=%d succeeded result=%r", task_id, ctx.attempt_no, result)
            if self.on_success is not None:
                self.on_success(task_id, ctx.lease_token, result)

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=False)
