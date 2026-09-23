"""pytest 公共夹具。

* 使用 :class:`FakeClock` 让时间（租约超时）完全可控、结果可复现；
* 提供 ``make_service`` 构造本地编排服务与可注册处理器的执行器；
* 提供 ``log_case`` 在单测日志中打印「输入」与「判定依据」。
"""
from __future__ import annotations

import logging
import time

import pytest

from orchestration.clock import FakeClock
from orchestration.executor import LocalExecutor
from orchestration.models import TaskState
from orchestration.service import OrchestrationService

logger = logging.getLogger("tests")


def log_case(title: str, **fields) -> None:
    """统一的单测输入/判定日志格式。"""
    rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
    logger.info("CASE %s | %s", title, rendered)


def wait_for_state(
    service: OrchestrationService,
    task_id: str,
    states: set[TaskState],
    timeout: float = 3.0,
    interval: float = 0.01,
):
    """反复 tick + 等待，直到任务进入给定状态集合（确定性地推进异步执行）。"""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        service.schedule_tick()
        task = service.get(task_id)
        last = task.state
        if task.state in states:
            return task
        time.sleep(interval)
    raise AssertionError(
        f"task {task_id} did not reach {states} within {timeout}s; last={last}"
    )


def wait_terminal(service: OrchestrationService, task_id: str, timeout: float = 3.0):
    return wait_for_state(
        service,
        task_id,
        {
            TaskState.COMPLETED,
            TaskState.FAILED,
            TaskState.CANCELLED,
            TaskState.BLOCKED,
        },
        timeout=timeout,
    )


@pytest.fixture
def fake_clock() -> FakeClock:
    return FakeClock(start=1000.0)


@pytest.fixture
def make_service(fake_clock):
    created: list[OrchestrationService] = []

    def _make(max_concurrency: int = 4, lease_timeout: float = 10.0,
              max_workers: int = 16) -> OrchestrationService:
        executor = LocalExecutor(max_workers=max_workers)
        service = OrchestrationService(
            clock=fake_clock,
            executor=executor,
            max_concurrency=max_concurrency,
            lease_timeout=lease_timeout,
        )
        created.append(service)
        return service

    yield _make

    for svc in created:
        svc.executor.shutdown()


@pytest.fixture(autouse=True)
def _log_separator(request):
    logger.info("==== BEGIN %s ====", request.node.nodeid)
    yield
    logger.info("==== END   %s ====", request.node.nodeid)
