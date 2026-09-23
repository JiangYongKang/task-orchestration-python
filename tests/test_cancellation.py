"""取消与非法状态跃迁测试（错误原因必须可区分）。"""
from __future__ import annotations

import pytest

from orchestration.errors import ErrorCode, OrchestrationError
from orchestration.models import TaskSpec, TaskState
from tests.conftest import log_case, wait_terminal


def _settle(service, task_id, token, ok=True, result=None):
    if ok:
        service.report_success(task_id, token, result)
    else:
        service.report_failure(task_id, token, "boom")


def test_cancel_pending_succeeds_and_is_idempotent_reject(make_service):
    service = make_service()
    service.executor.register("echo", lambda p, ctx: p)
    t = service.submit(TaskSpec(handler="echo")).task

    log_case("cancel_pending", task=t.id, from_state=t.state.value)
    cancelled = service.cancel(t.id, reason="no longer needed")
    assert cancelled.state is TaskState.CANCELLED

    log_case("cancel_twice", task=t.id, expectation="task_cancelled")
    with pytest.raises(OrchestrationError) as ei:
        service.cancel(t.id)
    assert ei.value.code == ErrorCode.TASK_CANCELLED


def test_cancel_completed_rejected_with_distinct_code(make_service):
    import threading

    service = make_service()
    release = threading.Event()
    service.executor.register("blocking", lambda p, ctx: release.wait(2))
    t = service.submit(TaskSpec(handler="blocking")).task
    service.schedule_tick()
    assert service.get(t.id).state is TaskState.RUNNING
    token = service.get(t.id).lease_token
    release.set()
    import time

    for _ in range(200):
        if service.get(t.id).state is TaskState.COMPLETED:
            break
        time.sleep(0.01)
    assert t.state is TaskState.COMPLETED

    log_case("cancel_completed", task=t.id, expectation="task_completed (409)")
    with pytest.raises(OrchestrationError) as ei:
        service.cancel(t.id)
    log_case("cancel_completed_verdict", code=ei.value.code, reason=ei.value.reason)
    assert ei.value.code == ErrorCode.TASK_COMPLETED


def test_cancel_failed_rejected(make_service):
    service = make_service()
    service.executor.register("boom", lambda p, ctx: (_ for _ in ()).throw(RuntimeError("x")))
    t = service.submit(TaskSpec(handler="boom", max_retries=0)).task
    wait_terminal(service, t.id)
    assert service.get(t.id).state is TaskState.FAILED
    with pytest.raises(OrchestrationError) as ei:
        service.cancel(t.id)
    assert ei.value.code == ErrorCode.ALREADY_TERMINAL


def test_cancel_blocked_rejected(make_service):
    service = make_service()
    service.executor.register("echo", lambda p, ctx: "ok")
    service.executor.register("boom", lambda p, ctx: (_ for _ in ()).throw(RuntimeError("x")))
    parent = service.submit(TaskSpec(handler="boom", max_retries=0)).task
    child = service.submit(TaskSpec(handler="echo", dependencies=(parent.id,))).task
    wait_terminal(service, parent.id)
    wait_terminal(service, child.id)
    assert service.get(child.id).state is TaskState.BLOCKED

    log_case(
        "cancel_blocked",
        task=child.id,
        terminal_reason=service.get(child.id).terminal.reason.value,
        expectation="task_blocked",
    )
    with pytest.raises(OrchestrationError) as ei:
        service.cancel(child.id)
    log_case("cancel_blocked_verdict", code=ei.value.code, reason=ei.value.reason)
    assert ei.value.code == ErrorCode.TASK_BLOCKED


def test_cancel_running_rejected(make_service):
    service = make_service()
    import threading
    ev = threading.Event()
    service.executor.register("slow", lambda p, ctx: ev.wait(2))
    t = service.submit(TaskSpec(handler="slow")).task
    service.schedule_tick()
    assert service.get(t.id).state is TaskState.RUNNING
    with pytest.raises(OrchestrationError) as ei:
        service.cancel(t.id)
    assert ei.value.code == ErrorCode.TASK_RUNNING
    ev.set()
    wait_terminal(service, t.id)
