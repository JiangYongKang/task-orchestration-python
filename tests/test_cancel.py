"""取消运行中任务：名额释放、下游判死、迟到上报一律无效。"""

import logging

import pytest

from app.clock import FakeClock
from app.errors import InvalidStateTransitionError
from app.models import TaskSpec, TaskState
from app.service import OrchestrationService

logger = logging.getLogger("test.cancel")


def test_cancel_running_releases_slot_immediately() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    running = svc.submit(TaskSpec(idempotency_key="running", priority=5))
    queued = svc.submit(TaskSpec(idempotency_key="queued", priority=1))
    logger.info("输入: 全局上限=1, running(p=5) 先跑, queued(p=1) 排队")

    svc.schedule_once()
    assert svc.get(running.id).state is TaskState.RUNNING
    assert svc.get(queued.id).state is TaskState.READY

    svc.cancel(running.id)
    assert svc.get(running.id).state is TaskState.CANCELLED
    # 判定依据: 取消后租约被作废，并发名额立即让出
    assert svc.get(running.id).lease_owner is None
    assert svc.get(running.id).lease_expire_at_ms is None

    dispatched = svc.schedule_once()
    logger.info("取消后第2轮派发=%s", dispatched)
    assert dispatched == [queued.id]
    assert svc.get(queued.id).state is TaskState.RUNNING


def test_cancel_running_marks_downstream_unexecutable() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=2, clock=clock)
    a = svc.submit(TaskSpec(idempotency_key="A"))
    b = svc.submit(TaskSpec(idempotency_key="B", dependencies=frozenset({"A"})))
    c = svc.submit(TaskSpec(idempotency_key="C", dependencies=frozenset({"B"})))
    logger.info("输入: 链 A -> B -> C，取消运行中的 A")

    svc.schedule_once()
    assert svc.get(a.id).state is TaskState.RUNNING
    svc.cancel(a.id)

    svc.schedule_once()
    # 判定依据: 下游一律判 FAILED(upstream_failed)，不再继续排队
    assert svc.get(b.id).state is TaskState.FAILED
    assert svc.get(b.id).failure_reason == "upstream_failed"
    assert svc.get(c.id).state is TaskState.FAILED
    logger.info(
        "下游判定: B=%s C=%s reason=%s",
        svc.get(b.id).state.value, svc.get(c.id).state.value,
        svc.get(c.id).failure_reason,
    )


def test_late_reports_after_cancel_are_rejected() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t = svc.submit(TaskSpec(idempotency_key="T", lease_timeout_ms=1_000))
    svc.schedule_once()
    owner = svc.get(t.id).lease_owner
    assert owner is not None

    svc.cancel(t.id)
    logger.info("输入: 取消运行中任务后，原执行者 owner=%s 迟到上报", owner)

    # 判定依据: 取消后无论隔多久，心跳/成功/失败都不能改动结果
    clock.advance(10_000)
    assert svc.heartbeat(t.id, owner) is False
    with pytest.raises(InvalidStateTransitionError) as ei:
        svc.succeed(t.id, owner)
    assert ei.value.reason == "already_settled"
    with pytest.raises(InvalidStateTransitionError) as ei2:
        svc.fail(t.id, owner, "late failure")
    assert ei2.value.reason == "already_settled"

    task = svc.get(t.id)
    assert task.state is TaskState.CANCELLED
    assert task.failure_reason is None
    logger.info("迟到上报全部被拒, 状态保持 %s, 事件数=%d", task.state.value, len(task.events))


def test_cancel_queued_states_still_work() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    blocker = svc.submit(TaskSpec(idempotency_key="blocker", priority=9))
    ready = svc.submit(TaskSpec(idempotency_key="ready", priority=1))
    svc.schedule_once()  # blocker 占名额，ready 留在 READY
    assert svc.get(ready.id).state is TaskState.READY

    cancelled = svc.cancel(ready.id)
    assert cancelled.state is TaskState.CANCELLED
    logger.info("READY 任务取消成功 state=%s", cancelled.state.value)
