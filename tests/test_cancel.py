"""取消能力测试：取消运行中任务、名额释放、下游级联、迟到上报拒绝。"""

import logging

import pytest

from app.clock import FakeClock
from app.errors import InvalidStateTransitionError
from app.models import TaskSpec, TaskState
from app.service import OrchestrationService

logger = logging.getLogger("test.cancel")


def test_cancel_running_task_frees_slot_immediately() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t1 = svc.submit(TaskSpec(idempotency_key="t1", priority=5))
    t2 = svc.submit(TaskSpec(idempotency_key="t2", priority=1))
    logger.info("输入: 全局上限=1, t1(优先级5) 与 t2(优先级1) 排队")

    assert svc.schedule_once() == [t1.id]
    assert svc.get(t1.id).state is TaskState.RUNNING
    assert svc.get(t2.id).state is TaskState.READY

    svc.cancel(t1.id)
    task1 = svc.get(t1.id)
    logger.info(
        "取消运行中的 t1: state=%s lease_owner=%s lease_expire_at=%s",
        task1.state.value, task1.lease_owner, task1.lease_expire_at_ms,
    )
    # 判定依据: 立即终态 CANCELLED，租约被作废
    assert task1.state is TaskState.CANCELLED
    assert task1.lease_owner is None
    assert task1.lease_expire_at_ms is None

    # 判定依据: 名额立刻让出，下一轮调度 t2 获得执行权
    assert svc.schedule_once() == [t2.id]
    assert svc.get(t2.id).state is TaskState.RUNNING
    logger.info("下一轮调度派发=%s -> 取消释放的名额被 t2 接管", [t2.id])


def test_cancel_running_task_fails_downstream() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=2, clock=clock)
    a = svc.submit(TaskSpec(idempotency_key="A"))
    b = svc.submit(TaskSpec(idempotency_key="B", dependencies=frozenset({"A"})))
    c = svc.submit(TaskSpec(idempotency_key="C", dependencies=frozenset({"B"})))
    logger.info("输入: 链 A -> B -> C，A 运行中被取消")

    svc.schedule_once()
    assert svc.get(a.id).state is TaskState.RUNNING
    svc.cancel(a.id)

    svc.schedule_once()
    # 判定依据: 下游一律判不可执行 FAILED，不再继续排队
    assert svc.get(b.id).state is TaskState.FAILED
    assert svc.get(b.id).failure_reason == "upstream_failed"
    assert svc.get(c.id).state is TaskState.FAILED
    logger.info(
        "下游级联: B=%s(reason=%s) C=%s(reason=%s)",
        svc.get(b.id).state.value, svc.get(b.id).failure_reason,
        svc.get(c.id).state.value, svc.get(c.id).failure_reason,
    )
    # 再跑一轮也不会复活
    assert svc.schedule_once() == []
    assert svc.get(b.id).state is TaskState.FAILED


def test_late_reports_from_cancelled_owner_are_rejected() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t = svc.submit(TaskSpec(idempotency_key="T"))
    svc.schedule_once()
    owner = svc.get(t.id).lease_owner
    logger.info("输入: 任务运行中 owner=%s，随后取消，再模拟原执行者迟到上报", owner)

    svc.cancel(t.id)

    # 判定依据: 迟到的心跳 / 成功 / 失败都不能改动结果
    assert svc.heartbeat(t.id, owner) is False
    with pytest.raises(InvalidStateTransitionError) as ei:
        svc.succeed(t.id, owner)
    assert ei.value.reason == "already_settled"
    with pytest.raises(InvalidStateTransitionError) as ei:
        svc.fail(t.id, owner, "late failure")
    assert ei.value.reason == "already_settled"

    task = svc.get(t.id)
    logger.info(
        "迟到上报后任务保持 state=%s failure_reason=%s",
        task.state.value, task.failure_reason,
    )
    assert task.state is TaskState.CANCELLED
    assert task.failure_reason is None

    # 判定依据: 已取消任务不会再被调度拉回
    clock.advance(60_000)
    assert svc.schedule_once() == []
    assert svc.get(t.id).state is TaskState.CANCELLED


def test_cancel_retry_waiting_task_stops_future_retries() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t = svc.submit(
        TaskSpec(idempotency_key="T", max_retries=3, retry_delay_ms=1000)
    )
    svc.schedule_once()
    owner = svc.get(t.id).lease_owner
    svc.fail(t.id, owner, "boom")
    assert svc.get(t.id).state is TaskState.FAILED_RETRYING
    logger.info(
        "输入: 任务失败进入重试等待 next_retry_at=%s，此时取消",
        svc.get(t.id).next_retry_at_ms,
    )

    svc.cancel(t.id)
    assert svc.get(t.id).state is TaskState.CANCELLED
    assert svc.get(t.id).next_retry_at_ms is None

    # 判定依据: 过了重试到点时间也不会再被放行调度
    clock.advance(5_000)
    assert svc.schedule_once() == []
    assert svc.get(t.id).state is TaskState.CANCELLED
    logger.info("重试等待中的任务取消后，到点也不会复活")
