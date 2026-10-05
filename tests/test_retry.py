"""重试闭环测试：退避等待、到点自动重新调度、重试额度逐次耗尽、查询字段。"""

import logging

from app.clock import FakeClock
from app.models import TaskSpec, TaskState
from app.service import OrchestrationService

logger = logging.getLogger("test.retry")


def test_retry_waits_for_delay_then_reruns() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t = svc.submit(
        TaskSpec(idempotency_key="T", max_retries=2, retry_delay_ms=1000)
    )
    svc.schedule_once()
    owner1 = svc.get(t.id).lease_owner
    svc.fail(t.id, owner1, "boom#1")

    task = svc.get(t.id)
    logger.info(
        "输入: max_retries=2 retry_delay=1000；第1次失败后 state=%s next_retry_at=%s",
        task.state.value, task.next_retry_at_ms,
    )
    assert task.state is TaskState.FAILED_RETRYING
    assert task.next_retry_at_ms == 1000

    # 判定依据: 没到点不参与调度
    clock.advance(999)
    assert svc.schedule_once() == []
    assert svc.get(t.id).state is TaskState.FAILED_RETRYING
    logger.info("now=999 < next_retry_at=1000 -> 未放行，未被调度")

    # 判定依据: 到点后自动重新参与调度并真的被执行
    clock.advance(1)
    assert svc.schedule_once() == [t.id]
    task = svc.get(t.id)
    assert task.state is TaskState.RUNNING
    assert task.attempts == 2
    assert task.next_retry_at_ms is None
    logger.info("now=1000 到点 -> 重新派发 attempts=2 owner 轮换")


def test_retry_backoff_increases_with_attempts() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t = svc.submit(
        TaskSpec(
            idempotency_key="T",
            max_retries=2,
            retry_delay_ms=100,
            retry_backoff_multiplier=3.0,
        )
    )
    svc.schedule_once()
    svc.fail(t.id, svc.get(t.id).lease_owner, "boom#1")
    first_due = svc.get(t.id).next_retry_at_ms
    logger.info("第1次重试等待: next_retry_at=%s (期望 100 = 100*3^0)", first_due)
    assert first_due == 100

    clock.advance(100)
    assert svc.schedule_once() == [t.id]
    svc.fail(t.id, svc.get(t.id).lease_owner, "boom#2")
    second_due = svc.get(t.id).next_retry_at_ms
    logger.info("第2次重试等待: next_retry_at=%s (期望 400 = 100 + 100*3^1)", second_due)
    # 判定依据: 退避按重试次数递增
    assert second_due == 400

    clock.advance(299)
    assert svc.schedule_once() == []
    clock.advance(1)
    assert svc.schedule_once() == [t.id]
    assert svc.get(t.id).attempts == 3
    logger.info("退避到点后第3次执行真的发生 attempts=3")


def test_all_configured_retries_actually_happen_until_exhausted() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t = svc.submit(
        TaskSpec(idempotency_key="T", max_retries=2, retry_delay_ms=500)
    )
    logger.info("输入: max_retries=2 -> 期望共 3 次真实执行后终态 FAILED")

    for round_no in range(3):
        dispatched = svc.schedule_once()
        assert dispatched == [t.id], f"第{round_no + 1}次执行未发生"
        task = svc.get(t.id)
        assert task.state is TaskState.RUNNING
        logger.info(
            "第%d次执行: attempts=%d owner=%s",
            round_no + 1, task.attempts, task.lease_owner,
        )
        svc.fail(t.id, task.lease_owner, f"boom#{round_no + 1}")
        clock.advance(500)

    task = svc.get(t.id)
    # 判定依据: 每次配置的尝试都真实发生，额度用尽后终态失败且原因可查
    assert task.attempts == 3
    assert task.state is TaskState.FAILED
    assert task.failure_reason == "retries_exhausted"
    assert task.failure_detail == "boom#3"
    assert svc.schedule_once() == []
    logger.info(
        "终态: state=%s attempts=%d reason=%s detail=%s",
        task.state.value, task.attempts, task.failure_reason, task.failure_detail,
    )


def test_retry_eventually_succeeds() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t = svc.submit(
        TaskSpec(idempotency_key="T", max_retries=3, retry_delay_ms=200)
    )
    svc.schedule_once()
    svc.fail(t.id, svc.get(t.id).lease_owner, "boom#1")

    clock.advance(200)
    assert svc.schedule_once() == [t.id]
    svc.succeed(t.id, svc.get(t.id).lease_owner)

    task = svc.get(t.id)
    # 判定依据: 重试后成功，attempts 记录真实尝试次数
    assert task.state is TaskState.SUCCEEDED
    assert task.attempts == 2
    logger.info("重试闭环: 第2次执行成功 state=SUCCEEDED attempts=2")


def test_lease_timeout_retry_also_respects_delay() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t = svc.submit(
        TaskSpec(
            idempotency_key="T",
            max_retries=1,
            lease_timeout_ms=100,
            retry_delay_ms=1000,
        )
    )
    svc.schedule_once()
    clock.advance(101)  # 租约超时
    svc.schedule_once()  # 回收 -> 进入重试等待（而不是立即重跑）

    task = svc.get(t.id)
    logger.info(
        "租约超时回收后: state=%s next_retry_at=%s",
        task.state.value, task.next_retry_at_ms,
    )
    # 判定依据: 失联重试同样遵守重试等待时间
    assert task.state is TaskState.FAILED_RETRYING
    assert task.next_retry_at_ms == 1101
    assert svc.schedule_once() == []

    clock.advance(1000)
    assert svc.schedule_once() == [t.id]
    assert svc.get(t.id).attempts == 2
    logger.info("等待到点后失联任务被重新执行 attempts=2")
