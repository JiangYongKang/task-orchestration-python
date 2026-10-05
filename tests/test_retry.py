"""重试退避：等待时间可配、按次数递增、到点必须真的重新执行、额度闭环。"""

import logging

from app.clock import FakeClock
from app.models import TaskSpec, TaskState
from app.service import OrchestrationService

logger = logging.getLogger("test.retry")


def _run_and_fail(svc: OrchestrationService, task_id: str, detail: str) -> None:
    owner = svc.get(task_id).lease_owner
    assert owner is not None
    svc.fail(task_id, owner, detail)


def test_retry_waits_for_delay_then_actually_reruns() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t = svc.submit(
        TaskSpec(idempotency_key="T", max_retries=1, retry_delay_ms=500)
    )
    svc.schedule_once()
    _run_and_fail(svc, t.id, "boom")

    task = svc.get(t.id)
    assert task.state is TaskState.FAILED_RETRYING
    # 判定依据: 查询可见已试次数与下一次重试时间
    assert task.attempts == 1
    assert task.next_retry_at_ms == 500
    logger.info("输入: retry_delay_ms=500; 失败后 next_retry_at=%s attempts=%d",
                task.next_retry_at_ms, task.attempts)

    # 没到时间：不参与调度
    clock.advance(499)
    assert svc.schedule_once() == []
    assert svc.get(t.id).state is TaskState.FAILED_RETRYING
    logger.info("now=499 < 500 -> 未派发, state=%s", svc.get(t.id).state.value)

    # 时间一到：自动重新参与调度并真的被执行
    clock.advance(1)
    assert svc.schedule_once() == [t.id]
    task = svc.get(t.id)
    assert task.state is TaskState.RUNNING
    assert task.attempts == 2
    assert task.next_retry_at_ms is None  # 等待已消费
    logger.info("now=500 到点 -> 重新派发 attempts=%d state=%s", task.attempts, task.state.value)


def test_retry_backoff_increases_with_attempts() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t = svc.submit(
        TaskSpec(
            idempotency_key="T",
            max_retries=2,
            retry_delay_ms=100,
            retry_backoff_multiplier=2.0,
        )
    )
    svc.schedule_once()
    _run_and_fail(svc, t.id, "fail-1")
    # 第 1 次失败后等待 100 * 2^0 = 100ms
    assert svc.get(t.id).next_retry_at_ms == 100
    logger.info("第1次失败 next_retry_at=100 (100*2^0)")

    clock.advance(100)
    assert svc.schedule_once() == [t.id]
    _run_and_fail(svc, t.id, "fail-2")
    # 第 2 次失败后等待 100 * 2^1 = 200ms -> 到点时刻 300
    assert svc.get(t.id).next_retry_at_ms == 300
    logger.info("第2次失败 next_retry_at=300 (100+100*2^1) 判定依据: 退避随次数递增")

    clock.advance(199)  # now=299，未到点
    assert svc.schedule_once() == []
    clock.advance(1)    # now=300，到点
    assert svc.schedule_once() == [t.id]
    assert svc.get(t.id).attempts == 3


def test_all_retries_actually_happen_until_success() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t = svc.submit(
        TaskSpec(idempotency_key="T", max_retries=2, retry_delay_ms=10)
    )
    svc.schedule_once()
    for i in (1, 2):
        _run_and_fail(svc, t.id, f"fail-{i}")
        clock.advance(10)
        dispatched = svc.schedule_once()
        logger.info("第%d次失败后到点重派=%s attempts=%d",
                    i, dispatched, svc.get(t.id).attempts)
        assert dispatched == [t.id]  # 每次重试都实实在在发生

    owner = svc.get(t.id).lease_owner
    svc.succeed(t.id, owner)
    task = svc.get(t.id)
    assert task.state is TaskState.SUCCEEDED
    assert task.attempts == 3  # 1 次首试 + 2 次重试
    logger.info("闭环: attempts=%d state=%s", task.attempts, task.state.value)


def test_retries_exhausted_marks_failed_with_reason() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t = svc.submit(
        TaskSpec(idempotency_key="T", max_retries=1, retry_delay_ms=10)
    )
    svc.schedule_once()
    _run_and_fail(svc, t.id, "first")
    clock.advance(10)
    svc.schedule_once()
    _run_and_fail(svc, t.id, "second")

    task = svc.get(t.id)
    # 判定依据: 额度用尽 -> 终态 FAILED，可查询最终失败原因与尝试次数
    assert task.state is TaskState.FAILED
    assert task.attempts == 2
    assert task.failure_reason == "retries_exhausted"
    assert task.failure_detail == "second"
    # 终态后不再被调度
    clock.advance(10_000)
    assert svc.schedule_once() == []
    logger.info("额度用尽: attempts=%d reason=%s detail=%s",
                task.attempts, task.failure_reason, task.failure_detail)


def test_lease_lost_retry_also_honors_backoff() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t = svc.submit(
        TaskSpec(
            idempotency_key="T",
            max_retries=1,
            lease_timeout_ms=100,
            retry_delay_ms=1_000,
        )
    )
    svc.schedule_once()
    clock.advance(101)  # 租约超时
    svc.schedule_once()  # 回收 -> LOST，等待退避
    task = svc.get(t.id)
    assert task.state is TaskState.LOST
    assert task.next_retry_at_ms == 101 + 1_000
    logger.info("失联回收后等待重试 next_retry_at=%s", task.next_retry_at_ms)

    clock.advance(999)  # now=1100 未到点
    assert svc.schedule_once() == []
    clock.advance(1)    # now=1101 到点
    assert svc.schedule_once() == [t.id]
    assert svc.get(t.id).attempts == 2
