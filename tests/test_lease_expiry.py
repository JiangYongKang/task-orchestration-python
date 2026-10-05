"""租约过期边界测试：凭证已过期但调度器尚未回收时，原执行者的补交上报不算数。"""

import logging

import pytest

from app.clock import FakeClock
from app.errors import InvalidStateTransitionError
from app.models import TaskSpec, TaskState
from app.service import OrchestrationService

logger = logging.getLogger("test.lease_expiry")


def test_expired_lease_late_reports_rejected_before_reclaim() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t = svc.submit(
        TaskSpec(idempotency_key="T", max_retries=1, lease_timeout_ms=100)
    )
    svc.schedule_once()
    zombie = svc.get(t.id).lease_owner
    logger.info(
        "输入: 任务运行中 owner=%s lease_expire_at=%s；时钟越过租约但不触发调度回收",
        zombie, svc.get(t.id).lease_expire_at_ms,
    )

    # 时钟越过租约到期点，但不调用 schedule_once -> 调度器还没来得及回收
    clock.advance(150)
    assert svc.get(t.id).state is TaskState.RUNNING  # 尚未回收，仍是 RUNNING

    # 判定依据: 过期凭证的心跳 / 成功 / 失败一律不算数
    assert svc.heartbeat(t.id, zombie) is False
    with pytest.raises(InvalidStateTransitionError) as ei:
        svc.succeed(t.id, zombie)
    assert ei.value.reason == "lease_expired"
    with pytest.raises(InvalidStateTransitionError) as ei:
        svc.fail(t.id, zombie, "late")
    assert ei.value.reason == "lease_expired"
    logger.info("过期凭证的迟到上报全部被拒绝 reason=lease_expired，任务未被结算")
    assert svc.get(t.id).state is TaskState.RUNNING

    # 调度器回收后由新执行者重新跑，新执行者的上报才算数
    redispatched = svc.schedule_once()
    assert redispatched == [t.id]
    new_owner = svc.get(t.id).lease_owner
    assert new_owner != zombie
    assert svc.get(t.id).attempts == 2
    logger.info("回收后重新派发 attempts=2 new_owner=%s", new_owner)

    svc.succeed(t.id, new_owner)
    assert svc.get(t.id).state is TaskState.SUCCEEDED
    logger.info("判定依据: 只有重新接管的一方结算成功 -> SUCCEEDED")


def test_expired_lease_fail_report_does_not_consume_retry_budget() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t = svc.submit(
        TaskSpec(idempotency_key="T", max_retries=1, lease_timeout_ms=50)
    )
    svc.schedule_once()
    zombie = svc.get(t.id).lease_owner

    clock.advance(60)  # 过期但未回收
    with pytest.raises(InvalidStateTransitionError):
        svc.fail(t.id, zombie, "zombie failure")
    logger.info(
        "过期凭证的失败上报被拒绝: attempts=%d (未发生重复结算)",
        svc.get(t.id).attempts,
    )
    # 判定依据: 过期上报没有触发任何结算，attempts 不变、无失败原因
    assert svc.get(t.id).attempts == 1
    assert svc.get(t.id).failure_reason is None

    # 回收重跑后，新执行者正常失败才消耗重试额度
    svc.schedule_once()
    new_owner = svc.get(t.id).lease_owner
    svc.fail(t.id, new_owner, "real failure")
    task = svc.get(t.id)
    # max_retries=1 且 attempts=2 -> 额度耗尽，终态 FAILED
    assert task.state is TaskState.FAILED
    assert task.failure_reason == "retries_exhausted"
    assert task.failure_detail == "real failure"
    logger.info(
        "新执行者失败结算生效: state=%s reason=%s detail=%s",
        task.state.value, task.failure_reason, task.failure_detail,
    )
