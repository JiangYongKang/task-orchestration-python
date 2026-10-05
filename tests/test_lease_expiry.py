"""执行凭证过期边界：调度尚未回收时，过期执行者的补交上报同样不算数。"""

import logging

import pytest

from app.clock import FakeClock
from app.errors import InvalidStateTransitionError
from app.models import TaskSpec, TaskState
from app.service import OrchestrationService

logger = logging.getLogger("test.lease_expiry")


def test_expired_lease_late_report_rejected_before_reclaim() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t = svc.submit(TaskSpec(idempotency_key="T", max_retries=1, lease_timeout_ms=100))
    svc.schedule_once()
    owner = svc.get(t.id).lease_owner
    assert owner is not None
    logger.info("输入: 任务租约 100ms, owner=%s; 时钟推进到 150ms 但不触发调度回收", owner)

    # 凭证已过期（now=150 > expire=100），但调度器尚未 run_once 回收：
    # 任务仍是 RUNNING 且 owner 未变 —— 此刻补交的上报必须不算数。
    clock.advance(150)
    assert svc.get(t.id).state is TaskState.RUNNING

    assert svc.heartbeat(t.id, owner) is False
    with pytest.raises(InvalidStateTransitionError) as ei:
        svc.succeed(t.id, owner)
    assert ei.value.reason == "lease_expired"
    with pytest.raises(InvalidStateTransitionError) as ei2:
        svc.fail(t.id, owner, "late")
    assert ei2.value.reason == "lease_expired"
    logger.info("过期凭证的心跳/成功/失败均被拒 reason=lease_expired, 状态=%s",
                svc.get(t.id).state.value)
    assert svc.get(t.id).state is TaskState.RUNNING  # 未被迟到上报改动

    # 调度回收后由新接管方重新执行，新持有者的结算才算数。
    redispatched = svc.schedule_once()
    assert redispatched == [t.id]
    new_owner = svc.get(t.id).lease_owner
    assert new_owner != owner
    assert svc.get(t.id).attempts == 2
    logger.info("重新接管: attempts=2 new_owner=%s", new_owner)

    svc.succeed(t.id, new_owner)
    assert svc.get(t.id).state is TaskState.SUCCEEDED
    # 旧 owner 再补交依然无效
    with pytest.raises(InvalidStateTransitionError):
        svc.succeed(t.id, owner)


def test_expired_lease_heartbeat_does_not_extend() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t = svc.submit(TaskSpec(idempotency_key="T", max_retries=0, lease_timeout_ms=50))
    svc.schedule_once()
    owner = svc.get(t.id).lease_owner

    clock.advance(60)  # 过期 10ms
    ok = svc.heartbeat(t.id, owner)
    expire_at = svc.get(t.id).lease_expire_at_ms
    logger.info("输入: 过期后心跳 ok=%s expire_at=%s (不得顺延)", ok, expire_at)
    # 判定依据: 过期心跳返回 False 且到期时间没有被顺延
    assert ok is False
    assert expire_at == 50

    svc.schedule_once()  # 回收：无重试额度 -> FAILED
    assert svc.get(t.id).state is TaskState.FAILED
    assert svc.get(t.id).failure_reason == "lease_timeout"
