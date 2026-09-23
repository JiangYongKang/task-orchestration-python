"""执行器测试：心跳租约、失败重试计数、失联后迟到结算拒绝。"""

import logging

import pytest

from app.clock import FakeClock
from app.errors import InvalidStateTransitionError
from app.executor import Executor
from app.models import Task, TaskState, transit
from app.storage import TaskStore

logger = logging.getLogger("test.executor")


def _running_task(store: TaskStore, clock: FakeClock, *, max_retries: int = 0) -> tuple[Task, Executor]:
    task = Task(
        id=Task.new_id(),
        idempotency_key="k",
        payload="",
        priority=0,
        dependencies=frozenset(),
        max_retries=max_retries,
        lease_timeout_ms=100,
        created_at_ms=0,
        updated_at_ms=0,
    )
    transit(task, TaskState.READY, now_ms=0, reason="test")
    store.put(task)
    ex = Executor(store, clock)
    return task, ex


def test_heartbeat_extends_lease_and_wrong_owner_rejected() -> None:
    clock = FakeClock(0)
    store = TaskStore()
    task, ex = _running_task(store, clock)
    transit(task, TaskState.RUNNING, now_ms=0, reason="dispatch")
    owner = ex.start(task.id)
    assert owner is not None
    assert task.lease_expire_at_ms == 100

    clock.advance(50)
    assert ex.heartbeat(task.id, owner) is True
    assert task.lease_expire_at_ms == 150  # 判定依据: 心跳后到期时间被顺延
    logger.info("心跳续约成功 expire_at=%s", task.lease_expire_at_ms)

    assert ex.heartbeat(task.id, "impostor") is False
    logger.info("非持有者心跳被拒绝")


def test_success_settles_once() -> None:
    clock = FakeClock(0)
    store = TaskStore()
    task, ex = _running_task(store, clock)
    transit(task, TaskState.RUNNING, now_ms=0, reason="dispatch")
    owner = ex.start(task.id)
    ex.succeed(task.id, owner)
    assert task.state is TaskState.SUCCEEDED

    # 终态后同一 owner 再次结算被拒绝
    with pytest.raises(InvalidStateTransitionError) as ei:
        ex.succeed(task.id, owner)
    assert ei.value.reason == "already_settled"
    logger.info("重复成功结算被拒绝 reason=%s", ei.value.reason)


def test_retry_then_exhaust_with_traceable_attempts() -> None:
    clock = FakeClock(0)
    store = TaskStore()
    task, ex = _running_task(store, clock, max_retries=1)

    # 第 1 次执行失败 -> FAILED_RETRYING
    transit(task, TaskState.RUNNING, now_ms=0, reason="dispatch#1")
    owner1 = ex.start(task.id)
    ex.fail(task.id, owner1, "boom#1")
    assert task.state is TaskState.FAILED_RETRYING
    assert task.attempts == 1
    assert task.failure_reason == "execution_failed"
    logger.info("第1次失败 state=%s attempts=%d", task.state.value, task.attempts)

    # 调度器重新放行 -> 第 2 次执行，重试额度耗尽 -> FAILED
    transit(task, TaskState.READY, now_ms=10, reason="retry_ready")
    transit(task, TaskState.RUNNING, now_ms=10, reason="dispatch#2")
    owner2 = ex.start(task.id)
    assert owner2 != owner1
    ex.fail(task.id, owner2, "boom#2")
    assert task.state is TaskState.FAILED
    assert task.attempts == 2
    assert task.failure_reason == "retries_exhausted"
    assert task.failure_detail == "boom#2"
    logger.info(
        "重试耗尽终态 FAILED attempts=%d reason=%s detail=%s",
        task.attempts, task.failure_reason, task.failure_detail,
    )


def test_late_settle_after_lease_revoked_is_rejected() -> None:
    clock = FakeClock(0)
    store = TaskStore()
    task, ex = _running_task(store, clock, max_retries=3)
    transit(task, TaskState.RUNNING, now_ms=0, reason="dispatch")
    zombie_owner = ex.start(task.id)

    # 模拟调度器检测到租约超时：RUNNING -> LOST 并清除 owner
    transit(task, TaskState.LOST, now_ms=200, reason="lease_timeout")
    task.lease_owner = None
    task.lease_expire_at_ms = None

    # 失联执行者"复活"后迟到的成功/失败结算必须被拒绝（state 已是 LOST）
    with pytest.raises(InvalidStateTransitionError) as ei:
        ex.succeed(task.id, zombie_owner)
    assert ei.value.reason == "lease_lost"
    with pytest.raises(InvalidStateTransitionError) as ei:
        ex.fail(task.id, zombie_owner, "late")
    assert ei.value.reason == "lease_lost"
    logger.info("失联执行者迟到结算被拒绝，任务保持 state=%s 未被回退", task.state.value)
    assert task.state is TaskState.LOST
