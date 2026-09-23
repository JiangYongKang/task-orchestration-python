"""执行器：模拟任务执行、心跳续约与执行结果结算。

一致性约定：
- 所有方法只接受「当前租约持有者 owner」操作；租约被调度器回收后，
  失联执行者的迟到心跳 / succeed / fail 全部被拒绝（owner_mismatch / lease_lost），
  因此重试与重新调度不会造成重复结算或状态回退。
- succeed / fail 只能在 RUNNING 状态结算一次；终态任务再次结算被拒绝。
"""

from __future__ import annotations

import logging
import uuid

from app.errors import InvalidStateTransitionError, TaskNotFoundError
from app.clock import Clock, SystemClock
from app.models import Task, TaskState, transit
from app.storage import TaskStore

logger = logging.getLogger("orchestration")


def new_owner_token() -> str:
    return "exec-" + uuid.uuid4().hex


class Executor:
    """执行器。"""

    def __init__(self, store: TaskStore, clock: Clock | None = None) -> None:
        self.store = store
        self.clock = clock or SystemClock()

    def start(self, task_id: str, owner: str | None = None) -> str | None:
        """开始执行：由调度器在已将任务置为 RUNNING 后调用，建立初始租约。

        返回租约持有者 token；若任务不存在或不在 RUNNING 则返回 None。
        """
        token = owner or new_owner_token()
        with self.store.lock():
            task = self.store.get(task_id)
            if task is None or task.state is not TaskState.RUNNING:
                return None
            task.lease_owner = token
            task.attempts += 1
            task.lease_expire_at_ms = self.clock.now_ms() + task.lease_timeout_ms
            task.updated_at_ms = self.clock.now_ms()
            task.events.append(
                f"{self.clock.now_ms()}: lease_granted owner={token} attempt={task.attempts}"
            )
            logger.info("租约授予 task=%s owner=%s attempt=%d", task_id, token, task.attempts)
            return token

    def heartbeat(self, task_id: str, owner: str) -> bool:
        """心跳续约：仅当前租约持有者可续约；返回是否成功。"""
        with self.store.lock():
            task = self.store.get(task_id)
            if task is None:
                raise TaskNotFoundError(task_id)
            if task.lease_owner != owner:
                logger.warning("心跳拒绝 task=%s owner=%s reason=owner_mismatch", task_id, owner)
                return False
            if task.state is not TaskState.RUNNING:
                logger.warning(
                    "心跳拒绝 task=%s state=%s reason=not_running", task_id, task.state.value
                )
                return False
            task.lease_expire_at_ms = self.clock.now_ms() + task.lease_timeout_ms
            task.updated_at_ms = self.clock.now_ms()
            return True

    def _require_running_owner(self, task_id: str, owner: str) -> Task:
        task = self.store.get(task_id)
        if task is None:
            raise TaskNotFoundError(task_id)
        if task.state is not TaskState.RUNNING:
            # 终态重复结算、以及租约回收后其它非运行态的迟到结算，都在此拒绝。
            if task.state in (
                TaskState.SUCCEEDED,
                TaskState.FAILED,
                TaskState.CANCELLED,
            ):
                reason = "already_settled"
            else:
                reason = "lease_lost"
            raise InvalidStateTransitionError(
                task_id, task.state.value, "*", reason=reason
            )
        if task.lease_owner != owner:
            # RUNNING 但租约已不属于该执行者：迟到结算必须被拒绝。
            raise InvalidStateTransitionError(
                task_id, task.state.value, "*", reason="lease_owner_mismatch"
            )
        return task

    def succeed(self, task_id: str, owner: str) -> None:
        with self.store.lock():
            task = self._require_running_owner(task_id, owner)
            now = self.clock.now_ms()
            transit(task, TaskState.SUCCEEDED, now_ms=now, reason="executor_succeeded")
            task.lease_owner = None
            task.lease_expire_at_ms = None
            logger.info("任务成功 task=%s attempts=%d", task_id, task.attempts)

    def fail(self, task_id: str, owner: str, detail: str) -> None:
        """执行失败结算：仍有重试额度 -> FAILED_RETRYING，否则 FAILED（终态）。"""
        with self.store.lock():
            task = self._require_running_owner(task_id, owner)
            now = self.clock.now_ms()
            # attempts 是「已经开始执行的次数」；剩余额度 = max_retries - (attempts - 1)
            remaining = task.max_retries - (task.attempts - 1)
            task.lease_owner = None
            task.lease_expire_at_ms = None
            if remaining > 0:
                transit(
                    task,
                    TaskState.FAILED_RETRYING,
                    now_ms=now,
                    reason="execution_failed_retry",
                    failure_reason="execution_failed",
                    failure_detail=detail,
                )
                logger.info(
                    "任务失败待重试 task=%s attempt=%d remaining=%d detail=%s",
                    task_id, task.attempts, remaining, detail,
                )
            else:
                transit(
                    task,
                    TaskState.FAILED,
                    now_ms=now,
                    reason="retries_exhausted",
                    failure_reason="retries_exhausted",
                    failure_detail=detail,
                )
                logger.info(
                    "任务最终失败 task=%s attempts=%d detail=%s",
                    task_id, task.attempts, detail,
                )
