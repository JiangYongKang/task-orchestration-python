"""编排服务门面：提交、查询、取消等用例入口。

线程安全约定：
- submit 的「查幂等键 -> 占位 -> 建任务」整体在 store 锁内完成，
  因此并发提交同一 idempotency_key 只会产生一个任务，其余调用收敛到同一实体；
- cancel 与调度/结算互斥（store 锁），杜绝取消与结算竞争造成的重复结算。
"""

from __future__ import annotations

import logging

from app.clock import Clock, SystemClock
from app.errors import InvalidStateTransitionError, TaskNotFoundError
from app.executor import Executor
from app.models import (
    Task,
    TaskSpec,
    TaskState,
    transit,
)
from app.scheduler import Scheduler
from app.storage import TaskStore

logger = logging.getLogger("orchestration")


class OrchestrationService:
    """对外应用服务。"""

    def __init__(
        self,
        max_concurrency: int = 1,
        clock: Clock | None = None,
    ) -> None:
        self.clock = clock or SystemClock()
        self.store = TaskStore()
        self.executor = Executor(self.store, self.clock)
        self.scheduler = Scheduler(
            self.store, self.clock, max_concurrency, executor=self.executor
        )

    # ---------- 提交 ----------
    def submit(self, spec: TaskSpec) -> Task:
        now = self.clock.now_ms()
        with self.store.lock():
            existing = self.store.get_by_idempotency_key(spec.idempotency_key)
            if existing is not None:
                # 幂等收敛：重复提交返回同一任务，仅记录提交次数，不重复入队。
                existing.submit_count += 1
                existing.updated_at_ms = now
                existing.events.append(
                    f"{now}: submit_dedup idempotency_key={spec.idempotency_key} "
                    f"submit_count={existing.submit_count}"
                )
                logger.info(
                    "幂等收敛 key=%s -> task=%s submit_count=%d",
                    spec.idempotency_key,
                    existing.id,
                    existing.submit_count,
                )
                return existing

            task_id = Task.new_id()
            task = Task(
                id=task_id,
                idempotency_key=spec.idempotency_key,
                payload=spec.payload,
                priority=spec.priority,
                dependencies=frozenset(spec.dependencies),
                max_retries=spec.max_retries,
                lease_timeout_ms=spec.lease_timeout_ms,
                created_at_ms=now,
                updated_at_ms=now,
            )
            # put 原子写入实体并建立幂等键索引（锁内完成）。
            self.store.put(task)
            task.events.append(
                f"{now}: submitted idempotency_key={spec.idempotency_key} "
                f"priority={spec.priority} deps={sorted(spec.dependencies)}"
            )
            logger.info(
                "任务提交 id=%s key=%s priority=%d deps=%s",
                task.id,
                spec.idempotency_key,
                spec.priority,
                sorted(spec.dependencies),
            )
            return task

    # ---------- 查询 ----------
    def get(self, task_id: str) -> Task:
        task = self.store.get(task_id)
        if task is None:
            raise TaskNotFoundError(task_id)
        return task

    # ---------- 取消 ----------
    def cancel(self, task_id: str) -> Task:
        with self.store.lock():
            task = self.store.get(task_id)
            if task is None:
                raise TaskNotFoundError(task_id)

            if task.state is TaskState.CANCELLED:
                raise InvalidStateTransitionError(
                    task_id, task.state.value, TaskState.CANCELLED.value,
                    reason="already_cancelled",
                )
            if task.state is TaskState.SUCCEEDED:
                raise InvalidStateTransitionError(
                    task_id, task.state.value, TaskState.CANCELLED.value,
                    reason="already_succeeded",
                )
            if task.state is TaskState.FAILED:
                raise InvalidStateTransitionError(
                    task_id, task.state.value, TaskState.CANCELLED.value,
                    reason="already_failed",
                )
            if task.state is TaskState.RUNNING:
                # 运行中的任务由执行者结算；取消请求拒绝，避免执行者失联前的双写竞争。
                raise InvalidStateTransitionError(
                    task_id, task.state.value, TaskState.CANCELLED.value,
                    reason="task_running",
                )

            # PENDING / BLOCKED / READY / LOST / FAILED_RETRYING 允许取消。
            transit(task, TaskState.CANCELLED, now_ms=self.clock.now_ms(), reason="cancel_requested")
            logger.info("任务取消 id=%s state=%s", task.id, task.state.value)
            return task

    # ---------- 调度与执行的门面转发 ----------
    def schedule_once(self) -> list[str]:
        return self.scheduler.run_once()

    def heartbeat(self, task_id: str, owner: str) -> bool:
        return self.executor.heartbeat(task_id, owner)

    def succeed(self, task_id: str, owner: str) -> None:
        self.executor.succeed(task_id, owner)

    def fail(self, task_id: str, owner: str, detail: str) -> None:
        self.executor.fail(task_id, owner, detail)

    def start(self, task_id: str, owner: str) -> bool:
        return self.executor.start(task_id, owner)
