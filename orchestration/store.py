"""线程安全的本地任务存储（模拟任务队列与索引）。

所有读写都受同一把可重入锁保护；编排服务在多步操作期间也会持有这把锁，
从而保证「幂等键查重 + 插入」「状态检查 + 跃迁」等复合操作的原子性。
"""
from __future__ import annotations

import threading

from .errors import ErrorCode, OrchestrationError
from .models import Task, TaskState


class TaskStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._tasks: dict[str, Task] = {}
        # 幂等键 -> 任务 id
        self._idempotency_index: dict[str, str] = {}

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def get(self, task_id: str) -> Task | None:
        with self._lock:
            return self._tasks.get(task_id)

    def get_required(self, task_id: str) -> Task:
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                raise OrchestrationError(
                    ErrorCode.TASK_NOT_FOUND, f"task {task_id!r} not found", task_id
                )
            return task

    def get_by_idempotency_key(self, key: str) -> Task | None:
        with self._lock:
            task_id = self._idempotency_index.get(key)
            return self._tasks.get(task_id) if task_id is not None else None

    def put(self, task: Task) -> None:
        """插入或更新任务；首次插入时登记幂等键索引。"""
        with self._lock:
            is_new = task.id not in self._tasks
            self._tasks[task.id] = task
            if is_new:
                self._idempotency_index[task.idempotency_key] = task.id

    def all_tasks(self) -> list[Task]:
        with self._lock:
            return list(self._tasks.values())

    def tasks_in_state(self, state: TaskState) -> list[Task]:
        with self._lock:
            return [t for t in self._tasks.values() if t.state is state]
