"""本地任务存储：用锁 + 字典模拟持久化任务队列。

所有读写均在同一把可重入锁下进行，保证：
- 幂等键 -> 任务 id 的索引与任务实体一致；
- 并发提交同一幂等键只有一个调用方成功占位；
- 调度循环对多个任务的复合判定是原子快照。
"""

from __future__ import annotations

import threading

from app.models import Task, TaskState


class TaskStore:
    """线程安全的任务存储。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._tasks: dict[str, Task] = {}
        self._idempotency_index: dict[str, str] = {}

    def lock(self) -> threading.RLock:
        """返回存储级锁；复合操作（判定 + 写入）应在此锁内完成。"""
        return self._lock

    def get(self, task_id: str) -> Task | None:
        with self._lock:
            return self._tasks.get(task_id)

    def get_by_idempotency_key(self, key: str) -> Task | None:
        with self._lock:
            task_id = self._idempotency_index.get(key)
            if task_id is None:
                return None
            return self._tasks.get(task_id)

    def put(self, task: Task) -> None:
        """写入或覆盖任务，并维护幂等键索引。"""
        with self._lock:
            self._tasks[task.id] = task
            self._idempotency_index[task.idempotency_key] = task.id

    def reserve_idempotency_key(self, key: str, task_id: str) -> bool:
        """原子占位：幂等键首次出现返回 True，重复出现返回 False。

        成功占位意味着调用方负责随后 put 对应任务；失败方应通过
        get_by_idempotency_key 取回已收敛的任务。
        """
        with self._lock:
            existing = self._idempotency_index.get(key)
            if existing is not None:
                return False
            self._idempotency_index[key] = task_id
            return True

    def all(self) -> list[Task]:
        with self._lock:
            return list(self._tasks.values())

    def resolve(self, ref: str) -> Task | None:
        """依赖解析：依赖引用既可以是任务 id，也可以是幂等键。"""
        with self._lock:
            task = self._tasks.get(ref)
            if task is not None:
                return task
            dep_id = self._idempotency_index.get(ref)
            if dep_id is None:
                return None
            return self._tasks.get(dep_id)

    def by_state(self, state: TaskState) -> list[Task]:
        with self._lock:
            return [t for t in self._tasks.values() if t.state == state]
