"""可区分的领域错误类型。"""

from __future__ import annotations


class TaskError(Exception):
    """所有任务领域错误的基类。"""

    def __init__(self, message: str, *, reason: str) -> None:
        super().__init__(message)
        self.message = message
        self.reason = reason


class TaskNotFoundError(TaskError):
    """任务不存在。"""

    def __init__(self, task_id: str) -> None:
        super().__init__(f"任务不存在: {task_id}", reason="task_not_found")
        self.task_id = task_id


class InvalidStateTransitionError(TaskError):
    """非法状态跃迁，附带可区分原因。"""

    def __init__(self, task_id: str, from_state: str, to_state: str, reason: str) -> None:
        super().__init__(
            f"非法状态跃迁: {task_id} {from_state} -> {to_state} ({reason})",
            reason=reason,
        )
        self.task_id = task_id
        self.from_state = from_state
        self.to_state = to_state
