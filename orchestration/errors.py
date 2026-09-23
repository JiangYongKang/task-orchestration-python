"""错误类型与可区分的错误码。

所有非法操作都抛出 :class:`OrchestrationError`，其中 ``code`` 是机器可区分的
稳定错误码，``reason`` 是给人看的说明文本。
"""
from __future__ import annotations


class ErrorCode:
    # 提交阶段
    TASK_NOT_FOUND = "task_not_found"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"
    UNKNOWN_DEPENDENCY = "unknown_dependency"
    DEPENDENCY_CYCLE = "dependency_cycle"
    INVALID_PAYLOAD = "invalid_payload"

    # 状态跃迁阶段
    ALREADY_TERMINAL = "already_terminal"
    TASK_COMPLETED = "task_completed"
    TASK_CANCELLED = "task_cancelled"
    TASK_BLOCKED = "task_blocked"
    TASK_RUNNING = "task_running"
    ILLEGAL_STATE_TRANSITION = "illegal_state_transition"

    # 执行 / 租约阶段
    LEASE_EXPIRED = "lease_expired"
    LEASE_HEARTBEAT_NOT_RUNNING = "heartbeat_not_running"
    LEASE_TOKEN_MISMATCH = "lease_token_mismatch"
    RETRIES_EXHAUSTED = "retries_exhausted"

    # 通用
    HANDLER_NOT_FOUND = "handler_not_found"


class OrchestrationError(Exception):
    """业务错误基类，携带稳定错误码与任务 id。"""

    def __init__(self, code: str, reason: str, task_id: str | None = None):
        super().__init__(reason)
        self.code = code
        self.reason = reason
        self.task_id = task_id

    def __str__(self) -> str:  # pragma: no cover - 调试用
        prefix = f"[{self.code}]"
        if self.task_id:
            prefix += f" task={self.task_id}"
        return f"{prefix} {self.reason}"

    def to_dict(self) -> dict:
        return {"code": self.code, "reason": self.reason, "task_id": self.task_id}
