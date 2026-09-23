"""领域模型与任务状态机定义。

状态机::

    PENDING ──调度成功──▶ RUNNING ──成功──────────────▶ COMPLETED
       │                   │
       │                   ├──业务失败(可重试)──▶ PENDING (attempt+1)
       │                   ├──业务失败(重试耗尽)▶ FAILED
       │                   └──租约超时(可重试)──▶ PENDING (attempt+1)
       │
       ├──用户取消──▶ CANCELLED
       └──上游 FAILED/CANCELLED/BLOCKED ──▶ BLOCKED

* COMPLETED / FAILED / CANCELLED / BLOCKED 为终态。
* PENDING 表示在排队（依赖未就绪或没有配额），就绪与否由 :class:`Scheduler` 判定。
* RUNNING 持有租约，需要执行者心跳；租约超时后被回收为 PENDING。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .errors import ErrorCode, OrchestrationError


class TaskState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    BLOCKED = "BLOCKED"


TERMINAL_STATES = frozenset(
    {TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCELLED, TaskState.BLOCKED}
)
# 这些终态会把下游一并标记为 BLOCKED
PROPAGATING_STATES = frozenset({TaskState.FAILED, TaskState.CANCELLED, TaskState.BLOCKED})


class AttemptState(str, Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    LEASE_TIMEOUT = "LEASE_TIMEOUT"


class TerminalReason(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UPSTREAM_FAILED = "upstream_failed"
    UPSTREAM_CANCELLED = "upstream_cancelled"
    UPSTREAM_BLOCKED = "upstream_blocked"
    RETRIES_EXHAUSTED = "retries_exhausted"


@dataclass
class Attempt:
    """一次执行尝试（拿到执行权即生成一条）。"""

    attempt_no: int
    started_at: float
    lease_token: str
    state: AttemptState = AttemptState.RUNNING
    ended_at: float | None = None
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "attempt_no": self.attempt_no,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "lease_token": self.lease_token,
            "state": self.state.value,
            "error": self.error,
        }


@dataclass
class TerminalInfo:
    reason: TerminalReason
    at: float
    detail: str = ""
    upstream_task_id: str | None = None

    def to_dict(self) -> dict:
        return {
            "reason": self.reason.value,
            "at": self.at,
            "detail": self.detail,
            "upstream_task_id": self.upstream_task_id,
        }


@dataclass
class Task:
    id: str
    idempotency_key: str
    handler: str
    payload: dict
    dependencies: frozenset[str]
    priority: int
    max_retries: int
    created_at: float

    state: TaskState = TaskState.PENDING
    attempts: list[Attempt] = field(default_factory=list)
    terminal: TerminalInfo | None = None
    result: object = None
    # 当前租约
    lease_token: str | None = None
    lease_expire_at: float | None = None
    last_heartbeat_at: float | None = None

    @property
    def attempt_count(self) -> int:
        return len(self.attempts)

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    def current_attempt(self) -> Attempt | None:
        for a in reversed(self.attempts):
            if a.state is AttemptState.RUNNING:
                return a
        return None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "idempotency_key": self.idempotency_key,
            "handler": self.handler,
            "payload": self.payload,
            "dependencies": sorted(self.dependencies),
            "priority": self.priority,
            "max_retries": self.max_retries,
            "created_at": self.created_at,
            "state": self.state.value,
            "attempts": [a.to_dict() for a in self.attempts],
            "terminal": self.terminal.to_dict() if self.terminal else None,
            "result": self.result,
            "lease_token": self.lease_token,
            "lease_expire_at": self.lease_expire_at,
            "last_heartbeat_at": self.last_heartbeat_at,
        }


@dataclass(frozen=True)
class TaskSpec:
    handler: str
    payload: dict = field(default_factory=dict)
    idempotency_key: str | None = None
    dependencies: tuple[str, ...] = ()
    priority: int = 0
    max_retries: int = 3


@dataclass(frozen=True)
class ScheduleDecision:
    """一次调度 tick 对单个任务给出的判定依据（可解释性）。"""

    task_id: str
    dispatched: bool
    reason: str
    detail: str = ""

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "dispatched": self.dispatched,
            "reason": self.reason,
            "detail": self.detail,
        }


def reject_cancel(task: Task) -> OrchestrationError:
    """根据任务当前状态生成「取消被拒绝」的可区分错误。"""
    if task.state is TaskState.COMPLETED:
        return OrchestrationError(
            ErrorCode.TASK_COMPLETED,
            "task already completed and cannot be cancelled",
            task.id,
        )
    if task.state is TaskState.CANCELLED:
        return OrchestrationError(
            ErrorCode.TASK_CANCELLED,
            "task already cancelled",
            task.id,
        )
    if task.state is TaskState.FAILED:
        return OrchestrationError(
            ErrorCode.ALREADY_TERMINAL,
            "task already failed and cannot be cancelled",
            task.id,
        )
    if task.state is TaskState.BLOCKED:
        return OrchestrationError(
            ErrorCode.TASK_BLOCKED,
            "task is blocked by an upstream terminal task and cannot be cancelled",
            task.id,
        )
    if task.state is TaskState.RUNNING:
        return OrchestrationError(
            ErrorCode.TASK_RUNNING,
            "task is currently running; cancellation of running tasks is not supported",
            task.id,
        )
    return OrchestrationError(
        ErrorCode.ILLEGAL_STATE_TRANSITION,
        f"cannot cancel task in state {task.state.value}",
        task.id,
    )
