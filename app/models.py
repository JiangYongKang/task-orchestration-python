"""领域模型：任务状态、任务实体与提交参数。"""

from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass, field


class TaskState(str, enum.Enum):
    """任务状态机：

    PENDING   -> READY | BLOCKED | CANCELLED
    READY     -> RUNNING | CANCELLED
    RUNNING   -> SUCCEEDED | FAILED_RETRYING | LOST | CANCELLED
    LOST      -> FAILED_RETRYING | FAILED
    FAILED_RETRYING -> READY
    SUCCEEDED / FAILED / CANCELLED 为终态。
    """

    PENDING = "PENDING"                  # 已提交，尚未参与调度判定
    BLOCKED = "BLOCKED"                  # 依赖未全部成功
    READY = "READY"                      # 可被调度
    RUNNING = "RUNNING"                  # 已获得执行权
    SUCCEEDED = "SUCCEEDED"              # 终态：成功
    FAILED = "FAILED"                    # 终态：重试耗尽 / 依赖失败传播
    FAILED_RETRYING = "FAILED_RETRYING"  # 执行失败，等待重试
    LOST = "LOST"                        # 租约超时（执行者失联）
    CANCELLED = "CANCELLED"              # 终态：被取消


TERMINAL_STATES = frozenset(
    {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED}
)


@dataclass
class TaskSpec:
    """任务提交参数（外部输入）。"""

    idempotency_key: str
    payload: str = ""
    priority: int = 0                       # 数值越大优先级越高
    dependencies: frozenset[str] = frozenset()
    max_retries: int = 0
    lease_timeout_ms: int = 5_000
    group: str = "default"                  # 业务分组，用于分组并发配额
    retry_delay_ms: int = 0                 # 重试基础等待时间（0 表示立即重试）
    retry_backoff_multiplier: float = 1.0   # 退避倍率：第 n 次重试等待 delay * multiplier^(n-1)


@dataclass
class Task:
    """任务实体（存储中的聚合根）。"""

    id: str
    idempotency_key: str
    payload: str
    priority: int
    dependencies: frozenset[str]
    max_retries: int
    lease_timeout_ms: int
    group: str = "default"
    retry_delay_ms: int = 0
    retry_backoff_multiplier: float = 1.0
    state: TaskState = TaskState.PENDING
    attempts: int = 0
    submit_count: int = 1
    failure_reason: str | None = None
    failure_detail: str | None = None
    lease_owner: str | None = None
    lease_expire_at_ms: int | None = None
    next_retry_at_ms: int | None = None     # FAILED_RETRYING 状态下下次可重试的时间点
    created_at_ms: int = 0
    updated_at_ms: int = 0
    events: list[str] = field(default_factory=list)

    @staticmethod
    def new_id() -> str:
        return uuid.uuid4().hex


# 合法状态跃迁表；任何不在表内的跃迁必须被拒绝。
ALLOWED_TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    TaskState.PENDING: frozenset(
        {TaskState.BLOCKED, TaskState.READY, TaskState.CANCELLED, TaskState.FAILED}
    ),
    TaskState.BLOCKED: frozenset(
        {TaskState.READY, TaskState.CANCELLED, TaskState.FAILED}
    ),
    TaskState.READY: frozenset(
        {TaskState.RUNNING, TaskState.CANCELLED, TaskState.FAILED}
    ),
    TaskState.RUNNING: frozenset(
        {
            TaskState.SUCCEEDED,
            TaskState.FAILED,
            TaskState.FAILED_RETRYING,
            TaskState.LOST,
            TaskState.CANCELLED,
        }
    ),
    TaskState.LOST: frozenset(
        {TaskState.FAILED_RETRYING, TaskState.FAILED, TaskState.CANCELLED}
    ),
    TaskState.FAILED_RETRYING: frozenset({TaskState.READY, TaskState.CANCELLED}),
    TaskState.SUCCEEDED: frozenset(),
    TaskState.FAILED: frozenset(),
    TaskState.CANCELLED: frozenset(),
}


def can_transit(src: TaskState, dst: TaskState) -> bool:
    return dst in ALLOWED_TRANSITIONS.get(src, frozenset())


def retry_delay_for(task: Task) -> int:
    """计算当前这次重试前的等待毫秒数（按已尝试次数退避）。

    第 1 次重试等待 retry_delay_ms，之后每次乘 retry_backoff_multiplier。
    """
    if task.retry_delay_ms <= 0:
        return 0
    exponent = max(0, task.attempts - 1)
    return int(task.retry_delay_ms * (task.retry_backoff_multiplier ** exponent))


def transit(
    task: Task,
    dst: TaskState,
    *,
    now_ms: int,
    reason: str,
    failure_reason: str | None = None,
    failure_detail: str | None = None,
) -> None:
    """执行一次经过校验的状态跃迁，并记录可追溯事件。

    任何未在 ALLOWED_TRANSITIONS 中声明的跃迁都会抛出 AssertionError；
    业务层应先判定再调用，并把业务可区分原因通过 reason/failure_reason 传入。
    """
    src = task.state
    if not can_transit(src, dst):
        raise AssertionError(f"非法状态跃迁（表外）: {task.id} {src.value} -> {dst.value}")
    task.state = dst
    task.updated_at_ms = now_ms
    task.events.append(f"{now_ms}: {src.value}->{dst.value} reason={reason}")
    if failure_reason is not None:
        task.failure_reason = failure_reason
    if failure_detail is not None:
        task.failure_detail = failure_detail
