"""编排服务门面：提交 / 查询 / 取消 / 心跳 / 调度 / 租约回收。

线程模型：所有状态跃迁都在 :class:`~orchestration.store.TaskStore` 的同一把
可重入锁内完成，executor 线程只通过回调进入本服务，回调内部再加锁，因此
提交幂等、单次执行权、结算幂等等不变量在并发下仍然成立。
"""
from __future__ import annotations

import itertools
import logging
import uuid

from .clock import Clock
from .errors import ErrorCode, OrchestrationError
from .executor import LocalExecutor
from .models import (
    PROPAGATING_STATES,
    Attempt,
    AttemptState,
    ScheduleDecision,
    Task,
    TaskSpec,
    TaskState,
    TerminalReason,
    TerminalInfo,
    reject_cancel,
)
from .scheduler import Scheduler
from .store import TaskStore

logger = logging.getLogger("orchestration")


class OrchestrationConfig:
    def __init__(
        self,
        max_concurrency: int = 4,
        lease_timeout: float = 30.0,
        heartbeat_interval: float = 10.0,
    ) -> None:
        self.max_concurrency = max_concurrency
        self.lease_timeout = lease_timeout
        self.heartbeat_interval = heartbeat_interval


class SubmitResult:
    """提交结果；``created=False`` 表示命中幂等键并收敛到既有任务。"""

    def __init__(self, task: Task, created: bool) -> None:
        self.task = task
        self.created = created


class OrchestrationService:
    def __init__(
        self,
        clock: Clock,
        executor: LocalExecutor,
        max_concurrency: int = 4,
        lease_timeout: float = 30.0,
    ) -> None:
        self.clock = clock
        self.executor = executor
        self.max_concurrency = max(1, max_concurrency)
        self.lease_timeout = lease_timeout
        self.store = TaskStore()
        self.scheduler = Scheduler(self)
        self._id_seq = itertools.count(1)

        # 注入执行器回调（executor 线程 -> 本服务）
        executor.on_success = self._on_attempt_success
        executor.on_failure = self._on_attempt_failure
        executor.set_heartbeat_callback(self.heartbeat)

    # ------------------------------------------------------------------
    # 提交 / 查询 / 取消
    # ------------------------------------------------------------------

    def submit(self, spec: TaskSpec) -> SubmitResult:
        self._validate_spec(spec)
        with self.store.lock:
            # 1) 幂等收敛：同 key 必须返回同一个任务
            if spec.idempotency_key is not None:
                existing = self.store.get_by_idempotency_key(spec.idempotency_key)
                if existing is not None:
                    logger.info(
                        "submit idempotent-hit key=%s task=%s state=%s",
                        spec.idempotency_key, existing.id, existing.state.value,
                    )
                    return SubmitResult(existing, created=False)

            # 2) 依赖必须全部存在
            deps = frozenset(spec.dependencies)
            for dep_id in deps:
                if self.store.get(dep_id) is None:
                    raise OrchestrationError(
                        ErrorCode.UNKNOWN_DEPENDENCY,
                        f"dependency {dep_id!r} does not exist",
                    )

            # 3) 防御性环检测（提交语义上 DAG 由提交顺序保证，这里兜底）
            if self._would_create_cycle(deps):
                raise OrchestrationError(
                    ErrorCode.DEPENDENCY_CYCLE,
                    "submitting this task would create a dependency cycle",
                )

            task_id = f"task-{next(self._id_seq):06d}"
            task = Task(
                id=task_id,
                idempotency_key=spec.idempotency_key or f"auto:{task_id}",
                handler=spec.handler,
                payload=dict(spec.payload),
                dependencies=deps,
                priority=spec.priority,
                max_retries=spec.max_retries,
                created_at=self.clock.now(),
            )
            self.store.put(task)
            logger.info(
                "submit accepted task=%s handler=%s key=%s deps=%s priority=%d",
                task_id, spec.handler, task.idempotency_key,
                sorted(deps), spec.priority,
            )
            return SubmitResult(task, created=True)

    def get(self, task_id: str) -> Task:
        return self.store.get_required(task_id)

    def cancel(self, task_id: str, reason: str = "user requested") -> Task:
        with self.store.lock:
            task = self.store.get_required(task_id)
            if task.state is not TaskState.PENDING:
                raise reject_cancel(task)
            self._mark_cancelled(task, TerminalReason.CANCELLED, reason)
            logger.info("cancel accepted task=%s detail=%r", task_id, reason)
            # 取消沿依赖边向下游传播
            self._propagate_blocked(task)
            return task

    # ------------------------------------------------------------------
    # 心跳与结算（由 executor 线程调用）
    # ------------------------------------------------------------------

    def heartbeat(self, task_id: str, lease_token: str) -> None:
        with self.store.lock:
            task = self.store.get_required(task_id)
            attempt = self._validate_lease(task, lease_token)
            now = self.clock.now()
            task.last_heartbeat_at = now
            task.lease_expire_at = now + self.lease_timeout
            attempt_state_note = attempt.state.value
            logger.info(
                "heartbeat task=%s attempt=%d lease=%s expire_at=%.3f(%s)",
                task_id, attempt.attempt_no, lease_token,
                task.lease_expire_at, attempt_state_note,
            )

    def report_success(self, task_id: str, lease_token: str, result: object) -> None:
        with self.store.lock:
            task = self.store.get_required(task_id)
            attempt = self._validate_lease(task, lease_token)
            attempt.state = AttemptState.SUCCEEDED
            attempt.ended_at = self.clock.now()
            task.state = TaskState.COMPLETED
            task.result = result
            task.terminal = TerminalInfo(
                reason=TerminalReason.COMPLETED,
                at=self.clock.now(),
                detail="handler returned successfully",
            )
            self._clear_lease(task)
            logger.info(
                "settle success task=%s attempt=%d result=%r",
                task_id, attempt.attempt_no, result,
            )

    def report_failure(self, task_id: str, lease_token: str, error: str) -> None:
        with self.store.lock:
            task = self.store.get_required(task_id)
            attempt = self._validate_lease(task, lease_token)
            attempt.state = AttemptState.FAILED
            attempt.ended_at = self.clock.now()
            attempt.error = error
            self._clear_lease(task)
            logger.warning(
                "settle failure task=%s attempt=%d error=%s",
                task_id, attempt.attempt_no, error,
            )
            self._handle_attempt_end(
                task,
                timeout=False,
                last_error=error,
            )

    # ------------------------------------------------------------------
    # 调度与租约回收
    # ------------------------------------------------------------------

    def schedule_tick(self) -> list[ScheduleDecision]:
        return self.scheduler.tick()

    def reclaim_expired_leases(self) -> list[str]:
        """回收所有失联（租约超时）的运行中尝试。返回被回收的任务 id 列表。"""
        reclaimed: list[str] = []
        now = self.clock.now()
        with self.store.lock:
            for task in self.store.tasks_in_state(TaskState.RUNNING):
                if task.lease_expire_at is None or now < task.lease_expire_at:
                    continue
                attempt = task.current_attempt()
                if attempt is not None:
                    attempt.state = AttemptState.LEASE_TIMEOUT
                    attempt.ended_at = now
                    attempt.error = "lease timed out: worker lost contact"
                token = task.lease_token
                self._clear_lease(task)
                logger.warning(
                    "lease timeout task=%s attempt=%d lease=%s now=%.3f",
                    task.id, attempt.attempt_no if attempt else -1,
                    token, now,
                )
                self._handle_attempt_end(
                    task,
                    timeout=True,
                    last_error="lease timed out: worker lost contact",
                )
                reclaimed.append(task.id)
        if reclaimed:
            logger.info("reclaimed %d expired lease(s): %s", len(reclaimed), reclaimed)
        return reclaimed

    def running_count(self) -> int:
        with self.store.lock:
            return len(self.store.tasks_in_state(TaskState.RUNNING))

    # ------------------------------------------------------------------
    # 内部：派发
    # ------------------------------------------------------------------

    def dispatch(self, task: Task) -> str:
        """调用方必须已确认就绪且持有锁。发放租约并异步执行，返回 lease_token。"""
        now = self.clock.now()
        lease_token = uuid.uuid4().hex
        attempt_no = task.attempt_count + 1
        task.attempts.append(
            Attempt(
                attempt_no=attempt_no,
                started_at=now,
                lease_token=lease_token,
            )
        )
        task.state = TaskState.RUNNING
        task.lease_token = lease_token
        task.lease_expire_at = now + self.lease_timeout
        task.last_heartbeat_at = now
        logger.info(
            "dispatch-grant task=%s attempt=%d lease=%s expire_at=%.3f",
            task.id, attempt_no, lease_token, task.lease_expire_at,
        )
        # 异步执行；submit 自身不持锁地把任务投入线程池
        self.executor.submit(task, lease_token, attempt_no)
        return lease_token

    # ------------------------------------------------------------------
    # 内部：执行器回调（过期尝试的结算将被拒绝 => 不重复结算）
    # ------------------------------------------------------------------

    def _on_attempt_success(self, task_id: str, lease_token: str, result: object) -> None:
        try:
            self.report_success(task_id, lease_token, result)
        except OrchestrationError as exc:
            logger.warning(
                "stale success ignored task=%s lease=%s code=%s reason=%s",
                task_id, lease_token, exc.code, exc.reason,
            )

    def _on_attempt_failure(self, task_id: str, lease_token: str, error: str) -> None:
        try:
            self.report_failure(task_id, lease_token, error)
        except OrchestrationError as exc:
            logger.warning(
                "stale failure ignored task=%s lease=%s code=%s reason=%s",
                task_id, lease_token, exc.code, exc.reason,
            )

    # ------------------------------------------------------------------
    # 内部：租约校验 / 清理
    # ------------------------------------------------------------------

    def _validate_lease(self, task: Task, lease_token: str) -> Attempt:
        if task.state is not TaskState.RUNNING:
            raise OrchestrationError(
                ErrorCode.LEASE_HEARTBEAT_NOT_RUNNING,
                f"task is {task.state.value}, not RUNNING; stale attempt rejected",
                task.id,
            )
        if task.lease_token is None or task.lease_token != lease_token:
            raise OrchestrationError(
                ErrorCode.LEASE_TOKEN_MISMATCH,
                "lease token mismatch; attempt was superseded or never granted",
                task.id,
            )
        if task.lease_expire_at is not None and self.clock.now() > task.lease_expire_at:
            raise OrchestrationError(
                ErrorCode.LEASE_EXPIRED,
                "lease has expired; attempt is no longer authoritative",
                task.id,
            )
        attempt = task.current_attempt()
        if attempt is None or attempt.lease_token != lease_token:
            raise OrchestrationError(
                ErrorCode.LEASE_TOKEN_MISMATCH,
                "no running attempt matching the lease token",
                task.id,
            )
        return attempt

    @staticmethod
    def _clear_lease(task: Task) -> None:
        task.lease_token = None
        task.lease_expire_at = None

    # ------------------------------------------------------------------
    # 内部：尝试结束后的统一裁决（重试 / 失败 / 传播）
    # ------------------------------------------------------------------

    def _handle_attempt_end(self, task: Task, *, timeout: bool, last_error: str) -> None:
        """调用方持锁。决定重试排队还是终态失败；失败后传播 BLOCKED。"""
        used_attempts = task.attempt_count
        can_retry = used_attempts <= task.max_retries
        if can_retry:
            task.state = TaskState.PENDING
            logger.info(
                "retry-enqueue task=%s used_attempts=%d max_retries=%d (%s)",
                task.id, used_attempts, task.max_retries,
                "lease timeout" if timeout else "business failure",
            )
            return

        task.state = TaskState.FAILED
        task.terminal = TerminalInfo(
            reason=TerminalReason.RETRIES_EXHAUSTED,
            at=self.clock.now(),
            detail=(
                f"retries exhausted after {used_attempts} attempt(s); "
                f"last error: {last_error}"
            ),
        )
        logger.error(
            "task failed permanently task=%s attempts=%d last_error=%s",
            task.id, used_attempts, last_error,
        )
        self._propagate_blocked(task)

    def _propagate_blocked(self, root: Task) -> None:
        """root 已进入传播性终态（FAILED/CANCELLED/BLOCKED），标记其全部下游。

        BFS 沿反向依赖边推进；只影响仍在 PENDING 的任务，RUNNING 的下游
        由其自身租约/结算路径决定（失败结算同样会继续向下传播）。
        """
        if root.state not in PROPAGATING_STATES:
            return
        if root.state is TaskState.CANCELLED:
            root_reason = TerminalReason.UPSTREAM_CANCELLED
        elif root.state is TaskState.FAILED:
            root_reason = TerminalReason.UPSTREAM_FAILED
        else:
            root_reason = TerminalReason.UPSTREAM_BLOCKED

        # 反向邻接表：dep -> [dependents]
        reverse: dict[str, list[str]] = {}
        for t in self.store.all_tasks():
            for dep in t.dependencies:
                reverse.setdefault(dep, []).append(t.id)

        queue = [root.id]
        visited = {root.id}
        while queue:
            current_id = queue.pop(0)
            for dependent_id in reverse.get(current_id, []):
                if dependent_id in visited:
                    continue
                visited.add(dependent_id)
                dep_task = self.store.get(dependent_id)
                if dep_task is None:
                    continue
                if dep_task.state is TaskState.PENDING:
                    dep_task.state = TaskState.BLOCKED
                    dep_task.terminal = TerminalInfo(
                        reason=root_reason,
                        at=self.clock.now(),
                        detail=(
                            f"upstream task {current_id!r} is "
                            f"{self.store.get(current_id).state.value}"
                        ),
                        upstream_task_id=current_id,
                    )
                    logger.info(
                        "propagate blocked task=%s due_to=%s upstream=%s",
                        dep_task.id, root_reason.value, current_id,
                    )
                    queue.append(dep_task.id)
                elif dep_task.state in PROPAGATING_STATES:
                    queue.append(dep_task.id)

    def _mark_cancelled(
        self, task: Task, reason: TerminalReason, detail: str
    ) -> None:
        task.state = TaskState.CANCELLED
        task.terminal = TerminalInfo(
            reason=reason, at=self.clock.now(), detail=detail
        )

    # ------------------------------------------------------------------
    # 内部：校验
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_spec(spec: TaskSpec) -> None:
        if not isinstance(spec.handler, str) or not spec.handler.strip():
            raise OrchestrationError(
                ErrorCode.INVALID_PAYLOAD, "handler must be a non-empty string"
            )
        if not isinstance(spec.priority, int):
            raise OrchestrationError(
                ErrorCode.INVALID_PAYLOAD, "priority must be an integer"
            )
        if spec.max_retries < 0:
            raise OrchestrationError(
                ErrorCode.INVALID_PAYLOAD, "max_retries must be >= 0"
            )

    def _would_create_cycle(self, new_deps: frozenset[str]) -> bool:
        """新任务尚无 id、不会被任何既有任务依赖，故不可能形成环。

        保留为显式检查点：若未来允许按客户端指定 id 提交，可在此实现 DFS。
        """
        return False
