"""调度器：依赖就绪判定、优先级排序、并发配额、租约回收、重试退避与失败传播。

每轮 run_once 在 store 全局锁内原子执行五个阶段：
1. lease_reclaim   : RUNNING 中租约到期的任务 -> LOST -> READY/FAILED(重试耗尽)
2. propagate       : 依赖出现 FAILED/CANCELLED 的未决任务 -> FAILED（级联）
3. classify        : PENDING/BLOCKED 任务按依赖状态 -> READY 或保持 BLOCKED
4. retry_release   : FAILED_RETRYING / 等待重试的 LOST 任务，到达 next_retry_at_ms 才回 READY
5. dispatch        : READY 任务按 (优先级降序, 创建时间升序) 派发，
                     同时受全局并发上限与分组并发上限约束

一致性保证：
- 派发与 lease_granted 同锁完成，因此同一任务在并发调度下只会获得一次执行权；
- 依赖失败/取消的下游直接判 FAILED（reason=upstream_failed），不会继续排队；
- 重试必须等到 next_retry_at_ms 之后才会重新参与调度；到点后本轮即回 READY，
  保证「配了几次重试就实实在在执行几次」，不会出现只标记等待却不再执行；
- 任何时刻 RUNNING 总数不超过全局上限，且每个分组在跑数不超过其分组上限。
"""

from __future__ import annotations

import logging
import threading

from app.clock import Clock, SystemClock
from app.executor import Executor
from app.models import Task, TaskState, transit
from app.storage import TaskStore

logger = logging.getLogger("orchestration")

# 依赖不可能成功的上游状态：下游直接判失败。
UPSTREAM_BAD = frozenset({TaskState.FAILED, TaskState.CANCELLED})


class Scheduler:
    """单轮调度器。"""

    def __init__(
        self,
        store: TaskStore,
        clock: Clock | None = None,
        max_concurrency: int = 1,
        executor: Executor | None = None,
        group_limits: dict[str, int] | None = None,
    ) -> None:
        self.store = store
        self.clock = clock or SystemClock()
        self.max_concurrency = max(1, max_concurrency)
        self.executor = executor
        # 分组并发上限：group -> 最大同时在跑数；未配置的分组只受全局上限约束。
        self.group_limits: dict[str, int] = dict(group_limits or {})
        # 进程级派发串行化：即便多线程同时触发 run_once，也只能一轮轮进入。
        self._dispatch_lock = threading.Lock()

    def set_group_limit(self, group: str, limit: int) -> None:
        """设置/更新某个分组的并发上限（limit 必须 >= 1）。"""
        if limit < 1:
            raise ValueError(f"分组并发上限必须 >= 1: group={group} limit={limit}")
        self.group_limits[group] = limit
        logger.info("分组并发上限设置 group=%s limit=%d", group, limit)

    def run_once(self) -> list[str]:
        with self._dispatch_lock:
            with self.store.lock():
                now = self.clock.now_ms()
                self._reclaim_leases(now)
                self._propagate_failures(now)
                self._classify(now)
                self._release_retries(now)
                return self._dispatch(now)

    # ---------- 阶段 1：租约回收 ----------
    def _reclaim_leases(self, now: int) -> None:
        for task in self.store.all():
            if task.state is not TaskState.RUNNING:
                continue
            expire = task.lease_expire_at_ms
            if expire is None or now < expire:
                continue
            old_owner = task.lease_owner
            task.lease_owner = None
            task.lease_expire_at_ms = None
            transit(
                task, TaskState.LOST, now_ms=now,
                reason=f"lease_timeout owner={old_owner} expire_at={expire}",
            )
            logger.warning(
                "租约超时回收 task=%s owner=%s attempt=%d", task.id, old_owner, task.attempts
            )
            # attempts = 已执行次数；仍有重试额度才重新排队。
            remaining = task.max_retries - (task.attempts - 1)
            if remaining > 0:
                # 与执行失败一致：重试前需要等待退避时间，到点由 retry_release 阶段放行。
                wait_ms = task.retry_wait_ms()
                task.next_retry_at_ms = now + wait_ms
                if wait_ms <= 0:
                    transit(task, TaskState.READY, now_ms=now, reason="lease_lost_requeue")
                    logger.info("失联任务重新排队 task=%s remaining=%d", task.id, remaining)
                else:
                    logger.info(
                        "失联任务等待重试 task=%s remaining=%d wait_ms=%d next_retry_at=%d",
                        task.id, remaining, wait_ms, task.next_retry_at_ms,
                    )
            else:
                transit(
                    task, TaskState.FAILED, now_ms=now,
                    reason="retries_exhausted_after_lease_timeout",
                    failure_reason="lease_timeout",
                    failure_detail=f"executor {old_owner} lost, no retry budget",
                )
                logger.info("失联任务重试耗尽 -> FAILED task=%s", task.id)

    # ---------- 阶段 2：上游失败/取消的级联传播 ----------
    def _propagate_failures(self, now: int) -> None:
        # 级联可能多轮：新 FAILED 的任务也会影响其下游。
        while True:
            changed = False
            for task in self.store.all():
                if task.state in (
                    TaskState.PENDING, TaskState.BLOCKED, TaskState.READY,
                ):
                    bad_dep = self._first_bad_dependency(task)
                    if bad_dep is not None:
                        transit(
                            task, TaskState.FAILED, now_ms=now,
                            reason="upstream_failed",
                            failure_reason="upstream_failed",
                            failure_detail=f"dependency {bad_dep[0]} is {bad_dep[1]}",
                        )
                        logger.info(
                            "失败传播 task=%s -> FAILED 判定依据: 依赖 %s 状态=%s",
                            task.id, bad_dep[0], bad_dep[1],
                        )
                        changed = True
            if not changed:
                return

    def _first_bad_dependency(self, task: Task) -> tuple[str, str] | None:
        for dep_ref in sorted(task.dependencies):
            dep = self.store.resolve(dep_ref)
            if dep is None:
                # 依赖不存在视为不可满足。
                return (dep_ref, "MISSING")
            if dep.state in UPSTREAM_BAD:
                return (dep_ref, dep.state.value)
        return None

    # ---------- 阶段 3：就绪分类 ----------
    def _classify(self, now: int) -> None:
        for task in self.store.all():
            if task.state not in (TaskState.PENDING, TaskState.BLOCKED):
                continue
            if self._first_bad_dependency(task) is not None:
                continue  # 阶段 2 已在下一轮处理
            if all(
                (dep := self.store.resolve(dep_ref)) is not None
                and dep.state is TaskState.SUCCEEDED
                for dep_ref in task.dependencies
            ):
                transit(task, TaskState.READY, now_ms=now, reason="dependencies_satisfied")
                logger.info(
                    "依赖就绪 task=%s 判定依据: deps=%s 全部 SUCCEEDED",
                    task.id, sorted(task.dependencies),
                )
            elif task.state is TaskState.PENDING:
                transit(task, TaskState.BLOCKED, now_ms=now, reason="waiting_dependencies")

    # ---------- 阶段 4：重试退避放行 ----------
    def _release_retries(self, now: int) -> None:
        """到达 next_retry_at_ms 的等待重试任务回到 READY，本轮即可参与派发。

        覆盖两种等待形态：
        - FAILED_RETRYING：执行失败结算时写入 next_retry_at_ms；
        - LOST（仍有额度）：租约回收时写入 next_retry_at_ms，等退避到点。
        """
        for task in self.store.all():
            if task.state is TaskState.FAILED_RETRYING:
                due = task.next_retry_at_ms
            elif task.state is TaskState.LOST and task.next_retry_at_ms is not None:
                due = task.next_retry_at_ms
            else:
                continue
            if due is None or now < due:
                logger.info(
                    "重试未到点 task=%s state=%s next_retry_at=%s now=%d -> 本轮不调度",
                    task.id, task.state.value, due, now,
                )
                continue
            transit(task, TaskState.READY, now_ms=now, reason="retry_due")
            logger.info(
                "重试到点放行 task=%s attempt=%d next_retry_at=%s now=%d",
                task.id, task.attempts, due, now,
            )

    # ---------- 阶段 5：配额 + 优先级派发 ----------
    def _dispatch(self, now: int) -> list[str]:
        running = sum(
            1 for t in self.store.all() if t.state is TaskState.RUNNING
        )
        slots = self.max_concurrency - running
        if slots <= 0:
            logger.info("配额已满 running=%d limit=%d -> 本轮不派发", running, self.max_concurrency)
            return []

        # 各分组当前在跑数：分组上限与全局上限同时约束派发。
        group_running: dict[str, int] = {}
        for t in self.store.all():
            if t.state is TaskState.RUNNING:
                group_running[t.group] = group_running.get(t.group, 0) + 1

        ready = self.store.by_state(TaskState.READY)
        # 优先级高者先跑；同优先级按创建时间（再按 id 兜底）保证可复现顺序。
        # 分组之间公平地按这个统一顺序抢名额：分组满了只跳过该组任务，不阻塞其它分组。
        ready.sort(key=lambda t: (-t.priority, t.created_at_ms, t.id))

        dispatched: list[str] = []
        for task in ready:
            if slots <= 0:
                break
            group_limit = self.group_limits.get(task.group)
            if group_limit is not None and group_running.get(task.group, 0) >= group_limit:
                logger.info(
                    "分组配额已满 task=%s group=%s running=%d limit=%d -> 跳过该任务",
                    task.id, task.group, group_running.get(task.group, 0), group_limit,
                )
                continue
            transit(task, TaskState.RUNNING, now_ms=now, reason="scheduler_dispatch")
            owner = None
            if self.executor is not None:
                owner = self.executor.start(task.id)
            task.events.append(
                f"{now}: dispatch order_priority={task.priority} group={task.group} owner={owner}"
            )
            dispatched.append(task.id)
            slots -= 1
            group_running[task.group] = group_running.get(task.group, 0) + 1
            logger.info(
                "派发任务 task=%s priority=%d group=%s owner=%s 剩余全局配额=%d",
                task.id, task.priority, task.group, owner, slots,
            )
        return dispatched
