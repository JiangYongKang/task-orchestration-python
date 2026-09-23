"""调度器：按依赖就绪、优先级、并发配额决定谁获得执行权。

调度规则（每次 ``tick``）：

1. 只考察 ``PENDING`` 任务；终态与 ``RUNNING`` 任务不参与。
2. 就绪条件：所有直接依赖均为 ``COMPLETED``。
   * 有依赖仍在 PENDING/RUNNING → 等待（waiting_dependency）；
   * 有依赖为 FAILED/CANCELLED/BLOCKED → 该任务本应已被传播为 BLOCKED，
     若仍被考察到则记 blocked_by_upstream（防御性分支）。
3. 就绪任务按 (priority 降序, created_at 升序, id 升序) 排序，保证可复现。
4. 按剩余配额（max_concurrency - running 数）依次派发；
   配额不足时跳过（quota_full），且本轮不再派发后续任务（避免低优先级挤占）。
5. 未注册 handler 的就绪任务不会获得执行权（handler_not_registered）。

状态检查与派发在服务的同一把锁内完成，因此同一任务在并发 tick 下
至多获得一次执行权。
"""
from __future__ import annotations

import logging

from .errors import OrchestrationError
from .models import ScheduleDecision, Task, TaskState

logger = logging.getLogger("orchestration.scheduler")


class Scheduler:
    def __init__(self, service: "OrchestrationService") -> None:  # noqa: F821
        self.service = service

    def is_ready(self, task: Task) -> tuple[bool, str]:
        """返回 (是否就绪, 判定依据)。调用方应持有 store 锁。"""
        if task.state is not TaskState.PENDING:
            return False, f"not_pending({task.state.value})"
        if not task.dependencies:
            return True, "no_dependencies"
        pending_deps: list[str] = []
        blocking_deps: list[str] = []
        for dep_id in sorted(task.dependencies):
            dep = self.service.store.get(dep_id)
            if dep is None:
                return False, f"unknown_dependency({dep_id})"
            if dep.state is TaskState.COMPLETED:
                continue
            if dep.state in (TaskState.PENDING, TaskState.RUNNING):
                pending_deps.append(f"{dep_id}:{dep.state.value}")
            else:
                blocking_deps.append(f"{dep_id}:{dep.state.value}")
        if blocking_deps:
            return False, "blocked_by_upstream[" + ",".join(blocking_deps) + "]"
        if pending_deps:
            return False, "waiting_dependency[" + ",".join(pending_deps) + "]"
        return True, "all_dependencies_completed"

    def tick(self) -> list[ScheduleDecision]:
        decisions: list[ScheduleDecision] = []
        with self.service.store.lock:
            running = len(
                self.service.store.tasks_in_state(TaskState.RUNNING)
            )
            capacity = self.service.max_concurrency - running
            if capacity <= 0:
                logger.info(
                    "scheduler tick: quota_full running=%d max=%d (no task dispatched)",
                    running, self.service.max_concurrency,
                )

            candidates = [
                t for t in self.service.store.tasks_in_state(TaskState.PENDING)
            ]
            # 可复现的确定性排序：优先级高者优先；同级先到先得
            candidates.sort(
                key=lambda t: (-t.priority, t.created_at, t.id)
            )

            quota_closed = capacity <= 0
            for task in candidates:
                ready, why = self.is_ready(task)
                if not ready:
                    decision = ScheduleDecision(
                        task.id, False, "not_ready", why
                    )
                    decisions.append(decision)
                    logger.info(
                        "scheduler decision task=%s dispatched=False reason=not_ready detail=%s",
                        task.id, why,
                    )
                    continue

                if not self.service.executor.has_handler(task.handler):
                    decision = ScheduleDecision(
                        task.id,
                        False,
                        "handler_not_registered",
                        f"handler {task.handler!r} is not registered",
                    )
                    decisions.append(decision)
                    logger.info(
                        "scheduler decision task=%s dispatched=False reason=handler_not_registered",
                        task.id,
                    )
                    continue

                if quota_closed:
                    decision = ScheduleDecision(
                        task.id,
                        False,
                        "quota_full",
                        f"running={running} max_concurrency={self.service.max_concurrency}",
                    )
                    decisions.append(decision)
                    logger.info(
                        "scheduler decision task=%s dispatched=False reason=quota_full detail=running=%d/max=%d",
                        task.id, running, self.service.max_concurrency,
                    )
                    continue

                # 派发：状态跃迁与执行权发放在锁内一次完成
                try:
                    token = self.service.dispatch(task)
                except OrchestrationError as exc:
                    decision = ScheduleDecision(
                        task.id, False, exc.code, exc.reason
                    )
                    decisions.append(decision)
                    logger.error(
                        "scheduler decision task=%s dispatched=False reason=%s detail=%s",
                        task.id, exc.code, exc.reason,
                    )
                    continue

                capacity -= 1
                if capacity <= 0:
                    quota_closed = True
                decision = ScheduleDecision(
                    task.id,
                    True,
                    "dispatched",
                    f"priority={task.priority} lease={token} remaining_capacity={capacity}",
                )
                decisions.append(decision)
                logger.info(
                    "scheduler decision task=%s dispatched=True reason=dispatched detail=%s",
                    task.id, decision.detail,
                )
        return decisions
