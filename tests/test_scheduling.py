"""调度规则测试：依赖就绪、优先级、并发配额、单次执行权。"""
from __future__ import annotations

import threading
import time

from orchestration.models import ScheduleDecision, TaskSpec, TaskState
from tests.conftest import log_case


def test_priority_order_is_deterministic(make_service):
    service = make_service(max_concurrency=1)
    order: list[str] = []
    gate = threading.Event()

    def handler(payload, ctx):
        order.append(payload["name"])
        gate.wait(2)
        return payload["name"]

    service.executor.register("h", handler)

    low = service.submit(TaskSpec(handler="h", payload={"name": "low"}, priority=0)).task
    high = service.submit(TaskSpec(handler="h", payload={"name": "high"}, priority=10)).task
    mid = service.submit(TaskSpec(handler="h", payload={"name": "mid"}, priority=5)).task

    log_case(
        "priority_ordering",
        max_concurrency=1,
        inputs=[
            ("low", low.id, 0), ("high", high.id, 10), ("mid", mid.id, 5)
        ],
        expectation="dispatch order high -> mid -> low",
    )
    decisions = service.schedule_tick()
    first = [d for d in decisions if d.dispatched]
    assert len(first) == 1
    log_case("priority_ordering_first", dispatched=first[0].task_id, basis=first[0].detail)
    assert first[0].task_id == high.id

    # 释放 high，继续 drain
    gate.set()
    seen = [high.id]
    for expected in (mid.id, low.id):
        for _ in range(200):
            service.schedule_tick()
            if service.get(expected).state in (TaskState.RUNNING, TaskState.COMPLETED):
                break
            time.sleep(0.01)
        seen.append(expected)
    log_case("priority_ordering_verdict", observed_order=seen)
    assert seen == [high.id, mid.id, low.id]


def test_quota_never_exceeded(make_service):
    service = make_service(max_concurrency=2)
    gate = threading.Event()
    service.executor.register("h", lambda p, ctx: gate.wait(2))
    tasks = [
        service.submit(TaskSpec(handler="h", payload={"i": i})).task
        for i in range(6)
    ]
    decisions = service.schedule_tick()
    dispatched = [d.task_id for d in decisions if d.dispatched]
    quota_full = [d for d in decisions if d.reason == "quota_full"]
    log_case(
        "quota_limit",
        max_concurrency=2,
        pending=6,
        dispatched=dispatched,
        running=service.running_count(),
        quota_full_decisions=[d.task_id for d in quota_full],
    )
    assert len(dispatched) == 2
    assert service.running_count() == 2
    assert len(quota_full) == 4

    # 再 tick 也不会突破配额
    decisions2 = service.schedule_tick()
    assert all(not d.dispatched for d in decisions2)
    assert service.running_count() == 2
    gate.set()


def test_dependency_gating_and_ready_reasons(make_service):
    service = make_service()
    gate = threading.Event()
    service.executor.register("h", lambda p, ctx: gate.wait(2))
    a = service.submit(TaskSpec(handler="h", payload={"name": "a"})).task
    b = service.submit(TaskSpec(handler="h", payload={"name": "b"}, dependencies=(a.id,))).task

    decisions = service.schedule_tick()
    by_id = {d.task_id: d for d in decisions}
    log_case(
        "dependency_waiting",
        a=a.id, b=b.id,
        decision_a=(by_id[a.id].dispatched, by_id[a.id].reason, by_id[a.id].detail),
        decision_b=(by_id[b.id].dispatched, by_id[b.id].reason, by_id[b.id].detail),
    )
    assert by_id[a.id].dispatched
    assert not by_id[b.id].dispatched
    assert by_id[b.id].reason == "not_ready"
    assert "waiting_dependency" in by_id[b.id].detail
    gate.set()


def test_concurrent_ticks_grant_execution_once(make_service):
    """并发调度 tick：同一任务只能获得一次执行权、只产生一条 attempt。"""
    from collections import Counter
    from concurrent.futures import ThreadPoolExecutor

    service = make_service(max_concurrency=4)
    starts = threading.Event()

    def handler(payload, ctx):
        starts.wait(2)  # 阻塞到所有 tick 结束，使配额始终占满
        return "ok"

    service.executor.register("h", handler)
    tasks = [service.submit(TaskSpec(handler="h", payload={"i": i})).task for i in range(12)]

    log_case(
        "concurrent_ticks",
        threads=8, tasks=len(tasks), max_concurrency=4,
        expectation="at most one grant per task; running never exceeds quota",
    )

    def tick(_):
        return service.schedule_tick()

    with ThreadPoolExecutor(max_workers=8) as pool:
        all_decisions: list[ScheduleDecision] = sum(
            pool.map(tick, range(8)), []
        )

    grants = Counter(d.task_id for d in all_decisions if d.dispatched)
    running_tasks = service.store.tasks_in_state(TaskState.RUNNING)
    log_case(
        "concurrent_ticks_verdict",
        grant_counts=dict(grants),
        running_count=len(running_tasks),
        max_attempts=max(len(service.get(t.id).attempts) for t in tasks),
    )
    # 核心不变量：每个任务最多被派发一次，在跑任务数不超过配额
    assert all(count == 1 for count in grants.values())
    assert len(running_tasks) <= 4
    assert len(running_tasks) == 4  # 处理器阻塞，应恰好占满配额
    for t in running_tasks:
        assert len(t.attempts) == 1
    starts.set()


def test_unregistered_handler_not_dispatched(make_service):
    service = make_service()
    t = service.submit(TaskSpec(handler="missing")).task
    decisions = service.schedule_tick()
    d = next(x for x in decisions if x.task_id == t.id)
    log_case(
        "missing_handler",
        task=t.id, handler="missing",
        decision=(d.reason, d.detail),
        state=service.get(t.id).state.value,
    )
    assert d.reason == "handler_not_registered"
    assert service.get(t.id).state is TaskState.PENDING
