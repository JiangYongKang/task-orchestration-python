"""调度器测试：优先级、配额、依赖、失败传播、租约超时重调度。"""

import logging
import threading

from app.clock import FakeClock
from app.models import TaskSpec, TaskState
from app.service import OrchestrationService

logger = logging.getLogger("test.scheduler")


def test_priority_order_and_concurrency_cap() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=2, clock=clock)
    # 输入: 3 个无依赖任务，优先级 1/5/3
    svc.submit(TaskSpec(idempotency_key="low", priority=1))
    svc.submit(TaskSpec(idempotency_key="high", priority=5))
    svc.submit(TaskSpec(idempotency_key="mid", priority=3))

    first = svc.schedule_once()
    running = [svc.get(t) for t in first]
    logger.info("第1轮派发 ids=%s 优先级=%s (上限2)", first, [t.priority for t in running])
    assert len(first) == 2
    assert running[0].priority == 5 and running[1].priority == 3
    # 判定依据: 实际在跑数量绝不超过上限
    assert sum(t.state is TaskState.RUNNING for t in svc.store.all()) == 2

    second = svc.schedule_once()
    assert second == []  # 配额已满
    logger.info("第2轮派发=%s -> 配额已满不空跑", second)


def test_dependency_blocks_then_readies_downstream() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=5, clock=clock)
    a = svc.submit(TaskSpec(idempotency_key="A"))
    b = svc.submit(TaskSpec(idempotency_key="B", dependencies=frozenset({"A"})))

    svc.schedule_once()
    assert svc.get(a.id).state is TaskState.RUNNING
    assert svc.get(b.id).state is TaskState.BLOCKED
    logger.info("输入 B 依赖 A: A=RUNNING 时 B=%s", svc.get(b.id).state.value)

    # 直接按 key 取不到 A 运行 owner，改用事件里的租约；这里从任务读 lease_owner
    owner = svc.get(a.id).lease_owner
    svc.succeed(a.id, owner)
    svc.schedule_once()
    assert svc.get(b.id).state is TaskState.RUNNING
    logger.info("A 成功后下一轮 B=RUNNING，判定依据: deps 全部 SUCCEEDED")


def test_dependency_failure_propagates_downstream() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=5, clock=clock)
    a = svc.submit(TaskSpec(idempotency_key="A", max_retries=0))
    b = svc.submit(TaskSpec(idempotency_key="B", dependencies=frozenset({"A"})))
    c = svc.submit(TaskSpec(idempotency_key="C", dependencies=frozenset({"B"})))
    logger.info("输入: 链 A -> B -> C，A 无重试额度")

    svc.schedule_once()
    owner = svc.get(a.id).lease_owner
    svc.fail(a.id, owner, "fatal")
    assert svc.get(a.id).state is TaskState.FAILED

    svc.schedule_once()
    # 判定依据: B、C 必须被判为不可执行 FAILED，而不是继续排队
    assert svc.get(b.id).state is TaskState.FAILED
    assert svc.get(c.id).state is TaskState.FAILED
    assert svc.get(c.id).failure_reason == "upstream_failed"
    logger.info(
        "失败传播结果: B=%s(reason=%s) C=%s(reason=%s detail=%s)",
        svc.get(b.id).state.value, svc.get(b.id).failure_reason,
        svc.get(c.id).state.value, svc.get(c.id).failure_reason,
        svc.get(c.id).failure_detail,
    )


def test_cancelled_dependency_marks_downstream_failed() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=5, clock=clock)
    a = svc.submit(TaskSpec(idempotency_key="A"))
    b = svc.submit(TaskSpec(idempotency_key="B", dependencies=frozenset({"A"})))
    svc.cancel(a.id)
    svc.schedule_once()
    assert svc.get(b.id).state is TaskState.FAILED
    assert svc.get(b.id).failure_reason == "upstream_failed"
    logger.info("上游取消: B=%s reason=%s", svc.get(b.id).state.value,
                svc.get(b.id).failure_reason)


def test_lease_timeout_reschedules_then_succeeds() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t = svc.submit(TaskSpec(idempotency_key="T", max_retries=2, lease_timeout_ms=100))
    svc.schedule_once()
    zombie = svc.get(t.id).lease_owner
    assert zombie is not None
    assert svc.get(t.id).attempts == 1

    # 租约超时前不回收
    clock.advance(99)
    assert svc.schedule_once() == []
    assert svc.get(t.id).state is TaskState.RUNNING

    # 超时 -> 重新排队 -> 再次被派发（attempts=2，无重复结算）
    clock.advance(2)
    redispatched = svc.schedule_once()
    assert redispatched == [t.id]
    assert svc.get(t.id).state is TaskState.RUNNING
    assert svc.get(t.id).attempts == 2
    new_owner = svc.get(t.id).lease_owner
    assert new_owner != zombie
    logger.info("租约超时重调度 task=%s attempts 1->2 owner 轮换", t.id)

    # 失联执行者迟到结算无效，新执行者成功结算
    svc.succeed(t.id, new_owner)
    assert svc.get(t.id).state is TaskState.SUCCEEDED


def test_lease_timeout_without_retry_budget_fails() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=1, clock=clock)
    t = svc.submit(TaskSpec(idempotency_key="T", max_retries=0, lease_timeout_ms=10))
    svc.schedule_once()
    clock.advance(11)
    svc.schedule_once()
    assert svc.get(t.id).state is TaskState.FAILED
    assert svc.get(t.id).failure_reason == "lease_timeout"
    logger.info("无重试额度的失联任务 -> FAILED reason=lease_timeout")


def test_concurrent_scheduling_dispatches_each_task_once() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=100, clock=clock)
    for i in range(20):
        svc.submit(TaskSpec(idempotency_key=f"k{i}"))

    dispatch_sets: list[list[str]] = []
    barrier = threading.Barrier(6)

    def loop() -> None:
        barrier.wait()
        for _ in range(10):
            dispatch_sets.append(svc.schedule_once())

    threads = [threading.Thread(target=loop) for _ in range(6)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    all_dispatched = [tid for batch in dispatch_sets for tid in batch]
    logger.info("输入: 20 任务 / 6 线程并发调度; 派发序列长度=%d", len(all_dispatched))
    # 判定依据: 同一任务只能获得一次执行权
    assert len(all_dispatched) == len(set(all_dispatched)) == 20
    assert all(svc.get(tid).attempts == 1 for tid in all_dispatched)
