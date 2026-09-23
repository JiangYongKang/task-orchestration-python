"""端到端综合场景：多任务、依赖、配额、租约失联重试的完整闭环。"""

import logging

from app.clock import FakeClock
from app.models import TaskSpec, TaskState
from app.service import OrchestrationService

logger = logging.getLogger("test.integration")


def test_end_to_end_dag_with_failure_and_retry() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=2, clock=clock)

    # 输入 DAG:
    #   extract(优先级1) 与 extract2(优先级1) -> transform(优先级5, max_retries=1) -> load
    extract = svc.submit(TaskSpec(idempotency_key="extract", priority=1))
    extract2 = svc.submit(TaskSpec(idempotency_key="extract2", priority=1))
    transform = svc.submit(
        TaskSpec(
            idempotency_key="transform",
            priority=5,
            max_retries=1,
            lease_timeout_ms=100,
            dependencies=frozenset({"extract", "extract2"}),
        )
    )
    load = svc.submit(
        TaskSpec(idempotency_key="load", priority=9, dependencies=frozenset({"transform"}))
    )
    logger.info("输入: extract/extract2 -> transform(retry=1) -> load; 并发上限=2")

    # 第 1 轮：两个 extract 同时跑；transform BLOCKED；load 继续等
    d1 = svc.schedule_once()
    assert set(d1) == {extract.id, extract2.id}
    assert svc.get(transform.id).state is TaskState.BLOCKED
    logger.info("轮次1: 派发=%s, transform=BLOCKED", d1)

    # 两个上游成功
    for tid in d1:
        svc.succeed(tid, svc.get(tid).lease_owner)

    # 第 2 轮：transform 就绪（load 优先级虽高但依赖未满足，不参与）
    d2 = svc.schedule_once()
    assert d2 == [transform.id]
    assert svc.get(load.id).state is TaskState.BLOCKED
    logger.info("轮次2: 仅派发 transform；高优先级的 load 仍 BLOCKED（依赖未就绪）")

    # transform 执行者失联：租约超时后重调度，第 2 次执行成功
    clock.advance(101)
    d3 = svc.schedule_once()
    assert d3 == [transform.id]
    assert svc.get(transform.id).attempts == 2
    logger.info("轮次3: transform 失联后重调度 attempts=2")
    svc.succeed(transform.id, svc.get(transform.id).lease_owner)

    # 第 4 轮：load 就绪并成功
    d4 = svc.schedule_once()
    assert d4 == [load.id]
    svc.succeed(load.id, svc.get(load.id).lease_owner)

    final_states = {
        t.idempotency_key: (t.state.value, t.attempts, t.failure_reason)
        for t in svc.store.all()
    }
    logger.info("终态结果: %s", final_states)
    assert svc.get(load.id).state is TaskState.SUCCEEDED
    assert all(
        svc.get(t).state is TaskState.SUCCEEDED
        for t in (extract.id, extract2.id, transform.id, load.id)
    )
    # transform 经历了 2 次执行权授予，但只有 1 次成功结算
    assert svc.get(transform.id).attempts == 2
    assert len([e for e in svc.get(transform.id).events if "SUCCEEDED" in e]) == 1
