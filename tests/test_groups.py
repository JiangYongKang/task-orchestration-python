"""分组并发配额：分组上限与全局上限同时生效，分组间按优先级公平抢名额。"""

import logging

from app.clock import FakeClock
from app.models import TaskSpec, TaskState
from app.service import OrchestrationService

logger = logging.getLogger("test.groups")


def _running_by_group(svc: OrchestrationService) -> dict[str, int]:
    counts: dict[str, int] = {}
    for t in svc.store.all():
        if t.state is TaskState.RUNNING:
            counts[t.group] = counts.get(t.group, 0) + 1
    return counts


def test_group_limit_caps_one_group_without_blocking_others() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(
        max_concurrency=10, clock=clock, group_limits={"A": 1}
    )
    a1 = svc.submit(TaskSpec(idempotency_key="a1", group="A", priority=5))
    a2 = svc.submit(TaskSpec(idempotency_key="a2", group="A", priority=4))
    b1 = svc.submit(TaskSpec(idempotency_key="b1", group="B", priority=3))
    logger.info("输入: 全局上限=10, 分组 A 上限=1; A 有 a1(p5)/a2(p4), B 有 b1(p3)")

    dispatched = svc.schedule_once()
    logger.info("第1轮派发=%s 分组在跑=%s", dispatched, _running_by_group(svc))
    # 判定依据: A 组最多 1 个在跑；B 组不受 A 组配额影响
    assert set(dispatched) == {a1.id, b1.id}
    assert svc.get(a2.id).state is TaskState.READY
    assert _running_by_group(svc) == {"A": 1, "B": 1}

    # A 组名额释放后，a2 立即补上
    svc.succeed(a1.id, svc.get(a1.id).lease_owner)
    assert svc.schedule_once() == [a2.id]
    assert _running_by_group(svc) == {"A": 1, "B": 1}
    logger.info("a1 成功后 a2 补位, 分组在跑=%s", _running_by_group(svc))


def test_global_cap_still_enforced_across_groups() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(
        max_concurrency=2, clock=clock, group_limits={"A": 5, "B": 5}
    )
    tasks = [
        svc.submit(TaskSpec(idempotency_key=f"a{i}", group="A")) for i in range(2)
    ] + [
        svc.submit(TaskSpec(idempotency_key=f"b{i}", group="B")) for i in range(2)
    ]
    logger.info("输入: 全局上限=2, 分组上限各=5, A/B 各 2 个任务")

    svc.schedule_once()
    running_total = sum(_running_by_group(svc).values())
    logger.info("派发后在跑总数=%d 分组分布=%s", running_total, _running_by_group(svc))
    # 判定依据: 任何时刻全局在跑数不超上限，即使各分组额度还有富余
    assert running_total == 2
    assert svc.schedule_once() == []  # 全局满了就不再派发


def test_groups_compete_fairly_by_priority() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(
        max_concurrency=2, clock=clock, group_limits={"A": 1, "B": 1}
    )
    a_low = svc.submit(TaskSpec(idempotency_key="a_low", group="A", priority=1))
    a_high = svc.submit(TaskSpec(idempotency_key="a_high", group="A", priority=9))
    b_mid = svc.submit(TaskSpec(idempotency_key="b_mid", group="B", priority=5))
    logger.info("输入: 各组上限=1; A: p9/p1, B: p5")

    dispatched = svc.schedule_once()
    # 判定依据: 全局统一按优先级抢名额 —— A 组 p9 与 B 组 p5 先跑
    assert set(dispatched) == {a_high.id, b_mid.id}
    assert svc.get(a_low.id).state is TaskState.READY
    logger.info("第1轮派发=%s (a_high 与 b_mid)", dispatched)

    # a_high 完成后，A 组空出名额，剩下的 a_low(p1) 才能跑
    svc.succeed(a_high.id, svc.get(a_high.id).lease_owner)
    assert svc.schedule_once() == [a_low.id]
    logger.info("a_high 完成后 a_low 补位")


def test_unconfigured_group_only_bounded_by_global_cap() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=3, clock=clock)
    svc.set_group_limit("A", 1)
    for i in range(3):
        svc.submit(TaskSpec(idempotency_key=f"x{i}", group="X"))
    svc.submit(TaskSpec(idempotency_key="a0", group="A", priority=10))
    svc.submit(TaskSpec(idempotency_key="a1", group="A", priority=9))
    logger.info("输入: 全局=3, 仅 A 限 1; X 组 3 个任务未配置分组上限")

    svc.schedule_once()
    counts = _running_by_group(svc)
    logger.info("派发后分组在跑=%s", counts)
    # 判定依据: 未配置分组只受全局上限约束；A 组仍被自己的上限卡住
    assert counts.get("X", 0) == 2  # a0(p10) 先抢 1 个全局名额，X 拿剩下 2 个
    assert counts.get("A", 0) == 1
    assert sum(counts.values()) == 3
