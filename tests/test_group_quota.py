"""分组配额测试：分组并发上限、全局上限兜底、分组间按优先级公平抢名额。"""

import logging

from app.clock import FakeClock
from app.models import TaskSpec, TaskState
from app.service import OrchestrationService

logger = logging.getLogger("test.group_quota")


def _running_count(svc: OrchestrationService, group: str | None = None) -> int:
    return sum(
        1
        for t in svc.store.all()
        if t.state is TaskState.RUNNING and (group is None or t.group == group)
    )


def test_group_limit_caps_single_group_but_not_others() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(
        max_concurrency=10, clock=clock, group_limits={"search": 1}
    )
    # 输入: search 组 3 个任务（组上限 1），ads 组 2 个任务（无组上限）
    for i in range(3):
        svc.submit(TaskSpec(idempotency_key=f"s{i}", group="search", priority=5))
    for i in range(2):
        svc.submit(TaskSpec(idempotency_key=f"a{i}", group="ads", priority=1))

    dispatched = svc.schedule_once()
    logger.info(
        "第1轮派发=%s search在跑=%d(上限1) ads在跑=%d 全局在跑=%d(上限10)",
        dispatched,
        _running_count(svc, "search"),
        _running_count(svc, "ads"),
        _running_count(svc),
    )
    # 判定依据: search 组最多 1 个在跑；ads 组不受 search 组配额挤占
    assert _running_count(svc, "search") == 1
    assert _running_count(svc, "ads") == 2
    assert len(dispatched) == 3

    # search 组配额被占满时，其余 search 任务继续等待
    assert svc.schedule_once() == []
    logger.info("第2轮: search 组配额满，无新派发")

    # search 在跑任务成功后，同组下一个补位
    running_search = [
        t for t in svc.store.all()
        if t.group == "search" and t.state is TaskState.RUNNING
    ][0]
    svc.succeed(running_search.id, running_search.lease_owner)
    dispatched = svc.schedule_once()
    assert len(dispatched) == 1
    assert svc.get(dispatched[0]).group == "search"
    logger.info("search 任务完成后同组补位派发=%s", dispatched)


def test_global_limit_caps_total_across_groups() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(
        max_concurrency=2, clock=clock, group_limits={"g1": 5, "g2": 5}
    )
    # 输入: 两个组各 2 个任务，组上限都很宽，但全局上限只有 2
    for i in range(2):
        svc.submit(TaskSpec(idempotency_key=f"g1-{i}", group="g1", priority=5))
        svc.submit(TaskSpec(idempotency_key=f"g2-{i}", group="g2", priority=5))

    svc.schedule_once()
    logger.info(
        "全局在跑=%d (全局上限=2, 组上限=5/5)", _running_count(svc)
    )
    # 判定依据: 任何时刻全局在跑数绝不超过全局上限
    assert _running_count(svc) == 2
    assert svc.schedule_once() == []


def test_groups_compete_by_priority_fairly() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(
        max_concurrency=2, clock=clock, group_limits={"lowg": 2, "highg": 2}
    )
    # 输入: lowg 组低优先级任务先提交，highg 组高优先级任务后提交
    svc.submit(TaskSpec(idempotency_key="low1", group="lowg", priority=1))
    svc.submit(TaskSpec(idempotency_key="low2", group="lowg", priority=1))
    svc.submit(TaskSpec(idempotency_key="high1", group="highg", priority=9))
    svc.submit(TaskSpec(idempotency_key="high2", group="highg", priority=8))

    dispatched = svc.schedule_once()
    priorities = sorted(svc.get(tid).priority for tid in dispatched)
    logger.info(
        "输入: lowg(优先级1,1) 与 highg(优先级9,8) 抢 2 个全局名额 -> 派发优先级=%s",
        priorities,
    )
    # 判定依据: 分组之间按优先级公平竞争全局名额
    assert priorities == [8, 9]
    assert _running_count(svc, "highg") == 2
    assert _running_count(svc, "lowg") == 0


def test_group_limit_releases_slot_on_cancel() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(
        max_concurrency=5, clock=clock, group_limits={"batch": 1}
    )
    # 显式拉开优先级，保证 t1 先拿到组内唯一名额（与提交顺序无关）
    t1 = svc.submit(TaskSpec(idempotency_key="b1", group="batch", priority=1))
    t2 = svc.submit(TaskSpec(idempotency_key="b2", group="batch", priority=0))
    svc.schedule_once()
    assert svc.get(t1.id).state is TaskState.RUNNING
    assert svc.get(t2.id).state is TaskState.READY

    svc.cancel(t1.id)
    dispatched = svc.schedule_once()
    logger.info(
        "取消 batch 组在跑任务后: t1=%s 下一轮派发=%s",
        svc.get(t1.id).state.value, dispatched,
    )
    # 判定依据: 取消同时释放全局名额与分组名额
    assert dispatched == [t2.id]
    assert _running_count(svc, "batch") == 1


def test_unconfigured_group_only_bounded_by_global_limit() -> None:
    clock = FakeClock(0)
    svc = OrchestrationService(max_concurrency=3, clock=clock)
    for i in range(5):
        svc.submit(TaskSpec(idempotency_key=f"d{i}"))  # 默认 default 组，无组上限

    svc.schedule_once()
    logger.info("未配置组上限的 default 组: 在跑=%d (仅受全局上限3约束)", _running_count(svc))
    # 判定依据: 未配置的分组只受全局上限约束
    assert _running_count(svc) == 3
