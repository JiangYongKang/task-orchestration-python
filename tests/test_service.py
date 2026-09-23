"""服务层测试：并发提交幂等收敛、取消的可区分原因。"""

import logging
import threading

import pytest

from app.clock import FakeClock
from app.errors import InvalidStateTransitionError, TaskNotFoundError
from app.models import TaskSpec, TaskState
from app.service import OrchestrationService

logger = logging.getLogger("test.service")


def test_concurrent_submit_same_idempotency_key_converges() -> None:
    svc = OrchestrationService(clock=FakeClock())
    spec = TaskSpec(idempotency_key="job-1", payload="p", priority=5)
    logger.info("输入: 8 线程并发提交同一 spec key=%s priority=%d", spec.idempotency_key, spec.priority)

    results: list = []
    barrier = threading.Barrier(8)

    def worker() -> None:
        barrier.wait()
        results.append(svc.submit(spec))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ids = {t.id for t in results}
    logger.info("判定依据: 产生任务 id 集合=%s (期望大小 1)", ids)
    assert len(ids) == 1
    task = svc.get(next(iter(ids)))
    assert task.submit_count == 8
    assert len(svc.store.all()) == 1


def test_submit_distinct_keys_create_distinct_tasks() -> None:
    svc = OrchestrationService(clock=FakeClock())
    t1 = svc.submit(TaskSpec(idempotency_key="a"))
    t2 = svc.submit(TaskSpec(idempotency_key="b"))
    logger.info("输入: 不同 key a/b -> ids=%s,%s", t1.id, t2.id)
    assert t1.id != t2.id


def test_get_unknown_task_raises() -> None:
    svc = OrchestrationService()
    with pytest.raises(TaskNotFoundError):
        svc.get("nope")


def test_cancel_pending_succeeds() -> None:
    svc = OrchestrationService(clock=FakeClock())
    task = svc.submit(TaskSpec(idempotency_key="c1"))
    cancelled = svc.cancel(task.id)
    assert cancelled.state is TaskState.CANCELLED


def test_cancel_terminal_states_have_distinct_reasons() -> None:
    svc = OrchestrationService(clock=FakeClock())

    cancelled = svc.submit(TaskSpec(idempotency_key="c"))
    svc.cancel(cancelled.id)
    with pytest.raises(InvalidStateTransitionError) as ei:
        svc.cancel(cancelled.id)
    logger.info("已取消任务再次取消 -> reason=%s", ei.value.reason)
    assert ei.value.reason == "already_cancelled"

    succeeded = svc.submit(TaskSpec(idempotency_key="s"))
    # 手动构造成功终态以验证可区分原因
    succeeded.state = TaskState.SUCCEEDED
    with pytest.raises(InvalidStateTransitionError) as ei:
        svc.cancel(succeeded.id)
    logger.info("已成功任务被取消 -> reason=%s", ei.value.reason)
    assert ei.value.reason == "already_succeeded"

    failed = svc.submit(TaskSpec(idempotency_key="f"))
    failed.state = TaskState.FAILED
    failed.failure_reason = "retries_exhausted"
    with pytest.raises(InvalidStateTransitionError) as ei:
        svc.cancel(failed.id)
    logger.info("已失败任务被取消 -> reason=%s", ei.value.reason)
    assert ei.value.reason == "already_failed"


def test_cancel_unknown_task_raises_not_found() -> None:
    svc = OrchestrationService()
    with pytest.raises(TaskNotFoundError):
        svc.cancel("ghost")
