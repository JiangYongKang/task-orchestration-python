"""并发提交与幂等键收敛测试。"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from orchestration.errors import ErrorCode, OrchestrationError
from orchestration.models import TaskSpec
from tests.conftest import log_case


def test_concurrent_same_idempotency_key_collapses_to_one(make_service):
    service = make_service()
    service.executor.register("echo", lambda p, ctx: p)

    def submit(i):
        return service.submit(
            TaskSpec(
                handler="echo",
                payload={"i": i},
                idempotency_key="shared-key",
                priority=i % 3,
            )
        )

    log_case(
        "concurrent_idempotent_submit",
        threads=40,
        idempotency_key="shared-key",
        expectation="exactly 1 task created; all results share one id",
    )
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(submit, range(40)))

    ids = {r.task.id for r in results}
    created_count = sum(1 for r in results if r.created)

    log_case(
        "concurrent_idempotent_submit_verdict",
        distinct_task_ids=sorted(ids),
        created_count=created_count,
        total_submissions=len(results),
        stored_tasks=len(service.store.all_tasks()),
    )
    assert ids == {"task-000001"}
    assert created_count == 1
    assert len(service.store.all_tasks()) == 1


def test_distinct_keys_create_distinct_tasks(make_service):
    service = make_service()
    service.executor.register("echo", lambda p, ctx: p)
    r1 = service.submit(TaskSpec(handler="echo", idempotency_key="a"))
    r2 = service.submit(TaskSpec(handler="echo", idempotency_key="b"))
    log_case("distinct_keys", key_a_task=r1.task.id, key_b_task=r2.task.id)
    assert r1.created and r2.created
    assert r1.task.id != r2.task.id


def test_auto_idempotency_when_key_omitted(make_service):
    service = make_service()
    service.executor.register("echo", lambda p, ctx: p)
    r1 = service.submit(TaskSpec(handler="echo"))
    r2 = service.submit(TaskSpec(handler="echo"))
    assert r1.task.id != r2.task.id
    assert r1.task.idempotency_key == f"auto:{r1.task.id}"


def test_unknown_dependency_rejected(make_service):
    service = make_service()
    log_case(
        "unknown_dependency",
        input_dependencies=["does-not-exist"],
        expectation="rejected with unknown_dependency before creation",
    )
    with pytest.raises(OrchestrationError) as ei:
        service.submit(TaskSpec(handler="echo", dependencies=("does-not-exist",)))
    log_case("unknown_dependency_verdict", code=ei.value.code, reason=ei.value.reason)
    assert ei.value.code == ErrorCode.UNKNOWN_DEPENDENCY


def test_invalid_spec_rejected(make_service):
    service = make_service()
    with pytest.raises(OrchestrationError) as ei:
        service.submit(TaskSpec(handler=""))
    assert ei.value.code == ErrorCode.INVALID_PAYLOAD
    with pytest.raises(OrchestrationError) as ei:
        service.submit(TaskSpec(handler="x", max_retries=-1))
    assert ei.value.code == ErrorCode.INVALID_PAYLOAD


def test_get_unknown_task(make_service):
    service = make_service()
    with pytest.raises(OrchestrationError) as ei:
        service.get("nope")
    assert ei.value.code == ErrorCode.TASK_NOT_FOUND
