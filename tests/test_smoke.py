"""骨架冒烟测试：仅验证包可导入、模型可构造。"""


def test_skeleton_imports() -> None:
    from app.models import Task, TaskSpec, TaskState
    from app.service import OrchestrationService

    spec = TaskSpec(idempotency_key="k1")
    task = Task(
        id=Task.new_id(),
        idempotency_key=spec.idempotency_key,
        payload=spec.payload,
        priority=spec.priority,
        dependencies=spec.dependencies,
        max_retries=spec.max_retries,
        lease_timeout_ms=spec.lease_timeout_ms,
        state=TaskState.PENDING,
    )
    assert task.state == TaskState.PENDING
    assert OrchestrationService is not None
