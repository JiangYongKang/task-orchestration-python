"""依赖失败 / 取消传播测试：下游必须判定为不可执行（BLOCKED）而非继续排队。"""
from __future__ import annotations

from orchestration.models import TaskSpec, TaskState, TerminalReason
from tests.conftest import log_case, wait_terminal


def test_failure_propagates_through_chain_and_diamond(make_service):
    service = make_service()
    service.executor.register(
        "boom", lambda p, ctx: (_ for _ in ()).throw(RuntimeError("disk full"))
    )
    service.executor.register("echo", lambda p, ctx: p)

    root = service.submit(TaskSpec(handler="boom", max_retries=1)).task
    mid1 = service.submit(TaskSpec(handler="echo", dependencies=(root.id,))).task
    mid2 = service.submit(TaskSpec(handler="echo", dependencies=(root.id,))).task
    leaf = service.submit(
        TaskSpec(handler="echo", dependencies=(mid1.id, mid2.id))
    ).task
    isolated = service.submit(TaskSpec(handler="echo")).task

    log_case(
        "failure_diamond",
        graph={"root(boom)": root.id, "mid1": mid1.id, "mid2": mid2.id,
               "leaf": leaf.id, "isolated": isolated.id},
        expectation="root fails -> mid1/mid2/leaf BLOCKED; isolated COMPLETED",
    )
    for t in (root, mid1, mid2, leaf, isolated):
        wait_terminal(service, t.id)

    states = {name: service.get(t.id).state for name, t in [
        ("root", root), ("mid1", mid1), ("mid2", mid2),
        ("leaf", leaf), ("isolated", isolated),
    ]}
    reasons = {name: service.get(t.id).terminal.reason for name, t in [
        ("mid1", mid1), ("mid2", mid2), ("leaf", leaf),
    ]}
    log_case(
        "failure_diamond_verdict",
        states={k: v.value for k, v in states.items()},
        reasons={k: v.value for k, v in reasons.items()},
        leaf_upstream=service.get(leaf.id).terminal.upstream_task_id,
    )

    assert states["root"] is TaskState.FAILED
    assert states["mid1"] is TaskState.BLOCKED
    assert states["mid2"] is TaskState.BLOCKED
    assert states["leaf"] is TaskState.BLOCKED
    assert states["isolated"] is TaskState.COMPLETED
    assert reasons["mid1"] is TerminalReason.UPSTREAM_FAILED
    assert reasons["leaf"] is TerminalReason.UPSTREAM_FAILED
    # 追溯链：leaf 的直接上游之一
    assert service.get(leaf.id).terminal.upstream_task_id in {mid1.id, mid2.id}


def test_blocked_task_never_dispatched(make_service):
    service = make_service()
    service.executor.register("boom", lambda p, ctx: (_ for _ in ()).throw(RuntimeError("x")))
    calls = []
    service.executor.register("echo", lambda p, ctx: calls.append(p) or p)

    root = service.submit(TaskSpec(handler="boom", max_retries=0)).task
    child = service.submit(TaskSpec(handler="echo", dependencies=(root.id,))).task

    wait_terminal(service, root.id)
    wait_terminal(service, child.id)
    # 再 tick 多轮，BLOCKED 任务也绝不可能被派发
    decisions = service.schedule_tick() + service.schedule_tick()
    log_case(
        "blocked_never_runs",
        child=child.id,
        child_state=service.get(child.id).state.value,
        handler_invocations=len(calls),
        post_block_decisions=[(d.task_id, d.reason) for d in decisions],
    )
    assert calls == []
    assert service.get(child.id).state is TaskState.BLOCKED


def test_cancel_propagates_downstream(make_service):
    service = make_service()
    service.executor.register("echo", lambda p, ctx: p)
    parent = service.submit(TaskSpec(handler="echo")).task
    child = service.submit(TaskSpec(handler="echo", dependencies=(parent.id,))).task
    grandchild = service.submit(
        TaskSpec(handler="echo", dependencies=(child.id,))
    ).task

    # 在调度发生前直接取消 parent（PENDING）
    service.cancel(parent.id, reason="cleanup")
    wait_terminal(service, child.id)
    wait_terminal(service, grandchild.id)

    log_case(
        "cancel_propagation",
        parent=(parent.id, service.get(parent.id).state.value),
        child=(child.id, service.get(child.id).state.value,
               service.get(child.id).terminal.reason.value),
        grandchild=(grandchild.id, service.get(grandchild.id).state.value,
                    service.get(grandchild.id).terminal.reason.value),
    )
    assert service.get(parent.id).state is TaskState.CANCELLED
    assert service.get(child.id).state is TaskState.BLOCKED
    assert service.get(child.id).terminal.reason is TerminalReason.UPSTREAM_CANCELLED
    assert service.get(grandchild.id).state is TaskState.BLOCKED
    assert (
        service.get(grandchild.id).terminal.reason
        is TerminalReason.UPSTREAM_CANCELLED
    )


def test_failed_terminal_detail_is_traceable(make_service):
    service = make_service()
    service.executor.register(
        "boom", lambda p, ctx: (_ for _ in ()).throw(ValueError("bad value 42"))
    )
    t = service.submit(TaskSpec(handler="boom", max_retries=2)).task
    wait_terminal(service, t.id)
    fresh = service.get(t.id)
    log_case(
        "failure_trace",
        task=t.id,
        attempts=[(a.attempt_no, a.state.value, a.error) for a in fresh.attempts],
        terminal_reason=fresh.terminal.reason.value,
        terminal_detail=fresh.terminal.detail,
    )
    assert fresh.state is TaskState.FAILED
    assert len(fresh.attempts) == 3  # 初次 + 2 次重试
    assert fresh.terminal.reason is TerminalReason.RETRIES_EXHAUSTED
    assert "bad value 42" in fresh.terminal.detail
    assert all(a.state.value == "FAILED" for a in fresh.attempts)
