"""心跳与租约机制测试：失联回收、重新调度、过期结算拒绝（fencing）。"""
from __future__ import annotations

import threading
import time

import pytest

from orchestration.errors import ErrorCode, OrchestrationError
from orchestration.models import AttemptState, TaskSpec, TaskState
from tests.conftest import log_case, wait_terminal


def test_heartbeat_renews_lease(fake_clock, make_service):
    service = make_service(lease_timeout=10.0)
    release = threading.Event()
    service.executor.register("long", lambda p, ctx: release.wait(2))
    t = service.submit(TaskSpec(handler="long")).task
    service.schedule_tick()
    assert service.get(t.id).state is TaskState.RUNNING
    token = service.get(t.id).lease_token
    first_expire = service.get(t.id).lease_expire_at

    fake_clock.advance(5)
    service.heartbeat(t.id, token)
    renewed = service.get(t.id).lease_expire_at
    log_case(
        "heartbeat_renewal",
        task=t.id, lease=token,
        first_expire=first_expire, renewed_expire=renewed,
        advance=5.0,
    )
    assert renewed > first_expire

    # 心跳期间即使推进到「原过期时间之后」，租约仍未过期，不会被回收
    fake_clock.advance(6)  # 距首次心跳已 11，但距续租仅 6
    assert service.reclaim_expired_leases() == []
    release.set()
    wait_terminal(service, t.id)
    assert service.get(t.id).state is TaskState.COMPLETED


def test_lost_worker_lease_reclaimed_and_retried(fake_clock, make_service):
    service = make_service(max_concurrency=1, lease_timeout=10.0)
    observed: list[int] = []
    late_token_holder = {}
    first_started = threading.Event()
    allow_finish = threading.Event()

    def handler(payload, ctx):
        observed.append(ctx.attempt_no)
        if ctx.attempt_no == 1:
            # 模拟执行者失联：不再心跳，记录 token 供稍后"晚到结算"
            late_token_holder["token"] = ctx.lease_token
            first_started.set()
            allow_finish.wait(2)
            # 尝试心跳 —— 此时租约早已被回收，必须被拒绝
            with pytest.raises(OrchestrationError) as ei:
                ctx.heartbeat()
            late_token_holder["heartbeat_code"] = ei.value.code
            return "stale-attempt-1"
        # 第二次尝试（重新调度后）正常完成
        time.sleep(0.05)
        return "recovered-attempt-2"

    service.executor.register("flaky-contact", handler)
    t = service.submit(TaskSpec(handler="flaky-contact", max_retries=2)).task
    service.schedule_tick()
    first_started.wait(2)

    log_case(
        "lease_lost",
        task=t.id, lease_timeout=10.0,
        state_before=f"RUNNING token={late_token_holder['token'][:8]}",
        expectation="advance clock past timeout -> reclaim -> retry",
    )

    fake_clock.advance(11)
    reclaimed = service.reclaim_expired_leases()
    mid = service.get(t.id)
    log_case(
        "lease_reclaimed",
        reclaimed=reclaimed,
        state=mid.state.value,
        attempt1=mid.attempts[0].state.value,
        attempt1_error=mid.attempts[0].error,
    )
    assert reclaimed == [t.id]
    assert mid.state is TaskState.PENDING
    assert mid.attempts[0].state is AttemptState.LEASE_TIMEOUT

    # 重新调度：新租约、新 attempt
    service.schedule_tick()
    fresh = service.get(t.id)
    log_case(
        "lease_redispatch",
        state=fresh.state.value,
        attempt_count=len(fresh.attempts),
        token_rotated=fresh.lease_token != late_token_holder["token"],
    )
    assert fresh.state is TaskState.RUNNING
    assert len(fresh.attempts) == 2
    assert fresh.lease_token != late_token_holder["token"]

    terminal = wait_terminal(service, t.id, timeout=3.0)
    allow_finish.set()
    # 等旧线程的晚到心跳断言完成
    time.sleep(0.1)

    log_case(
        "lease_retry_verdict",
        final=terminal.state.value,
        result=terminal.result,
        attempts=[(a.attempt_no, a.state.value) for a in terminal.attempts],
        stale_heartbeat_code=late_token_holder.get("heartbeat_code"),
    )
    assert terminal.state is TaskState.COMPLETED
    assert terminal.result == "recovered-attempt-2"
    assert [a.state for a in terminal.attempts] == [
        AttemptState.LEASE_TIMEOUT, AttemptState.SUCCEEDED
    ]
    assert late_token_holder.get("heartbeat_code") in {
        ErrorCode.LEASE_EXPIRED,
        ErrorCode.LEASE_TOKEN_MISMATCH,
        ErrorCode.LEASE_HEARTBEAT_NOT_RUNNING,
    }


def test_stale_success_after_reclaim_does_not_double_settle(fake_clock, make_service):
    service = make_service(lease_timeout=10.0)
    first_gate = threading.Event()
    service.executor.register(
        "hung", lambda p, ctx: first_gate.wait(2)
    )
    t = service.submit(TaskSpec(handler="hung", max_retries=0)).task
    service.schedule_tick()
    old_token = service.get(t.id).lease_token

    fake_clock.advance(11)
    service.reclaim_expired_leases()
    assert service.get(t.id).state is TaskState.FAILED

    log_case(
        "stale_settlement_fencing",
        task=t.id, old_lease=old_token[:8],
        state_after_reclaim="FAILED",
        expectation="late success/failure reports rejected; state stays FAILED",
    )
    for fn, label in [
        (lambda: service.report_success(t.id, old_token, "late"), "success"),
        (lambda: service.report_failure(t.id, old_token, "late error"), "failure"),
        (lambda: service.heartbeat(t.id, old_token), "heartbeat"),
    ]:
        with pytest.raises(OrchestrationError) as ei:
            fn()
        log_case("stale_report_rejected", report=label, code=ei.value.code)
        assert ei.value.code == ErrorCode.LEASE_HEARTBEAT_NOT_RUNNING

    fresh = service.get(t.id)
    assert fresh.state is TaskState.FAILED
    assert fresh.result is None
    first_gate.set()


def test_retries_exhausted_after_repeated_lease_timeouts(fake_clock, make_service):
    """反复失联 -> 重试用尽，最终 FAILED 且原因可追溯为重试耗尽。"""
    service = make_service(lease_timeout=10.0)
    entered = threading.Event()

    def always_lost(payload, ctx):
        entered.set()
        time.sleep(0.05)  # 让测试推进虚拟时钟并回收
        return "never-counts"

    service.executor.register("lost", always_lost)
    t = service.submit(TaskSpec(handler="lost", max_retries=2)).task

    for cycle in range(3):
        service.schedule_tick()
        entered.wait(2)
        entered.clear()
        fake_clock.advance(11)
        service.reclaim_expired_leases()
        log_case(
            f"timeout_cycle_{cycle + 1}",
            state=service.get(t.id).state.value,
            attempts=len(service.get(t.id).attempts),
        )

    fresh = service.get(t.id)
    log_case(
        "timeouts_exhausted_verdict",
        state=fresh.state.value,
        attempt_states=[a.state.value for a in fresh.attempts],
        terminal_reason=fresh.terminal.reason.value,
        terminal_detail=fresh.terminal.detail,
    )
    assert fresh.state is TaskState.FAILED
    assert len(fresh.attempts) == 3
    assert all(a.state is AttemptState.LEASE_TIMEOUT for a in fresh.attempts)
    assert fresh.terminal.detail.startswith("retries exhausted after 3 attempt(s)")
    assert "lease timed out" in fresh.terminal.detail
