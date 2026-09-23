"""服务入口：装配本地编排服务并启动 FastAPI。

运行：``uv run fastapi dev main.py``（或 ``python -m fastapi dev main.py``）。
"""
from __future__ import annotations

import logging

from orchestration.api import build_default_service, create_app, start_background_daemon
from orchestration.executor import ExecutionContext

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

service = build_default_service()


# ---- 注册内置示例处理器 ----
def echo_handler(payload: dict, ctx: ExecutionContext) -> dict:
    return {"task_id": ctx.task_id, "attempt": ctx.attempt_no, "echo": payload}


def flaky_handler(payload: dict, ctx: ExecutionContext) -> dict:
    """根据 payload["fail_times"] 前 N 次抛错，用于演示失败重试。"""
    import itertools

    state = flaky_handler.__dict__.setdefault("_calls", {})
    n = next(state.setdefault(ctx.task_id, itertools.count(1)))
    fail_times = int(payload.get("fail_times", 0))
    if n <= fail_times:
        raise RuntimeError(f"simulated failure {n}/{fail_times}")
    return {"task_id": ctx.task_id, "attempt": n, "ok": True}


service.executor.register("echo", echo_handler)
service.executor.register("flaky", flaky_handler)

app = create_app(service)
_stop_event = start_background_daemon(service, interval=1.0)


@app.get("/")
def read_root():
    return {
        "service": "task-orchestration",
        "docs": "/docs",
        "max_concurrency": service.max_concurrency,
        "lease_timeout": service.lease_timeout,
    }
