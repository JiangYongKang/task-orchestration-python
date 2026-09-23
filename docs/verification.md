# 验证方法与关键场景复现

## 1. 自动化测试

```bash
uv run pytest                          # 全部 31 个用例
uv run pytest -v                       # 查看用例名
uv run pytest --log-cli-level=INFO -s  # 打印输入(CASE ...)与判定依据(... verdict)
```

测试使用 `FakeClock` 驱动时间、用线程池跑真实处理器，因此除并发交错外结果确定；
每个关键用例通过 `log_case()` 打印**输入**与**判定依据**，例如：

```
CASE failure_diamond | graph={...} expectation='root fails -> mid1/mid2/leaf BLOCKED; ...'
CASE failure_diamond_verdict | states={'root': 'FAILED', 'mid1': 'BLOCKED', ...}
CASE lease_lost | task='task-000001' lease_timeout=10.0 ...
CASE lease_reclaimed | reclaimed=['task-000001'] state='PENDING' attempt1='LEASE_TIMEOUT'
CASE lease_redispatch | state='RUNNING' attempt_count=2 token_rotated=True
CASE lease_retry_verdict | final='COMPLETED' attempts=[(1,'LEASE_TIMEOUT'),(2,'SUCCEEDED')]
```

## 2. 需求到测试的对照

| 需求点 | 对应用例 |
| --- | --- |
| 并发提交同一幂等键不产生重复任务 | `test_concurrent_same_idempotency_key_collapses_to_one`（40 线程/16 并发） |
| 幂等命中收敛到同一任务 | 同上 + `test_distinct_keys_*`、`test_submit_idempotent_via_http` |
| 非法状态跃迁被拒绝且原因可区分 | `tests/test_cancellation.py` 全部（5 种不同错误码） |
| 依赖未就绪不调度，依据可见 | `test_dependency_gating_and_ready_reasons` |
| 优先级排序确定可复现 | `test_priority_order_is_deterministic` |
| 并发上限不被突破 | `test_quota_never_exceeded`（6 任务/配额 2，4 条 quota_full） |
| 并发调度单次执行权 | `test_concurrent_ticks_grant_execution_once`（8 tick 线程） |
| 依赖失败传播（链/菱形） | `test_failure_propagates_through_chain_and_diamond` |
| 取消传播 | `test_cancel_propagates_downstream` |
| BLOCKED 永不再被派发 | `test_blocked_task_never_dispatched` |
| 心跳续租防误回收 | `test_heartbeat_renews_lease` |
| 失联租约超时回收并重试 | `test_lost_worker_lease_reclaimed_and_retried` |
| 旧执行者晚到结算被 fencing | `test_stale_success_after_reclaim_does_not_double_settle` |
| 反复超时重试用尽、原因可追溯 | `test_retries_exhausted_after_repeated_lease_timeouts` |
| 失败原因/尝试链可追溯 | `test_failed_terminal_detail_is_traceable` |

## 3. 手动复现：依赖失败传播

```bash
uv run fastapi dev main.py &
BASE=http://127.0.0.1:8000

# root 前 3 次都失败（初次 + 2 次重试后 FAILED）
ROOT=$(curl -s -X POST $BASE/tasks -H 'content-type: application/json' \
  -d '{"handler":"flaky","payload":{"fail_times":99},"max_retries":2}' | jq -r .task.id)
curl -s -X POST $BASE/tasks -H 'content-type: application/json' \
  -d "{\"handler\":\"echo\",\"dependencies\":[\"$ROOT\"]}" > /dev/null

sleep 4   # 等待后台守护线程完成 3 次尝试与传播
curl -s $BASE/tasks | jq '.tasks[] | {id, state, terminal: .terminal.reason}'
# root    -> FAILED / retries_exhausted
# 下游    -> BLOCKED / upstream_failed
```

## 4. 手动复现：优先级 + 并发配额

```bash
for i in 1 2 3 4; do
  curl -s -X POST $BASE/tasks -H 'content-type: application/json' \
    -d "{\"handler\":\"flaky\",\"payload\":{\"fail_times\":0},\"priority\":$i}" > /dev/null
done
curl -s -X POST $BASE/scheduler/tick | jq '.running, [.decisions[] | {task_id, dispatched, reason}]'
# "running": 4（等于 max_concurrency），无额外派发；priority=4 的任务最先获得执行权
```

## 5. 手动复现：租约超时重试（需制造失联处理器）

库方式（推荐，时间可控、完全可复现）：

```python
from orchestration.clock import FakeClock
from orchestration.executor import LocalExecutor
from orchestration.service import OrchestrationService
from orchestration.models import TaskSpec
import threading, time

clock = FakeClock()
gate = threading.Event()
ex = LocalExecutor({"slow": lambda p, ctx: gate.wait(5)})
svc = OrchestrationService(clock, ex, max_concurrency=1, lease_timeout=10.0)
t = svc.submit(TaskSpec(handler="slow", max_retries=1)).task

svc.schedule_tick()                 # attempt 1 RUNNING
clock.advance(11)                   # 推进虚拟时钟，超过租约
print(svc.reclaim_expired_leases())  # ['task-000001'] -> attempt1=LEASE_TIMEOUT, 回到 PENDING
svc.schedule_tick()                 # attempt 2 获得全新 lease_token
gate.set()                          # 放行，attempt 2 正常 COMPLETED
```

预期：`attempts = [LEASE_TIMEOUT, SUCCEEDED]`，最终 COMPLETED；
若旧执行者在此之后用旧 token 调 `complete`，服务返回 `lease_token_mismatch` /
`heartbeat_not_running` 并忽略，不会覆盖结果。

## 6. 判定依据（可解释性）在哪看

* `POST /scheduler/tick` 响应中每个任务一条 `{dispatched, reason, detail}`：
  `dispatched / not_ready(waiting_dependency|blocked_by_upstream) /
  quota_full / handler_not_registered`。
* 服务日志：`dispatch-grant`、`scheduler decision`、`retry-enqueue`、
  `lease timeout`、`propagate blocked`、`stale ... ignored`。
* 任务实体：`state`、`attempts[*]`（每次尝试的起止时间、token、错误）、
  `terminal`（终态原因、详情、上游任务 id）。
