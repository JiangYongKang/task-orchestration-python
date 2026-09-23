# HTTP 接口与错误码说明

所有响应均为 JSON。业务错误统一形如：

```json
{"error": {"code": "task_cancelled", "reason": "task already cancelled", "task_id": "task-000001"}}
```

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks` | 提交任务；同 `idempotency_key` 收敛为同一任务（响应体 `created` 区分新建/命中） |
| GET | `/tasks` | 列出全部任务及其尝试记录 |
| GET | `/tasks/{task_id}` | 查询单个任务 |
| POST | `/tasks/{task_id}/cancel` | 取消排队中的任务（仅 PENDING 可取消），并向下游传播 BLOCKED |
| POST | `/tasks/{task_id}/heartbeat` | 执行者续租，body：`{"lease_token": "..."}` |
| POST | `/tasks/{task_id}/complete` | 上报成功，body：`{"lease_token": "...", "result": ...}` |
| POST | `/tasks/{task_id}/fail` | 上报失败，body：`{"lease_token": "...", "error": "..."}` |
| POST | `/scheduler/tick` | 手动驱动一轮调度，返回 `running/max_concurrency/decisions` |
| POST | `/leases/reclaim` | 手动回收超时租约，返回被回收任务 id 列表 |

## 提交任务请求体

```json
{
  "handler": "echo",
  "payload": {"any": "json"},
  "idempotency_key": "job-2026-09-23-01",
  "dependencies": ["task-000001"],
  "priority": 10,
  "max_retries": 3
}
```

* `priority` 越大越优先；同级按提交时间先到先得。
* `max_retries` 是「失败后的重试次数」，总尝试次数 = `max_retries + 1`。
* 省略 `idempotency_key` 时自动生成，任务之间相互独立。

## 任务响应体（关键字段）

```json
{
  "id": "task-000003",
  "state": "FAILED",
  "priority": 0,
  "dependencies": ["task-000002"],
  "attempts": [
    {"attempt_no": 1, "state": "FAILED", "error": "RuntimeError: boom",
     "started_at": 1000.0, "ended_at": 1000.2, "lease_token": "..."},
    {"attempt_no": 2, "state": "FAILED", "error": "RuntimeError: boom",
     "started_at": 1000.4, "ended_at": 1000.6, "lease_token": "..."}
  ],
  "terminal": {
    "reason": "retries_exhausted",
    "at": 1000.6,
    "detail": "retries exhausted after 2 attempt(s); last error: RuntimeError: boom",
    "upstream_task_id": null
  },
  "lease_expire_at": null,
  "last_heartbeat_at": null
}
```

终态任务的 `terminal.reason` 取值：

`completed / failed / cancelled / upstream_failed / upstream_cancelled /
upstream_blocked / retries_exhausted`

> 注：`failed` 保留给直接失败且无重试预算场景的语义位；当前实现的最终失败
> 统一记为 `retries_exhausted`（即使 `max_retries=0`，表示 1 次尝试后耗尽），
> 并在 `detail` 中给出最后一次错误，保证失败原因唯一且可追溯。

## 错误码映射

| 错误码 | HTTP | 触发场景 |
| --- | --- | --- |
| `task_not_found` | 404 | 查询/操作不存在的任务 |
| `unknown_dependency` | 400 | 提交时引用不存在的依赖 |
| `invalid_payload` | 400 | handler 为空、`max_retries < 0` 等 |
| `dependency_cycle` | 400 | （预留）提交会形成依赖环 |
| `task_completed` | 409 | 取消已完成任务 |
| `task_cancelled` | 409 | 重复取消 |
| `already_terminal` | 409 | 取消已失败任务 |
| `task_blocked` | 409 | 取消因上游终态而 BLOCKED 的任务 |
| `task_running` | 409 | 取消正在运行的任务 |
| `lease_expired` | 409 | 心跳/结算时租约已超时 |
| `lease_token_mismatch` | 409 | 心跳/结算 token 与当前租约不符（旧尝试 fencing） |
| `heartbeat_not_running` | 409 | 对非 RUNNING 状态的任务心跳/结算（含晚到结算） |
| `retries_exhausted` | 400 | （预留）显式对已耗尽重试的任务再触发 |
| `handler_not_found` | 400 | （预留）执行期发现处理器缺失 |
