# 设计说明：调度规则、任务状态机与一致性保证

## 1. 模块结构

```
orchestration/
├── errors.py     # 稳定错误码 + OrchestrationError（机器可区分的失败原因）
├── models.py     # 任务状态机、Task/Attempt/TerminalInfo/TaskSpec/ScheduleDecision
├── clock.py      # Clock 抽象：SystemClock（生产）/ FakeClock（确定性测试）
├── store.py      # TaskStore：一把 RLock 保护的内存存储 + 幂等键索引
├── executor.py   # LocalExecutor：线程池执行，携带租约 token 的心跳/结算回调
├── scheduler.py  # Scheduler：就绪判定 + 优先级排序 + 配额派发
├── service.py    # OrchestrationService：提交/查询/取消/心跳/结算/回收 门面
└── api.py        # FastAPI 适配层（可选，核心逻辑不依赖它）
```

## 2. 任务状态机

```
                         ┌──────────────────────── 调度派发（发放租约）
                         ▼
 PENDING ┌──────────────────────────► RUNNING ──成功上报(lease_token 校验通过)──► COMPLETED
   ▲     │                              │
   │     │用户取消                      ├─业务失败(attempt <= max_retries)──► PENDING(attempt+1)
   │     ▼                              ├─业务失败(重试耗尽)─────────────────► FAILED
 CANCELLED                              └─租约超时回收(可重试同上)
   │
   └─ 上游 FAILED/CANCELLED/BLOCKED 传播 ─► BLOCKED

终态：COMPLETED、FAILED、CANCELLED、BLOCKED（不可逆）
```

关键点：

* **没有独立的 QUEUED/READY 状态**：`PENDING` 统一表示排队，「是否就绪」是调度器
  每轮依据依赖状态**动态判定**的结果，通过 `ScheduleDecision.reason/detail` 输出依据，
  避免引入难以维护的派生状态。
* **BLOCKED 是终态**。只要任一（传递）依赖进入 FAILED/CANCELLED/BLOCKED，
  尚未开始执行的下游立即被标记 BLOCKED，而不是继续排队；`terminal.reason` 区分
  `upstream_failed / upstream_cancelled / upstream_blocked`，并记录直接触发的
  `upstream_task_id`，便于追溯。
* 传播用 BFS 沿反向依赖边完成，菱形/长链只会处理一次（`visited` 去重）。
* RUNNING 期间不支持取消（返回 `task_running`），保证执行权与取消语义简单可靠；
  执行上下文仍保留 `is_cancelled()` 钩子供后续扩展协作式取消。

### 非法状态跃迁的错误码

对终态/运行中任务再次取消，按**当前状态**给出不同错误码（HTTP 均为 409）：

| 当前状态 | 错误码 |
| --- | --- |
| COMPLETED | `task_completed` |
| CANCELLED | `task_cancelled`（重复取消） |
| FAILED | `already_terminal` |
| BLOCKED | `task_blocked` |
| RUNNING | `task_running` |
| 其它非 PENDING 异常情形 | `illegal_state_transition` |

## 3. 提交与幂等

1. 提交在 `TaskStore` 的同一把 `RLock` 内完成「按幂等键查重 + 插入 + 建索引」，
   因此**并发提交同一幂等键只有第一个调用创建任务**，其余调用返回同一个任务
   （`SubmitResult.created=False`），不会产生重复任务或重复执行。
2. 幂等键省略时自动生成 `auto:<task_id>`，两次无键提交是两个独立任务。
3. 提交时校验：依赖必须全部存在（`unknown_dependency`）、handler 非空、
   `max_retries >= 0`（`invalid_payload`）。当前按「先提交上游」的方式建图，
   DAG 由提交顺序天然保证；`_would_create_cycle` 为未来按客户端 id 提交预留检查点。
4. 幂等收敛是「同 key 返回同任务」，**不**比较两次提交的参数；这是收敛而非冲突，
   因此不返回错误。

## 4. 调度规则（每轮 `schedule_tick`）

1. 只考察 `PENDING` 任务；RUNNING 与终态不参与。
2. **就绪判定**（`Scheduler.is_ready`）——所有直接依赖均为 COMPLETED：
   * 依赖仍 PENDING/RUNNING → 不派发，`reason=not_ready`，
     `detail=waiting_dependency[<id>:<state>, ...]`；
   * 依赖处于 FAILED/CANCELLED/BLOCKED → `detail=blocked_by_upstream[...]`
     （正常情况下该任务此前已被传播为 BLOCKED，此为防御分支）。
3. **排序确定性**：就绪候选按 `(-priority, created_at, id)` 排序，优先级高者先跑，
   同级先到先得；相同输入永远得到相同派发顺序。
4. **配额**：每轮开始计算 `capacity = max_concurrency - running_count`，
   逐个派发并递减；`capacity=0` 后剩余就绪任务记 `quota_full` 且本轮不再派发
   （不会让排序靠后的任务插队）。**任何时刻实际在跑任务数 ≤ max_concurrency**。
5. 未注册 handler 的任务记 `handler_not_registered`，不获得执行权，保持 PENDING。
6. 状态跃迁与执行权发放（写 RUNNING、写 Attempt、发 lease_token、投递线程池）
   在同一把锁内一次完成，所以**并发 tick 下每个任务最多获得一次执行权、
   只产生一条 Attempt**。

每个被考察任务都会产生一条 `ScheduleDecision{task_id, dispatched, reason, detail}`，
并在日志中打印——这就是「可解释的调度结果」。

## 5. 心跳、租约与失败重试

### 租约（lease）

* 派发时生成随机 `lease_token`（fencing token），`lease_expire_at = now + lease_timeout`，
  同时创建一条 `Attempt(attempt_no, started_at, lease_token, state=RUNNING)`。
* 执行者通过 `ExecutionContext.heartbeat()` 续租：校验通过后
  `lease_expire_at = now + lease_timeout`。
* `reclaim_expired_leases()` 扫描所有 RUNNING 任务，`now > lease_expire_at` 即判定失联：
  当前 Attempt 置为 `LEASE_TIMEOUT`，清除租约，随后进入统一的「尝试结束裁决」。

### 重试预算

* 总尝试次数 = `max_retries + 1`（初次 + N 次重试）。
* 业务失败与租约超时走**同一个裁决函数** `_handle_attempt_end`：
  * `attempt_count <= max_retries` → 回到 PENDING 等待下一轮调度（日志写明 used/max 与原因）；
  * 否则进入 FAILED 终态，`terminal.reason=retries_exhausted`，
    `detail="retries exhausted after N attempt(s); last error: ..."`，
    并把 BLOCKED 传播给下游。

### 为什么不会重复结算 / 状态回退

所有结算（成功/失败/心跳）都必须通过 `_validate_lease`：

1. 任务当前不是 RUNNING → `heartbeat_not_running`（任务已终态或已回收重排）；
2. token 与当前租约不一致 → `lease_token_mismatch`（旧尝试被新派发取代）；
3. 当前时间超过租约 → `lease_expired`。

因此「失联执行者晚到的成功/失败上报」与「回收后重试的第二次执行」不可能同时生效：
先到且持有**当前**租约的上报才被接受，旧上报只记一条
`stale ... ignored` 警告并被丢弃。结算在锁内完成，状态只能沿状态机向前，
不会出现 COMPLETED 被覆盖回 PENDING、或一次执行被结算两次的情况。

## 6. 并发模型

* 进程内多线程：HTTP/后台线程提交与调度，线程池工作线程上报结果。
* 所有复合操作（查重+插入、状态检查+跃迁、就绪判定+派发）都在 `TaskStore.lock`
  （可重入锁）内完成；工作线程的回调进入服务后同样先获取这把锁。
* 时间来自可注入的 `Clock`：测试用 `FakeClock.advance()` 精确制造租约超时，
  不依赖真实 sleep，因此租约/重试结果逐次运行可复现。

## 7. 后台驱动（可选）

`api.start_background_daemon(service, interval=1.0)` 启动一个守护线程，周期性执行
`scheduler_tick()` 与 `reclaim_expired_leases()`；核心服务本身不创建线程，
测试可手动驱动，保证确定性。
