# 本地任务编排与执行调度服务 — 设计说明

## 1. 模块结构

```
app/
  models.py     领域模型：TaskState 状态机、跃迁表 ALLOWED_TRANSITIONS、transit()
  errors.py     可区分错误：TaskNotFoundError / InvalidStateTransitionError(reason)
  storage.py    TaskStore：RLock + dict 模拟持久化；幂等键原子占位 reserve_idempotency_key
  clock.py      Clock / SystemClock / FakeClock（注入时钟保证测试可复现）
  scheduler.py  四阶段单轮调度：租约回收 → 失败传播 → 就绪分类 → 配额派发
  executor.py   租约授予 / 心跳续约 / 成功失败结算（owner 校验，杜绝迟到结算）
  service.py    OrchestrationService 门面：submit / get / cancel / schedule_once
  api.py        FastAPI 路由与错误码映射
tests/          存储、服务、执行器、调度器、HTTP API、端到端共 6 组测试
main.py         uvicorn 入口（app = create_app()）
```

## 2. 任务状态机

```
                 ┌────────────── 依赖未满足 ──────────────┐
 PENDING ──► BLOCKED ──(依赖全部 SUCCEEDED)──► READY ──► RUNNING ──┬──► SUCCEEDED
   │            │ 上游 FAILED/CANCELLED/MISSING  │         │      ├──► FAILED_RETRYING ──► READY
   │            └──────────────► FAILED ◄────────┘         │      ├──► LOST ──(仍有额度)──► READY
   └──────────────────────────► FAILED（上游失败传播）      │      │       └──(额度耗尽)──► FAILED
 CANCELLED（PENDING/BLOCKED/READY/LOST/FAILED_RETRYING 可取消）    └──► CANCELLED
 SUCCEEDED / FAILED / CANCELLED 为终态，任何再次跃迁都被拒绝。
```

跃迁规则集中在 `app/models.py` 的 `ALLOWED_TRANSITIONS`，所有状态变更只能经
`transit()` 完成，它同时：

1. 校验跃迁合法（表外跃迁直接报错）；
2. 写入一条可追溯事件到 `task.events`（时间戳、from→to、reason）；
3. 记录 `failure_reason` / `failure_detail`，保证失败原因可区分、可追溯。

**非法取消的可区分原因**（HTTP 409，`reason` 字段）：

| 当前状态 | reason |
|---|---|
| CANCELLED | `already_cancelled` |
| SUCCEEDED | `already_succeeded` |
| FAILED | `already_failed` |
| RUNNING | `task_running`（运行中任务只能由持有者结算） |

迟到结算的原因：`lease_owner_mismatch`（仍 RUNNING 但 owner 已轮换）、
`lease_lost`（任务已离开 RUNNING）、`already_settled`（终态重复结算）。

## 3. 幂等提交

- `TaskSpec.idempotency_key` 为唯一收敛键。
- `submit()` 的「查索引 → 建任务 → 写索引」整体在 `TaskStore` 的同一把锁内完成；
  并发提交同一 key 只会创建一个任务，其余调用返回**同一实体**，
  仅累加 `submit_count` 并记录 `submit_dedup` 事件，不重复入队。
- 依赖引用既可以是任务 id，也可以是幂等键（`TaskStore.resolve()`）。

## 4. 调度规则（Scheduler.run_once，单轮四阶段，全程持存储锁）

1. **租约回收 lease_reclaim**：`RUNNING` 且 `now >= lease_expire_at_ms` 的任务
   先置 `LOST`；剩余重试额度 `max_retries - (attempts-1) > 0` 则回到 `READY`，
   否则以 `failure_reason=lease_timeout` 终态 FAILED。
2. **失败传播 propagate**：任意依赖处于 `FAILED / CANCELLED` 或依赖不存在，
   未决任务（PENDING/BLOCKED/READY）直接判 `FAILED`，
   `failure_reason=upstream_failed`，并级联传播到下游——**下游不会继续排队**。
3. **就绪分类 classify**：依赖全部 `SUCCEEDED` → `READY`；否则 `PENDING→BLOCKED`。
4. **配额派发 dispatch**：统计当前 RUNNING 数量，空闲槽位
   `slots = max_concurrency - running`；READY 队列按
   `(priority 降序, created_at 升序, id 升序)` 排序后依次派发，
   同锁完成 `READY→RUNNING` 与租约授予。

**并发安全**：`run_once` 外层还有一把进程级 `_dispatch_lock`，多线程并发触发
调度时串行进入；配合存储锁，任一时刻在跑任务数绝不超过上限，
且同一任务在并发调度下只会获得一次执行权（attempts 只增一次）。

## 5. 心跳与租约（Executor）

- 派发即授予租约：`lease_owner=<token>`、`lease_expire_at_ms=now+lease_timeout_ms`、
  `attempts += 1`。
- `heartbeat(task_id, owner)`：只有当前持有者、且任务 RUNNING 才能续约，
  顺延到期时间；非法 owner 或非 RUNNING 返回 `False`。
- `succeed / fail`：先校验「RUNNING 且 owner 匹配」。
  - 失败时剩余额度 > 0 → `FAILED_RETRYING`，下轮调度回 `READY`；
  - 额度耗尽 → `FAILED`，`failure_reason=retries_exhausted`。
- 执行者失联被回收后即使"复活"，其迟到的心跳/成功/失败均被拒绝
  （`lease_lost` / `lease_owner_mismatch`），因此**重试与重新调度不会造成
  同一任务重复结算或状态回退**。

## 6. HTTP API

| 方法与路径 | 说明 |
|---|---|
| `POST /tasks` | 提交任务（body 含 idempotency_key/priority/dependencies/max_retries/lease_timeout_ms） |
| `GET /tasks/{id}` | 查询任务（含 state、attempts、failure_reason、events 时间线） |
| `POST /tasks/{id}/cancel` | 取消；终态冲突返回 409 + 可区分 reason |
| `POST /schedule` | 触发一轮调度，返回 `{dispatched: [task_id,...]}` |
| `POST /tasks/{id}/heartbeat?owner=` | 心跳续约 |
| `POST /tasks/{id}/succeed?owner=` | 执行者上报成功 |
| `POST /tasks/{id}/fail?owner=&detail=` | 执行者上报失败 |
| `GET /health` | 健康检查 |

## 7. 验证方法

```bash
# 运行全部单测（日志会打印每个用例的输入与判定依据）
python -m pytest tests/ -v

# 仅看关键场景
python -m pytest tests/test_service.py      # 并发提交幂等收敛、取消原因可区分
python -m pytest tests/test_scheduler.py    # 优先级/配额、依赖失败传播、租约超时重试
python -m pytest tests/test_executor.py     # 心跳续约、重试计数、迟到结算拒绝
python -m pytest tests/test_integration.py  # DAG 端到端闭环

# 启动 HTTP 服务
uvicorn main:app --reload
```

测试全部使用 `FakeClock`（手动推进毫秒时间）与内存存储，不依赖真实时间与 IO，
结果完全可复现。关键场景覆盖：

- 8 线程并发提交同一幂等键 → 只产生 1 个任务，submit_count=8；
- 依赖链 A→B→C 中 A 失败 → B、C 均 FAILED(upstream_failed)，不排队；
- 租约超时 → LOST→READY 重新派发，attempts 1→2、owner 轮换；
  失联执行者迟到结算被拒，新持有者唯一成功结算；
- 6 线程并发调度 20 任务 → 每个任务恰好派发一次；
- 在跑数量恒不超过 `max_concurrency`。
