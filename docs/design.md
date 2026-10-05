# 本地任务编排与执行调度服务 — 设计说明

## 1. 模块结构

```
app/
  models.py     领域模型：TaskState 状态机、跃迁表 ALLOWED_TRANSITIONS、transit()
  errors.py     可区分错误：TaskNotFoundError / InvalidStateTransitionError(reason)
  storage.py    TaskStore：RLock + dict 模拟持久化；幂等键原子占位 reserve_idempotency_key
  clock.py      Clock / SystemClock / FakeClock（注入时钟保证测试可复现）
  scheduler.py  五阶段单轮调度：租约回收 → 失败传播 → 重试放行 → 就绪分类 → 配额派发
  executor.py   租约授予 / 心跳续约 / 成功失败结算（owner + 租约有效期双重校验）
  service.py    OrchestrationService 门面：submit / get / cancel / schedule_once
  api.py        FastAPI 路由与错误码映射
tests/          存储、服务、执行器、调度器、取消、租约过期、重试、分组配额、HTTP API、端到端
main.py         uvicorn 入口（app = create_app()）
```

## 2. 任务状态机

```
                 ┌────────────── 依赖未满足 ──────────────┐
 PENDING ──► BLOCKED ──(依赖全部 SUCCEEDED)──► READY ──► RUNNING ──┬──► SUCCEEDED
   │            │ 上游 FAILED/CANCELLED/MISSING  │         │      ├──► FAILED_RETRYING ─(到点)─► READY
   │            └──────────────► FAILED ◄────────┘         │      ├──► LOST ─(仍有额度)──► FAILED_RETRYING
   └──────────────────────────► FAILED（上游失败传播）      │      │       └──(额度耗尽)──► FAILED
 CANCELLED（PENDING/BLOCKED/READY/RUNNING/LOST/FAILED_RETRYING 均可取消） └──► CANCELLED
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

迟到结算/上报的原因：`lease_owner_mismatch`（仍 RUNNING 但 owner 已轮换）、
`lease_lost`（任务已离开 RUNNING）、`already_settled`（终态重复结算）、
`lease_expired`（租约已过期但调度器尚未回收，见 §5）。

## 3. 取消（含运行中任务）

- 任意非终态都可取消，包括 **RUNNING**：取消立即作废租约
  （清空 `lease_owner` / `lease_expire_at_ms` / `next_retry_at_ms`），
  全局与分组并发名额当场释放，下一轮调度即可被其它任务接管。
- 取消是终态：原执行者之后无论隔多久上报心跳/成功/失败，
  都会因 `not_running` / `already_settled` 被拒绝，状态不会被拉回。
- 依赖已取消任务的下游由失败传播阶段统一判 `FAILED(upstream_failed)`，
  不再继续排队；重试等待中（FAILED_RETRYING）的任务取消后，
  到点也不会再被放行。

## 4. 幂等提交

- `TaskSpec.idempotency_key` 为唯一收敛键。
- `submit()` 的「查索引 → 建任务 → 写索引」整体在 `TaskStore` 的同一把锁内完成；
  并发提交同一 key 只会创建一个任务，其余调用返回**同一实体**，
  仅累加 `submit_count` 并记录 `submit_dedup` 事件，不重复入队。
- 依赖引用既可以是任务 id，也可以是幂等键（`TaskStore.resolve()`）。

## 5. 调度规则（Scheduler.run_once，单轮五阶段，全程持存储锁）

1. **租约回收 lease_reclaim**：`RUNNING` 且 `now >= lease_expire_at_ms` 的任务
   先置 `LOST`；剩余重试额度 `max_retries - (attempts-1) > 0` 则进入
   `FAILED_RETRYING` 并按退避规则设置 `next_retry_at_ms`，
   否则以 `failure_reason=lease_timeout` 终态 FAILED。
2. **失败传播 propagate**：任意依赖处于 `FAILED / CANCELLED` 或依赖不存在，
   未决任务（PENDING/BLOCKED/READY）直接判 `FAILED`，
   `failure_reason=upstream_failed`，并级联传播到下游——**下游不会继续排队**。
3. **重试放行 release_retries**：`FAILED_RETRYING` 且
   `now >= next_retry_at_ms` 的任务回到 `READY`（清空 `next_retry_at_ms`）；
   未到点的继续等待，不参与本轮派发。这保证重试闭环：
   只要额度未尽，到点必然重新参与调度并真的被执行。
4. **就绪分类 classify**：依赖全部 `SUCCEEDED` → `READY`；否则 `PENDING→BLOCKED`。
5. **配额派发 dispatch**：空闲槽位同时受两层约束——
   全局 `slots = max_concurrency - running` 与分组
   `group_slots = group_limits[group] - group_running`（未配置的分组只受全局约束）。
   READY 队列按 `(priority 降序, created_at 升序, id 升序)` 全局统一排序后依次派发：
   分组配额已满的任务**跳过**（不阻塞其它分组），分组之间按优先级公平争抢全局名额；
   同锁完成 `READY→RUNNING` 与租约授予。

**并发安全**：`run_once` 外层还有一把进程级 `_dispatch_lock`，多线程并发触发
调度时串行进入；配合存储锁，任一时刻全局在跑任务数绝不超过 `max_concurrency`、
单组在跑数绝不超过该组上限，且同一任务在并发调度下只会获得一次执行权
（attempts 只增一次）。

## 6. 重试与退避

- `TaskSpec.retry_delay_ms`：重试前的基础等待毫秒数（默认 0 = 立即重试）。
- `TaskSpec.retry_backoff_multiplier`：退避倍率，第 n 次重试等待
  `retry_delay_ms * multiplier^(n-1)`（n 从 1 开始，按已尝试次数递增）。
- 执行失败与租约超时失联**统一**走 `FAILED_RETRYING` 等待重试，
  等待时长规则一致；`next_retry_at_ms` 在结算/回收时写入，到点由
  release_retries 阶段放行。
- 查询任务可见：`attempts`（已尝试次数）、`next_retry_at_ms`
  （下一次重试时间点，等待中才有值）、终态失败时的
  `failure_reason`（`execution_failed` / `retries_exhausted` /
  `lease_timeout` / `upstream_failed`）与 `failure_detail`。

## 7. 心跳与租约（Executor）

- 派发即授予租约：`lease_owner=<token>`、`lease_expire_at_ms=now+lease_timeout_ms`、
  `attempts += 1`。
- `heartbeat(task_id, owner)`：只有当前持有者、任务 RUNNING、且租约未过期
  才能续约，顺延到期时间；非法 owner、非 RUNNING 或租约已过期均返回 `False`。
- `succeed / fail`：先校验「RUNNING 且 owner 匹配且租约未过期」。
  - 失败时剩余额度 > 0 → `FAILED_RETRYING` 并写入 `next_retry_at_ms`；
  - 额度耗尽 → `FAILED`，`failure_reason=retries_exhausted`。
- 执行者失联被回收后即使"复活"，其迟到的心跳/成功/失败均被拒绝
  （`lease_lost` / `lease_owner_mismatch`），因此**重试与重新调度不会造成
  同一任务重复结算或状态回退**。
- **过期未回收边界**：租约已过 `lease_expire_at_ms`、但调度器尚未跑到回收时，
  原执行者补交的心跳/成功/失败同样被拒绝（`lease_expired`）——
  过期凭证不算数，必须由回收后重新接管的一方重新执行并结算。

## 8. 与原有能力的兼容范围

- **幂等提交**：新字段（group/retry_delay_ms/retry_backoff_multiplier）只随首次
  提交生效；同 key 重复提交仍收敛到首次创建的任务，不会用新参数覆盖旧任务。
- **优先级**：排序规则不变（优先级降序 → 创建时间 → id）；分组配额只在排序之后
  做跳过过滤，不改变优先级语义——组间仍是高优先级先抢全局名额。
- **全局并发上限**：语义不变且始终生效；分组上限是在其上的附加约束，
  两者同时满足才派发，因此全局在跑数任何时候都不超 `max_concurrency`。
- **任务状态变化**：`FAILED_RETRYING` 由「下轮立即回 READY」改为「到点才回 READY」，
  默认 `retry_delay_ms=0` 时行为与原来完全一致；`LOST` 不再直接回 READY，
  而是经 `FAILED_RETRYING` 走同一重试闭环（默认参数下同一轮内完成，外部无感）。
- **取消**：原「RUNNING 不可取消（task_running）」的限制解除；对终态任务取消的
  409 + 可区分 reason 行为不变。

## 9. HTTP API

| 方法与路径 | 说明 |
|---|---|
| `POST /tasks` | 提交任务（body 含 idempotency_key/priority/dependencies/max_retries/lease_timeout_ms/group/retry_delay_ms/retry_backoff_multiplier） |
| `GET /tasks/{id}` | 查询任务（含 state、attempts、max_retries、next_retry_at_ms、failure_reason、events 时间线） |
| `POST /tasks/{id}/cancel` | 取消（含运行中任务）；终态冲突返回 409 + 可区分 reason |
| `POST /schedule` | 触发一轮调度，返回 `{dispatched: [task_id,...]}` |
| `POST /tasks/{id}/heartbeat?owner=` | 心跳续约 |
| `POST /tasks/{id}/succeed?owner=` | 执行者上报成功 |
| `POST /tasks/{id}/fail?owner=&detail=` | 执行者上报失败 |
| `GET /health` | 健康检查 |

分组并发上限在构造服务时配置：
`OrchestrationService(max_concurrency=4, group_limits={"search": 1, "ads": 2})`。

## 10. 验证方法

```bash
# 运行全部单测（日志会打印每个用例的输入与判定依据）
python -m pytest tests/ -v

# 仅看关键场景
python -m pytest tests/test_service.py      # 并发提交幂等收敛、取消原因可区分
python -m pytest tests/test_scheduler.py    # 优先级/配额、依赖失败传播、租约超时重试
python -m pytest tests/test_executor.py     # 心跳续约、重试计数、迟到结算拒绝
python -m pytest tests/test_cancel.py       # 取消运行中任务、名额释放、下游级联、迟到上报拒绝
python -m pytest tests/test_lease_expiry.py # 租约过期未回收边界：补交上报不算数
python -m pytest tests/test_retry.py        # 退避等待、到点重跑、重试额度逐次耗尽
python -m pytest tests/test_group_quota.py  # 分组配额、全局上限、分组间优先级公平
python -m pytest tests/test_integration.py  # DAG 端到端闭环

# 启动 HTTP 服务
uvicorn main:app --reload
```

测试全部使用 `FakeClock`（手动推进毫秒时间）与内存存储，不依赖真实时间与 IO，
结果完全可复现。关键场景覆盖：

- 8 线程并发提交同一幂等键 → 只产生 1 个任务，submit_count=8；
- 依赖链 A→B→C 中 A 失败 → B、C 均 FAILED(upstream_failed)，不排队；
- 租约超时 → LOST→FAILED_RETRYING→READY 重新派发，attempts 1→2、owner 轮换；
  失联执行者迟到结算被拒，新持有者唯一成功结算；
- 运行中任务被取消 → 名额立即释放、下游级联 FAILED、原执行者迟到上报无效；
- 租约过期但调度未回收 → 原执行者补交的心跳/成功/失败以 lease_expired 拒绝；
- 重试等待未到点不调度、到点自动重跑，max_retries 次尝试全部真实发生后
  才终态 FAILED(retries_exhausted)；
- 分组配额：单组不超组上限、全局不超 max_concurrency、分组间按优先级公平争抢；
- 6 线程并发调度 20 任务 → 每个任务恰好派发一次。
