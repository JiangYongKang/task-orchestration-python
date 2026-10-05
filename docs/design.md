# 本地任务编排与执行调度服务 — 设计说明

## 1. 模块结构

```
app/
  models.py     领域模型：TaskState 状态机、跃迁表 ALLOWED_TRANSITIONS、transit()、退避计算
  errors.py     可区分错误：TaskNotFoundError / InvalidStateTransitionError(reason)
  storage.py    TaskStore：RLock + dict 模拟持久化；幂等键原子占位 reserve_idempotency_key
  clock.py      Clock / SystemClock / FakeClock（注入时钟保证测试可复现）
  scheduler.py  五阶段单轮调度：租约回收 → 失败传播 → 就绪分类 → 重试放行 → 配额派发
  executor.py   租约授予 / 心跳续约 / 成功失败结算（owner + 凭证有效期双重校验）
  service.py    OrchestrationService 门面：submit / get / cancel / schedule_once / set_group_limit
  api.py        FastAPI 路由与错误码映射
tests/          存储、服务、执行器、调度器、取消、凭证过期、重试退避、分组配额、HTTP API、端到端
main.py         uvicorn 入口（app = create_app()）
```

## 2. 任务状态机

```
                 ┌────────────── 依赖未满足 ──────────────┐
 PENDING ──► BLOCKED ──(依赖全部 SUCCEEDED)──► READY ──► RUNNING ──┬──► SUCCEEDED
   │            │ 上游 FAILED/CANCELLED/MISSING  │         │      ├──► FAILED_RETRYING ─(到 next_retry_at)─► READY
   │            └──────────────► FAILED ◄────────┘         │      ├──► LOST ─(仍有额度, 同样退避)─► READY
   └──────────────────────────► FAILED（上游失败传播）      │      │       └──(额度耗尽)──► FAILED
 CANCELLED（PENDING/BLOCKED/READY/RUNNING/LOST/FAILED_RETRYING 均可取消）└──► CANCELLED
 SUCCEEDED / FAILED / CANCELLED 为终态，任何再次跃迁都被拒绝。
```

跃迁规则集中在 `app/models.py` 的 `ALLOWED_TRANSITIONS`，所有状态变更只能经
`transit()` 完成，它同时：

1. 校验跃迁合法（表外跃迁直接报错）；
2. 写入一条可追溯事件到 `task.events`（时间戳、from→to、reason）；
3. 记录 `failure_reason` / `failure_detail`，保证失败原因可区分、可追溯。

**取消规则**：

- 除三个终态外的所有状态都可取消，包括 **RUNNING**：取消立即作废租约
  （`lease_owner`/`lease_expire_at_ms` 清空），占用的全局与分组并发名额当轮即释放；
  下游由调度器失败传播判 `FAILED(upstream_failed)`，不再排队。
- 取消后原执行者无论隔多久上报心跳/成功/失败，都会因
  `owner_mismatch` / `already_settled` 被拒绝，状态不会从 CANCELLED 拉回。

**非法取消的可区分原因**（HTTP 409，`reason` 字段）：

| 当前状态 | reason |
|---|---|
| CANCELLED | `already_cancelled` |
| SUCCEEDED | `already_succeeded` |
| FAILED | `already_failed` |

迟到结算的原因：`lease_owner_mismatch`（仍 RUNNING 但 owner 已轮换）、
`lease_expired`（凭证已过期、调度尚未回收，此刻补交的上报不算数）、
`lease_lost`（任务已离开 RUNNING）、`already_settled`（终态重复结算）。

## 3. 幂等提交

- `TaskSpec.idempotency_key` 为唯一收敛键。
- `submit()` 的「查索引 → 建任务 → 写索引」整体在 `TaskStore` 的同一把锁内完成；
  并发提交同一 key 只会创建一个任务，其余调用返回**同一实体**，
  仅累加 `submit_count` 并记录 `submit_dedup` 事件，不重复入队。
- 依赖引用既可以是任务 id，也可以是幂等键（`TaskStore.resolve()`）。

## 4. 调度规则（Scheduler.run_once，单轮五阶段，全程持存储锁）

1. **租约回收 lease_reclaim**：`RUNNING` 且 `now >= lease_expire_at_ms` 的任务
   先置 `LOST`；剩余重试额度 `max_retries - (attempts-1) > 0` 则按退避等待重试
   （等待为 0 时当轮回 `READY`），否则以 `failure_reason=lease_timeout` 终态 FAILED。
2. **失败传播 propagate**：任意依赖处于 `FAILED / CANCELLED` 或依赖不存在，
   未决任务（PENDING/BLOCKED/READY）直接判 `FAILED`，
   `failure_reason=upstream_failed`，并级联传播到下游——**下游不会继续排队**。
3. **就绪分类 classify**：依赖全部 `SUCCEEDED` → `READY`；否则 `PENDING→BLOCKED`。
4. **重试放行 retry_release**：`FAILED_RETRYING`（及等待重试的 `LOST`）任务，
   `now >= next_retry_at_ms` 才回 `READY` 参与本轮派发；未到点不调度。
   这保证「配了几次重试就实实在在执行几次」，到点当轮即重新跑起来。
5. **配额派发 dispatch**：统计当前 RUNNING 数量，空闲槽位
   `slots = max_concurrency - running`；READY 队列按
   `(priority 降序, created_at 升序, id 升序)` 排序后依次派发，
   同锁完成 `READY→RUNNING` 与租约授予。
   除全局上限外，每个任务还受所属 `group` 的分组并发上限约束：
   分组满了只跳过该组任务（continue），不阻塞其它分组——
   分组之间按统一优先级顺序公平抢名额；未配置上限的分组只受全局上限约束。

**重试退避**：`retry_wait = retry_delay_ms * retry_backoff_multiplier^(attempts-1)`，
在失败结算 / 租约回收时写入 `next_retry_at_ms = now + retry_wait`；
新一轮租约授予时清零。`retry_delay_ms=0`（默认）表示立即重试，行为与旧版一致。

**并发安全**：`run_once` 外层还有一把进程级 `_dispatch_lock`，多线程并发触发
调度时串行进入；配合存储锁，任一时刻全局在跑数与每组在跑数都不超各自上限，
且同一任务在并发调度下只会获得一次执行权（attempts 只增一次）。

## 5. 心跳与租约（Executor）

- 派发即授予租约：`lease_owner=<token>`、`lease_expire_at_ms=now+lease_timeout_ms`、
  `attempts += 1`，同时清零 `next_retry_at_ms`（重试等待已消费）。
- `heartbeat(task_id, owner)`：只有当前持有者、任务 RUNNING、**且凭证未过期**
  才能续约，顺延到期时间；非法 owner、非 RUNNING 或凭证过期均返回 `False`
  （过期心跳不会顺延到期时间）。
- `succeed / fail`：先校验「RUNNING 且 owner 匹配且凭证未过期」。
  - 失败时剩余额度 > 0 → `FAILED_RETRYING` 并写入 `next_retry_at_ms`（退避）；
  - 额度耗尽 → `FAILED`，`failure_reason=retries_exhausted`。
- **凭证过期边界**：执行凭证已过期、只是调度还没来得及回收时，原执行者
  在此之后才补交的心跳/成功/失败同样被拒（`lease_expired`），
  必须由重新接管的一方重新执行后再结算。
- 执行者失联被回收后即使"复活"，其迟到的心跳/成功/失败均被拒绝
  （`lease_lost` / `lease_owner_mismatch`），因此**重试与重新调度不会造成
  同一任务重复结算或状态回退**。

## 6. HTTP API

| 方法与路径 | 说明 |
|---|---|
| `POST /tasks` | 提交任务（body 含 idempotency_key/priority/dependencies/max_retries/lease_timeout_ms/group/retry_delay_ms/retry_backoff_multiplier） |
| `GET /tasks/{id}` | 查询任务（含 state、attempts、next_retry_at_ms、failure_reason、events 时间线） |
| `POST /tasks/{id}/cancel` | 取消（含运行中任务）；终态冲突返回 409 + 可区分 reason |
| `PUT /groups/{group}/concurrency` | 设置分组并发上限（body `{"limit": N}`，N >= 1） |
| `POST /schedule` | 触发一轮调度，返回 `{dispatched: [task_id,...]}` |
| `POST /tasks/{id}/heartbeat?owner=` | 心跳续约 |
| `POST /tasks/{id}/succeed?owner=` | 执行者上报成功 |
| `POST /tasks/{id}/fail?owner=&detail=` | 执行者上报失败 |
| `GET /health` | 健康检查 |

## 7. 与原有能力的兼容范围

- **幂等提交**：不变。重复提交收敛到同一任务；`group` / 重试参数以首次提交为准。
- **优先级**：不变。全局与分组配额都在同一优先级序上分配。
- **全局并发上限**：不变且仍然硬约束；分组上限是在其之下的附加约束，
  任何时刻全局在跑数不超 `max_concurrency`，单组不超其分组上限。
- **任务状态变化**：状态机只新增了「RUNNING→CANCELLED 由 cancel 触发」这一
  已有跃迁的实际启用，以及 FAILED_RETRYING/LOST 回 READY 增加了时间门槛；
  终态集合与失败传播语义不变。`retry_delay_ms=0`（默认）时重试行为与旧版完全一致。
- **取消**：原「运行中不可取消（task_running）」的限制取消，运行中任务现在可取消；
  终态取消的 409 + reason 语义不变。

## 8. 验证方法

```bash
# 运行全部单测（日志会打印每个用例的输入与判定依据）
python -m pytest tests/ -v

# 仅看关键场景
python -m pytest tests/test_service.py      # 并发提交幂等收敛、取消原因可区分
python -m pytest tests/test_scheduler.py    # 优先级/配额、依赖失败传播、租约超时重试
python -m pytest tests/test_executor.py     # 心跳续约、重试计数、迟到结算拒绝
python -m pytest tests/test_cancel.py       # 取消运行中任务：名额释放、下游判死、迟到上报无效
python -m pytest tests/test_lease_expiry.py # 凭证过期后迟交上报被拒、新接管方重新执行
python -m pytest tests/test_retry.py        # 重试退避：未到点不调度、到点真的重跑、额度闭环
python -m pytest tests/test_groups.py       # 分组配额与全局上限、分组间按优先级公平抢名额
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
- 取消运行中任务 → 名额立即释放给排队任务，下游判 FAILED，
  原执行者迟到的心跳/成功/失败全部无效，状态保持 CANCELLED；
- 凭证过期但调度尚未回收 → 过期执行者补交的心跳/成功/失败被拒（lease_expired），
  回收后由新 owner 重新执行并成功结算；
- 重试退避 → 未到 next_retry_at_ms 不派发，到点当轮重新执行；
  退避时长按次数递增（100→200ms）；max_retries 次重试全部真实发生后
  成功（attempts=3）或以 retries_exhausted 终态失败；
- 分组配额 → A 组限 1 时 B 组照常派发；全局上限 2 时各组额度富余也只跑 2 个；
  分组间按统一优先级序抢名额；
- 6 线程并发调度 20 任务 → 每个任务恰好派发一次；
- 在跑数量恒不超过 `max_concurrency`。
