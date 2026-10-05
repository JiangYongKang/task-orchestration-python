# task-orchestration-python

本地任务编排与执行调度服务：用本地锁 + 内存存储模拟任务队列与执行器，
在**依赖、优先级、全局与分组并发上限、失败重试退避、心跳租约、运行中取消**
约束下给出一致且可解释的调度结果。

## 快速开始

```bash
pip install -e .            # 或 uv sync
uvicorn main:app --reload   # 启动 HTTP 服务
python -m pytest tests/ -v  # 运行全部测试（日志含输入与判定依据）
```

## 核心能力

- **提交 / 查询 / 取消**：`idempotency_key` 幂等收敛，并发提交不产生重复任务；
  运行中任务也可取消——立即作废租约、释放并发名额，下游判 `FAILED(upstream_failed)`，
  原执行者之后的心跳/成功/失败一律被拒（`already_settled`），状态不会被拉回；
  终态重复取消返回可区分原因（`already_cancelled` / `already_succeeded` / `already_failed`）。
- **调度约束**：依赖就绪、优先级（同级按创建时间）、全局并发上限 + 分组并发上限；
  分组间按统一优先级顺序公平抢名额，某组满了只跳过该组任务；
  依赖失败/取消的下游直接判 `FAILED(upstream_failed)` 而非继续排队。
- **失败重试退避**：`retry_delay_ms` + `retry_backoff_multiplier` 配置重试等待，
  第 n 次重试等待 `delay * multiplier^(n-1)`；未到点不调度，到点当轮自动重新派发，
  配了几次重试就实实在在执行几次；查询可见 `attempts` / `next_retry_at_ms` /
  `failure_reason`。
- **心跳与租约**：执行者失联租约超时 → `LOST` → 有额度按退避重新排队、无额度终态失败；
  凭证过期后（即使调度尚未回收）原执行者的迟到上报同样被拒（`lease_expired`），
  必须由重新接管的一方重新执行；重试/重调度不产生重复结算或状态回退。
- **可解释性**：每个任务保存 `events` 状态时间线与 `failure_reason/failure_detail`。

## 文档

详见 [docs/design.md](docs/design.md)：状态机图、五阶段调度规则、API 与验证方法。
