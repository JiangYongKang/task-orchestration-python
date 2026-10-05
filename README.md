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
  运行中的任务也可取消——立即作废租约、释放并发名额、下游级联判 FAILED，
  原执行者迟到的任何上报都不再算数；终态取消返回可区分原因
  （`already_cancelled` / `already_succeeded` / `already_failed`）。
- **调度约束**：依赖就绪、优先级（同级按创建时间）、全局并发上限 +
  按业务分组的并发配额（`group_limits`），分组间按优先级公平抢名额；
  依赖失败/取消的下游直接判 `FAILED(upstream_failed)` 而非继续排队。
- **失败重试闭环**：`retry_delay_ms` + `retry_backoff_multiplier` 配置退避等待，
  未到点不调度、到点自动重新执行；配置的每次重试都真实发生，
  查询可见 `attempts` / `next_retry_at_ms` / `failure_reason`。
- **心跳与租约**：执行者失联租约超时 → `LOST` → 有额度走重试闭环、无额度终态失败；
  迟到结算一律拒绝；租约已过期但调度尚未回收时，原执行者补交的上报
  同样拒绝（`lease_expired`），必须由重新接管的一方重新跑。
- **可解释性**：每个任务保存 `events` 状态时间线与 `failure_reason/failure_detail`。

## 文档

详见 [docs/design.md](docs/design.md)：状态机图、五阶段调度规则、取消/重试/分组配额、
与原有能力的兼容范围、API 与本地验证方法。
