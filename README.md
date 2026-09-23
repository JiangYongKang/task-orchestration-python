# task-orchestration-python

本地任务编排与执行调度服务：用本地锁 + 内存存储模拟任务队列与执行器，
在**依赖、优先级、并发上限、失败重试、心跳租约**约束下给出一致且可解释的调度结果。

## 快速开始

```bash
pip install -e .            # 或 uv sync
uvicorn main:app --reload   # 启动 HTTP 服务
python -m pytest tests/ -v  # 运行全部测试（日志含输入与判定依据）
```

## 核心能力

- **提交 / 查询 / 取消**：`idempotency_key` 幂等收敛，并发提交不产生重复任务；
  终态取消等非法跃迁返回可区分原因（`already_cancelled` / `already_succeeded` /
  `already_failed` / `task_running`）。
- **调度约束**：依赖就绪、优先级（同级按创建时间）、并发上限；
  依赖失败/取消的下游直接判 `FAILED(upstream_failed)` 而非继续排队。
- **心跳与租约**：执行者失联租约超时 → `LOST` → 有额度重新排队、无额度终态失败；
  迟到结算一律拒绝，重试/重调度不产生重复结算或状态回退。
- **可解释性**：每个任务保存 `events` 状态时间线与 `failure_reason/failure_detail`。

## 文档

详见 [docs/design.md](docs/design.md)：状态机图、四阶段调度规则、API 与验证方法。
