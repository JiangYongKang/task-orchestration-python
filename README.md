# 本地任务编排与执行调度服务

一个纯本地实现的任务编排服务：用线程安全的内存存储模拟任务队列，用线程池模拟执行器，
在**依赖、优先级、并发上限、失败重试、心跳租约**等约束下给出一致且可解释的调度结果。

* 语言/运行时：Python ≥ 3.14，FastAPI（HTTP 适配层）
* 核心逻辑不依赖 Web 框架，可直接在进程内以库方式使用与测试
* 时间通过可注入的 `Clock` 控制，租约超时类行为在测试中完全确定、可复现

## 快速开始

```bash
# 安装依赖（含开发依赖 pytest / httpx）
uv sync

# 启动 HTTP 服务（含每秒一次的后台调度 + 租约回收守护线程）
uv run fastapi dev main.py
# 打开 http://127.0.0.1:8000/docs 查看接口
```

提交一个任务并手动驱动调度（测试/演示时也可不依赖后台线程）：

```bash
curl -s -X POST localhost:8000/tasks -H 'content-type: application/json' \
  -d '{"handler":"echo","payload":{"k":"v"},"idempotency_key":"job-1"}'

curl -s -X POST localhost:8000/scheduler/tick         # 触发一轮调度，返回判定依据
curl -s localhost:8000/tasks/task-000001              # 查询状态
```

内置示例处理器：`echo`（原样回显）、`flaky`（`payload.fail_times` 控制前 N 次失败）。

## 运行测试

```bash
uv run pytest                                   # 静默摘要
uv run pytest --log-cli-level=INFO -s           # 打印每个用例的输入与判定依据
uv run pytest tests/test_lease_retry.py -v      # 只跑租约/重试场景
```

测试覆盖：

| 场景 | 文件 |
| --- | --- |
| 并发提交同一幂等键只产生一个任务 | `tests/test_submission_concurrency.py` |
| 取消的非法状态跃迁（错误码可区分） | `tests/test_cancellation.py` |
| 依赖就绪判定、优先级、配额不超限、并发 tick 单次执行权 | `tests/test_scheduling.py` |
| 依赖失败/取消沿链路与菱形传播为 BLOCKED | `tests/test_dependency_propagation.py` |
| 心跳续租、失联回收、超时重试、过期结算拒绝（fencing） | `tests/test_lease_retry.py` |
| HTTP 状态码/错误码映射、幂等响应 | `tests/test_api.py` |

## 作为库使用

```python
from orchestration.clock import FakeClock
from orchestration.executor import LocalExecutor
from orchestration.service import OrchestrationService
from orchestration.models import TaskSpec

executor = LocalExecutor()
executor.register("echo", lambda payload, ctx: {"echo": payload})
service = OrchestrationService(FakeClock(), executor,
                               max_concurrency=4, lease_timeout=30.0)

r = service.submit(TaskSpec(
    handler="echo", payload={"x": 1},
    idempotency_key="job-1",   # 同 key 并发提交也只会有一个任务
    dependencies=("task-000001",),
    priority=10, max_retries=3,
))
service.schedule_tick()              # 返回 list[ScheduleDecision]（判定依据）
service.reclaim_expired_leases()     # 回收失联任务
```

## 深入阅读

* [`docs/design.md`](docs/design.md) —— 状态机、调度规则、幂等与租约的设计细节
* [`docs/api.md`](docs/api.md) —— HTTP 接口与错误码对照表
* [`docs/verification.md`](docs/verification.md) —— 验证方法与关键场景复现步骤
