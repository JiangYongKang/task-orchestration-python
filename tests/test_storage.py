"""存储层测试：幂等键原子占位。"""

import threading

from app.models import Task, TaskState
from app.storage import TaskStore


def _make_task(key: str) -> Task:
    import logging

    logging.getLogger("test").info("输入: 构造任务 idempotency_key=%s", key)
    return Task(
        id=Task.new_id(),
        idempotency_key=key,
        payload="",
        priority=0,
        dependencies=frozenset(),
        max_retries=0,
        lease_timeout_ms=5000,
        state=TaskState.PENDING,
    )


def test_reserve_idempotency_key_converges_under_concurrency() -> None:
    store = TaskStore()
    winners: list[bool] = []
    barrier = threading.Barrier(8)

    def worker() -> None:
        barrier.wait()
        task = _make_task("dup-key")
        won = store.reserve_idempotency_key("dup-key", task.id)
        if won:
            store.put(task)
        winners.append(won)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 判定依据：原子占位只有一个 True；所有调用方收敛到同一任务
    assert winners.count(True) == 1
    resolved = store.get_by_idempotency_key("dup-key")
    assert resolved is not None
    assert store.get(resolved.id) is resolved
    assert len(store.all()) == 1
