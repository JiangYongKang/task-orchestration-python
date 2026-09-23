"""可注入的时钟，保证测试结果可复现。"""

from __future__ import annotations

import time


class Clock:
    """单调毫秒时钟。"""

    def now_ms(self) -> int:
        raise NotImplementedError


class SystemClock(Clock):
    def now_ms(self) -> int:
        return time.monotonic_ns() // 1_000_000


class FakeClock(Clock):
    """测试用手动推进的时钟。"""

    def __init__(self, start_ms: int = 0) -> None:
        self._now = start_ms

    def now_ms(self) -> int:
        return self._now

    def advance(self, ms: int) -> None:
        self._now += ms
