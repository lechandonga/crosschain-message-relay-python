"""Deterministic virtual clock and virtual-time event loop.

Real wall-clock time is never used by relay logic. ``VirtualTimeEventLoop``
jumps its clock straight to the next scheduled timer callback on every
iteration, so:

  * concurrent ``sleep``s resume in deadline order (earliest first),
  * backoff/timeouts complete instantly in real time yet take the exact
    virtual duration,
  * runs are bit-for-bit reproducible.

Workers parked on a queue/future (no deadline) simply wait for real IO like
a normal loop, which is what keeps a served process alive when idle.
"""

from __future__ import annotations

import asyncio
import contextvars
import heapq


class Clock:
    """Monotonic millisecond clock owned by the (possibly virtual) loop."""

    def __init__(self, start_ms: int = 0) -> None:
        self._now_ms = start_ms

    def now_ms(self) -> int:
        return self._now_ms

    def now(self) -> float:
        return self._now_ms / 1000.0

    def advance(self, delta_ms: int) -> int:
        """Explicitly move time forward (synchronous tests / bookkeeping)."""
        if delta_ms < 0:
            raise ValueError("clock cannot move backwards")
        self._now_ms += delta_ms
        return self._now_ms

    def advance_to(self, when_ms: int) -> int:
        if when_ms < self._now_ms:
            raise ValueError("clock cannot move backwards")
        self._now_ms = when_ms
        return self._now_ms

    async def sleep(self, ms: int) -> None:
        if ms <= 0:
            await asyncio.sleep(0)
            return
        # Scheduled on the virtual loop: the loop fast-forwards to 0.001*ms.
        await asyncio.sleep(ms / 1000.0)


class VirtualTimeEventLoop(asyncio.SelectorEventLoop):
    """Selector event loop whose time only jumps to scheduled timers."""

    def __init__(self, clock: Clock) -> None:
        self._vclock = clock
        super().__init__()

    def time(self) -> float:  # type: ignore[override]
        return self._vclock.now()

    def _run_once(self) -> None:  # type: ignore[override]
        """Process everything runnable at one virtual instant, then return.

        Semantics, in order:
          1. If callbacks are ready (or timers are already due at the current
             instant), drain *all* of them — including tasks that those
             callbacks make newly ready at the same instant — without moving
             the clock. This is what lets a timer firing at t=50 resume its
             coroutine while the clock still reads 50.
          2. Otherwise, if future timers exist, fast-forward virtual time to
             the earliest deadline (no wall-clock wait).
          3. Otherwise block on the selector for real IO/events (keeps an
             idle served process alive without busy-spinning).
        The base runner calls us repeatedly, so we only need one pass.
        """
        while True:
            scheduled = self._scheduled
            while scheduled and getattr(scheduled[0], "_cancelled", False):
                heapq.heappop(scheduled)

            # expire any timers due at the current instant
            now = self.time()
            due: list[asyncio.Handle] = []
            while scheduled and scheduled[0].when() <= now + 1e-6:
                handle = heapq.heappop(scheduled)
                if not getattr(handle, "_cancelled", False):
                    due.append(handle)
            for handle in due:
                self._ready.append(handle)

            if self._ready:
                ntodo = len(self._ready)
                for _ in range(ntodo):
                    handle = self._ready.popleft()
                    if not getattr(handle, "_cancelled", False):
                        handle._run()
                # drain work newly queued at the same instant, then return
                if self._ready:
                    continue
                return

            # nothing ready: advance or block
            if scheduled:
                nxt = scheduled[0].when()
                if nxt > self.time():
                    self._vclock.advance_to(int(round(nxt * 1000)))
                continue  # now the timer is due -> handled on next pass
            # no ready, no timers: wait for real IO without blocking forever
            event_list = self._selector.select(0.05)
            self._process_events(event_list)
            return

class VirtualTimeEventLoopPolicy(asyncio.DefaultEventLoopPolicy):
    def __init__(self, clock: Clock) -> None:
        super().__init__()
        self._vclock = clock

    def new_event_loop(self) -> asyncio.AbstractEventLoop:  # type: ignore[override]
        return VirtualTimeEventLoop(self._vclock)


def run_virtual(main_coro_factory, *, start_ms: int = 0):
    """Run ``await main_coro_factory(clock)`` on a virtual-time loop.

    ``main_coro_factory`` receives the shared :class:`Clock` so chains and
    the relayer can be constructed inside the loop with the same time base.
    """
    clock = Clock(start_ms=start_ms)
    loop = VirtualTimeEventLoop(clock)
    try:
        return loop.run_until_complete(main_coro_factory(clock))
    finally:
        loop.close()


_context: contextvars.ContextVar[Clock | None] = contextvars.ContextVar(
    "virtual_clock", default=None
)
