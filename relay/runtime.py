"""Background runtime: bounded queue, worker pool, backpressure, ticks."""

from __future__ import annotations

import asyncio

from .clock import Clock
from .relayer import Relayer
from .errors import BackpressureError


class RelayRuntime:
    """Async worker pool that drives the relayer on a deterministic clock.

    The queue is bounded (``queue_capacity``); full queues raise
    BackpressureError instead of growing without limit. At most
    ``max_inflight`` workers process messages concurrently.
    """

    def __init__(self, relayer: Relayer, clock: Clock) -> None:
        self.relayer = relayer
        self.clock = clock
        self._queue: "asyncio.Queue[str | None]" | None = None
        self._workers: list[asyncio.Task[None]] = []
        self._inflight = 0
        self._processed = 0
        self._rejected_full = 0

    @property
    def inflight(self) -> int:
        return self._inflight

    @property
    def processed(self) -> int:
        return self._processed

    @property
    def queue_size(self) -> int:
        raise NotImplementedError

    async def start(self) -> None:
        raise NotImplementedError

    async def stop(self) -> None:
        raise NotImplementedError

    async def submit(self, message_id: str) -> None:
        """Enqueue a message id; raise BackpressureError if at capacity."""
        raise NotImplementedError

    async def scan_and_enqueue(self) -> list[str]:
        raise NotImplementedError

    async def join(self) -> None:
        """Wait until the queue drains (workers keep running)."""
        raise NotImplementedError

    async def _worker(self) -> None:
        raise NotImplementedError
