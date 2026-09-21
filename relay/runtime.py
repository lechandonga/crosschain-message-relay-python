"""Background runtime: bounded queue, worker pool, backpressure, stats."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from .clock import Clock
from .errors import BackpressureError, RelayError
from .models import MessageStatus
from .relayer import Relayer

log = logging.getLogger("runtime")


@dataclass
class RuntimeStats:
    submitted: int = 0
    processed: int = 0
    delivered: int = 0
    failed: int = 0
    rejected_not_final: int = 0
    rejected_permanent: int = 0   # forged/replay/duplicate/reorg/order
    backpressure: int = 0
    errors_by_reason: dict[str, int] = field(default_factory=dict)


class RelayRuntime:
    """Worker pool driving the relayer.

    * the pending queue is bounded by ``queue_capacity``; ``submit`` raises
      BackpressureError when full rather than growing memory without limit;
    * at most ``max_inflight`` workers run at once (semaphore);
    * duplicate ids already queued are coalesced (at most one pending entry),
      so a burst of observations cannot enqueue the same message unbounded.
    """

    def __init__(self, relayer: Relayer, clock: Clock) -> None:
        self.relayer = relayer
        self.clock = clock
        cfg = relayer.config
        self._capacity = cfg.queue_capacity
        self._inflight = 0
        self._queue: asyncio.Queue[str] | None = None
        self._workers: list[asyncio.Task[None]] = []
        self._pending: set[str] = set()
        self.stats = RuntimeStats()

    @property
    def queue_size(self) -> int:
        return self._queue.qsize() if self._queue is not None else 0

    @property
    def inflight(self) -> int:
        return self._inflight

    @property
    def capacity(self) -> int:
        return self._capacity

    # ---- lifecycle -------------------------------------------------------
    async def start(self) -> None:
        if self._queue is not None:
            return
        self._queue = asyncio.Queue(maxsize=self._capacity)
        self._workers = [
            asyncio.create_task(self._worker(), name=f"relay-worker-{i}")
            for i in range(self.relayer.config.max_inflight)
        ]

    async def stop(self) -> None:
        if self._queue is None:
            return
        for _ in self._workers:
            await self._queue.put(None)  # poison pill per worker
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers = []
        self._queue = None

    # ---- intake ----------------------------------------------------------
    async def submit(self, message_id: str) -> bool:
        """Enqueue a message id.

        Returns False if the id is already pending (coalesced, not an error);
        raises BackpressureError if the bounded queue is full.
        """
        if self._queue is None:
            raise RuntimeError("runtime not started")
        if message_id in self._pending:
            return False
        if self._queue.qsize() >= self._capacity:
            self.stats.backpressure += 1
            raise BackpressureError(
                f"queue full ({self._capacity}); back off and retry"
            )
        self._pending.add(message_id)
        await self._queue.put(message_id)
        self.stats.submitted += 1
        return True

    async def scan_and_enqueue(self) -> list[str]:
        """Scan both chains and enqueue every newly observed message."""
        newly = self.relayer.scan()
        enqueued: list[str] = []
        for mid in newly:
            try:
                if await self.submit(mid):
                    enqueued.append(mid)
            except BackpressureError:
                # bounded: stop draining this pass; caller retries later
                break
        return enqueued

    async def join(self) -> None:
        """Wait until all currently queued work has been picked up."""
        if self._queue is not None:
            await self._queue.join()

    def pending_ids(self) -> set[str]:
        return set(self._pending)

    # ---- worker ----------------------------------------------------------
    async def _worker(self) -> None:
        assert self._queue is not None
        while True:
            item = await self._queue.get()
            try:
                if item is None:
                    return
                self._inflight += 1
                try:
                    await self._process(item)
                finally:
                    self._inflight -= 1
            finally:
                if item is not None:
                    self._pending.discard(item)
                    self._queue.task_done()

    async def _process(self, message_id: str) -> None:
        self.stats.processed += 1
        try:
            status = await self.relayer.relay_once(message_id)
        except RelayError as exc:
            self.stats.errors_by_reason[exc.reason] = (
                self.stats.errors_by_reason.get(exc.reason, 0) + 1
            )
            reason = exc.reason
            if reason == "not_finalized":
                self.stats.rejected_not_final += 1
            elif reason in (
                "forged_proof",
                "replay_detected",
                "duplicate_delivery",
                "reorg_rollback",
                "stale_nonce",
                "target_rejected",
                "retries_exhausted",
            ):
                self.stats.rejected_permanent += 1
            if reason in ("target_rejected", "retries_exhausted"):
                self.stats.failed += 1
            log.info(
                "process id=%s rejected reason=%s retryable=%s",
                message_id, reason, exc.retryable,
            )
        except Exception:  # pragma: no cover - defensive
            log.exception("process id=%s unexpected error", message_id)
        else:
            if status is MessageStatus.DELIVERED:
                self.stats.delivered += 1
            log.info("process id=%s status=%s", message_id, status.value)
