"""The relay pipeline: observe -> prove/validate -> ordered deliver/retry.

Concurrency model: every message id has its own asyncio lock, so workers
handling *different* messages run concurrently while workers handling the
*same* message serialise to the serial-execution outcome. All state
transitions on a message happen between the same lock's acquire/release.
"""

from __future__ import annotations

import asyncio
import logging
import threading

from .chain import SimulatedChain
from .clock import Clock
from .config import RelayConfig
from .crypto import sign_message
from .errors import (
    DuplicateDeliveryError,
    RelayError,
    ReorgRollbackError,
    RetriesExhaustedError,
    StaleNonceError,
    TargetRejectedError,
    TimeoutError_,
    TransientFailureError,
)
from .models import Message, MessageStatus, Proof, RelayAttempt
from .proof import ProofValidator
from .store import RelayStore

log = logging.getLogger("relayer")


class Relayer:
    def __init__(
        self,
        chain_a: SimulatedChain,
        chain_b: SimulatedChain,
        store: RelayStore,
        config: RelayConfig,
        clock: Clock,
        *,
        attester_secret: str = "local-secret",
    ) -> None:
        self.chain_a = chain_a
        self.chain_b = chain_b
        self.store = store
        self.config = config
        self.clock = clock
        self.secret = attester_secret
        self.validator = ProofValidator(config.finality_depth, config.stale_window)
        self._locks_guard = threading.Lock()
        self._delivery_locks: dict[str, asyncio.Lock] = {}

    def chains(self) -> tuple[SimulatedChain, SimulatedChain]:
        return self.chain_a, self.chain_b

    def source_for(self, message: Message) -> SimulatedChain:
        return self.chain_a if message.source_chain == "A" else self.chain_b

    def target_for(self, message: Message) -> SimulatedChain:
        return self.chain_b if message.target_chain == "B" else self.chain_a

    def _lock_for(self, message_id: str) -> asyncio.Lock:
        with self._locks_guard:
            lock = self._delivery_locks.get(message_id)
            if lock is None:
                lock = asyncio.Lock()
                self._delivery_locks[message_id] = lock
            return lock

    # ---- observation -----------------------------------------------------
    def scan(self) -> list[str]:
        """Pull canonical messages from both chains into the store.

        Deterministic order: chain A then B, then height/nonce. Returns the
        message ids newly observed in this pass.
        """
        newly: list[str] = []
        for chain in (self.chain_a, self.chain_b):
            for message in chain.canonical_messages():
                before = self.store.get(message.message_id)
                record = self.store.observe(
                    message,
                    now=self.clock.now(),
                    note="observed in canonical block",
                )
                if before is None:
                    newly.append(message.message_id)
                    log.info(
                        "observe id=%s chain=%s height=%s nonce=%s amount=%s",
                        message.message_id,
                        chain.chain_id,
                        message.source_height,
                        message.nonce,
                        message.amount,
                    )
                else:
                    _ = record
        return newly

    # ---- delivery --------------------------------------------------------
    async def relay_once(self, message_id: str, *, proof: Proof | None = None) -> MessageStatus:
        """Run the full policy for one message exactly once (per caller).

        Raises a domain error carrying a distinct ``reason`` for every
        rejection; successful completion returns DELIVERED.
        """
        lock = self._lock_for(message_id)
        async with lock:
            record = self.store.get(message_id)
            if record is None:
                raise KeyError(f"unknown message {message_id}; scan first")

            # terminal-state short circuits give distinct, stable reasons
            if record.status is MessageStatus.DELIVERED:
                raise DuplicateDeliveryError(
                    f"{message_id} already delivered at target height "
                    f"{record.delivered_height}"
                )
            if record.status is MessageStatus.SOURCE_REORGED:
                raise ReorgRollbackError(
                    f"{message_id} source inclusion was reorged out"
                )
            if record.status is MessageStatus.FAILED:
                raise RelayError(f"{message_id} terminal-failed: {record.note}")
            if record.status is MessageStatus.EXPIRED:
                raise StaleNonceError(f"{message_id} expired in ordering window")

            source = self.chain_a if record.source_chain == "A" else self.chain_b
            target = self.chain_b if record.target_chain == "B" else self.chain_a

            # the message may have disappeared from the source canonical chain
            if not source.has_message(message_id) or not source.is_message_canonical(message_id):
                self.store.transition(
                    message_id,
                    MessageStatus.SOURCE_REORGED,
                    now=self.clock.now(),
                    note="source inclusion not canonical at relay time",
                )
                raise ReorgRollbackError(f"{message_id} no longer canonical on source")

            message = source.get_message(message_id)
            supplied_proof = proof if proof is not None else sign_message(message, self.secret)

            # ---- validation: authenticate -> finality -> ordering --------
            self.validator.authenticate(supplied_proof, message, self.secret)
            self.validator.check_finality(message, source)

            head_height = source.head().height
            next_nonce = self.store.next_nonce(message.source_chain, message.channel)
            try:
                self.validator.check_order(
                    message,
                    channel_next_nonce=next_nonce,
                    source_head_height=head_height,
                )
            except StaleNonceError as exc:
                if not exc.retryable:
                    self.store.transition(
                        message_id,
                        MessageStatus.EXPIRED,
                        now=self.clock.now(),
                        note="ordering gap never filled within stale window",
                    )
                raise

            if record.status is not MessageStatus.FINALIZED:
                self.store.transition(
                    message_id,
                    MessageStatus.FINALIZED,
                    now=self.clock.now(),
                    note="proof valid and source-final; ready for delivery",
                )

            # ---- delivery with bounded backoff retries -------------------
            try:
                delivered_height = await self._deliver_with_retry(message, target)
            except RetriesExhaustedError:
                self.store.transition(
                    message_id,
                    MessageStatus.FAILED,
                    now=self.clock.now(),
                    note="retries_exhausted",
                )
                raise
            except TargetRejectedError:
                self.store.transition(
                    message_id,
                    MessageStatus.FAILED,
                    now=self.clock.now(),
                    note="target_rejected",
                )
                raise

            # success: cursor + record move together under the message lock
            self.store.advance_nonce(message.source_chain, message.channel, message.nonce)
            self.store.transition(
                message_id,
                MessageStatus.DELIVERED,
                now=self.clock.now(),
                delivered_height=delivered_height,
                note="delivered",
            )
            log.info(
                "delivered id=%s %s->%s nonce=%s target_height=%s",
                message_id,
                message.source_chain,
                message.target_chain,
                message.nonce,
                delivered_height,
            )
            return MessageStatus.DELIVERED

    async def _deliver_with_retry(self, message: Message, target: SimulatedChain) -> int:
        """Attempt delivery with exponential backoff; never double-debits.

        target.deliver applies balances atomically and raises all retryable
        faults before moving funds, so a retry after a fault is safe.
        """
        cfg = self.config
        for attempt in range(1, cfg.max_attempts + 1):
            started = self.clock.now_ms()
            try:
                height = target.deliver(message)
            except DuplicateDeliveryError:
                # another path already applied it canonically: idempotent success
                return target.delivered_heights()[message.message_id]
            except (TransientFailureError, TimeoutError_) as exc:
                finished = self.clock.now_ms()
                self.store.record_attempt(
                    message.message_id,
                    RelayAttempt(
                        attempt=attempt,
                        started_at=started / 1000.0,
                        finished_at=finished / 1000.0,
                        outcome=exc.reason,
                        detail=str(exc),
                    ),
                )
                log.info(
                    "retryable id=%s attempt=%s reason=%s",
                    message.message_id,
                    attempt,
                    exc.reason,
                )
                if attempt == cfg.max_attempts:
                    raise RetriesExhaustedError(
                        f"{message.message_id}: {cfg.max_attempts} attempts exhausted "
                        f"(last={exc.reason})"
                    ) from exc
                backoff = min(
                    cfg.backoff_cap_ms,
                    cfg.base_backoff_ms * (2 ** (attempt - 1)),
                )
                await self.clock.sleep(backoff)
                continue
            else:
                self.store.record_attempt(
                    message.message_id,
                    RelayAttempt(
                        attempt=attempt,
                        started_at=started / 1000.0,
                        finished_at=self.clock.now_ms() / 1000.0,
                        outcome="delivered",
                        detail=f"target_height={height}",
                    ),
                )
                return height
        # unreachable: loop either returns or raises
        raise RuntimeError("retry loop fell through")  # pragma: no cover

    # ---- reorg reconciliation -------------------------------------------
    def reconcile(self) -> list[str]:
        """Rebuild records and channel cursors against canonical chain state.

        Deterministic rules after a reorg of any depth:
          * source inclusion gone -> SOURCE_REORGED (terminal). If the target
            delivery still stands it is *not* silently undone: the record
            keeps delivered_height and the note explains the disposition.
          * source stands, target delivery gone -> TARGET_REORGED,
            redeliverable; relay_once re-applies it at most once.
          * both stand -> DELIVERED unchanged.
        Channel cursors are recomputed from scratch so no nonce is skipped or
        reused. Returns affected message ids in store order.
        """
        affected: list[str] = []
        for record in self.store.all_records():
            source = self.chain_a if record.source_chain == "A" else self.chain_b
            target = self.chain_b if record.target_chain == "B" else self.chain_a
            source_ok = source.is_message_canonical(record.message_id)
            target_ok = target.is_delivery_canonical(record.message_id)

            new_status: MessageStatus | None = None
            note: str | None = None

            if not source_ok:
                if record.status is not MessageStatus.SOURCE_REORGED:
                    new_status = MessageStatus.SOURCE_REORGED
                    note = (
                        "source inclusion reverted; "
                        + (
                            f"target delivery at {record.delivered_height} stands"
                            if target_ok
                            else "no canonical target delivery"
                        )
                    )
            elif not target_ok and record.status is MessageStatus.DELIVERED:
                new_status = MessageStatus.TARGET_REORGED
                note = "target delivery reverted; message redeliverable"
            elif source_ok and target_ok and record.status is MessageStatus.TARGET_REORGED:
                new_status = MessageStatus.DELIVERED
                note = "delivery canonical again"

            if new_status is not None:
                self.store.transition(
                    record.message_id,
                    new_status,
                    now=self.clock.now(),
                    note=note or "",
                )
                affected.append(record.message_id)
                log.info("reconcile id=%s -> %s (%s)", record.message_id, new_status.value, note)

        self._rebuild_cursors()
        return affected

    def _rebuild_cursors(self) -> None:
        """Deterministically recompute per-channel next nonce from records.

        Cursor = 1 + largest n such that every nonce up to n is delivered
        canonically; messages whose source vanished are ignored for ordering.
        """
        delivered_nonces: dict[tuple[str, str], list[int]] = {}
        all_nonces: dict[tuple[str, str], list[int]] = {}
        for record in self.store.all_records():
            source = self.chain_a if record.source_chain == "A" else self.chain_b
            key = (record.source_chain, record.channel)
            all_nonces.setdefault(key, []).append(record.nonce)
            if (
                source.is_message_canonical(record.message_id)
                and record.status is MessageStatus.DELIVERED
            ):
                delivered_nonces.setdefault(key, []).append(record.nonce)

        for key, nonces in all_nonces.items():
            minimum = min(nonces)
            delivered = set(delivered_nonces.get(key, ()))
            cursor = minimum
            while cursor in delivered:
                cursor += 1
            self.store.reset_nonce(key[0], key[1], cursor)
