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
        # serialises [revalidate + deliver + finalize] against reconcile():
        # both sides take this re-entrant lock, so a reconciliation can never
        # interleave inside a delivery attempt -- not even when reconcile is
        # invoked from another thread (e.g. the HTTP threadpool)
        self._reorg_guard = threading.RLock()

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
            with self._reorg_guard:
                if not source.has_message(message_id) or not source.is_message_canonical(
                    message_id
                ):
                    self._mark_source_reorged(message_id, source, target)
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
                    with self._reorg_guard:
                        current = self.store.get(message_id)
                        if current is None or current.status is not MessageStatus.SOURCE_REORGED:
                            funds = self._release_locked_funds(message, source)
                            self.store.transition(
                                message_id,
                                MessageStatus.EXPIRED,
                                now=self.clock.now(),
                                note=f"ordering gap never filled within stale window; {funds}",
                            )
                raise

            # re-validate under the reorg guard before arming delivery: a
            # reconcile that landed during validation must not be overwritten
            with self._reorg_guard:
                self._ensure_still_deliverable(message, source, target)
                if record.status is not MessageStatus.FINALIZED:
                    self.store.transition(
                        message_id,
                        MessageStatus.FINALIZED,
                        now=self.clock.now(),
                        note="proof valid and source-final; ready for delivery",
                    )

            # ---- delivery with bounded backoff retries -------------------
            try:
                delivered_height = await self._deliver_with_retry(message, source, target)
            except RetriesExhaustedError:
                with self._reorg_guard:
                    self._fail_terminally(message, source, reason="retries_exhausted")
                raise
            except TargetRejectedError:
                with self._reorg_guard:
                    self._fail_terminally(message, source, reason="target_rejected")
                raise

            # success: the record, the channel cursor and the source send
            # settlement were already finalised atomically with the winning
            # delivery attempt (inside the reorg guard, see _finalize_delivery)
            log.info(
                "delivered id=%s %s->%s nonce=%s target_height=%s",
                message_id,
                message.source_chain,
                message.target_chain,
                message.nonce,
                delivered_height,
            )
            return MessageStatus.DELIVERED

    # ---- terminal settlement ----------------------------------------------
    def _mark_source_reorged(
        self, message_id: str, source: SimulatedChain, target: SimulatedChain
    ) -> None:
        """Converge a record to the terminal source-reorged state.

        Same rule reconcile() applies: if the target delivery stands the
        escrow lock is retained (exactly one funds effect); otherwise the
        note records where the locked funds went.
        """
        note = "source inclusion not canonical at relay time"
        if target.is_delivery_canonical(message_id):
            source.ensure_send_settled(message_id)
            note += "; target delivery stands, escrow lock retained"
        else:
            note += f"; funds {source.send_disposition(message_id)}"
        self.store.transition(
            message_id,
            MessageStatus.SOURCE_REORGED,
            now=self.clock.now(),
            note=note,
        )

    def _ensure_still_deliverable(
        self, message: Message, source: SimulatedChain, target: SimulatedChain
    ) -> None:
        """Re-validate right before a delivery attempt (under the reorg guard).

        A reconcile may have run while this worker was parked on backoff: the
        message could now be terminal (source-reorged / failed / expired) or
        already delivered. Retrying past any of those would either credit the
        target on top of a refund or overwrite a terminal conclusion, so the
        attempt refuses with the same distinct reasons relay_once uses. A
        vanished source inclusion that no reconcile has seen yet is converged
        to the same terminal state reconcile would choose.
        """
        record = self.store.get(message.message_id)
        if record is not None:
            if record.status is MessageStatus.DELIVERED:
                raise DuplicateDeliveryError(
                    f"{message.message_id} already delivered at target height "
                    f"{record.delivered_height}"
                )
            if record.status is MessageStatus.SOURCE_REORGED:
                raise ReorgRollbackError(
                    f"{message.message_id} source inclusion was reorged out"
                )
            if record.status is MessageStatus.FAILED:
                raise RelayError(f"{message.message_id} terminal-failed: {record.note}")
            if record.status is MessageStatus.EXPIRED:
                raise StaleNonceError(
                    f"{message.message_id} expired in ordering window"
                )
        if not source.has_message(message.message_id) or not source.is_message_canonical(
            message.message_id
        ):
            self._mark_source_reorged(message.message_id, source, target)
            raise ReorgRollbackError(
                f"{message.message_id} no longer canonical on source"
            )

    def _finalize_delivery(self, message: Message, source: SimulatedChain, height: int) -> None:
        """Atomically settle a successful delivery (under the reorg guard).

        Cursor + record move together, and the source send is marked settled
        so a later source reorg retains the escrow backing instead of
        refunding the sender. If a reconciliation already pinned the terminal
        source-reorged conclusion for this message, the delivery still stands
        on the target, so the escrow is retained and the reorg conclusion is
        kept -- never overwritten back to a success.
        """
        record = self.store.get(message.message_id)
        if record is not None and record.status is MessageStatus.SOURCE_REORGED:
            source.ensure_send_settled(message.message_id)
            self.store.transition(
                message.message_id,
                MessageStatus.SOURCE_REORGED,
                now=self.clock.now(),
                delivered_height=height,
                note="source inclusion reverted; target delivery stands; "
                "escrow lock retained",
            )
            raise ReorgRollbackError(
                f"{message.message_id} source inclusion was reorged out"
            )
        source.confirm_external_delivery(message.message_id)
        self.store.advance_nonce(message.source_chain, message.channel, message.nonce)
        self.store.transition(
            message.message_id,
            MessageStatus.DELIVERED,
            now=self.clock.now(),
            delivered_height=height,
            note="delivered",
        )

    def _release_locked_funds(self, message: Message, source: SimulatedChain) -> str:
        """Give a terminal message's locked escrow a deterministic home.

        The sender is refunded exactly once across any interleaving of
        failure handling, reorgs and reconciliations. The returned phrase is
        appended to the record note so the disposition stays queryable.
        """
        if source.refund_failed_send(message.message_id):
            log.info(
                "refund id=%s sender=%s amount=%s (terminal)",
                message.message_id,
                message.sender,
                message.amount,
            )
            return f"refunded {message.amount} to {message.sender}"
        return f"funds {source.send_disposition(message.message_id)}"

    def _fail_terminally(self, message: Message, source: SimulatedChain, *, reason: str) -> None:
        """Settle a permanently failed message: funds home + channel unblocked.

        A permanent failure only fails *this* message: its locked escrow is
        refunded (exactly once) and the channel cursor consumes its nonce, so
        later messages on the same channel keep flowing instead of stalling
        behind a terminal record. A terminal source-reorged conclusion
        already pinned by reconciliation is never overwritten.
        """
        record = self.store.get(message.message_id)
        if record is not None and record.status is MessageStatus.SOURCE_REORGED:
            return
        funds = self._release_locked_funds(message, source)
        self.store.advance_nonce(message.source_chain, message.channel, message.nonce)
        self.store.transition(
            message.message_id,
            MessageStatus.FAILED,
            now=self.clock.now(),
            note=f"{reason}; {funds}",
        )

    async def _deliver_with_retry(
        self, message: Message, source: SimulatedChain, target: SimulatedChain
    ) -> int:
        """Attempt delivery with exponential backoff; never double-debits.

        target.deliver applies balances atomically and raises all retryable
        faults before moving funds, so a retry after a fault is safe. Every
        attempt first re-validates the message under the reorg guard: a
        reconcile that ran while this worker was parked on backoff turns the
        next attempt into a refusal instead of a late delivery. The winning
        attempt finalises record + cursor + settlement inside the same guard
        section, so the pair is serialisable against reconcile().
        """
        cfg = self.config
        for attempt in range(1, cfg.max_attempts + 1):
            started = self.clock.now_ms()
            backoff = 0
            with self._reorg_guard:
                self._ensure_still_deliverable(message, source, target)
                try:
                    height = target.deliver(message)
                except DuplicateDeliveryError:
                    # another path already applied it canonically: idempotent success
                    height = target.delivered_heights()[message.message_id]
                    self._finalize_delivery(message, source, height)
                    return height
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
                    self._finalize_delivery(message, source, height)
                    return height
            # the backoff sleep happens *outside* the reorg guard, so a
            # reconcile can proceed while this worker is parked; the next
            # attempt re-validates before touching the target
            await self.clock.sleep(backoff)
        # unreachable: loop either returns or raises
        raise RuntimeError("retry loop fell through")  # pragma: no cover

    # ---- reorg reconciliation -------------------------------------------
    def reconcile(self) -> list[str]:
        """Rebuild records and channel cursors against canonical chain state.

        Deterministic rules after a reorg of any depth:
          * source inclusion gone -> SOURCE_REORGED (terminal). If the target
            delivery still stands it is *not* silently undone: the record
            keeps delivered_height, the source escrow lock is retained (or
            clawed back exactly once if the reorg had already refunded), and
            the note explains the disposition -- the message keeps exactly
            one funds effect across both chains. If no canonical target
            delivery exists either, the orphaned escrow lock is released back
            to the sender exactly once, so locked funds never hang forever.
          * source stands, target delivery gone -> TARGET_REORGED,
            redeliverable; relay_once re-applies it at most once.
          * both stand -> DELIVERED unchanged.
        Channel cursors are recomputed from scratch so no nonce is skipped or
        reused. Returns affected message ids in store order. Idempotent:
        running it again moves no funds and changes no record. The whole pass
        runs under the reorg guard, so it is serialisable against in-flight
        delivery attempts even across threads.
        """
        with self._reorg_guard:
            return self._reconcile_locked()

    def _reconcile_locked(self) -> list[str]:
        affected: list[str] = []
        for record in self.store.all_records():
            source = self.chain_a if record.source_chain == "A" else self.chain_b
            target = self.chain_b if record.target_chain == "B" else self.chain_a
            source_ok = source.is_message_canonical(record.message_id)
            target_ok = target.is_delivery_canonical(record.message_id)

            if not source_ok:
                delivered_h = record.delivered_height
                if target_ok:
                    # the delivery stands: the source escrow lock must back it
                    # exactly once -- retained across the reorg, or clawed back
                    # if the reorg refunded before the delivery was known
                    compensated = source.ensure_send_settled(record.message_id)
                    moved = compensated
                    if delivered_h is None:
                        delivered_h = target.delivered_heights().get(record.message_id)
                    funds_note = (
                        "sender refund clawed back to escrow (compensated)"
                        if compensated
                        else "escrow lock retained"
                    )
                    note = (
                        f"source inclusion reverted; target delivery at "
                        f"{delivered_h} stands; {funds_note}"
                    )
                else:
                    # no valid delivery anywhere: the escrow lock backs
                    # nothing, so it is released to the sender exactly once
                    # instead of hanging in escrow forever
                    released = source.release_orphaned_send(record.message_id)
                    moved = released
                    if released:
                        m = source.get_message(record.message_id)
                        funds_note = f"refunded {m.amount} to {m.sender}"
                    else:
                        funds_note = f"funds {source.send_disposition(record.message_id)}"
                    note = (
                        "source inclusion reverted; no canonical target "
                        f"delivery; {funds_note}"
                    )
                # transition when the status changes, or when this pass moved
                # funds for an already-terminal record (the note must catch
                # up with the new disposition); otherwise leave it untouched
                # so repeated reconciliations are exact no-ops
                if record.status is not MessageStatus.SOURCE_REORGED or moved:
                    self.store.transition(
                        record.message_id,
                        MessageStatus.SOURCE_REORGED,
                        now=self.clock.now(),
                        delivered_height=delivered_h,
                        note=note,
                    )
                    affected.append(record.message_id)
                    log.info(
                        "reconcile id=%s -> %s (%s)",
                        record.message_id,
                        MessageStatus.SOURCE_REORGED.value,
                        note,
                    )
            elif not target_ok and record.status is MessageStatus.DELIVERED:
                self.store.transition(
                    record.message_id,
                    MessageStatus.TARGET_REORGED,
                    now=self.clock.now(),
                    note="target delivery reverted; message redeliverable",
                )
                affected.append(record.message_id)
                log.info(
                    "reconcile id=%s -> %s (target delivery reverted)",
                    record.message_id,
                    MessageStatus.TARGET_REORGED.value,
                )
            elif source_ok and target_ok and record.status is MessageStatus.TARGET_REORGED:
                self.store.transition(
                    record.message_id,
                    MessageStatus.DELIVERED,
                    now=self.clock.now(),
                    note="delivery canonical again",
                )
                affected.append(record.message_id)
                log.info(
                    "reconcile id=%s -> %s (delivery canonical again)",
                    record.message_id,
                    MessageStatus.DELIVERED.value,
                )

        self._rebuild_cursors()
        return affected

    def _rebuild_cursors(self) -> None:
        """Deterministically recompute per-channel next nonce from records.

        Cursor = 1 + largest n such that every nonce up to n is *consumed*:
        delivered canonically, or terminally settled (failed / expired) --
        a permanent failure must not wedge the channel behind it. Messages
        whose source inclusion vanished (source_reorged) are *not* consumed:
        their nonce is free for a resubmission.
        """
        consumed_nonces: dict[tuple[str, str], list[int]] = {}
        all_nonces: dict[tuple[str, str], list[int]] = {}
        for record in self.store.all_records():
            source = self.chain_a if record.source_chain == "A" else self.chain_b
            key = (record.source_chain, record.channel)
            all_nonces.setdefault(key, []).append(record.nonce)
            consumed = (
                record.status in (MessageStatus.FAILED, MessageStatus.EXPIRED)
                or (
                    source.is_message_canonical(record.message_id)
                    and record.status is MessageStatus.DELIVERED
                )
            )
            if consumed:
                consumed_nonces.setdefault(key, []).append(record.nonce)

        for key, nonces in all_nonces.items():
            minimum = min(nonces)
            consumed = set(consumed_nonces.get(key, ()))
            cursor = minimum
            while cursor in consumed:
                cursor += 1
            self.store.reset_nonce(key[0], key[1], cursor)
