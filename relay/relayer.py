"""The relay pipeline: observe -> prove/validate -> ordered deliver/retry.

Concurrency model: every message id has its own asyncio lock, so workers
handling *different* messages run concurrently while workers handling the
*same* message serialise to the serial-execution outcome. All state
transitions on a message happen between the same lock's acquire/release.

A backoff ``sleep`` parks the asyncio coroutine but does not block other
threads -- and ``reconcile`` / the HTTP reorg endpoints run synchronously in
worker threads. Each message therefore additionally has a short-lived
*commit* ``threading.Lock`` guarding only the atomic "verify -> move funds
-> write record/cursor" sections. A reconciliation can run during a backoff
but can never interleave with a funds/record commit, so every outcome is
equivalent to some serial ordering.
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
        # per-message threading locks for the funds/record commit sections;
        # they serialise async delivery commits against sync reconciliation
        # (which may run in another thread while a retry sleeps in this one)
        self._commit_locks: dict[str, threading.Lock] = {}

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

    def _commit_lock_for(self, message_id: str) -> threading.Lock:
        """Threading lock guarding one message's funds/record commit section.

        Held only for brief synchronous sections (never across a sleep), so
        it makes delivery commits and reconciliation mutually exclusive
        without ever blocking retries on their backoff.
        """
        with self._locks_guard:
            lock = self._commit_locks.get(message_id)
            if lock is None:
                lock = threading.Lock()
                self._commit_locks[message_id] = lock
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
            terminal = self._terminal_error_for(record)
            if terminal is not None:
                raise terminal

            source = self.chain_a if record.source_chain == "A" else self.chain_b
            target = self.chain_b if record.target_chain == "B" else self.chain_a

            # the message may have disappeared from the source canonical chain
            if not source.has_message(message_id) or not source.is_message_canonical(message_id):
                message = source.get_message(message_id)
                with self._commit_lock_for(message_id):
                    status, note, delivered_h = self._settle_source_removed(
                        message, source, target
                    )
                    self.store.transition(
                        message_id,
                        status,
                        now=self.clock.now(),
                        delivered_height=delivered_h,
                        note=note,
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
                    with self._commit_lock_for(message_id):
                        # a reconcile on another thread may have settled a
                        # reorg between the check and this commit; a terminal
                        # verdict already on the record must not be overwritten
                        fresh = self.store.get(message_id)
                        if fresh is not None and self._terminal_error_for(fresh) is None:
                            funds = self._release_locked_funds(message, source)
                            self.store.transition(
                                message_id,
                                MessageStatus.EXPIRED,
                                now=self.clock.now(),
                                note=f"ordering gap never filled within stale window; {funds}",
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
                delivered_height = await self._deliver_with_retry(message, source, target)
            except RetriesExhaustedError as exc:
                self._fail_terminally(message, source, reason="retries_exhausted")
                # if a concurrent reconcile already owned the terminal verdict,
                # surface that verdict's reason instead of masking it
                fresh = self.store.get(message_id)
                if fresh is not None and fresh.status is not MessageStatus.FAILED:
                    raise self._terminal_error_for(fresh) from exc
                raise
            except TargetRejectedError as exc:
                self._fail_terminally(message, source, reason="target_rejected")
                fresh = self.store.get(message_id)
                if fresh is not None and fresh.status is not MessageStatus.FAILED:
                    raise self._terminal_error_for(fresh) from exc
                raise

            # success: cursor + record move together under the message lock,
            # and the source send is marked settled so a later source reorg
            # retains the escrow backing instead of refunding the sender.
            # Re-check under the commit lock: a reconcile may have settled a
            # reorg between the target commit and this continuation (it runs
            # on another thread); if so, the just-applied delivery is
            # reconciled against that fact in one atomic disposition and the
            # terminal record stands (target payout stands, escrow retained).
            with self._commit_lock_for(message_id):
                latest = self.store.get(message_id)
                if latest is not None and latest.status is MessageStatus.SOURCE_REORGED:
                    status, note, delivered_h = self._settle_source_removed(
                        message, source, target
                    )
                    self.store.transition(
                        message_id,
                        status,
                        now=self.clock.now(),
                        delivered_height=delivered_h,
                        note=note,
                    )
                    raise ReorgRollbackError(
                        f"{message_id} source reorged during delivery commit"
                    )
                source.confirm_external_delivery(message_id)
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

    # ---- terminal settlement ----------------------------------------------
    def _terminal_error_for(self, record) -> RelayError | None:
        """The rejection a terminal record must keep returning (stable reason)."""
        if record.status is MessageStatus.DELIVERED:
            return DuplicateDeliveryError(
                f"{record.message_id} already delivered at target height "
                f"{record.delivered_height}"
            )
        if record.status is MessageStatus.SOURCE_REORGED:
            return ReorgRollbackError(f"{record.message_id} source inclusion was reorged out")
        if record.status is MessageStatus.FAILED:
            return RelayError(f"{record.message_id} terminal-failed: {record.note}")
        if record.status is MessageStatus.EXPIRED:
            return StaleNonceError(f"{record.message_id} expired in ordering window")
        return None

    def _settle_source_removed(
        self,
        message: Message,
        source: SimulatedChain,
        target: SimulatedChain,
    ) -> tuple[MessageStatus, str, int | None]:
        """Settle a message whose source inclusion is no longer canonical.

        This is the *only* place a SOURCE_REORGED terminal record is decided,
        shared by the delivery path (a retry waking up after a backoff during
        which a source reorg was reconciled) and by ``reconcile``. The state
        is therefore independent of interleaving order and recomputation is
        idempotent.

        Dispositions (each message ends with exactly one funds effect):
          * a canonical target delivery still stands -> it is NOT undone;
            the escrow backing is retained (or a premature refund is clawed
            back exactly once); history keeps ``delivered_height``;
          * no canonical target delivery stands -> nothing can be redelivered,
            so any orphaned lock is returned to the sender exactly once.
        The message is terminal and not redeliverable in both cases.
        """
        message_id = message.message_id
        target_ok = target.is_delivery_canonical(message_id)
        delivered_h = target.delivered_heights().get(message_id) if target_ok else None
        if target_ok:
            source.ensure_send_settled(message_id)
            # the compensation history is persistent, so the note is stable
            # across every later reconciliation (idempotent affected list)
            compensated = source.was_send_compensated(message_id)
            note = (
                f"source inclusion reverted; target delivery at {delivered_h} stands; "
                + (
                    "sender refund clawed back to escrow (compensated); "
                    if compensated
                    else "escrow lock retained; "
                )
                + "not redeliverable"
            )
            return MessageStatus.SOURCE_REORGED, note, delivered_h
        _, disposition = source.release_unbacked_send(message_id)
        # the note describes the *stable final disposition*, not whether this
        # particular call moved funds, so repeated reconciliation reproduces
        # it byte-for-byte (affected stays empty on the second run)
        note = (
            f"source inclusion reverted; no canonical target delivery; "
            f"{disposition} {message.amount} to {message.sender}; "
            "target rollback history preserved; not redeliverable"
        )
        return MessageStatus.SOURCE_REORGED, note, None

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
        behind a terminal record. If a reorg reconciliation already committed
        a terminal verdict concurrently, that verdict is left untouched.
        """
        with self._commit_lock_for(message.message_id):
            fresh = self.store.get(message.message_id)
            if fresh is not None and self._terminal_error_for(fresh) is not None:
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
        self,
        message: Message,
        source: SimulatedChain,
        target: SimulatedChain,
    ) -> int:
        """Attempt delivery with exponential backoff; never double-debits.

        target.deliver applies balances atomically and raises all retryable
        faults before moving funds, so a retry after a fault is safe.

        Every attempt re-checks, *inside the per-message commit lock* and
        right before the target commit, that the source inclusion is still
        canonical and the record has not been terminally settled by a
        reconciliation (which may run on another thread while this coroutine
        is parked on a backoff sleep). If a source reorg has been reconciled,
        the terminal disposition is committed here and the attempt aborts
        before any target credit can be created -- a sleeping retry can
        never override a settled reorg conclusion.
        """
        cfg = self.config
        message_id = message.message_id
        for attempt in range(1, cfg.max_attempts + 1):
            started = self.clock.now_ms()
            try:
                with self._commit_lock_for(message_id):
                    fresh = self.store.get(message_id)
                    if fresh is not None:
                        terminal = self._terminal_error_for(fresh)
                        if terminal is not None:
                            raise terminal
                    if not source.is_message_canonical(message_id):
                        status, note, delivered_h = self._settle_source_removed(
                            message, source, target
                        )
                        self.store.transition(
                            message_id,
                            status,
                            now=self.clock.now(),
                            delivered_height=delivered_h,
                            note=note,
                        )
                        raise ReorgRollbackError(
                            f"{message_id} source inclusion reorged during retry backoff"
                        )
                    try:
                        height = target.deliver(message)
                    except DuplicateDeliveryError:
                        # another path already applied it canonically within
                        # this commit section: idempotent success
                        height = target.delivered_heights()[message_id]
                    self.store.record_attempt(
                        message_id,
                        RelayAttempt(
                            attempt=attempt,
                            started_at=started / 1000.0,
                            finished_at=self.clock.now_ms() / 1000.0,
                            outcome="delivered",
                            detail=f"target_height={height}",
                        ),
                    )
                    return height
            except (DuplicateDeliveryError, ReorgRollbackError):
                raise
            except (TransientFailureError, TimeoutError_) as exc:
                finished = self.clock.now_ms()
                self.store.record_attempt(
                    message_id,
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
                    message_id,
                    attempt,
                    exc.reason,
                )
                if attempt == cfg.max_attempts:
                    raise RetriesExhaustedError(
                        f"{message_id}: {cfg.max_attempts} attempts exhausted "
                        f"(last={exc.reason})"
                    ) from exc
                backoff = min(
                    cfg.backoff_cap_ms,
                    cfg.base_backoff_ms * (2 ** (attempt - 1)),
                )
                await self.clock.sleep(backoff)
                continue
        # unreachable: loop either returns or raises
        raise RuntimeError("retry loop fell through")  # pragma: no cover

    # ---- reorg reconciliation -------------------------------------------
    def reconcile(self) -> list[str]:
        """Rebuild records and channel cursors against canonical chain state.

        Deterministic rules after a reorg of any depth:
          * source inclusion gone -> SOURCE_REORGED (terminal), settled by
            ``_settle_source_removed`` so the outcome is identical no matter
            whether the delivery path or reconciliation reaches it first:
            a standing target delivery keeps the escrow backing (a premature
            refund is clawed back once); with no standing target delivery an
            orphaned lock is returned to the sender once. The record keeps
            its history (delivered_height / attempts) and its note explains
            the funds destination; the message is not redeliverable.
          * source stands, target delivery gone -> TARGET_REORGED,
            redeliverable; relay_once re-applies it at most once. If the
            target later vanishes while the record already settled as
            SOURCE_REORGED, the disposition converges to "refunded to sender"
            regardless of the order of the two reorgs.
          * both stand -> DELIVERED unchanged.
          * failed / expired are *message-specific* terminal verdicts: a
            later source reorg never overwrites their status, note or funds
            disposition (already refunded), keeping permanent failures and
            expiry distinct from transient faults.
        Each per-record funds/record section takes the same commit lock the
        delivery path uses, so reconciliation cannot interleave with an
        in-flight commit (it may still run during a retry backoff). Channel
        cursors are recomputed from scratch so no nonce is skipped or reused.
        Returns affected message ids in store order. Idempotent: running it
        again moves no funds and changes no record.
        """
        affected: list[str] = []
        for record in self.store.all_records():
            source = self.chain_a if record.source_chain == "A" else self.chain_b
            target = self.chain_b if record.target_chain == "B" else self.chain_a
            source_ok = source.is_message_canonical(record.message_id)
            target_ok = target.is_delivery_canonical(record.message_id)

            with self._commit_lock_for(record.message_id):
                new_status: MessageStatus | None = None
                note: str | None = None
                delivered_h: int | None = None

                if not source_ok:
                    if record.status in (
                        MessageStatus.FAILED,
                        MessageStatus.EXPIRED,
                    ):
                        # terminal verdicts stay stable through a later reorg;
                        # their funds were already released exactly once
                        pass
                    else:
                        # unify *every* "source gone" record (observed,
                        # finalized, delivered whose target also vanished, or
                        # an already-reconciled target reorg) through the same
                        # settlement the delivery path uses
                        message = source.get_message(record.message_id)
                        new_status, note, delivered_h = self._settle_source_removed(
                            message, source, target
                        )
                        # a height learned earlier (incl. a rolled-back
                        # delivery) is kept as history
                        if delivered_h is None and record.delivered_height is not None:
                            delivered_h = record.delivered_height
                        # reconciliation only reports a change when the record
                        # actually moves, so repeated recomputation yields []
                        if (
                            record.status is new_status
                            and record.note == note
                            and (
                                delivered_h is None
                                or record.delivered_height == delivered_h
                            )
                        ):
                            new_status = None
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
                        delivered_height=delivered_h,
                        note=note or "",
                    )
                    affected.append(record.message_id)
                    log.info(
                        "reconcile id=%s -> %s (%s)",
                        record.message_id,
                        new_status.value,
                        note,
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
