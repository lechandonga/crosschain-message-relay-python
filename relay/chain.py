"""Simulated blockchain: hash-linked blocks, accounts, reorgs, faults.

Effective ledger model (per direction):
  * source ``submit`` : sender -amount, ``escrow`` +amount   (locked)
  * target ``deliver``: ``escrow`` -amount, receiver +amount
  * source reorg      : removed sends are reversed -> sender refunded
  * target reorg      : removed deliveries are reversed (escrow restored),
                        so the message becomes redeliverable exactly once

Each chain therefore conserves its own token supply across every operation.
"""

from __future__ import annotations

import hashlib
import itertools
import threading
from collections import defaultdict

from .clock import Clock
from .errors import (
    DuplicateDeliveryError,
    RelayError,
    TargetRejectedError,
    TimeoutError_,
    TransientFailureError,
)
from .models import Block, Message

ESCROW_ACCOUNT = "escrow"


class _Block(Block):
    """Mutable block implementation carrying a cached content hash."""

    def __init__(
        self,
        height: int,
        parent_hash: str,
        timestamp: float,
        message_ids: list[str] | None = None,
    ) -> None:
        super().__init__(height, parent_hash, timestamp, list(message_ids or ()))
        self._hash = self._compute_hash()

    def _compute_hash(self) -> str:
        body = f"{self.height}|{self.parent_hash}|{self.timestamp}|{','.join(self.message_ids)}"
        return hashlib.sha256(body.encode("utf-8")).hexdigest()[:16]

    @property
    def hash(self) -> str:
        return self._hash

    def recompute_hash(self) -> None:
        self._hash = self._compute_hash()


class SimulatedChain:
    def __init__(
        self,
        chain_id: str,
        *,
        genesis_balances: dict[str, int] | None = None,
        clock: Clock | None = None,
    ) -> None:
        if clock is None:
            clock = Clock()
        self.chain_id = chain_id
        self.clock = clock
        self._lock = threading.RLock()
        self._id_seq = itertools.count(1)
        # canonical chain, genesis at height 0
        genesis = _Block(0, "0x0", self.clock.now())
        self._blocks: list[_Block] = [genesis]
        # balances in the *canonical* state, rebuilt on reorg
        self._balances: dict[str, int] = defaultdict(int)
        for account, amount in (genesis_balances or {}).items():
            if amount < 0:
                raise ValueError("genesis balance must be non-negative")
            self._balances[account] = amount
        # messages keyed by id; each remembers the block height that first
        # (canonically) included it, and whether it is currently canonical
        self._messages: dict[str, Message] = {}
        self._message_canonical: dict[str, bool] = {}
        # target-side deliveries: message_id -> delivery height
        self._delivered: dict[str, int] = {}
        # target-side bookkeeping needed to reverse a delivery on reorg;
        # the target never holds the source Message object
        self._delivery_info: dict[str, tuple[str, int]] = {}
        # set of delivered ids currently included canonically (target chain)
        self._delivered_canonical: dict[str, bool] = {}
        # pending submissions for the next block
        self._pending: list[Message] = []
        # fault injection
        self._transient_left: dict[str, int] = defaultdict(int)
        self._timeout_left: dict[str, int] = defaultdict(int)
        self._rejected_accounts: set[str] = set()

    # ------------------------------------------------------------------ locks
    def _account_key(self, account: str) -> str:
        return account

    # ---- block production ------------------------------------------------
    def submit_message(
        self,
        *,
        channel: str,
        sender: str,
        receiver: str,
        amount: int,
        nonce: int,
    ) -> Message:
        with self._lock:
            if amount <= 0:
                raise TargetRejectedError(f"amount must be positive, got {amount}")
            if self._balances.get(sender, 0) < amount:
                raise TargetRejectedError(
                    f"insufficient balance: {sender} has {self._balances.get(sender, 0)}, "
                    f"needs {amount}"
                )
            # NOTE: nonce replay is deliberately *not* rejected at submission:
            # a replayed message must enter a block so the relay layer can
            # reject it with a distinct replay_detected decision.
            mid = f"{self.chain_id}-msg-{next(self._id_seq)}"
            self._pending.append(
                Message(
                    message_id=mid,
                    source_chain=self.chain_id,
                    target_chain="B" if self.chain_id == "A" else "A",
                    channel=channel,
                    sender=sender,
                    receiver=receiver,
                    amount=amount,
                    nonce=nonce,
                    source_height=0,  # assigned on inclusion
                    source_block_hash="",
                    created_at=self.clock.now(),
                )
            )
            return self._pending[-1]

    def mine(self, count: int = 1) -> list[Block]:
        """Produce blocks; each block includes whatever is pending at its turn."""
        with self._lock:
            produced: list[_Block] = []
            for _ in range(count):
                parent = self._blocks[-1]
                block = _Block(parent.height + 1, parent.hash, self.clock.now())
                # drain pending messages into this block
                while self._pending:
                    msg = self._pending.pop(0)
                    included = Message(
                        message_id=msg.message_id,
                        source_chain=msg.source_chain,
                        target_chain=msg.target_chain,
                        channel=msg.channel,
                        sender=msg.sender,
                        receiver=msg.receiver,
                        amount=msg.amount,
                        nonce=msg.nonce,
                        source_height=block.height,
                        source_block_hash=block.hash,
                        created_at=msg.created_at,
                    )
                    self._messages[included.message_id] = included
                    self._message_canonical[included.message_id] = True
                    block.message_ids.append(included.message_id)
                    # lock funds in escrow on the source side
                    self._debit(included.sender, included.amount)
                    self._credit(ESCROW_ACCOUNT, included.amount)
                self._blocks.append(block)
                produced.append(block)
            return list(produced)

    def head(self) -> Block:
        with self._lock:
            return self._blocks[-1]

    # ---- internal ledger -------------------------------------------------
    def _debit(self, account: str, amount: int) -> None:
        if amount < 0:
            raise ValueError("negative debit")
        if self._balances.get(account, 0) < amount:
            raise TargetRejectedError(
                f"insufficient balance: {account} {self._balances.get(account, 0)} < {amount}"
            )
        self._balances[account] -= amount

    def _credit(self, account: str, amount: int) -> None:
        if amount < 0:
            raise ValueError("negative credit")
        self._balances[account] = self._balances.get(account, 0) + amount

    # ---- read paths ------------------------------------------------------
    def get_message(self, message_id: str) -> Message:
        with self._lock:
            return self._messages[message_id]

    def has_message(self, message_id: str) -> bool:
        with self._lock:
            return message_id in self._messages

    def canonical_messages(self) -> list[Message]:
        with self._lock:
            out = [
                m
                for m in self._messages.values()
                if self._message_canonical.get(m.message_id, False)
            ]
            out.sort(key=lambda m: (m.source_height, m.nonce, m.message_id))
            return out

    def confirmations(self, height: int) -> int:
        """Blocks on top of ``height`` including the block itself (>=1)."""
        with self._lock:
            head_h = self._blocks[-1].height
            if height < 0 or height > head_h:
                return 0
            return head_h - height + 1

    def is_final(self, height: int, finality_depth: int) -> bool:
        return self.confirmations(height) >= finality_depth

    def balance_of(self, account: str) -> int:
        with self._lock:
            return self._balances.get(account, 0)

    def total_supply(self) -> int:
        with self._lock:
            return sum(self._balances.values())

    def block_at(self, height: int) -> Block:
        with self._lock:
            return self._blocks[height]

    # ---- target delivery -------------------------------------------------
    def deliver(self, message: Message, *, delivered_at: int | None = None) -> int:
        """Apply a delivery atomically inside a freshly produced block.

        A successful delivery mines a *delivery block* that records the
        message id, so reorg ownership is explicit and deterministic.

        Idempotent per message id: an already-canonical delivery raises
        DuplicateDeliveryError and changes nothing. Faults
        (transient/timeout/rejected) are raised before any block or balance
        change, so retries never double-debit.
        """
        with self._lock:
            # injected transient faults / timeouts happen first and change nothing
            if self._transient_left.get(message.message_id, 0) > 0:
                self._transient_left[message.message_id] -= 1
                raise TransientFailureError(
                    f"target {self.chain_id} temporary fault for {message.message_id}",
                    retryable=True,
                )
            if self._timeout_left.get(message.message_id, 0) > 0:
                self._timeout_left[message.message_id] -= 1
                raise TimeoutError_(
                    f"target {self.chain_id} timed out for {message.message_id}",
                    retryable=True,
                )
            if message.receiver in self._rejected_accounts:
                raise TargetRejectedError(
                    f"target {self.chain_id} rejects receiver {message.receiver}"
                )
            if self._delivered_canonical.get(message.message_id, False):
                height = self._delivered[message.message_id]
                raise DuplicateDeliveryError(
                    f"{message.message_id} already delivered at height {height}"
                )
            if self._balances.get(ESCROW_ACCOUNT, 0) < message.amount:
                raise TargetRejectedError(
                    f"escrow insufficient on {self.chain_id}: "
                    f"{self._balances.get(ESCROW_ACCOUNT, 0)} < {message.amount}"
                )

            if delivered_at is None:
                # produce a delivery block recording the id
                parent = self._blocks[-1]
                block = _Block(
                    parent.height + 1, parent.hash, self.clock.now(), [message.message_id]
                )
                height = block.height
            else:
                # callers may pin the delivery to an existing future block;
                # record the id in that block's content
                block = self._blocks[delivered_at]
                if message.message_id not in block.message_ids:
                    block.message_ids.append(message.message_id)
                    block.recompute_hash()
                height = delivered_at
            # atomic move: escrow -> receiver
            self._debit(ESCROW_ACCOUNT, message.amount)
            self._credit(message.receiver, message.amount)
            if delivered_at is None:
                self._blocks.append(block)
            self._delivered[message.message_id] = height
            self._delivery_info[message.message_id] = (message.receiver, message.amount)
            self._delivered_canonical[message.message_id] = True
            return height

    def delivered_heights(self) -> dict[str, int]:
        with self._lock:
            return {
                mid: h
                for mid, h in self._delivered.items()
                if self._delivered_canonical.get(mid, False)
            }

    # ---- reorg -----------------------------------------------------------
    def reorg_to(self, depth: int, *, empty_fork: bool = True) -> list[Block]:
        """Replace the last ``depth`` blocks with a fresh fork.

        State is rebuilt from genesis deterministically:
          * source sends in removed blocks vanish and escrow refunds sender
          * target deliveries in removed blocks reverse (escrow restored) and
            the message becomes redeliverable
        Returns the new canonical blocks.
        """
        with self._lock:
            if depth <= 0:
                raise ValueError("reorg depth must be positive")
            if depth > len(self._blocks) - 1:
                raise ValueError("cannot reorg deeper than the chain height")
            kept = self._blocks[: len(self._blocks) - depth]

            # capture which messages / deliveries disappear with the fork tip
            removed_heights = {b.height for b in self._blocks[len(kept):]}
            removed_messages = [
                mid
                for mid, m in self._messages.items()
                if m.source_height in removed_heights
                and self._message_canonical.get(mid, False)
            ]
            removed_blocks = self._blocks[len(kept):]
            removed_deliveries = [
                mid
                for b in removed_blocks
                for mid in b.message_ids
                if self._delivered_canonical.get(mid, False)
            ]

            # ---- roll back balances & markers ----------------------------
            for mid in sorted(removed_messages):
                m = self._messages[mid]
                # reverse source lock: escrow -> sender
                self._balances[ESCROW_ACCOUNT] -= m.amount
                self._balances[m.sender] = self._balances.get(m.sender, 0) + m.amount
                self._message_canonical[mid] = False
            for mid in sorted(removed_deliveries):
                receiver, amount = self._delivery_info[mid]
                # reverse target delivery: receiver -> escrow. The move is
                # only possible while the receiver still holds the funds;
                # otherwise funds have legitimately moved on and the message
                # is marked non-canonical for higher-level reconciliation.
                if self._balances.get(receiver, 0) >= amount:
                    self._balances[receiver] -= amount
                    self._balances[ESCROW_ACCOUNT] += amount
                self._delivered_canonical[mid] = False

            # ---- build replacement fork -----------------------------------
            self._blocks = list(kept)
            new_blocks: list[_Block] = []
            for _ in range(depth):
                parent = self._blocks[-1]
                block = _Block(
                    parent.height + 1,
                    parent.hash,
                    self.clock.now(),
                )
                self._blocks.append(block)
                new_blocks.append(block)
            return list(new_blocks)

    def is_message_canonical(self, message_id: str) -> bool:
        with self._lock:
            return self._message_canonical.get(message_id, False)

    def is_delivery_canonical(self, message_id: str) -> bool:
        with self._lock:
            return self._delivered_canonical.get(message_id, False)

    # ---- fault injection -------------------------------------------------
    def inject_transient(self, message_id: str, *, times: int = 1) -> None:
        with self._lock:
            self._transient_left[message_id] += times

    def inject_timeout(self, message_id: str, *, times: int = 1) -> None:
        with self._lock:
            self._timeout_left[message_id] += times

    def reject_account(self, account: str) -> None:
        with self._lock:
            self._rejected_accounts.add(account)
