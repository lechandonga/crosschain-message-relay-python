"""Core data types: messages, blocks, proofs and relay records."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class Direction(enum.Enum):
    A_TO_B = "A->B"
    B_TO_A = "B->A"


class MessageStatus(enum.Enum):
    """Lifecycle of a relayed message, as observed by the relay store."""

    UNKNOWN = "unknown"
    OBSERVED = "observed"          # seen on source, not yet final
    FINALIZED = "finalized"        # source-final, proof available
    DELIVERED = "delivered"        # applied on the target (canonical)
    SOURCE_REORGED = "source_reorged"   # terminal: source inclusion reverted
    TARGET_REORGED = "target_reorged"   # delivery reverted; redeliverable
    FAILED = "failed"              # terminal: target rejection / retries exhausted
    EXPIRED = "expired"            # terminal: stale nonce window passed on source


@dataclass(frozen=True)
class Message:
    """A cross-chain value-transfer message submitted on a source chain."""

    message_id: str
    source_chain: str
    target_chain: str
    channel: str
    sender: str
    receiver: str
    amount: int
    nonce: int                     # per-channel strictly increasing sequence
    source_height: int
    source_block_hash: str
    created_at: float

    @property
    def direction(self) -> Direction:
        return Direction.A_TO_B if self.source_chain == "A" else Direction.B_TO_A


@dataclass
class Block:
    """A block on a simulated chain. Blocks form a hash chain via parent."""

    height: int
    parent_hash: str
    timestamp: float
    message_ids: list[str] = field(default_factory=list)

    @property
    def hash(self) -> str:  # computed in chain.py; placeholder typing here
        raise NotImplementedError


@dataclass(frozen=True)
class Proof:
    """Attestation that a message was included in a source block.

    The signature covers every field; tampering with any of them invalidates
    the proof (forged_proof), distinct from reusing a valid proof (replay).
    """

    message_id: str
    source_chain: str
    target_chain: str
    source_height: int
    source_block_hash: str
    attester: str
    signature: str


@dataclass
class RelayAttempt:
    attempt: int
    started_at: float
    finished_at: float
    outcome: str                    # delivered / transient_failure / timeout / rejected
    detail: str = ""


@dataclass
class RelayRecord:
    """Relay-store view of one message: status, attempts, audit trail."""

    message_id: str
    channel: str
    nonce: int
    status: MessageStatus
    source_chain: str
    target_chain: str
    observed_height: int
    delivered_height: int | None = None
    attempts: list[RelayAttempt] = field(default_factory=list)
    updated_at: float = 0.0
    note: str = ""
