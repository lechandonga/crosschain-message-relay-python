"""Proof validation: authenticity, finality, ordering and replay rules.

Decision codes stay distinguishable:
  * forged_proof    -- signature/fields altered or proof for another message
  * not_finalized   -- not enough source confirmations yet (retryable)
  * reorg_rollback  -- inclusion no longer canonical
  * replay_detected -- nonce already consumed on this channel (valid proof)
  * stale_nonce     -- nonce gap (retryable until window) or expired (final)
"""

from __future__ import annotations

from . import crypto
from .chain import SimulatedChain
from .errors import (
    ForgedProofError,
    NotFinalizedError,
    ReorgRollbackError,
    ReplayError,
    StaleNonceError,
)
from .models import Message, Proof


class ProofValidator:
    def __init__(self, finality_depth: int, stale_window: int) -> None:
        self.finality_depth = finality_depth
        self.stale_window = stale_window

    def authenticate(self, proof: Proof, message: Message, secret: str) -> None:
        if not crypto.verify_proof(proof, message, secret):
            raise ForgedProofError(
                f"proof does not authentically attest {message.message_id} "
                f"(proof_id={proof.message_id}, attester={proof.attester})"
            )

    def check_finality(self, message: Message, source: SimulatedChain) -> None:
        # Canonical inclusion must still exist; a reverted inclusion is a
        # rollback, not mere lack of confirmations.
        if not source.is_message_canonical(message.message_id):
            raise ReorgRollbackError(
                f"{message.message_id} inclusion is no longer canonical on {source.chain_id}"
            )
        confirmations = source.confirmations(message.source_height)
        if confirmations < self.finality_depth:
            raise NotFinalizedError(
                f"{message.message_id} has {confirmations}/{self.finality_depth} confirmations"
            )

    def check_order(
        self,
        message: Message,
        *,
        channel_next_nonce: int,
        source_head_height: int,
    ) -> None:
        """Enforce per-channel strict ordering and the staleness window.

        Rule (identical on every run, no silent skipping):
          * nonce < expected -> the nonce was consumed earlier -> replay
          * nonce == expected -> in order, accepted
          * nonce > expected -> gap; wait while the message is within the
            stale window, otherwise the channel is expired -> permanent
        """
        if message.nonce < channel_next_nonce:
            raise ReplayError(
                f"channel {message.channel}: nonce {message.nonce} already consumed "
                f"(next expected {channel_next_nonce})"
            )
        if message.nonce > channel_next_nonce:
            age = source_head_height - message.source_height
            if age >= self.stale_window:
                raise StaleNonceError(
                    f"channel {message.channel}: gap before nonce {message.nonce} "
                    f"(expected {channel_next_nonce}) and message is {age} blocks old, "
                    f"past stale window {self.stale_window} -> expired",
                    retryable=False,
                )
            raise StaleNonceError(
                f"channel {message.channel}: nonce {message.nonce} ahead of expected "
                f"{channel_next_nonce}; gap not filled yet (age {age} < {self.stale_window})",
                retryable=True,
            )
