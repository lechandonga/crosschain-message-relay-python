"""Local-only proof signing and verification (no real credentials).

Signatures are HMAC-SHA256 keyed by a per-chain attester secret. Tampering
with any signed field fails verification -> ``forged_proof``, distinct from
reusing an intact proof (``replay_detected``).
"""

from __future__ import annotations

import hashlib
import hmac

from .models import Message, Proof

ATTESTER = "local-attester"


def _attestation_payload(
    message_id: str,
    source_chain: str,
    target_chain: str,
    source_height: int,
    source_block_hash: str,
) -> bytes:
    # Pipe-separated canonical form; fields never contain "|" in the sim.
    return "|".join(
        [message_id, source_chain, target_chain, str(source_height), source_block_hash]
    ).encode("utf-8")


def _digest(
    *,
    message_id: str,
    source_chain: str,
    target_chain: str,
    source_height: int,
    source_block_hash: str,
    secret: str,
) -> str:
    return hmac.new(
        secret.encode("utf-8"),
        _attestation_payload(
            message_id, source_chain, target_chain, source_height, source_block_hash
        ),
        hashlib.sha256,
    ).hexdigest()


def sign_message(message: Message, secret: str) -> Proof:
    return Proof(
        message_id=message.message_id,
        source_chain=message.source_chain,
        target_chain=message.target_chain,
        source_height=message.source_height,
        source_block_hash=message.source_block_hash,
        attester=ATTESTER,
        signature=_digest(
            message_id=message.message_id,
            source_chain=message.source_chain,
            target_chain=message.target_chain,
            source_height=message.source_height,
            source_block_hash=message.source_block_hash,
            secret=secret,
        ),
    )


def verify_proof(proof: Proof, message: Message, secret: str) -> bool:
    """Authentic iff: known attester, ids/chains agree, signature valid.

    Every comparison is part of the authenticity decision: a proof presented
    for the wrong message or with any altered field fails as a forgery.
    """
    if proof.attester != ATTESTER:
        return False
    if (
        proof.message_id,
        proof.source_chain,
        proof.target_chain,
        proof.source_height,
        proof.source_block_hash,
    ) != (
        message.message_id,
        message.source_chain,
        message.target_chain,
        message.source_height,
        message.source_block_hash,
    ):
        return False
    expected = _digest(
        message_id=proof.message_id,
        source_chain=proof.source_chain,
        target_chain=proof.target_chain,
        source_height=proof.source_height,
        source_block_hash=proof.source_block_hash,
        secret=secret,
    )
    return hmac.compare_digest(proof.signature, expected)
