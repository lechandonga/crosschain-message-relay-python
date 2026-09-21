"""Replay, forgery, duplicate and finality must be distinguishable."""

from __future__ import annotations

import dataclasses

import pytest

from relay.crypto import sign_message
from relay.errors import (
    DuplicateDeliveryError,
    ForgedProofError,
    NotFinalizedError,
    ReplayError,
    StaleNonceError,
)
from relay.models import MessageStatus


async def _finalize(world, make_message, **kw):
    msg = make_message(world, **kw)
    status = await world["relayer"].relay_once(msg.message_id)
    assert status is MessageStatus.DELIVERED
    return msg


async def test_not_finalized_blocks_delivery(world, make_message, log_case):
    msg = make_message(world, confirms=1)  # only 1 < depth 3
    with pytest.raises(NotFinalizedError) as ei:
        await world["relayer"].relay_once(msg.message_id)
    log_case("not_finalized", {"confirmations": 1}, ei.value.reason)
    assert ei.value.reason == "not_finalized" and ei.value.retryable
    assert world["store"].get(msg.message_id).status is MessageStatus.OBSERVED
    world["A"].mine(2)
    assert await world["relayer"].relay_once(msg.message_id) is MessageStatus.DELIVERED


async def test_forged_signature_rejected(world, make_message, log_case):
    msg = make_message(world, nonce=1)
    good = sign_message(msg, world["relayer"].secret)
    forged = dataclasses.replace(good, signature="0" * len(good.signature))
    with pytest.raises(ForgedProofError) as ei:
        await world["relayer"].relay_once(msg.message_id, proof=forged)
    log_case("forged_signature", forged.signature[:8], ei.value.reason)
    assert ei.value.reason == "forged_proof"
    assert world["store"].get(msg.message_id).status is not MessageStatus.DELIVERED


async def test_tampered_field_is_forgery(world, make_message, log_case):
    msg = make_message(world, nonce=1)
    good = sign_message(msg, world["relayer"].secret)
    tampered = dataclasses.replace(good, source_height=good.source_height + 1)
    with pytest.raises(ForgedProofError) as ei:
        await world["relayer"].relay_once(msg.message_id, proof=tampered)
    log_case("tampered_height", tampered.source_height, ei.value.reason)
    assert ei.value.reason == "forged_proof"


async def test_wrong_secret_is_forgery(world, make_message, log_case):
    msg = make_message(world, nonce=1)
    foreign = sign_message(msg, "attacker-secret")
    with pytest.raises(ForgedProofError) as ei:
        await world["relayer"].relay_once(msg.message_id, proof=foreign)
    log_case("foreign_attester", foreign.attester, ei.value.reason)
    assert ei.value.reason == "forged_proof"


async def test_delivered_then_duplicate_is_distinct_from_replay(world, make_message, log_case):
    msg = await _finalize(world, make_message, nonce=1)
    # re-presenting the very same delivered message -> duplicate_delivery
    with pytest.raises(DuplicateDeliveryError) as ei:
        await world["relayer"].relay_once(msg.message_id)
    log_case("duplicate", msg.message_id, ei.value.reason)
    assert ei.value.reason == "duplicate_delivery"


async def test_reused_nonce_is_replay_not_duplicate(world, make_message, log_case):
    await _finalize(world, make_message, nonce=1, amount=100)
    # a *different* message that reuses consumed nonce 1
    replay = make_message(world, nonce=1, amount=5, receiver="carol")
    with pytest.raises(ReplayError) as ei:
        await world["relayer"].relay_once(replay.message_id)
    log_case("replay_nonce", {"nonce": 1, "id": replay.message_id}, ei.value.reason)
    assert ei.value.reason == "replay_detected"
    # replay applied nothing: carol untouched, escrow unchanged
    assert world["B"].balance_of("carol") == 0


async def test_out_of_order_gap_waits_then_expires(world, make_message, log_case):
    await _finalize(world, make_message, nonce=1)
    gap = make_message(world, nonce=4)  # gap: expected 2
    with pytest.raises(StaleNonceError) as ei:
        await world["relayer"].relay_once(gap.message_id)
    log_case("gap_wait", {"nonce": 4, "expected": 2}, ei.value.reason)
    assert ei.value.reason == "stale_nonce" and ei.value.retryable
    # age the chain past the stale window (8 blocks) without filling the gap
    world["A"].mine(world["config"].stale_window)
    with pytest.raises(StaleNonceError) as ei2:
        await world["relayer"].relay_once(gap.message_id)
    log_case("gap_expired", {"age": ">=window"}, ei2.value.reason)
    assert not ei2.value.retryable


async def test_ordered_nonces_deliver_in_sequence(world, make_message):
    m1 = make_message(world, nonce=1, receiver="bob")
    m2 = make_message(world, nonce=2, receiver="carol")
    # delivering nonce 2 first must wait for nonce 1 -> replay/gap rules
    with pytest.raises(StaleNonceError):
        await world["relayer"].relay_once(m2.message_id)
    await world["relayer"].relay_once(m1.message_id)
    assert await world["relayer"].relay_once(m2.message_id) is MessageStatus.DELIVERED
    assert world["store"].channel_cursors() == {"A:ch": 3}
