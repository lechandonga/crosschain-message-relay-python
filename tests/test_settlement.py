"""Exception-time-series convergence: post-delivery source rollback,
permanent failure isolation, and interleaved delivery/retry/reconciliation.

Every test logs its INPUT (balances, depths, injected faults) and the exact
DECISION taken (terminal status + fund disposition), so runs are auditable
and reproducible.
"""

from __future__ import annotations

import asyncio

import pytest

from relay.chain import ESCROW_ACCOUNT
from relay.errors import (
    DuplicateDeliveryError,
    ReorgRollbackError,
    RetriesExhaustedError,
    TargetRejectedError,
)
from relay.models import MessageStatus


# ---------------------------------------------------------------------------
# 1. delivered first, source side rolled back afterwards: exactly one effect
# ---------------------------------------------------------------------------

async def test_delivered_then_source_reorg_keeps_single_effect(
    world, make_message, log_case
):
    amount = 100
    msg = make_message(world, amount=amount, nonce=1)
    await world["relayer"].relay_once(msg.message_id)
    A, B = world["A"], world["B"]
    alice0, bob0 = A.balance_of("alice"), B.balance_of("bob")
    assert (alice0, bob0) == (1000 - amount, amount)

    depth = A.head().height  # reorg away the whole chain including the send
    A.reorg_to(depth)
    affected = await world["relayer"].reconcile()
    rec = world["store"].get(msg.message_id)
    settlement = A.settlement(msg.message_id)
    log_case(
        "delivered_then_source_reorg",
        {"reorg_depth": depth, "affected": affected},
        {
            "status": rec.status.value,
            "delivered_height": rec.delivered_height,
            "settlement": settlement,
            "alice": A.balance_of("alice"),
            "bob": B.balance_of("bob"),
        },
    )

    assert msg.message_id in affected
    assert rec.status is MessageStatus.SOURCE_REORGED
    # history is not erased/overwritten
    assert rec.delivered_height is not None
    assert "not redeliverable" in rec.note
    # target delivery retained ...
    assert B.is_delivery_canonical(msg.message_id)
    assert B.balance_of("bob") == bob0
    # ... and the chain-level auto refund was reclaimed: sender got nothing
    # back, so no money is created across the two chains
    assert A.balance_of("alice") == alice0
    assert A.balance_of("alice") + B.balance_of("bob") == alice0 + bob0
    assert settlement["kind"] == "reclaim_refund"
    assert settlement["amount"] == amount
    # cannot be re-delivered; distinct terminal reason
    with pytest.raises(ReorgRollbackError):
        await world["relayer"].relay_once(msg.message_id)


async def test_delivered_then_source_reorg_is_idempotent_at_any_depth(
    world, make_message
):
    msg = make_message(world, amount=42, nonce=1)
    await world["relayer"].relay_once(msg.message_id)
    A, B = world["A"], world["B"]
    alice0, bob0 = A.balance_of("alice"), B.balance_of("bob")
    A.reorg_to(A.head().height)

    first = await world["relayer"].reconcile()
    note1 = world["store"].get(msg.message_id).note
    escrow1 = A.balance_of(ESCROW_ACCOUNT)
    assert first == [msg.message_id]

    # repeated reconciliation must not move funds again
    assert await world["relayer"].reconcile() == []
    assert world["store"].get(msg.message_id).note == note1
    assert A.balance_of(ESCROW_ACCOUNT) == escrow1
    assert A.balance_of("alice") == alice0 and B.balance_of("bob") == bob0

    # a deeper reorg that also removes the reclaim settlement block:
    # reconciliation re-issues the reclaim and converges back to the same
    # disposition (funds never get stuck in the sender's pocket or in limbo)
    h = A.settlement(msg.message_id)["height"]
    A.reorg_to(A.head().height - h + 1)
    second = await world["relayer"].reconcile()
    assert second == [msg.message_id]
    assert A.balance_of("alice") == alice0
    assert A.balance_of(ESCROW_ACCOUNT) == escrow1
    assert world["store"].get(msg.message_id).status is MessageStatus.SOURCE_REORGED
    assert await world["relayer"].reconcile() == []  # stable again


async def test_source_reorg_before_delivery_refunds_sender(
    world, make_message, log_case
):
    msg = make_message(world, amount=100, nonce=1)
    world["A"].reorg_to(world["A"].head().height)  # before any target delivery
    affected = await world["relayer"].reconcile()
    rec = world["store"].get(msg.message_id)
    log_case(
        "source_reorg_before_delivery",
        {"affected": affected},
        {"status": rec.status.value, "alice": world["A"].balance_of("alice")},
    )
    assert rec.status is MessageStatus.SOURCE_REORGED
    assert world["A"].balance_of("alice") == 1000   # chain-level refund stands
    assert world["B"].balance_of("bob") == 0
    assert world["A"].settlement(msg.message_id) is None  # nothing to reclaim
    with pytest.raises(ReorgRollbackError):
        await world["relayer"].relay_once(msg.message_id)


# ---------------------------------------------------------------------------
# 2. a permanent failure affects only its own message; funds get a home
# ---------------------------------------------------------------------------

async def test_target_rejected_refunds_and_channel_continues(
    world, make_message, log_case
):
    A, B, rel = world["A"], world["B"], world["relayer"]
    m1 = make_message(world, nonce=1, receiver="ghost", amount=100)
    B.reject_account("ghost")
    with pytest.raises(TargetRejectedError) as ei:
        await rel.relay_once(m1.message_id)
    rec1 = world["store"].get(m1.message_id)
    log_case(
        "permanent_reject_channel_continues",
        {"nonce": 1, "receiver": "ghost"},
        {
            "reason": ei.value.reason,
            "status": rec1.status.value,
            "settlement": A.settlement(m1.message_id),
            "cursor": world["store"].next_nonce("A", "ch"),
        },
    )

    # terminal state distinguishable from transient/timeout/expiry
    assert rec1.status is MessageStatus.FAILED
    assert ei.value.reason == "target_rejected" and not ei.value.retryable
    assert "target_rejected" in rec1.note and "not redeliverable" in rec1.note
    # locked funds have a definite home: back with the sender
    assert A.balance_of("alice") == 1000
    assert A.settlement(m1.message_id)["kind"] == "refund_locked"
    assert B.balance_of("ghost") == 0  # target never paid

    # later messages in the SAME channel still deliver
    m2 = make_message(world, nonce=2, receiver="bob", amount=200)
    assert await rel.relay_once(m2.message_id) is MessageStatus.DELIVERED
    assert B.balance_of("bob") == 200
    assert world["store"].next_nonce("A", "ch") == 3
    m3 = make_message(world, nonce=3, receiver="carol", amount=30)
    assert await rel.relay_once(m3.message_id) is MessageStatus.DELIVERED
    # the failed message itself stays terminal
    with pytest.raises(Exception):
        await rel.relay_once(m1.message_id)


async def test_retries_exhausted_refunds_and_channel_continues(
    world, make_message, log_case
):
    cfg = world["config"]
    A, B, rel = world["A"], world["B"], world["relayer"]
    m1 = make_message(world, nonce=1, receiver="bob", amount=55)
    B.inject_transient(m1.message_id, times=cfg.max_attempts + 3)
    with pytest.raises(RetriesExhaustedError) as ei:
        await rel.relay_once(m1.message_id)
    rec1 = world["store"].get(m1.message_id)
    log_case(
        "retries_exhausted_channel_continues",
        {"max_attempts": cfg.max_attempts, "attempts": len(rec1.attempts)},
        {
            "reason": ei.value.reason,
            "status": rec1.status.value,
            "alice": A.balance_of("alice"),
        },
    )
    # distinct from target_rejected and from timeout (transient)
    assert ei.value.reason == "retries_exhausted" and not ei.value.retryable
    assert rec1.status is MessageStatus.FAILED
    assert len(rec1.attempts) == cfg.max_attempts
    assert "retries_exhausted" in rec1.note
    assert A.balance_of("alice") == 1000          # refunded
    assert B.balance_of("bob") == 0               # never delivered

    m2 = make_message(world, nonce=2, receiver="carol", amount=77)
    assert await rel.relay_once(m2.message_id) is MessageStatus.DELIVERED
    assert B.balance_of("carol") == 77
    assert world["store"].next_nonce("A", "ch") == 3


async def test_failed_refund_reissued_after_settlement_reorg(
    world, make_message
):
    A, rel = world["A"], world["relayer"]
    m1 = make_message(world, nonce=1, receiver="ghost", amount=100)
    world["B"].reject_account("ghost")
    with pytest.raises(TargetRejectedError):
        await rel.relay_once(m1.message_id)
    assert A.balance_of("alice") == 1000
    # reorg away the refund block: funds are re-locked
    h = A.settlement(m1.message_id)["height"]
    A.reorg_to(A.head().height - h + 1)
    assert A.balance_of("alice") == 900  # re-locked
    affected = await rel.reconcile()
    assert m1.message_id in affected
    assert A.balance_of("alice") == 1000  # refunded again, never suspended
    assert world["store"].get(m1.message_id).status is MessageStatus.FAILED
    assert await rel.reconcile() == []


# ---------------------------------------------------------------------------
# 3. interleaving: delivery / retry / reconcile, same message many handlers
# ---------------------------------------------------------------------------

async def test_reorg_during_backoff_never_late_delivers(world, make_message):
    A, B, rel = world["A"], world["B"], world["relayer"]
    msg = make_message(world, nonce=1, receiver="bob", amount=100)
    B.inject_transient(msg.message_id, times=1)  # fault -> 50ms backoff

    async def reorg_while_sleeping():
        await asyncio.sleep(0.001)  # parked inside the backoff
        A.reorg_to(A.head().height)
        return await rel.reconcile()

    results = await asyncio.gather(
        rel.relay_once(msg.message_id),
        reorg_while_sleeping(),
        return_exceptions=True,
    )
    assert isinstance(results[0], ReorgRollbackError)
    assert B.delivered_heights() == {}      # the retried attempt never landed
    assert B.balance_of("bob") == 0
    assert A.balance_of("alice") == 1000    # refunded, nothing reclaimed
    rec = world["store"].get(msg.message_id)
    assert rec.status is MessageStatus.SOURCE_REORGED
    with pytest.raises(ReorgRollbackError):
        await rel.relay_once(msg.message_id)


async def test_concurrent_handlers_with_reorg_match_serial_run(
    world, make_message, log_case
):
    A, B, rel = world["A"], world["B"], world["relayer"]
    msg = make_message(world, nonce=1, receiver="zoe", amount=70)
    alice0 = A.balance_of("alice")

    async def reorg_soon():
        await asyncio.sleep(0.001)
        A.reorg_to(A.head().height)
        return await rel.reconcile()

    results = await asyncio.gather(
        *[rel.relay_once(msg.message_id) for _ in range(8)],
        reorg_soon(),
        return_exceptions=True,
    )
    outcomes = results[:-1]
    delivered = [r for r in outcomes if r is MessageStatus.DELIVERED]
    dup = [r for r in outcomes if isinstance(r, DuplicateDeliveryError)]
    reorg = [r for r in outcomes if isinstance(r, ReorgRollbackError)]
    log_case(
        "concurrent_handlers_plus_reorg",
        {"handlers": 8, "reorg_depth": "all"},
        {"delivered": len(delivered), "duplicate": len(dup), "reorg": len(reorg)},
    )
    # serial-equivalent: either delivery won before the reorg (1 delivered,
    # others duplicate/reorg) or reorg won first (all reorg) -- never both a
    # delivery AND a later reorg-created duplicate fund effect
    assert len(delivered) <= 1
    rec = world["store"].get(msg.message_id)
    assert rec.status in (MessageStatus.DELIVERED, MessageStatus.SOURCE_REORGED)
    if rec.status is MessageStatus.SOURCE_REORGED:
        # delivery stood -> refund reclaimed; otherwise never delivered
        if B.is_delivery_canonical(msg.message_id):
            assert A.balance_of("alice") == alice0
            assert B.balance_of("zoe") == 70
        else:
            assert B.balance_of("zoe") == 0
            assert A.balance_of("alice") == 1000
    else:
        assert len(delivered) == 1 and len(dup) == 7
    assert A.balance_of("alice") + B.balance_of("zoe") == alice0 + 70


async def test_reconcile_interleaving_with_inflight_delivery_is_stable(
    world, make_message
):
    # no chain change: reconcile racing delivery must report nothing and
    # never disturb the single delivery effect
    B, rel = world["B"], world["relayer"]
    msg = make_message(world, nonce=1, receiver="ivy", amount=25)
    B.inject_transient(msg.message_id, times=2)

    async def recon_soon():
        await asyncio.sleep(0.001)
        return await rel.reconcile()

    results = await asyncio.gather(
        rel.relay_once(msg.message_id), recon_soon(), return_exceptions=True
    )
    assert results[0] is MessageStatus.DELIVERED
    assert results[1] == []
    assert B.balance_of("ivy") == 25
    assert len(world["store"].get(msg.message_id).attempts) == 3
    assert await rel.reconcile() == []
