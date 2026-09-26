"""Terminal settlement: single funds effect, channel liveness, interleavings.

Scenarios pinned here (all deterministic, all log inputs + decision basis):
  * delivered on target, then the source inclusion is reorged away
    (any depth/timing) -> the delivery stands, the sender is NOT refunded,
    books on both chains together match the pre-reorg state exactly;
  * permanent failure (target_rejected / retries_exhausted) -> the message
    gets a distinct terminal note, its locked escrow is refunded exactly
    once, and later nonces on the same channel keep flowing;
  * delivery / retry / reconcile / reorg interleaved, incl. concurrent
    handlers on the same message -> outcome equals some serial execution,
    no half states, no duplicated funds movements.
"""

from __future__ import annotations

import asyncio

import pytest

from relay.chain import ESCROW_ACCOUNT
from relay.errors import (
    DuplicateDeliveryError,
    RelayError,
    ReorgRollbackError,
    RetriesExhaustedError,
    StaleNonceError,
    TargetRejectedError,
)
from relay.models import MessageStatus


def _books(world, *, sender="alice", receiver="bob"):
    a, b = world["A"], world["B"]
    return {
        "sender": a.balance_of(sender),
        "escrow_a": a.balance_of(ESCROW_ACCOUNT),
        "receiver": b.balance_of(receiver),
        "escrow_b": b.balance_of(ESCROW_ACCOUNT),
        "supply": (a.total_supply(), b.total_supply()),
    }


# ---------------------------------------------------------------------------
# A. delivered, then the source inclusion is reorged away
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("extra_blocks", [0, 2, 5])
async def test_delivered_then_source_reorg_any_depth(
    world, make_message, log_case, extra_blocks
):
    msg = make_message(world, amount=100, nonce=1)
    await world["relayer"].relay_once(msg.message_id)
    world["A"].mine(extra_blocks)  # reorg timing/depth varies
    before = _books(world)

    depth = world["A"].head().height - msg.source_height + 1
    world["A"].reorg_to(depth)
    affected = world["relayer"].reconcile()
    after = _books(world)
    rec = world["store"].get(msg.message_id)
    log_case(
        "delivered_then_source_reorg",
        {"extra_blocks": extra_blocks, "depth": depth, "affected": affected},
        {"status": rec.status.value, "note": rec.note, "books_equal": after == before},
    )

    assert msg.message_id in affected
    assert rec.status is MessageStatus.SOURCE_REORGED  # terminal
    assert rec.delivered_height is not None            # history preserved
    assert "stands" in rec.note                        # disposition explained
    # exactly one funds effect: books identical to the pre-reorg state
    assert after == before
    assert world["A"].send_disposition(msg.message_id) == "settled"
    # not redeliverable, ever
    with pytest.raises(ReorgRollbackError):
        await world["relayer"].relay_once(msg.message_id)
    # reconciliation is idempotent: no further funds movement, no record change
    assert world["relayer"].reconcile() == []
    assert _books(world) == before


async def test_reorg_before_confirmation_is_compensated(world, make_message, log_case):
    # narrow window: the delivery lands on B but the source reorg runs before
    # the relayer could mark the send settled -> the reorg refunds alice, and
    # reconcile must claw the refund back so one funds effect remains
    msg = make_message(world, amount=100, nonce=1)
    world["B"].deliver(msg)  # direct delivery; relayer not yet informed
    before = _books(world)

    head = world["A"].head().height
    world["A"].reorg_to(head)
    assert world["A"].balance_of("alice") == 1000  # refund happened pre-reconcile
    affected = world["relayer"].reconcile()
    rec = world["store"].get(msg.message_id)
    log_case(
        "reorg_before_confirmation",
        {"depth": head, "affected": affected},
        {"status": rec.status.value, "note": rec.note},
    )

    assert rec.status is MessageStatus.SOURCE_REORGED
    assert "compensated" in rec.note
    assert rec.delivered_height is not None  # recovered from the target chain
    assert _books(world) == before           # converged to the pre-reorg books
    assert world["A"].send_disposition(msg.message_id) == "settled"
    # repeating the state recomputation has no additional effect
    assert world["relayer"].reconcile() == []
    assert _books(world) == before


# ---------------------------------------------------------------------------
# B. permanent failure: distinct terminal state, funds home, channel alive
# ---------------------------------------------------------------------------


async def test_permanent_rejection_releases_channel_and_funds(
    world, make_message, log_case
):
    m1 = make_message(world, receiver="ghost", amount=100, nonce=1)
    world["B"].reject_account("ghost")
    with pytest.raises(TargetRejectedError):
        await world["relayer"].relay_once(m1.message_id)
    rec = world["store"].get(m1.message_id)
    log_case(
        "permanent_rejection",
        {"receiver": "ghost", "amount": 100},
        {"status": rec.status.value, "note": rec.note, "attempts": len(rec.attempts)},
    )

    # distinct terminal state: no retry attempts, reason preserved in the note
    assert rec.status is MessageStatus.FAILED
    assert rec.note.startswith("target_rejected")
    assert "refunded 100 to alice" in rec.note
    assert rec.attempts == []
    # locked funds have a deterministic home: back with the sender, once
    assert world["A"].balance_of("alice") == 1000
    assert world["A"].send_disposition(m1.message_id) == "refunded"
    # the channel is not wedged: the cursor consumed the failed nonce
    assert world["store"].next_nonce("A", "ch") == 2

    # later messages on the same channel deliver normally
    m2 = make_message(world, receiver="bob", amount=30, nonce=2)
    assert await world["relayer"].relay_once(m2.message_id) is MessageStatus.DELIVERED
    assert world["B"].balance_of("bob") == 30

    # the failed message stays terminal; retrying changes nothing (no 2nd refund)
    with pytest.raises(RelayError):
        await world["relayer"].relay_once(m1.message_id)
    # alice still holds her refund; only m2's 30 left her account (settled)
    assert world["A"].balance_of("alice") == 1000 - 30
    assert world["store"].get(m1.message_id).status is MessageStatus.FAILED


async def test_retries_exhausted_releases_channel_and_funds(
    world, make_message, log_case
):
    cfg = world["config"]
    m1 = make_message(world, receiver="bob", amount=40, nonce=1)
    world["B"].inject_transient(m1.message_id, times=cfg.max_attempts + 2)
    with pytest.raises(RetriesExhaustedError):
        await world["relayer"].relay_once(m1.message_id)
    rec = world["store"].get(m1.message_id)
    outcomes = [a.outcome for a in rec.attempts]
    log_case(
        "retries_exhausted",
        {"max_attempts": cfg.max_attempts},
        {"status": rec.status.value, "note": rec.note, "outcomes": outcomes},
    )

    # distinguishable from a permanent rejection: full retry audit trail
    assert rec.status is MessageStatus.FAILED
    assert rec.note.startswith("retries_exhausted")
    assert "refunded 40 to alice" in rec.note
    assert outcomes == ["transient_failure"] * cfg.max_attempts
    assert world["A"].balance_of("alice") == 1000
    assert world["B"].balance_of("bob") == 0
    assert world["store"].next_nonce("A", "ch") == 2

    m2 = make_message(world, receiver="carol", amount=25, nonce=2)
    assert await world["relayer"].relay_once(m2.message_id) is MessageStatus.DELIVERED
    assert world["B"].balance_of("carol") == 25


async def test_expired_message_funds_have_a_home(world, make_message, log_case):
    m1 = make_message(world, nonce=1, amount=10)
    await world["relayer"].relay_once(m1.message_id)  # cursor -> 2
    gap = make_message(world, nonce=3, amount=40)     # skips nonce 2
    with pytest.raises(StaleNonceError) as ei:
        await world["relayer"].relay_once(gap.message_id)
    assert ei.value.retryable
    world["A"].mine(world["config"].stale_window)     # gap never filled
    with pytest.raises(StaleNonceError) as ei2:
        await world["relayer"].relay_once(gap.message_id)
    rec = world["store"].get(gap.message_id)
    log_case(
        "expired",
        {"nonce": 3, "expected": 2},
        {"status": rec.status.value, "note": rec.note, "retryable": ei2.value.retryable},
    )

    assert not ei2.value.retryable
    # terminal and distinct from both failure and transient gap-waiting
    assert rec.status is MessageStatus.EXPIRED
    assert "stale window" in rec.note and "refunded 40 to alice" in rec.note
    # escrow released exactly once: 10 stays settled behind m1's delivery
    assert world["A"].balance_of("alice") == 1000 - 10
    assert world["A"].balance_of(ESCROW_ACCOUNT) == world["config"].escrow_initial + 10
    assert world["A"].send_disposition(gap.message_id) == "refunded"


# ---------------------------------------------------------------------------
# C. interleavings: outcome equals some serial execution
# ---------------------------------------------------------------------------


async def test_concurrent_redelivery_after_target_reorg(world, make_message, log_case):
    msg = make_message(world, amount=70, nonce=1, receiver="zoe")
    await world["relayer"].relay_once(msg.message_id)
    world["B"].mine(2)
    world["B"].reorg_to(3)  # delivery block gone
    world["relayer"].reconcile()
    assert world["store"].get(msg.message_id).status is MessageStatus.TARGET_REORGED
    assert world["B"].balance_of("zoe") == 0

    results = await asyncio.gather(
        *[world["relayer"].relay_once(msg.message_id) for _ in range(6)],
        return_exceptions=True,
    )
    delivered = [r for r in results if r is MessageStatus.DELIVERED]
    duplicates = [r for r in results if isinstance(r, DuplicateDeliveryError)]
    log_case(
        "concurrent_redelivery",
        {"racers": 6},
        {"delivered": len(delivered), "duplicate": len(duplicates)},
    )
    assert len(delivered) == 1 and len(duplicates) == 5
    assert world["B"].balance_of("zoe") == 70  # exactly one funds effect
    assert world["store"].next_nonce("A", "ch") == 2
    # state recomputation after the race changes nothing
    assert world["relayer"].reconcile() == []
    assert world["B"].balance_of("zoe") == 70


async def test_reconcile_during_retry_backoff_is_inert(world, make_message, log_case):
    msg = make_message(world, amount=33, nonce=1, receiver="zoe")
    world["B"].inject_transient(msg.message_id, times=1)
    before = _books(world, receiver="zoe")

    task = asyncio.create_task(world["relayer"].relay_once(msg.message_id))
    await asyncio.sleep(0)  # let the first attempt fail and park on backoff
    mid = world["relayer"].reconcile()  # state recomputation mid-retry
    status = await task
    log_case(
        "reconcile_mid_retry",
        {"transient": 1, "reconcile_affected": mid},
        {"status": status.value},
    )
    assert mid == []                       # in-flight message untouched
    assert status is MessageStatus.DELIVERED
    assert world["B"].balance_of("zoe") == 33
    outcomes = [a.outcome for a in world["store"].get(msg.message_id).attempts]
    assert outcomes == ["transient_failure", "delivered"]
    # books moved by exactly the delivery, nothing else
    after = _books(world, receiver="zoe")
    assert after["sender"] == before["sender"]
    assert after["receiver"] == before["receiver"] + 33
    assert after["supply"] == before["supply"]


async def test_concurrent_handlers_after_source_reorg(world, make_message, log_case):
    msg = make_message(world, amount=55, nonce=1)
    await world["relayer"].relay_once(msg.message_id)
    before = _books(world)
    # the /reorg endpoint performs reorg + reconcile atomically; handlers only
    # ever race the *reconciled* state
    world["A"].reorg_to(world["A"].head().height)  # inclusion gone
    world["relayer"].reconcile()

    results = await asyncio.gather(
        *[world["relayer"].relay_once(msg.message_id) for _ in range(3)],
        return_exceptions=True,
    )
    again = world["relayer"].reconcile()
    rec = world["store"].get(msg.message_id)
    log_case(
        "concurrent_after_source_reorg",
        {"handlers": 3},
        {
            "errors": [getattr(r, "reason", r) for r in results],
            "status": rec.status.value,
            "note": rec.note,
        },
    )
    assert all(isinstance(r, ReorgRollbackError) for r in results)
    assert rec.status is MessageStatus.SOURCE_REORGED
    assert again == []                   # recomputation is idempotent
    assert _books(world) == before       # one funds effect regardless of the race
