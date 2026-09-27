"""Interleaved delivery / retry / reorg / reconcile settlement.

Pinned scenarios (deterministic on the virtual clock; inputs and decision
bases are logged):
  * a retry parked on backoff must not deliver once the source inclusion was
    reorged out and reconciled -- the terminal reorg conclusion stands;
  * delivered, then the target delivery reverts, then the source inclusion
    is reorged away (either order) -> no valid delivery exists anywhere, so
    the locked escrow deterministically returns to the sender;
  * delivery / retry / reconcile / reorg interleaved or concurrent -> the
    outcome equals some serial execution: one funds effect, no half states,
    and re-running the state recomputation has no additional effect.
"""

from __future__ import annotations

import asyncio

import pytest

from relay.chain import ESCROW_ACCOUNT
from relay.errors import ReorgRollbackError
from relay.models import MessageStatus


def _books(world, *, sender="alice", receiver="zoe"):
    a, b = world["A"], world["B"]
    return {
        "sender": a.balance_of(sender),
        "escrow_a": a.balance_of(ESCROW_ACCOUNT),
        "receiver": b.balance_of(receiver),
        "escrow_b": b.balance_of(ESCROW_ACCOUNT),
        "supply": (a.total_supply(), b.total_supply()),
    }


# ---------------------------------------------------------------------------
# 1. retry parked on backoff + source reorg + reconcile -> retry must not
#    deliver and must not overwrite the terminal reorg conclusion
# ---------------------------------------------------------------------------


async def test_retry_waiting_source_reorg_reconciled_stays_terminal(
    world, make_message, log_case
):
    msg = make_message(world, amount=60, nonce=1, receiver="zoe")
    world["B"].inject_transient(msg.message_id, times=1)  # first attempt fails
    before = _books(world)

    task = asyncio.create_task(world["relayer"].relay_once(msg.message_id))
    await asyncio.sleep(0)  # attempt 1 failed; worker parked on backoff

    # the source inclusion is reorged out *while the retry waits*, and the
    # reconciliation completes before the backoff elapses
    head = world["A"].head().height
    world["A"].reorg_to(head)
    affected = world["relayer"].reconcile()
    reconciled = world["store"].get(msg.message_id)
    mid = (reconciled.status, reconciled.note)

    with pytest.raises(ReorgRollbackError):
        await task  # the parked retry must refuse, not deliver

    rec = world["store"].get(msg.message_id)
    books = _books(world)
    log_case(
        "retry_waiting_source_reorg",
        {"amount": 60, "depth": head, "affected": affected},
        {
            "reconciled": (mid[0].value, mid[1]),
            "final": (rec.status.value, rec.note),
            "books": books,
        },
    )

    assert msg.message_id in affected
    assert mid[0] is MessageStatus.SOURCE_REORGED
    # the terminal reorg conclusion was not overwritten by the late retry
    assert rec.status is MessageStatus.SOURCE_REORGED
    assert rec.note == mid[1]
    assert "no canonical target delivery" in rec.note
    assert "refund" in rec.note
    # no new credit on the target; the sender was refunded exactly once
    assert books["receiver"] == 0
    assert books["sender"] == 1000
    assert books["escrow_a"] == world["config"].escrow_initial
    assert books["supply"] == before["supply"]
    # stable against further relay attempts and further reconciliations
    with pytest.raises(ReorgRollbackError):
        await world["relayer"].relay_once(msg.message_id)
    assert world["relayer"].reconcile() == []
    again = world["store"].get(msg.message_id)
    assert again.status is MessageStatus.SOURCE_REORGED
    assert again.note == mid[1]
    assert _books(world) == books


# ---------------------------------------------------------------------------
# 2. delivered -> target reorg -> source reorg (both orders): with no valid
#    delivery anywhere, the locked escrow returns to the sender exactly once
# ---------------------------------------------------------------------------


async def test_target_reorg_then_source_reorg_refunds_sender(
    world, make_message, log_case
):
    msg = make_message(world, amount=80, nonce=1, receiver="zoe")
    await world["relayer"].relay_once(msg.message_id)
    assert world["B"].balance_of("zoe") == 80

    world["B"].mine(2)
    world["B"].reorg_to(3)  # target delivery reverted first
    world["relayer"].reconcile()
    assert (
        world["store"].get(msg.message_id).status is MessageStatus.TARGET_REORGED
    )

    head = world["A"].head().height
    world["A"].reorg_to(head)  # then the source inclusion is reorged out
    affected = world["relayer"].reconcile()
    rec = world["store"].get(msg.message_id)
    books = _books(world)
    log_case(
        "target_reorg_then_source_reorg",
        {"amount": 80, "depth": head, "affected": affected},
        {"status": rec.status.value, "note": rec.note, "books": books},
    )

    # no valid delivery exists anywhere: funds must not hang in escrow
    assert msg.message_id in affected
    assert rec.status is MessageStatus.SOURCE_REORGED
    assert "refunded 80 to alice" in rec.note
    assert rec.delivered_height is not None  # rollback history preserved
    assert books["sender"] == 1000
    assert books["receiver"] == 0
    assert books["escrow_a"] == world["config"].escrow_initial
    assert world["A"].send_disposition(msg.message_id) == "refunded"
    # terminal and idempotent: no second refund, no record change
    assert world["relayer"].reconcile() == []
    assert _books(world) == books
    with pytest.raises(ReorgRollbackError):
        await world["relayer"].relay_once(msg.message_id)


async def test_source_reorg_then_target_reorg_releases_escrow(
    world, make_message, log_case
):
    msg = make_message(world, amount=80, nonce=1, receiver="zoe")
    await world["relayer"].relay_once(msg.message_id)

    head = world["A"].head().height
    world["A"].reorg_to(head)  # source inclusion gone; delivery still stands
    world["relayer"].reconcile()
    rec = world["store"].get(msg.message_id)
    assert rec.status is MessageStatus.SOURCE_REORGED
    assert "stands" in rec.note
    assert world["A"].send_disposition(msg.message_id) == "settled"

    world["B"].reorg_to(1)  # now the target delivery reverts too
    affected = world["relayer"].reconcile()
    rec = world["store"].get(msg.message_id)
    books = _books(world)
    log_case(
        "source_reorg_then_target_reorg",
        {"amount": 80, "affected": affected},
        {"status": rec.status.value, "note": rec.note, "books": books},
    )

    # the delivery that backed the escrow lock no longer exists: release it
    assert msg.message_id in affected
    assert rec.status is MessageStatus.SOURCE_REORGED
    assert "refunded 80 to alice" in rec.note
    assert rec.delivered_height is not None  # history not erased
    assert books["sender"] == 1000
    assert books["escrow_a"] == world["config"].escrow_initial
    assert world["A"].send_disposition(msg.message_id) == "refunded"
    assert world["relayer"].reconcile() == []
    assert _books(world) == books


# ---------------------------------------------------------------------------
# 3. delivery / retry / reconcile / reorg interleaved or concurrent ->
#    equivalent to some serial execution, exactly one funds effect
# ---------------------------------------------------------------------------


async def test_interleaved_retry_reorg_reconcile_concurrent_handlers(
    world, make_message, log_case
):
    msg = make_message(world, amount=45, nonce=1, receiver="zoe")
    world["B"].inject_transient(msg.message_id, times=1)

    task = asyncio.create_task(world["relayer"].relay_once(msg.message_id))
    await asyncio.sleep(0)  # attempt 1 failed; worker parked on backoff

    world["A"].reorg_to(world["A"].head().height)  # inclusion gone mid-retry
    world["relayer"].reconcile()

    # the parked retry resumes while extra handlers race the same message
    results = await asyncio.gather(
        task,
        world["relayer"].relay_once(msg.message_id),
        world["relayer"].relay_once(msg.message_id),
        return_exceptions=True,
    )
    again = world["relayer"].reconcile()
    rec = world["store"].get(msg.message_id)
    books = _books(world)
    log_case(
        "interleaved_retry_reorg_concurrent",
        {"handlers": 3},
        {
            "errors": [getattr(r, "reason", r) for r in results],
            "status": rec.status.value,
            "note": rec.note,
            "books": books,
        },
    )

    # every handler converges to the reorg conclusion; nobody delivered
    assert all(isinstance(r, ReorgRollbackError) for r in results)
    assert rec.status is MessageStatus.SOURCE_REORGED
    assert books["receiver"] == 0
    assert books["sender"] == 1000  # refunded exactly once
    assert again == []              # recomputation after the race is inert
    assert _books(world) == books


async def test_double_reorg_concurrent_handlers_single_refund(
    world, make_message, log_case
):
    msg = make_message(world, amount=80, nonce=1, receiver="zoe")
    await world["relayer"].relay_once(msg.message_id)

    world["B"].reorg_to(1)                          # delivery reverted
    world["A"].reorg_to(world["A"].head().height)   # inclusion gone too
    first = world["relayer"].reconcile()

    results = await asyncio.gather(
        *[world["relayer"].relay_once(msg.message_id) for _ in range(3)],
        return_exceptions=True,
    )
    second = world["relayer"].reconcile()
    rec = world["store"].get(msg.message_id)
    books = _books(world)
    log_case(
        "double_reorg_concurrent",
        {"handlers": 3, "first_affected": first, "second_affected": second},
        {
            "errors": [getattr(r, "reason", r) for r in results],
            "status": rec.status.value,
            "note": rec.note,
            "books": books,
        },
    )

    assert msg.message_id in first
    assert all(isinstance(r, ReorgRollbackError) for r in results)
    assert rec.status is MessageStatus.SOURCE_REORGED
    assert "refunded 80 to alice" in rec.note
    assert books["sender"] == 1000          # exactly one refund
    assert books["receiver"] == 0
    assert books["escrow_a"] == world["config"].escrow_initial
    assert second == []                     # idempotent after the race
    assert _books(world) == books
