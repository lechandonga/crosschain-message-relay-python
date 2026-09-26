"""Reorg / rollback: deterministic disposition, no silent drop or double effect."""

from __future__ import annotations

import pytest

from relay.errors import DuplicateDeliveryError, ReorgRollbackError
from relay.models import MessageStatus


def _deliver(world, make_message, **kw):
    return make_message(world, **kw)


async def test_target_reorg_redelivers_exactly_once(world, make_message, log_case):
    msg = make_message(world, amount=100, nonce=1)
    await world["relayer"].relay_once(msg.message_id)
    assert world["B"].balance_of("bob") == 100
    # bury the delivery block then reorg it away
    world["B"].mine(2)
    world["B"].reorg_to(3)
    affected = await world["relayer"].reconcile()
    log_case("target_reorg", {"depth": 3, "affected": affected}, "TARGET_REORGED")
    assert msg.message_id in affected
    rec = world["store"].get(msg.message_id)
    assert rec.status is MessageStatus.TARGET_REORGED
    assert world["B"].balance_of("bob") == 0  # reversed
    # redelivery applies once; second attempt is duplicate
    assert await world["relayer"].relay_once(msg.message_id) is MessageStatus.DELIVERED
    assert world["B"].balance_of("bob") == 100
    with pytest.raises(DuplicateDeliveryError):
        await world["relayer"].relay_once(msg.message_id)
    assert world["B"].total_supply() == world["config"].escrow_initial


async def test_source_reorg_after_delivery_keeps_single_effect(world, make_message, log_case):
    amount = 100
    msg = make_message(world, amount=amount, nonce=1)
    await world["relayer"].relay_once(msg.message_id)
    alice_before = world["A"].balance_of("alice")
    bob_before = world["B"].balance_of("bob")
    assert bob_before == amount
    head = world["A"].head().height
    world["A"].reorg_to(head)  # remove the source inclusion
    affected = await world["relayer"].reconcile()
    log_case(
        "source_reorg_after_delivery",
        {"depth": head, "affected": affected},
        "SOURCE_REORGED + refund reclaimed, target delivery retained",
    )
    assert msg.message_id in affected
    rec = world["store"].get(msg.message_id)
    assert rec.status is MessageStatus.SOURCE_REORGED
    assert rec.delivered_height is not None  # history not erased
    # cannot be re-delivered: terminal, distinct reason
    with pytest.raises(ReorgRollbackError):
        await world["relayer"].relay_once(msg.message_id)
    # the chain-level auto-refund was reclaimed into source escrow: the
    # sender did NOT get the money back while the receiver keeps it
    assert world["A"].balance_of("alice") == alice_before
    assert world["B"].balance_of("bob") == bob_before
    # sender+receiver holdings across both chains equal the pre-reorg total
    assert (
        world["A"].balance_of("alice") + world["B"].balance_of("bob")
        == alice_before + bob_before
    )
    # a second reconciliation changes nothing (idempotent, no second reclaim)
    assert await world["relayer"].reconcile() == []
    assert world["A"].balance_of("alice") == alice_before


async def test_source_reorg_before_delivery_refunds(world, make_message, log_case):
    msg = make_message(world, amount=100, nonce=1, confirms=3)
    # reorg the source inclusion away *before* any target delivery
    head = world["A"].head().height
    world["A"].reorg_to(head)
    affected = await world["relayer"].reconcile()
    log_case("source_reorg_before_delivery", {"depth": head}, affected)
    rec = world["store"].get(msg.message_id)
    assert rec.status is MessageStatus.SOURCE_REORGED
    with pytest.raises(ReorgRollbackError):
        await world["relayer"].relay_once(msg.message_id)
    # never delivered -> escrow refunded to sender by the reorg, chain conserved
    assert world["A"].balance_of("alice") == 1000
    assert world["B"].balance_of("bob") == 0


async def test_reorg_is_reproducible(world, make_message):
    # the same reorg depth against the same pre-state yields the same record
    msg = make_message(world, amount=10, nonce=1)
    await world["relayer"].relay_once(msg.message_id)
    world["B"].mine(1)
    before = world["store"].get(msg.message_id).status
    world["B"].reorg_to(2)
    await world["relayer"].reconcile()
    status1 = world["store"].get(msg.message_id).status
    note1 = world["store"].get(msg.message_id).note
    # reconcile is idempotent: running again changes nothing
    affected2 = await world["relayer"].reconcile()
    assert world["store"].get(msg.message_id).status is status1
    assert world["store"].get(msg.message_id).note == note1
    assert affected2 == []
    assert before is MessageStatus.DELIVERED


async def test_cursor_rebuilt_after_reorg(world, make_message):
    m1 = make_message(world, nonce=1, amount=10)
    await world["relayer"].relay_once(m1.message_id)
    m2 = make_message(world, nonce=2, amount=20, receiver="carol")
    await world["relayer"].relay_once(m2.message_id)
    assert world["store"].next_nonce("A", "ch") == 3
    # reorg out m2's inclusion on the source while its target delivery stands
    h2 = m2.source_height
    depth = world["A"].head().height - h2 + 1
    world["A"].reorg_to(depth)
    await world["relayer"].reconcile()
    # m2 terminal with target delivery retained; its nonce stays consumed so
    # channel order does not unwind
    assert world["store"].get(m2.message_id).status is MessageStatus.SOURCE_REORGED
    assert world["store"].next_nonce("A", "ch") == 3
    # sender cannot reuse nonce 2: a replacement send must use a new nonce
    m3 = make_message(world, nonce=3, amount=5, receiver="dave")
    assert await world["relayer"].relay_once(m3.message_id) is MessageStatus.DELIVERED
    assert world["store"].next_nonce("A", "ch") == 4
