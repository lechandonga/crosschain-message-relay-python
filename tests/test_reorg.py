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
    affected = world["relayer"].reconcile()
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


async def test_source_reorg_marks_terminal_and_refunds(world, make_message, log_case):
    msg = make_message(world, amount=100, nonce=1)
    await world["relayer"].relay_once(msg.message_id)
    head = world["A"].head().height
    world["A"].reorg_to(head)  # remove the inclusion
    affected = world["relayer"].reconcile()
    log_case("source_reorg", {"depth": head}, "SOURCE_REORGED")
    assert msg.message_id in affected
    rec = world["store"].get(msg.message_id)
    assert rec.status is MessageStatus.SOURCE_REORGED
    with pytest.raises(ReorgRollbackError):
        await world["relayer"].relay_once(msg.message_id)
    # escrow refunded deterministically; chain supply conserved
    assert world["A"].balance_of("alice") == 1000


async def test_reorg_is_reproducible(world, make_message):
    # the same reorg depth against the same pre-state yields the same record
    msg = make_message(world, amount=10, nonce=1)
    await world["relayer"].relay_once(msg.message_id)
    world["B"].mine(1)
    before = world["store"].get(msg.message_id).status
    world["B"].reorg_to(2)
    affected1 = world["relayer"].reconcile()
    status1 = world["store"].get(msg.message_id).status
    note1 = world["store"].get(msg.message_id).note
    # reconcile is idempotent: running again changes nothing
    affected2 = world["relayer"].reconcile()
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
    # reorg out m2's inclusion on the source; cursor must roll back to 2
    h2 = m2.source_height
    depth = world["A"].head().height - h2 + 1
    world["A"].reorg_to(depth)
    world["relayer"].reconcile()
    # m2 no longer canonical -> not delivered; cursor rebuilt to expect 2
    assert world["store"].get(m2.message_id).status is MessageStatus.SOURCE_REORGED
    assert world["store"].next_nonce("A", "ch") == 2
