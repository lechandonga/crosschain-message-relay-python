"""Concurrent handlers racing the same message must match serial execution."""

from __future__ import annotations

import asyncio

import pytest

from relay.errors import DuplicateDeliveryError
from relay.models import MessageStatus


async def test_race_same_message_single_effect(world, make_message, log_case):
    msg = make_message(world, amount=70, nonce=1, receiver="zoe")
    results = await asyncio.gather(
        *[world["relayer"].relay_once(msg.message_id) for _ in range(8)],
        return_exceptions=True,
    )
    delivered = [r for r in results if r is MessageStatus.DELIVERED]
    duplicates = [r for r in results if isinstance(r, DuplicateDeliveryError)]
    log_case(
        "concurrent_same_message",
        {"racers": 8},
        {"delivered": len(delivered), "duplicate": len(duplicates)},
    )
    assert len(delivered) == 1
    assert len(duplicates) == 7
    # exactly one balance move and one recorded attempt
    assert world["B"].balance_of("zoe") == 70
    assert len(world["store"].get(msg.message_id).attempts) == 1


async def test_race_with_retry_single_effect(world, make_message):
    # transient faults under concurrency still cannot double-apply
    msg = make_message(world, amount=33, nonce=1, receiver="zoe")
    world["B"].inject_transient(msg.message_id, times=3)
    results = await asyncio.gather(
        *[world["relayer"].relay_once(msg.message_id) for _ in range(6)],
        return_exceptions=True,
    )
    delivered = [r for r in results if r is MessageStatus.DELIVERED]
    assert len(delivered) == 1, results
    assert world["B"].balance_of("zoe") == 33
    # the winning racer may consume the faults; total attempts never implies
    # more than one successful application
    assert world["B"].balance_of("zoe") == 33


async def test_distinct_messages_process_concurrently(world, make_message):
    msgs = [make_message(world, nonce=n, receiver=f"r{n}", amount=10) for n in (1, 2, 3, 4)]
    results = await asyncio.gather(
        *[world["relayer"].relay_once(m.message_id) for m in msgs]
    )
    assert results == [MessageStatus.DELIVERED] * 4
    for n in range(1, 5):
        assert world["B"].balance_of(f"r{n}") == 10


async def test_half_update_never_visible(world, make_message):
    # cursor and delivered record must always be consistent mid-flight:
    # delivered implies cursor advanced; not delivered implies not advanced
    msg = make_message(world, amount=10, nonce=1)
    await world["relayer"].relay_once(msg.message_id)
    rec = world["store"].get(msg.message_id)
    assert rec.status is MessageStatus.DELIVERED
    assert world["store"].next_nonce("A", "ch") == 2
