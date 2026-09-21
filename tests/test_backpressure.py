"""Bounded queue / rate limiting: no unbounded memory, explicit backpressure."""

from __future__ import annotations

import asyncio

from relay.chain import ESCROW_ACCOUNT, SimulatedChain
from relay.config import build_config
from relay.errors import BackpressureError
from relay.models import MessageStatus
from relay.relayer import Relayer
from relay.runtime import RelayRuntime
from relay.store import RelayStore


def _clock():
    return asyncio.get_running_loop()._vclock


def _bounded(capacity=2, inflight=2, count=6):
    clock = _clock()
    cfg = build_config(queue_capacity=capacity, max_inflight=inflight)
    a = SimulatedChain("A", genesis_balances={"alice": 10_000}, clock=clock)
    b = SimulatedChain("B", genesis_balances={ESCROW_ACCOUNT: 10_000}, clock=clock)
    relayer = Relayer(a, b, RelayStore(), cfg, clock)
    runtime = RelayRuntime(relayer, clock)
    mids = []
    for n in range(1, count + 1):
        ph = a.submit_message(channel="c", sender="alice",
                             receiver=f"r{n}", amount=10, nonce=n)
        a.mine(3)
        relayer.scan()
        mids.append(ph.message_id)
    return relayer, runtime, a, b, mids, cfg


def _gate(relayer):
    gate = asyncio.Event()
    original = relayer.relay_once

    async def gated(mid):
        await gate.wait()
        return await original(mid)

    relayer.relay_once = gated
    return gate, original


async def test_backpressure_when_full_then_drain(log_case):
    relayer, runtime, a, b, mids, cfg = _bounded()
    await runtime.start()
    gate, _ = _gate(relayer)

    accepted, rejected = 0, []
    for mid in mids:
        try:
            if await runtime.submit(mid):
                accepted += 1
        except BackpressureError:
            rejected.append(mid)

    log_case(
        "backpressure",
        {"submitted": len(mids), "capacity": cfg.queue_capacity},
        {"accepted": accepted, "rejected": len(rejected)},
    )
    assert runtime.queue_size <= cfg.queue_capacity
    assert rejected  # at least one explicit rejection, never an unbounded queue
    assert runtime.stats.backpressure == len(rejected)

    gate.set()
    await runtime.join()
    await runtime.stop()

    # rejected ids are retryable afterwards and then apply exactly once
    for mid in rejected:
        assert await relayer.relay_once(mid) is MessageStatus.DELIVERED
    assert a.total_supply() == 10_000 and b.total_supply() == 10_000


async def test_duplicate_submit_coalesced():
    relayer, runtime, _a, b, mids, _cfg = _bounded(count=2)
    await runtime.start()
    gate, _ = _gate(relayer)
    assert await runtime.submit(mids[0]) is True
    assert await runtime.submit(mids[0]) is False  # already pending -> coalesced
    assert runtime.queue_size <= 1
    gate.set()
    await runtime.join()
    await runtime.stop()
    assert runtime.stats.delivered == 1
    assert b.balance_of("r1") == 10
