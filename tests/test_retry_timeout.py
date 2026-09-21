"""Retry/backoff/timeout: distinguishable faults, bounded retries, conservation."""

from __future__ import annotations

import pytest

from relay.errors import (
    RetriesExhaustedError,
    TargetRejectedError,
    TimeoutError_,
    TransientFailureError,
)
from relay.models import MessageStatus


async def test_transient_then_success_uses_exponential_backoff(world, make_message, log_case):
    msg = make_message(world, receiver="bob", amount=100, nonce=1)
    world["B"].inject_transient(msg.message_id, times=2)
    t0 = world["clock"].now_ms()
    status = await world["relayer"].relay_once(msg.message_id)
    elapsed = world["clock"].now_ms() - t0
    assert status is MessageStatus.DELIVERED
    # base=50: backoffs 50, 100
    assert elapsed == 50 + 100, elapsed
    log_case(
        "transient_retry",
        {"transient_times": 2, "backoff_ms": elapsed},
        "delivered after 2 retryable faults",
    )
    rec = world["store"].get(msg.message_id)
    assert [a.outcome for a in rec.attempts] == [
        "transient_failure",
        "transient_failure",
        "delivered",
    ]
    # conservation despite retries: receiver credited exactly once
    assert world["B"].balance_of("bob") == 100


async def test_timeout_is_retryable_and_distinct(world, make_message, log_case):
    msg = make_message(world, receiver="bob", amount=10, nonce=1)
    world["B"].inject_timeout(msg.message_id, times=1)
    status = await world["relayer"].relay_once(msg.message_id)
    assert status is MessageStatus.DELIVERED
    outcomes = [a.outcome for a in world["store"].get(msg.message_id).attempts]
    log_case("timeout_retry", {"timeout_times": 1}, outcomes)
    assert outcomes[0] == "timeout" and outcomes[-1] == "delivered"
    assert world["B"].balance_of("bob") == 10


async def test_retries_have_a_hard_cap(world, make_message, log_case):
    cfg = world["config"]
    msg = make_message(world, receiver="bob", amount=10, nonce=1)
    # more persistent faults than attempts -> exhaustion
    world["B"].inject_transient(msg.message_id, times=cfg.max_attempts + 5)
    with pytest.raises(RetriesExhaustedError) as ei:
        await world["relayer"].relay_once(msg.message_id)
    rec = world["store"].get(msg.message_id)
    log_case("retries_exhausted", {"max_attempts": cfg.max_attempts}, ei.value.reason)
    assert rec.status is MessageStatus.FAILED
    assert len(rec.attempts) == cfg.max_attempts
    # no partial balance effect after exhaustion
    assert world["B"].balance_of("bob") == 0
    assert world["B"].total_supply() == cfg.escrow_initial


async def test_permanent_rejection_is_not_retried(world, make_message, log_case):
    msg = make_message(world, receiver="ghost", amount=10, nonce=1)
    world["B"].reject_account("ghost")
    with pytest.raises(TargetRejectedError) as ei:
        await world["relayer"].relay_once(msg.message_id)
    rec = world["store"].get(msg.message_id)
    log_case("target_rejected", {"receiver": "ghost"}, ei.value.reason)
    assert rec.status is MessageStatus.FAILED
    assert len(rec.attempts) == 0  # permanent faults skip the retry loop
    assert world["B"].balance_of("ghost") == 0


async def test_backoff_is_capped(world, make_message):
    # inject enough faults to exceed the cap; ensure no sleep exceeds cap
    cfg = world["config"]
    msg = make_message(world, receiver="bob", amount=10, nonce=1)
    world["B"].inject_timeout(msg.message_id, times=cfg.max_attempts + 10)
    sleeps: list[int] = []
    orig = world["clock"].sleep

    async def tracking(ms: int):
        sleeps.append(ms)
        await orig(ms)

    world["clock"].sleep = tracking  # type: ignore[assignment]
    with pytest.raises(RetriesExhaustedError):
        await world["relayer"].relay_once(msg.message_id)
    assert sleeps, "expected at least one backoff sleep"
    assert max(sleeps) <= cfg.backoff_cap_ms
