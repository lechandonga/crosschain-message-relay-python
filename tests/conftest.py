"""Shared fixtures: deterministic virtual clock and a built local world.

Every async test runs on a VirtualTimeEventLoop so retry backoff and
timeouts take zero real time but exact virtual time, identically on every
run.
"""

from __future__ import annotations

import asyncio
import logging

import pytest_asyncio
import pytest

from relay.chain import ESCROW_ACCOUNT, SimulatedChain
from relay.clock import Clock, VirtualTimeEventLoop
from relay.config import build_config
from relay.relayer import Relayer
from relay.runtime import RelayRuntime
from relay.store import RelayStore

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("relay-test")


def log_decision(case: str, payload: object, decision: object) -> None:
    """Print the input and the exact basis for a decision (test audit log)."""
    log.info("CASE=%s INPUT=%r DECISION=%r", case, payload, decision)


@pytest.fixture
def log_case():
    return log_decision


def pytest_asyncio_loop_factories(config, item):
    """Provide the virtual-time loop factory to pytest-asyncio (no policy)."""
    def factory():
        return VirtualTimeEventLoop(Clock())

    return {"virtual": factory}


@pytest_asyncio.fixture
async def world():
    """A fully built two-chain world sharing the running loop's virtual clock."""
    # the running loop is a VirtualTimeEventLoop; reuse its clock so timers
    # scheduled by asyncio.sleep move the same clock the chains read
    running = asyncio.get_running_loop()
    clock = running._vclock
    cfg = build_config()
    chain_a = SimulatedChain(
        "A", genesis_balances={"alice": 1000, "bob": 1000, "carol": 1000}, clock=clock
    )
    chain_b = SimulatedChain(
        "B", genesis_balances={ESCROW_ACCOUNT: cfg.escrow_initial}, clock=clock
    )
    store = RelayStore()
    relayer = Relayer(chain_a, chain_b, store, cfg, clock)
    runtime = RelayRuntime(relayer, clock)
    await runtime.start()
    yield {
        "clock": clock,
        "config": cfg,
        "A": chain_a,
        "B": chain_b,
        "store": store,
        "relayer": relayer,
        "runtime": runtime,
    }
    await runtime.stop()


@pytest.fixture
def make_message():
    """Helper to submit+mine+scan one message, returning the stored Message."""
    def _make(world, *, chain="A", channel="ch", sender="alice",
              receiver="bob", amount=100, nonce=1, confirms=3):
        c = world[chain]
        placeholder = c.submit_message(
            channel=channel, sender=sender, receiver=receiver, amount=amount, nonce=nonce
        )
        c.mine(confirms)
        world["relayer"].scan()
        return c.get_message(placeholder.message_id)
    return _make
