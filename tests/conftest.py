"""共享测试夹具：确定时钟、注入式 sleep、双连接链与双向中继。"""
from __future__ import annotations

import logging

import pytest

from relay.chain import SimulatedChain
from relay.config import ChainConfig, RelayConfig
from relay.message import CrossChainMessage
from relay.relayer import Relayer

logging.basicConfig(level=logging.INFO,
                    format="%(levelname)s %(name)s: %(message)s")


class FakeClock:
    """确定性虚拟时钟：sleep 只推进时钟，不消耗真实时间。"""

    def __init__(self) -> None:
        self.now = 0.0

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def fake_clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def cfg(fake_clock: FakeClock) -> RelayConfig:
    return RelayConfig(backoff_base_ms=10, backoff_max_ms=80, max_retries=4)


@pytest.fixture
def chains(cfg: RelayConfig):
    a = SimulatedChain(ChainConfig("A", 2))
    b = SimulatedChain(ChainConfig("B", 2))
    a.connect(b)
    a.mint("alice", 1_000_000)
    b.mint("bob", 1_000_000)
    return a, b


@pytest.fixture
def relayers(chains, cfg, fake_clock):
    a, b = chains
    ab = Relayer(a, b, cfg, sleep=fake_clock.sleep, clock=fake_clock.time)
    ba = Relayer(b, a, cfg, sleep=fake_clock.sleep, clock=fake_clock.time)
    return ab, ba


@pytest.fixture
def env(chains, relayers):
    a, b = chains
    ab, ba = relayers
    return {"A": a, "B": b, "AB": ab, "BA": ba}


def make_msg(source="A", dest="B", sender="alice", recipient="carol",
             amount=100, nonce=0, payload="") -> CrossChainMessage:
    return CrossChainMessage(source, dest, sender, recipient, amount, nonce, payload)
