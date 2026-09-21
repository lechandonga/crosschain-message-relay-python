"""应用装配：配置校验 -> 建链/账本/中继 -> FastAPI。"""
from __future__ import annotations

from dataclasses import dataclass

from fastapi import FastAPI

from .api import create_app
from .chain import SimulatedChain
from .config import RelayConfig, default_config
from .relayer import Relayer


@dataclass
class AppContext:
    config: RelayConfig
    chain_a: SimulatedChain
    chain_b: SimulatedChain
    relayer_ab: Relayer
    relayer_ba: Relayer
    app: FastAPI


def build_context(config: RelayConfig | None = None,
                  *, seed_balances: dict[tuple[str, str], int] | None = None
                  ) -> AppContext:
    config = config or default_config()
    config.validate()  # 非法配置在此明确抛出 ConfigError
    chain_a = SimulatedChain(config.chain_a)
    chain_b = SimulatedChain(config.chain_b)
    chain_a.connect(chain_b)
    relayer_ab = Relayer(chain_a, chain_b, config)
    relayer_ba = Relayer(chain_b, chain_a, config)
    app = create_app(chain_a, chain_b, relayer_ab, relayer_ba)
    ctx = AppContext(config=config, chain_a=chain_a, chain_b=chain_b,
                     relayer_ab=relayer_ab, relayer_ba=relayer_ba, app=app)
    for (chain_name, account), amount in (seed_balances or {}).items():
        chain = chain_a if chain_name == chain_a.config.name else chain_b
        chain.mint(account, amount)
    return ctx


def build_app(config: RelayConfig | None = None) -> FastAPI:
    return build_context(config).app
