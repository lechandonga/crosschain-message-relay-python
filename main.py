"""本地启动入口：uv run uvicorn main:app"""
from __future__ import annotations

import os

from relay.app import build_context
from relay.config import ChainConfig, RelayConfig


def _build():
    # 环境变量可覆盖关键参数；非法值（含 0/负数）由 validate() 显式拒绝，
    # 绝不静默回落默认值
    def env_int(name: str) -> int | None:
        v = os.environ.get(name)
        if v is None or v == "":
            return None
        return int(v)

    depth = env_int("RELAY_FINALITY_DEPTH")
    base_depth = 2 if depth is None else depth
    ctx = build_context(
        RelayConfig(
            chain_a=ChainConfig("A", base_depth),
            chain_b=ChainConfig("B", base_depth),
            max_retries=_with_default(env_int("RELAY_MAX_RETRIES"), 5),
            backoff_base_ms=_with_default(env_int("RELAY_BACKOFF_BASE_MS"), 50),
            backoff_max_ms=_with_default(env_int("RELAY_BACKOFF_MAX_MS"), 2000),
            delivery_timeout_ms=_with_default(
                env_int("RELAY_DELIVERY_TIMEOUT_MS"), 1000
            ),
            max_pending=_with_default(env_int("RELAY_MAX_PENDING"), 128),
        ),
        seed_balances={
            ("A", "alice"): 1_000_000,
            ("B", "bob"): 1_000_000,
        },
    )
    return ctx


def _with_default(value: int | None, default: int) -> int:
    return default if value is None else value


_context = _build()
app = _context.app
