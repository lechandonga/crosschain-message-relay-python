"""Entry point: construct the local two-chain world and serve its API.

The simulated chains, relayer and worker runtime all run on a single
deterministic :class:`~relay.clock.VirtualTimeEventLoop` in a background
thread. Timers (retry backoff) fast-forward virtually, while the HTTP server
runs on uvicorn's own loop in the main thread and schedules relay work onto
the virtual loop via ``run_coroutine_threadsafe``. No real chain node,
credential or paid service is involved.
"""

from __future__ import annotations

import json
import os
import threading

from fastapi import FastAPI

from relay.api import create_app
from relay.chain import ESCROW_ACCOUNT, SimulatedChain
from relay.clock import Clock, VirtualTimeEventLoop
from relay.config import build_config
from relay.relayer import Relayer
from relay.runtime import RelayRuntime
from relay.store import RelayStore

DEFAULT_ACCOUNTS = {"alice": 1_000, "bob": 1_000, "carol": 1_000}


def _config_from_env():
    """Read overrides from RELAY_* env vars; invalid values are rejected."""
    fields = RelayConfig_fields()
    overrides: dict[str, int] = {}
    for name in fields:
        env = os.environ.get(f"RELAY_{name.upper()}")
        if env is None:
            continue
        try:
            overrides[name] = int(env)
        except ValueError as exc:  # explicit, never silently ignored
            raise ValueError(f"RELAY_{name.upper()}={env!r} must be an integer") from exc
    return build_config(**overrides)


def RelayConfig_fields() -> set[str]:
    from relay.config import RelayConfig

    return set(RelayConfig.__dataclass_fields__)


def build_world() -> tuple[Relayer, RelayRuntime, Clock]:
    """Construct chains, store, relayer and runtime on a fresh virtual clock."""
    cfg = _config_from_env()
    clock = Clock()
    chain_a = SimulatedChain("A", genesis_balances=dict(DEFAULT_ACCOUNTS), clock=clock)
    chain_b = SimulatedChain(
        "B", genesis_balances={ESCROW_ACCOUNT: cfg.escrow_initial}, clock=clock
    )
    store = RelayStore()
    relayer = Relayer(chain_a, chain_b, store, cfg, clock)
    runtime = RelayRuntime(relayer, clock)
    return relayer, runtime, clock


def build_app() -> FastAPI:
    """Build the API and start the virtual-loop runtime in a background thread."""
    relayer, runtime, clock = build_world()
    app = create_app(relayer, runtime=runtime, clock=clock)

    import asyncio

    vloop = VirtualTimeEventLoop(clock)

    def _run_virtual_loop() -> None:
        asyncio.set_event_loop(vloop)
        vloop.run_forever()

    thread = threading.Thread(target=_run_virtual_loop, name="virtual-relay-loop", daemon=True)
    thread.start()

    # start the worker pool on the virtual loop
    fut = asyncio.run_coroutine_threadsafe(runtime.start(), vloop)
    fut.result(timeout=5)

    @app.on_event("shutdown")
    async def _shutdown() -> None:  # pragma: no cover - lifecycle
        try:
            asyncio.run_coroutine_threadsafe(runtime.stop(), vloop).result(timeout=5)
        finally:
            vloop.call_soon_threadsafe(vloop.stop)

    app.state.virtual_loop = vloop
    app.state.virtual_thread = thread
    return app


app = build_app()
