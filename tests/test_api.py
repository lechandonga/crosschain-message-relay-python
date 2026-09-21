"""HTTP API integration: lifecycle, distinct rejection codes, state queries."""

from __future__ import annotations

import dataclasses

import pytest

httpx = pytest.importorskip("httpx")

from relay.api import create_app
from relay.chain import ESCROW_ACCOUNT, SimulatedChain
from relay.crypto import sign_message
from relay.relayer import Relayer
from relay.runtime import RelayRuntime
from relay.store import RelayStore


@pytest.fixture
async def client():
    clock = __import__("asyncio").get_running_loop()._vclock
    from relay.config import build_config
    cfg = build_config()
    a = SimulatedChain("A", genesis_balances={"alice": 1000}, clock=clock)
    b = SimulatedChain("B", genesis_balances={ESCROW_ACCOUNT: cfg.escrow_initial}, clock=clock)
    relayer = Relayer(a, b, RelayStore(), cfg, clock)
    runtime = RelayRuntime(relayer, clock)
    await runtime.start()
    app = create_app(relayer, runtime=runtime, clock=clock)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        yield c, relayer, a, b, clock
    await runtime.stop()


async def test_health_and_chains(client):
    c, *_ = client
    assert (await c.get("/health")).json()["status"] == "ok"
    chains = (await c.get("/chains")).json()
    assert chains["A"]["total_supply"] == 1000
    assert chains["B"]["total_supply"] > 0


async def test_full_lifecycle_and_duplicate_code(client):
    c, relayer, a, b, clock = client
    r = await c.post("/chains/A/submit", json={
        "chain": "A", "channel": "ch", "sender": "alice",
        "receiver": "bob", "amount": 100, "nonce": 1})
    assert r.status_code == 200
    mid = r.json()["pending_message_id"]
    await c.post("/mine", json={"chain": "A", "count": 3})
    await c.post("/scan")
    r = await c.post(f"/relay/{mid}")
    assert r.status_code == 200 and r.json()["status"] == "delivered"
    # duplicate -> 409 with stable reason
    r = await c.post(f"/relay/{mid}")
    assert r.status_code == 409 and r.json()["reason"] == "duplicate_delivery"


async def test_not_finalized_code(client):
    c, *_ = client
    r = await c.post("/chains/A/submit", json={
        "chain": "A", "channel": "ch", "sender": "alice",
        "receiver": "bob", "amount": 10, "nonce": 1})
    mid = r.json()["pending_message_id"]
    await c.post("/mine", json={"chain": "A", "count": 1})
    await c.post("/scan")
    r = await c.post(f"/relay/{mid}")
    assert r.status_code == 425 and r.json()["reason"] == "not_finalized"
    assert r.json()["retryable"] is True


async def test_forged_proof_not_directly_postable_but_status_queryable(client):
    # the API auto-signs; query endpoints still expose full state
    c, relayer, a, b, clock = client
    r = await c.post("/chains/A/submit", json={
        "chain": "A", "channel": "ch", "sender": "alice",
        "receiver": "bob", "amount": 10, "nonce": 1})
    mid = r.json()["pending_message_id"]
    await c.post("/mine", json={"chain": "A", "count": 3})
    await c.post("/scan")
    # exercise forged path directly through the relayer then read via API
    msg = a.get_message(mid)
    forged = dataclasses.replace(sign_message(msg, relayer.secret), signature="0")
    from relay.errors import ForgedProofError
    with pytest.raises(ForgedProofError):
        await relayer.relay_once(mid, proof=forged)
    r = await c.get(f"/messages/{mid}")
    assert r.json()["message"]["status"] != "delivered"


async def test_runtime_stats_and_backpressure_endpoint(client):
    c, relayer, a, b, clock = client
    for n in range(1, 4):
        await c.post("/chains/A/submit", json={
            "chain": "A", "channel": "ch", "sender": "alice",
            "receiver": f"r{n}", "amount": 10, "nonce": n})
        await c.post("/mine", json={"chain": "A", "count": 3})
    enq = await c.post("/runtime/scan-enqueue")
    assert enq.status_code == 200
    await c.post("/runtime/join")
    rt = (await c.get("/runtime")).json()
    assert rt["stats"]["delivered"] == 3


async def test_reorg_endpoint_reconciles(client):
    c, relayer, a, b, clock = client
    r = await c.post("/chains/A/submit", json={
        "chain": "A", "channel": "ch", "sender": "alice",
        "receiver": "bob", "amount": 100, "nonce": 1})
    mid = r.json()["pending_message_id"]
    await c.post("/mine", json={"chain": "A", "count": 3})
    await c.post("/scan")
    await c.post(f"/relay/{mid}")
    # reorg the whole A chain: source inclusion gone
    head = (await c.get("/chains/A")).json()["head_height"]
    r = await c.post("/reorg", json={"chain": "A", "depth": head})
    assert r.status_code == 200
    assert mid in r.json()["reconciled_message_ids"]
    r = await c.get(f"/messages/{mid}")
    assert r.json()["message"]["status"] == "source_reorged"


async def test_bad_submit_body_rejected(client):
    c, *_ = client
    r = await c.post("/chains/A/submit", json={
        "chain": "A", "channel": "ch", "sender": "alice",
        "receiver": "bob", "amount": -5, "nonce": 1})
    assert r.status_code == 422  # pydantic validation, never silently accepted
