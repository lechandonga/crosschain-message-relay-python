"""HTTP interface: drive the local chains/relayer and inspect all state.

Every rejection is rendered with its stable ``reason`` code and whether it
is retryable, so callers can distinguish forgery/replay/duplicate/finality/
rollback/backpressure instead of guessing from an HTTP status alone.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .chain import ESCROW_ACCOUNT, SimulatedChain
from .clock import Clock
from .errors import RelayError
from .models import MessageStatus
from .relayer import Relayer
from .runtime import RelayRuntime

# which error reasons map to which HTTP status (all are still explicit bodies)
_STATUS = {
    "forged_proof": 422,
    "duplicate_delivery": 409,
    "replay_detected": 409,
    "not_finalized": 425,          # Too Early
    "reorg_rollback": 410,         # Gone
    "stale_nonce": 409,
    "target_rejected": 422,
    "transient_failure": 503,
    "timeout": 504,
    "retries_exhausted": 503,
    "invalid_config": 400,
    "backpressure": 503,
}


class SubmitBody(BaseModel):
    chain: str = Field(pattern="^(A|B)$")
    channel: str = Field(min_length=1)
    sender: str = Field(min_length=1)
    receiver: str = Field(min_length=1)
    amount: int = Field(gt=0)
    nonce: int = Field(ge=1)


class MineBody(BaseModel):
    chain: str = Field(pattern="^(A|B)$")
    count: int = Field(default=1, ge=1, le=1000)


class ReorgBody(BaseModel):
    chain: str = Field(pattern="^(A|B)$")
    depth: int = Field(ge=1)


class FaultBody(BaseModel):
    chain: str = Field(pattern="^(A|B)$")
    message_id: str
    transient: int = Field(default=0, ge=0)
    timeout: int = Field(default=0, ge=0)
    reject_receiver: str | None = None


def _record_dict(record: object) -> dict[str, Any]:
    r = record
    return {
        "message_id": r.message_id,
        "channel": r.channel,
        "nonce": r.nonce,
        "status": r.status.value,
        "source_chain": r.source_chain,
        "target_chain": r.target_chain,
        "observed_height": r.observed_height,
        "delivered_height": r.delivered_height,
        "updated_at": r.updated_at,
        "note": r.note,
        "attempts": [
            {
                "attempt": a.attempt,
                "started_at": a.started_at,
                "finished_at": a.finished_at,
                "outcome": a.outcome,
                "detail": a.detail,
            }
            for a in r.attempts
        ],
    }


def _chain_snapshot(chain: SimulatedChain) -> dict[str, Any]:
    head = chain.head()
    return {
        "chain_id": chain.chain_id,
        "head_height": head.height,
        "head_hash": head.hash,
        "total_supply": chain.total_supply(),
        "escrow": chain.balance_of(ESCROW_ACCOUNT),
        "messages": [
            {
                "message_id": m.message_id,
                "height": m.source_height,
                "channel": m.channel,
                "nonce": m.nonce,
                "sender": m.sender,
                "receiver": m.receiver,
                "amount": m.amount,
                "canonical": chain.is_message_canonical(m.message_id),
            }
            for m in chain.canonical_messages()
        ],
        "deliveries": [
            {"message_id": mid, "height": h}
            for mid, h in sorted(chain.delivered_heights().items())
        ],
        "settlements": [
            {
                "message_id": mid,
                "kind": str(entry["kind"]),
                "sender": entry["sender"],
                "amount": entry["amount"],
                "height": entry["height"],
            }
            for mid, entry in sorted(chain.canonical_settlements().items())
        ],
    }


def create_app(
    relayer: Relayer,
    runtime: RelayRuntime | None = None,
    clock: Clock | None = None,
) -> FastAPI:
    app = FastAPI(title="Local Cross-Chain Message Relay", version="0.1.0")
    app.state.relayer = relayer
    app.state.runtime = runtime
    app.state.clock = clock

    def _chain(chain_id: str) -> SimulatedChain:
        a, b = relayer.chains()
        if chain_id == "A":
            return a
        return b

    @app.exception_handler(RelayError)
    async def _relay_error_handler(_request, exc: RelayError) -> JSONResponse:
        code = _STATUS.get(exc.reason, 400)
        return JSONResponse(
            status_code=code,
            content={
                "ok": False,
                "reason": exc.reason,
                "retryable": bool(exc.retryable),
                "detail": str(exc),
            },
        )

    @app.get("/")
    def root() -> dict[str, str]:
        return {"service": "local-crosschain-relay", "status": "ok"}

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "clock_ms": clock.now_ms() if clock else None,
            "config": relayer.config.__dict__,
        }

    @app.get("/chains")
    def chains() -> dict[str, Any]:
        a, b = relayer.chains()
        return {"A": _chain_snapshot(a), "B": _chain_snapshot(b)}

    @app.get("/chains/{chain_id}")
    def chain_one(chain_id: str) -> dict[str, Any]:
        return _chain_snapshot(_chain(chain_id))

    @app.post("/chains/{chain_id}/submit")
    def submit(chain_id: str, body: SubmitBody) -> dict[str, Any]:
        chain = _chain(chain_id)
        if body.chain != chain_id:
            # explicit rather than silently honouring one or the other
            raise RelayError("path chain and body chain differ")
        placeholder = chain.submit_message(
            channel=body.channel,
            sender=body.sender,
            receiver=body.receiver,
            amount=body.amount,
            nonce=body.nonce,
        )
        return {
            "ok": True,
            "pending_message_id": placeholder.message_id,
            "note": "included on the next mine() call",
        }

    @app.post("/mine")
    def mine(body: MineBody) -> dict[str, Any]:
        chain = _chain(body.chain)
        blocks = chain.mine(body.count)
        return {
            "ok": True,
            "chain": body.chain,
            "blocks": [{"height": b.height, "hash": b.hash} for b in blocks],
        }

    @app.post("/reorg")
    async def reorg(body: ReorgBody) -> dict[str, Any]:
        chain = _chain(body.chain)
        blocks = chain.reorg_to(body.depth)
        affected = await relayer.reconcile()
        return {
            "ok": True,
            "chain": body.chain,
            "new_tip": {"height": chain.head().height, "hash": chain.head().hash},
            "fork_blocks": [{"height": b.height, "hash": b.hash} for b in blocks],
            "reconciled_message_ids": affected,
        }

    @app.post("/faults")
    def faults(body: FaultBody) -> dict[str, Any]:
        chain = _chain(body.chain)
        if body.transient:
            chain.inject_transient(body.message_id, times=body.transient)
        if body.timeout:
            chain.inject_timeout(body.message_id, times=body.timeout)
        if body.reject_receiver:
            chain.reject_account(body.reject_receiver)
        return {"ok": True, "chain": body.chain, **body.model_dump()}

    @app.post("/scan")
    async def scan() -> dict[str, Any]:
        newly = relayer.scan()
        return {"ok": True, "newly_observed": newly}

    @app.post("/reconcile")
    async def reconcile() -> dict[str, Any]:
        affected = await relayer.reconcile()
        return {"ok": True, "affected": affected}

    @app.get("/messages")
    def messages(status: str | None = None) -> dict[str, Any]:
        if status is None:
            records = relayer.store.all_records()
        else:
            try:
                wanted = MessageStatus(status)
            except ValueError:
                raise RelayError(f"unknown status filter {status!r}")
            records = relayer.store.records_by_status(wanted)
        return {
            "ok": True,
            "cursors": relayer.store.channel_cursors(),
            "messages": [_record_dict(r) for r in records],
        }

    @app.get("/messages/{message_id}")
    def message_one(message_id: str) -> dict[str, Any]:
        record = relayer.store.get(message_id)
        if record is None:
            return JSONResponse(
                status_code=404,
                content={"ok": False, "reason": "unknown_message", "message_id": message_id},
            )
        return {"ok": True, "message": _record_dict(record)}

    @app.post("/relay/{message_id}")
    async def relay_one(message_id: str) -> dict[str, Any]:
        status = await relayer.relay_once(message_id)
        record = relayer.store.get(message_id)
        return {"ok": True, "status": status.value, "message": _record_dict(record)}

    @app.post("/runtime/scan-enqueue")
    async def scan_enqueue() -> dict[str, Any]:
        if runtime is None:
            return JSONResponse(status_code=400, content={"ok": False, "reason": "no_runtime"})
        enqueued = await runtime.scan_and_enqueue()
        return {"ok": True, "enqueued": enqueued}

    @app.post("/runtime/join")
    async def runtime_join() -> dict[str, Any]:
        if runtime is None:
            return JSONResponse(status_code=400, content={"ok": False, "reason": "no_runtime"})
        await runtime.join()
        return {"ok": True}

    @app.get("/runtime")
    def runtime_status() -> dict[str, Any]:
        if runtime is None:
            return {"ok": True, "runtime": "disabled"}
        s = runtime.stats
        return {
            "ok": True,
            "queue_size": runtime.queue_size,
            "capacity": runtime.capacity,
            "inflight": runtime.inflight,
            "stats": {
                "submitted": s.submitted,
                "processed": s.processed,
                "delivered": s.delivered,
                "failed": s.failed,
                "rejected_not_final": s.rejected_not_final,
                "rejected_permanent": s.rejected_permanent,
                "backpressure": s.backpressure,
                "errors_by_reason": s.errors_by_reason,
            },
        }

    return app
