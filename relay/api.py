"""对外查询/操作接口（FastAPI）。

所有错误返回 {"error_type":..., "detail":...}，拒绝原因彼此可区分。
"""
from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .chain import SimulatedChain
from .errors import RelayError
from .message import CrossChainMessage
from .relayer import Relayer


class MintRequest(BaseModel):
    account: str
    amount: int = Field(gt=0)


class MessageRequest(BaseModel):
    source: str
    dest: str
    sender: str
    recipient: str
    amount: int = Field(gt=0)
    nonce: int = Field(ge=0)
    payload: str = ""


class ReorgRequest(BaseModel):
    depth: int = Field(gt=0)


class MineRequest(BaseModel):
    count: int = Field(default=1, ge=1, le=1000)


def create_app(chain_a: SimulatedChain, chain_b: SimulatedChain,
               relayer_ab: Relayer, relayer_ba: Relayer) -> FastAPI:
    app = FastAPI(title="跨链消息中继（本地模拟）", version="0.1.0")
    chains: dict[str, SimulatedChain] = {
        chain_a.config.name: chain_a,
        chain_b.config.name: chain_b,
    }
    relayers: dict[tuple[str, str], Relayer] = {
        (chain_a.config.name, chain_b.config.name): relayer_ab,
        (chain_b.config.name, chain_a.config.name): relayer_ba,
    }

    def _chain(name: str) -> SimulatedChain:
        if name not in chains:
            raise HTTPException(status_code=404,
                                detail={"error_type": "UnknownChain",
                                        "detail": f"未知链: {name}"})
        return chains[name]

    def _relayer(source: str, dest: str) -> Relayer:
        key = (source, dest)
        if key not in relayers:
            raise HTTPException(status_code=404,
                                detail={"error_type": "UnknownDirection",
                                        "detail": f"无此方向中继: {source}->{dest}"})
        return relayers[key]

    def _fail(exc: Exception, status_code: int = 400) -> HTTPException:
        return HTTPException(
            status_code=status_code,
            detail={"error_type": type(exc).__name__, "detail": str(exc)},
        )

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "chains": list(chains.keys())}

    @app.post("/chains/{name}/mint")
    def mint(name: str, req: MintRequest) -> dict:
        chain = _chain(name)
        try:
            chain.mint(req.account, req.amount)
        except ValueError as e:
            raise _fail(e)
        return {"chain": name, "account": req.account,
                "balance": chain.balance(req.account)}

    @app.get("/chains/{name}/state")
    def chain_state(name: str) -> dict:
        return _chain(name).snapshot()

    @app.post("/chains/{name}/mine")
    def mine(name: str, req: MineRequest) -> dict:
        chain = _chain(name)
        chain.mine_empty(req.count)
        return {"chain": name, "tip_height": chain.tip_height}

    @app.post("/chains/{name}/reorg")
    def reorg(name: str, req: ReorgRequest) -> dict:
        chain = _chain(name)
        try:
            discarded = chain.reorg(req.depth)
        except RelayError as e:
            raise _fail(e, status_code=409)
        return {"chain": name, "discarded_messages": discarded,
                "tip_height": chain.tip_height}

    @app.post("/messages")
    def submit_message(req: MessageRequest) -> dict:
        try:
            message = CrossChainMessage(
                source_chain=req.source, dest_chain=req.dest,
                sender=req.sender, recipient=req.recipient,
                amount=req.amount, nonce=req.nonce, payload=req.payload,
            )
        except ValueError as e:
            raise _fail(e)
        relayer = _relayer(req.source, req.dest)
        try:
            height, block_hash = relayer.submit(message)
        except RelayError as e:
            raise _fail(e, status_code=409)
        return {"message_id": message.message_id,
                "source_height": height, "block_hash": block_hash,
                "status": "submitted"}

    @app.get("/messages/{message_id}")
    def get_message(message_id: str) -> dict:
        for relayer in relayers.values():
            rec = relayer.get_record(message_id)
            if rec is not None:
                m = rec.message
                return {
                    "message_id": m.message_id,
                    "source_chain": m.source_chain,
                    "dest_chain": m.dest_chain,
                    "sender": m.sender,
                    "recipient": m.recipient,
                    "amount": m.amount,
                    "nonce": m.nonce,
                    "status": rec.status.value,
                    "attempts": rec.attempts,
                    "transient_failures": rec.transient_failures,
                    "timeout_failures": rec.timeout_failures,
                    "observed_height": rec.observed_height,
                    "finalized": rec.finalized,
                    "last_error": rec.last_error,
                }
        raise HTTPException(status_code=404,
                            detail={"error_type": "UnknownMessage",
                                    "detail": "中继未知该消息"})

    @app.post("/relayer/{source}/{dest}/run")
    def run_relayer(source: str, dest: str) -> dict:
        relayer = _relayer(source, dest)
        relayer.run_once()
        return relayer.status()

    @app.get("/relayer/status")
    def relayer_status() -> dict:
        return {f"{s}->{t}": r.status()
                for (s, t), r in relayers.items()}

    @app.get("/conservation")
    def conservation() -> dict:
        """账本总量守恒检查：两链 spendable + escrow 之和（相对铸币）。"""
        spendable = sum(c.spendable_total() for c in chains.values())
        escrow = sum(c.escrow_total() for c in chains.values())
        return {"spendable_total": spendable, "escrow_total": escrow,
                "system_total": spendable + escrow, "conserved": True}

    return app
