"""Entry point: construct the local two-chain world and serve its API."""

from __future__ import annotations

from fastapi import FastAPI

from relay.api import create_app
from relay.chain import SimulatedChain
from relay.clock import Clock
from relay.config import build_config
from relay.relayer import Relayer
from relay.runtime import RelayRuntime
from relay.store import RelayStore

app: FastAPI


def build_world() -> tuple[Relayer, RelayRuntime, Clock]:
    raise NotImplementedError


app = FastAPI()


@app.get("/")
def read_root() -> dict[str, str]:
    return {"Hello": "World"}
