"""HTTP interface: drive the local chains/relayer and inspect all state."""

from __future__ import annotations

from fastapi import FastAPI

from .relayer import Relayer
from .runtime import RelayRuntime
from .clock import Clock


def create_app(
    relayer: Relayer,
    runtime: RelayRuntime | None = None,
    clock: Clock | None = None,
) -> FastAPI:
    """Build the FastAPI app bound to a fully constructed local world."""
    raise NotImplementedError
