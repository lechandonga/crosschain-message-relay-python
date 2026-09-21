"""Shared fixtures and logging helpers for relay tests."""

from __future__ import annotations

import logging

import pytest

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("relay-test")


def log_decision(case: str, payload: object, decision: object) -> None:
    log.info("case=%s input=%r decision=%r", case, payload, decision)


@pytest.fixture
def log_case():
    return log_decision
