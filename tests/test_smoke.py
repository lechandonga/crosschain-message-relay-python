"""Skeleton smoke test: imports and construction only."""

from __future__ import annotations

from relay.config import build_config


def test_config_builds():
    cfg = build_config()
    assert cfg.finality_depth == 3
