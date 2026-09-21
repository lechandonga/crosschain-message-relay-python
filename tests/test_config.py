"""Configuration: deterministic defaults and explicit rejection of bad input."""

from __future__ import annotations

import pytest

from relay.config import build_config
from relay.errors import InvalidConfigError


def test_defaults_are_deterministic():
    cfg = build_config()
    assert (
        cfg.finality_depth,
        cfg.max_attempts,
        cfg.base_backoff_ms,
        cfg.backoff_cap_ms,
        cfg.timeout_ms,
        cfg.stale_window,
        cfg.queue_capacity,
        cfg.max_inflight,
    ) == (3, 4, 50, 1000, 200, 8, 256, 8)


@pytest.mark.parametrize(
    "overrides",
    [
        {"finality_depth": 0},
        {"max_attempts": -1},
        {"base_backoff_ms": 0},
        {"queue_capacity": 0},
        {"stale_window": -3},
        {"escrow_initial": -1},
    ],
)
def test_invalid_values_are_rejected(overrides):
    with pytest.raises(InvalidConfigError) as ei:
        build_config(**overrides)
    assert "invalid configuration" in str(ei.value)


def test_cap_below_base_rejected():
    with pytest.raises(InvalidConfigError) as ei:
        build_config(base_backoff_ms=100, backoff_cap_ms=10)
    assert "backoff_cap_ms" in str(ei.value)


def test_unknown_key_rejected():
    with pytest.raises(InvalidConfigError) as ei:
        build_config(not_a_real_option=3)
    assert "unknown configuration keys" in str(ei.value)


def test_override_round_trip():
    cfg = build_config(finality_depth=5, queue_capacity=7)
    assert cfg.finality_depth == 5 and cfg.queue_capacity == 7
