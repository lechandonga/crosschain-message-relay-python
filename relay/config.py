"""Configuration with deterministic defaults and strict validation."""

from __future__ import annotations

from dataclasses import dataclass

from .errors import InvalidConfigError


@dataclass(frozen=True)
class RelayConfig:
    finality_depth: int = 3          # source block + (depth-1) successors
    max_attempts: int = 4            # 1 initial try + up to 3 retries
    base_backoff_ms: int = 50        # backoff(n) = base * 2**(n-1)
    backoff_cap_ms: int = 1000
    timeout_ms: int = 200            # simulated target-call deadline
    stale_window: int = 8            # nonce expiry window (source blocks)
    queue_capacity: int = 256        # bounded pending-work queue
    max_inflight: int = 8            # concurrent relay workers
    tick_interval_ms: int = 25       # simulated clock advance per tick
    escrow_initial: int = 1_000_000  # target bridge escrow endowment

    # (field, predicate, human description) — every failure lists its reason
    _RULES = (
        ("finality_depth", lambda v: isinstance(v, int) and v >= 1, "integer >= 1"),
        ("max_attempts", lambda v: isinstance(v, int) and v >= 1, "integer >= 1"),
        ("base_backoff_ms", lambda v: isinstance(v, int) and v >= 1, "integer >= 1"),
        ("backoff_cap_ms", lambda v: isinstance(v, int) and v >= 1, "integer >= 1"),
        ("timeout_ms", lambda v: isinstance(v, int) and v >= 1, "integer >= 1"),
        ("stale_window", lambda v: isinstance(v, int) and v >= 1, "integer >= 1"),
        ("queue_capacity", lambda v: isinstance(v, int) and v >= 1, "integer >= 1"),
        ("max_inflight", lambda v: isinstance(v, int) and v >= 1, "integer >= 1"),
        ("tick_interval_ms", lambda v: isinstance(v, int) and v >= 1, "integer >= 1"),
        ("escrow_initial", lambda v: isinstance(v, int) and v >= 0, "integer >= 0"),
    )

    def validate(self) -> "RelayConfig":
        problems: list[str] = []
        for name, ok, desc in self._RULES:  # type: ignore[attr-defined]
            value = getattr(self, name)
            if not ok(value):
                problems.append(f"{name}={value!r} must be {desc}")
        if self.backoff_cap_ms < self.base_backoff_ms:
            problems.append(
                "backoff_cap_ms must be >= base_backoff_ms "
                f"({self.backoff_cap_ms} < {self.base_backoff_ms})"
            )
        if problems:
            raise InvalidConfigError("invalid configuration: " + "; ".join(problems))
        return self


def build_config(**overrides: object) -> RelayConfig:
    """Construct and validate a :class:`RelayConfig`.

    Unknown keys are rejected explicitly rather than silently ignored.
    """
    allowed = set(RelayConfig.__dataclass_fields__)
    unknown = set(overrides) - allowed
    if unknown:
        raise InvalidConfigError(f"unknown configuration keys: {sorted(unknown)}")
    cfg = RelayConfig(**overrides)  # type: ignore[arg-type]
    return cfg.validate()
