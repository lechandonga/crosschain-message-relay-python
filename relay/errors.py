"""Domain errors.

Each rejection has a stable ``reason`` code so that duplicate delivery,
replay, forgery and finality violations remain distinguishable to callers.
``retryable`` distinguishes transient failures (timeout / temporary fault)
from permanent target-chain rejections.
"""

from __future__ import annotations


class RelayError(Exception):
    """Base class for all relay domain errors."""

    reason: str = "relay_error"
    retryable: bool = False

    def __init__(self, message: str = "", *, retryable: bool | None = None) -> None:
        super().__init__(message or self.reason)
        if retryable is not None:
            self.retryable = retryable


class ForgedProofError(RelayError):
    """Proof signature/body does not match the attested source message."""

    reason = "forged_proof"
    retryable = False


class DuplicateDeliveryError(RelayError):
    """Message already delivered on the target chain; no state changed."""

    reason = "duplicate_delivery"
    retryable = False


class ReplayError(RelayError):
    """Fresh-looking attempt that reuses a spent/non-canonical message or nonce."""

    reason = "replay_detected"
    retryable = False


class NotFinalizedError(RelayError):
    """Source block has not accumulated enough confirmations."""

    reason = "not_finalized"
    retryable = True


class ReorgRollbackError(RelayError):
    """Message was rolled back by a chain reorganisation."""

    reason = "reorg_rollback"
    retryable = False


class StaleNonceError(RelayError):
    """Out-of-order/expired channel nonce (gap or already consumed)."""

    reason = "stale_nonce"
    retryable = False


class TargetRejectedError(RelayError):
    """Target chain permanently refused the delivery (e.g. bad receiver)."""

    reason = "target_rejected"
    retryable = False


class TransientFailureError(RelayError):
    """Temporary target fault; safe to retry with backoff."""

    reason = "transient_failure"
    retryable = True


class TimeoutError_(RelayError):
    """Target did not respond within the configured deadline."""

    reason = "timeout"
    retryable = True


class RetriesExhaustedError(RelayError):
    """Retry budget spent; the message is terminal-failed."""

    reason = "retries_exhausted"
    retryable = False


class InvalidConfigError(RelayError):
    """Configuration value missing or out of its allowed domain."""

    reason = "invalid_config"
    retryable = False


class BackpressureError(RelayError):
    """Bounded work queue is full; caller must back off."""

    reason = "backpressure"
    retryable = True
