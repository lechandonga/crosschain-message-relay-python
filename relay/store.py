"""Relay state store: per-message records, channel cursors, audit trail.

All mutations happen under a single re-entrant lock, so concurrent relay
workers can never observe or leave a half-updated record. Status changes
are explicit and append-only in their audit trail.
"""

from __future__ import annotations

import threading

from .models import Message, MessageStatus, RelayAttempt, RelayRecord


class RelayStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._records: dict[str, RelayRecord] = {}
        self._next_nonce: dict[tuple[str, str], int] = {}

    @staticmethod
    def _channel_key(source_chain: str, channel: str) -> tuple[str, str]:
        return (source_chain, channel)

    def observe(self, message: Message, *, now: float, note: str = "") -> RelayRecord:
        """Insert a fresh observation; an existing record is returned untouched."""
        with self._lock:
            existing = self._records.get(message.message_id)
            if existing is not None:
                return existing
            record = RelayRecord(
                message_id=message.message_id,
                channel=message.channel,
                nonce=message.nonce,
                status=MessageStatus.OBSERVED,
                source_chain=message.source_chain,
                target_chain=message.target_chain,
                observed_height=message.source_height,
                updated_at=now,
                note=note,
            )
            self._records[message.message_id] = record
            # first time we see a channel, prime its cursor at this nonce
            key = self._channel_key(message.source_chain, message.channel)
            self._next_nonce.setdefault(key, message.nonce)
            return record

    def get(self, message_id: str) -> RelayRecord | None:
        with self._lock:
            return self._records.get(message_id)

    def transition(
        self,
        message_id: str,
        status: MessageStatus,
        *,
        now: float,
        delivered_height: int | None = None,
        note: str | None = None,
    ) -> RelayRecord:
        with self._lock:
            record = self._records[message_id]
            record.status = status
            record.updated_at = now
            if delivered_height is not None:
                record.delivered_height = delivered_height
            if note is not None:
                record.note = note
            return record

    def record_attempt(self, message_id: str, attempt: RelayAttempt) -> None:
        with self._lock:
            self._records[message_id].attempts.append(attempt)

    def next_nonce(self, source_chain: str, channel: str) -> int:
        with self._lock:
            return self._next_nonce.get(self._channel_key(source_chain, channel), 1)

    def advance_nonce(self, source_chain: str, channel: str, nonce: int) -> None:
        """Consume ``nonce``: the channel cursor becomes nonce + 1.

        Idempotent: advancing with a stale (already consumed) nonce is a
        no-op, which is what makes duplicate deliveries indistinguishable
        from retries at the cursor level (status still records the reason).
        """
        with self._lock:
            key = self._channel_key(source_chain, channel)
            current = self._next_nonce.get(key, 1)
            if nonce == current:
                self._next_nonce[key] = nonce + 1

    def reset_nonce(self, source_chain: str, channel: str, nonce: int) -> None:
        with self._lock:
            self._next_nonce[self._channel_key(source_chain, channel)] = nonce

    def channel_cursors(self) -> dict[str, int]:
        with self._lock:
            return {f"{src}:{ch}": n for (src, ch), n in sorted(self._next_nonce.items())}

    def all_records(self) -> list[RelayRecord]:
        with self._lock:
            return sorted(
                self._records.values(),
                key=lambda r: (r.source_chain, r.channel, r.nonce, r.message_id),
            )

    def records_by_status(self, status: MessageStatus) -> list[RelayRecord]:
        with self._lock:
            return [r for r in self.all_records() if r.status is status]
