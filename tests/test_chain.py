"""Simulated chain: ledger conservation, finality, delivery, faults, reorg."""

from __future__ import annotations

import pytest

from relay.chain import ESCROW_ACCOUNT, SimulatedChain
from relay.clock import Clock
from relay.errors import (
    DuplicateDeliveryError,
    TargetRejectedError,
    TimeoutError_,
    TransientFailureError,
)


def _chains(**ba):
    clock = Clock()
    a = SimulatedChain("A", genesis_balances=ba.pop("A", {"alice": 1000}), clock=clock)
    b = SimulatedChain("B", genesis_balances=ba.pop("B", {ESCROW_ACCOUNT: 5000}), clock=clock)
    return clock, a, b


def test_submit_locks_funds_and_conserves():
    _, a, _ = _chains()
    ph = a.submit_message(channel="ch", sender="alice", receiver="bob", amount=100, nonce=1)
    a.mine(1)
    m = a.get_message(ph.message_id)
    assert a.balance_of("alice") == 900
    assert a.balance_of(ESCROW_ACCOUNT) == 100
    assert a.total_supply() == 1000  # funds only moved, never created
    assert m.source_height == 1


def test_finality_tracks_confirmations():
    _, a, _ = _chains()
    ph = a.submit_message(channel="ch", sender="alice", receiver="bob", amount=1, nonce=1)
    a.mine(1)
    m = a.get_message(ph.message_id)
    assert a.confirmations(m.source_height) == 1
    assert not a.is_final(m.source_height, 3)
    a.mine(2)
    assert a.confirmations(m.source_height) == 3
    assert a.is_final(m.source_height, 3)
    assert not a.is_final(m.source_height, 4)


def test_delivery_is_idempotent_and_conserves():
    _, a, b = _chains()
    ph = a.submit_message(channel="ch", sender="alice", receiver="bob", amount=100, nonce=1)
    a.mine(3)
    m = a.get_message(ph.message_id)
    assert b.deliver(m) == 1  # a delivery block is produced
    assert b.balance_of("bob") == 100
    assert b.balance_of(ESCROW_ACCOUNT) == 4900
    with pytest.raises(DuplicateDeliveryError) as ei:
        b.deliver(m)
    assert ei.value.reason == "duplicate_delivery"
    assert b.balance_of("bob") == 100  # no double credit
    assert b.total_supply() == 5000


def test_retryable_faults_change_no_balance():
    _, a, b = _chains(A={"s": 50}, B={ESCROW_ACCOUNT: 100})
    ph = a.submit_message(channel="c", sender="s", receiver="r", amount=20, nonce=1)
    a.mine(1)
    m = a.get_message(ph.message_id)
    b.inject_transient(m.message_id, times=2)
    b.inject_timeout(m.message_id, times=1)
    for exc in (TransientFailureError, TransientFailureError, TimeoutError_):
        with pytest.raises(exc):
            b.deliver(m)
    assert b.balance_of("r") == 0
    assert b.balance_of(ESCROW_ACCOUNT) == 100
    assert b.head().height == 0  # faults mine no delivery block
    b.deliver(m)
    assert b.balance_of("r") == 20


def test_permanent_rejection_changes_nothing():
    _, a, b = _chains(A={"s": 50}, B={ESCROW_ACCOUNT: 100})
    ph = a.submit_message(channel="c", sender="s", receiver="bad", amount=20, nonce=1)
    a.mine(1)
    m = a.get_message(ph.message_id)
    b.reject_account("bad")
    with pytest.raises(TargetRejectedError):
        b.deliver(m)
    assert b.balance_of(ESCROW_ACCOUNT) == 100
    assert b.total_supply() == 100


def test_insufficient_sender_rejected_at_submit():
    _, a, _ = _chains()
    with pytest.raises(TargetRejectedError):
        a.submit_message(channel="ch", sender="alice", receiver="bob", amount=10_000, nonce=1)


def test_source_reorg_refunds_escrow():
    _, a, b = _chains()
    ph = a.submit_message(channel="ch", sender="alice", receiver="bob", amount=100, nonce=1)
    a.mine(3)
    m = a.get_message(ph.message_id)
    b.deliver(m)
    a.reorg_to(3)  # remove the inclusion block and the two above
    assert not a.is_message_canonical(m.message_id)
    assert a.balance_of("alice") == 1000
    assert a.balance_of(ESCROW_ACCOUNT) == 0
    assert a.total_supply() == 1000


def test_target_reorg_reverses_and_redelivers_once():
    _, a, b = _chains()
    ph = a.submit_message(channel="ch", sender="alice", receiver="bob", amount=100, nonce=1)
    a.mine(3)
    m = a.get_message(ph.message_id)
    b.deliver(m)
    b.mine(2)
    b.reorg_to(3)  # includes the delivery block at height 1
    assert not b.is_delivery_canonical(m.message_id)
    assert b.balance_of("bob") == 0
    assert b.balance_of(ESCROW_ACCOUNT) == 5000
    b.deliver(m)  # redelivery allowed exactly once
    assert b.balance_of("bob") == 100
    with pytest.raises(DuplicateDeliveryError):
        b.deliver(m)
    assert b.total_supply() == 5000


def test_shallow_reorg_preserves_deeper_delivery():
    _, a, b = _chains()
    ph = a.submit_message(channel="ch", sender="alice", receiver="bob", amount=100, nonce=1)
    a.mine(3)
    m = a.get_message(ph.message_id)
    b.deliver(m)
    b.mine(3)
    b.reorg_to(1)  # only the tip, delivery at height 1 survives
    assert b.is_delivery_canonical(m.message_id)
    assert b.balance_of("bob") == 100


def test_reorg_deeper_than_height_rejected():
    _, a, _ = _chains()
    with pytest.raises(ValueError):
        a.reorg_to(5)
