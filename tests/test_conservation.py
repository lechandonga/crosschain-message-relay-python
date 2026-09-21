"""Cross-chain conservation and bidirectional transfers."""

from __future__ import annotations

from relay.chain import ESCROW_ACCOUNT
from relay.models import MessageStatus


def _supply(world):
    return world["A"].total_supply(), world["B"].total_supply()


async def test_bidirectional_round_trip_conserves(world, make_message, log_case):
    sa0, sb0 = _supply(world)
    # A -> B
    m1 = make_message(world, chain="A", channel="ab", sender="alice",
                      receiver="bob", amount=120, nonce=1)
    await world["relayer"].relay_once(m1.message_id)
    assert world["B"].balance_of("bob") == 120
    # B -> A needs a funded B user; credit one via a delivery then send back
    m2 = make_message(world, chain="A", channel="ab", sender="alice",
                      receiver="carol", amount=80, nonce=2)
    await world["relayer"].relay_once(m2.message_id)

    # explicit B->A message: fund a B account from genesis is escrow-only,
    # so carol (credited on B) sends back across via B submit
    b = world["B"]
    # carol currently only exists on B with 80; submit B->A
    ph = b.submit_message(channel="ba", sender="carol", receiver="alice", amount=80, nonce=1)
    b.mine(3)
    world["relayer"].scan()
    await world["relayer"].relay_once(ph.message_id)
    sa1, sb1 = _supply(world)
    log_case("bidirectional", {"A->B": [120, 80], "B->A": 80}, (sa1, sb1))
    # each simulated chain conserves its own token supply independently
    assert sa1 == sa0
    assert sb1 == sb0
    assert world["A"].balance_of("alice") == 1000 - 120 - 80 + 80


async def test_many_transfers_conserve(world, make_message):
    sa0, sb0 = _supply(world)
    total = 0
    for n in range(1, 11):
        amount = n * 7
        m = make_message(world, channel="ch", sender="alice",
                         receiver=f"u{n}", amount=amount, nonce=n)
        assert await world["relayer"].relay_once(m.message_id) is MessageStatus.DELIVERED
        total += amount
    assert world["A"].balance_of(ESCROW_ACCOUNT) == total
    assert world["B"].balance_of(ESCROW_ACCOUNT) == sb0 - total
    assert _supply(world) == (sa0, sb0)


async def test_failed_transfer_conserves(world, make_message):
    sa0, sb0 = _supply(world)
    m = make_message(world, receiver="ghost", amount=50, nonce=1)
    world["B"].reject_account("ghost")
    try:
        await world["relayer"].relay_once(m.message_id)
    except Exception:
        pass
    # source escrow holds the locked funds, target escrow untouched; supplies intact
    assert world["B"].balance_of(ESCROW_ACCOUNT) == sb0
    assert _supply(world) == (sa0, sb0)
