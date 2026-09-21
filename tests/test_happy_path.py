"""最小正向链路：提交 -> 最终性 -> 投递 -> 守恒。"""
from relay.message import MessageStatus
from tests.conftest import make_msg


def test_full_lifecycle_and_conservation(env):
    a, b, ab = env["A"], env["B"], env["AB"]
    m = make_msg(amount=100, nonce=0)
    print(f"\n[输入] 提交消息 id={m.message_id[:12]} amount=100 nonce=0")
    ab.submit(m)
    ab.observe()
    assert ab.message_status(m.message_id) == MessageStatus.OBSERVED
    print("[判定] 区块未最终(tip=1, depth=2) -> 保持 observed，不投递")
    assert b.balance("carol") == 0

    a.mine_empty(2)
    status = ab.deliver_one(m.message_id)
    print(f"[判定] 推进到 tip=3 达到最终性 -> {status.value}")
    assert status == MessageStatus.DELIVERED
    assert b.balance("carol") == 100
    assert a.escrow_total() == 0

    total = (a.spendable_total() + a.escrow_total()
             + b.spendable_total() + b.escrow_total())
    print(f"[判定] 两链总量 spendable+escrow={total}，初始铸币=2,000,000，守恒")
    assert total == 2_000_000
