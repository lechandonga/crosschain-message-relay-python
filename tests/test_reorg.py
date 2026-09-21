"""重组与回滚：最终性是硬边界，已投递消息处置确定，托管退款守恒。"""
import pytest

from relay.errors import RelayError
from relay.message import MessageStatus
from tests.conftest import make_msg


def test_finalized_block_cannot_be_reorged(env):
    a, ab = env["A"], env["AB"]
    m = make_msg(nonce=0)
    ab.submit(m)
    a.mine_empty(2)  # 高度1达到最终性
    print("\n[输入] 尝试重组包含已最终高度1的 3 个区块")
    with pytest.raises(RelayError) as ei:
        a.reorg(3)
    print(f"[判定] 拒绝重组已最终区块: {ei.value}")


def test_delivered_message_survives_later_nonfinal_reorg(env):
    a, b, ab = env["A"], env["B"], env["AB"]
    m = make_msg(nonce=0, amount=100)
    ab.submit(m)
    a.mine_empty(2)
    ab.run_once()
    assert ab.message_status(m.message_id) == MessageStatus.DELIVERED

    print("\n[输入] 消息已投递后，回滚源链顶端一个不含消息的空区块")
    discarded = a.reorg(1)
    assert discarded == []
    print("[判定] 已生效结果不受影响：目标链 delivered 集合不变，bob/carol 余额不变")
    assert m.message_id in b.delivered
    assert b.balance("carol") == 100
    total = (a.spendable_total() + a.escrow_total()
             + b.spendable_total() + b.escrow_total())
    assert total == 2_000_000


def test_observed_message_reorged_then_refund_and_resubmit(env):
    a, b, ab = env["A"], env["B"], env["AB"]
    m = make_msg(nonce=0, amount=300)
    ab.submit(m)
    ab.observe()
    before = a.balance("alice")
    print(f"\n[输入] 已观察未最终的消息被重组；回滚前 alice={before} escrow={a.escrow_total()}")
    a.reorg(1)
    ab.observe()
    rec = ab.get_record(m.message_id)
    print(f"[判定] 状态={rec.status.value} 原因={rec.last_error}")
    assert rec.status == MessageStatus.ROLLED_BACK
    assert a.balance("alice") == 1_000_000  # 托管原路退回
    assert b.balance("carol") == 0

    # 重新提交到替代链后可正常完成
    ab.submit(m)
    a.mine_empty(2)
    ab.run_once()
    assert ab.message_status(m.message_id) == MessageStatus.DELIVERED
    assert b.balance("carol") == 300
    total = (a.spendable_total() + a.escrow_total()
             + b.spendable_total() + b.escrow_total())
    assert total == 2_000_000


def test_reorg_is_deterministic_across_runs(env):
    """同样的序列执行两次，结论一致（分支号、余额、状态可复现）。"""
    def run():
        a = type(env["A"])(env["A"].config)
        b = type(env["B"])(env["B"].config)
        a.connect(b)
        a.mint("alice", 1000)
        from relay.relayer import Relayer
        r = Relayer(a, b, env["AB"].config)
        m = make_msg(amount=100)
        r.submit(m)
        a.reorg(1)
        r.observe()
        r.submit(m)
        a.mine_empty(2)
        r.run_once()
        return (a.blocks[-1].branch, a.balance("alice"),
                b.balance("carol"),
                r.message_status(m.message_id).value)

    r1, r2 = run(), run()
    print(f"\n[判定] 两次运行结果: {r1} == {r2}")
    assert r1 == r2 == (1, 900, 100, "delivered")
