"""超时与暂时性故障分别计数；重试成功后账本守恒、无重复扣款。"""
from relay.message import MessageStatus
from tests.conftest import make_msg


def test_timeout_counts_separately_and_retry_succeeds_once(env, fake_clock):
    a, b, ab = env["A"], env["B"], env["AB"]
    m = make_msg(nonce=0, amount=88)
    ab.submit(m)
    a.mine_empty(2)
    ab.observe()

    print("\n[输入] 连续注入 3 次超时（max_retries=4，不会耗尽）")
    b.fault_mode = "timeout"
    b.fault_remaining = 3
    for _ in range(3):
        ab.deliver_one(m.message_id)
    rec = ab.get_record(m.message_id)
    print(f"[判定] 三次尝试后 status={rec.status.value} timeout={rec.timeout_failures} "
          f"transient={rec.transient_failures}")
    assert rec.timeout_failures == 3
    assert rec.transient_failures == 0
    assert b.balance("carol") == 0

    b.fault_mode = None
    ab.deliver_one(m.message_id)
    rec = ab.get_record(m.message_id)
    assert rec.status == MessageStatus.DELIVERED
    assert b.balance("carol") == 88
    total = (a.spendable_total() + a.escrow_total()
             + b.spendable_total() + b.escrow_total())
    print(f"[判定] 超时恢复后成功一次，系统总量={total}，虚钟退避={fake_clock.now*1000:.0f}ms")
    assert total == 2_000_000
    # 退避：10 + 20 + 40
    assert fake_clock.now == 0.07
