"""同一发送方按 nonce 有序；乱序暂缓判定一致；不同发送方互不阻塞。"""
from relay.message import MessageStatus
from tests.conftest import make_msg


def test_out_of_order_is_deferred_then_drained_in_order(env):
    a, ab = env["A"], env["AB"]
    # 故意乱序提交：2,0,1
    order = [2, 0, 1]
    msgs = {n: make_msg(nonce=n, amount=10 + n) for n in order}
    # 逐个提交并推进最终性（每个区块一个消息）
    for n in order:
        ab.submit(msgs[n])
    a.mine_empty(2)
    ab.observe()

    print("\n[输入] 先处理 nonce=2（缺 0,1）")
    s2 = ab.deliver_one(msgs[2].message_id)
    rec2 = ab.get_record(msgs[2].message_id)
    print(f"[判定] nonce=2 -> {s2.value}，依据：{rec2.last_error}")
    assert s2 == MessageStatus.FINALIZED  # 暂缓但不失败
    assert env["B"].balance("carol") == 0

    # nonce=0 先过，然后 nonce=1、2 依次可过
    ab.deliver_one(msgs[0].message_id)
    ab.deliver_one(msgs[1].message_id)
    s2b = ab.deliver_one(msgs[2].message_id)
    print(f"[判定] 补齐顺序后 nonce=2 -> {s2b.value}；carol 入账 "
          f"{env['B'].balance('carol')}（期望33=10+11+12）")
    assert s2b == MessageStatus.DELIVERED
    assert env["B"].balance("carol") == 33
    # 生效顺序严格为 0,1,2
    assert env["B"].delivered_order == [msgs[0].message_id,
                                        msgs[1].message_id,
                                        msgs[2].message_id]


def test_different_senders_do_not_block_each_other(env):
    a, ab = env["A"], env["AB"]
    env["A"].mint("dave", 1000)
    # alice 缺 nonce=0；dave 的消息不受影响
    m2 = make_msg(nonce=2, sender="alice", recipient="carol", amount=10)
    md = make_msg(nonce=0, sender="dave", recipient="carol", amount=7)
    ab.submit(m2)
    ab.submit(md)
    a.mine_empty(2)
    ab.observe()

    print("\n[输入] alice 缺号时，另一发送方 dave 独立投递")
    ab.deliver_one(m2.message_id)
    s = ab.deliver_one(md.message_id)
    print(f"[判定] dave nonce=0 -> {s.value}，carol={env['B'].balance('carol')}（期望7）")
    assert s == MessageStatus.DELIVERED
    assert env["B"].balance("carol") == 7


def test_rolled_back_message_is_consistently_stale_until_resubmit(env):
    a, ab = env["A"], env["AB"]
    m = make_msg(nonce=0, amount=40)
    ab.submit(m)
    ab.observe()
    a.reorg(1)

    # 无论观察多少次，结论一致：rolled_back
    for i in range(3):
        ab.observe()
        assert ab.message_status(m.message_id) == MessageStatus.ROLLED_BACK
    print("\n[判定] 被重组丢弃的消息重复观察结论一致：rolled_back，不静默丢失")
    # deliver_one 也不会让它悄悄生效
    s = ab.deliver_one(m.message_id)
    assert s == MessageStatus.ROLLED_BACK
    assert env["B"].balance("carol") == 0

    ab.submit(m)
    a.mine_empty(2)
    ab.run_once()
    assert ab.message_status(m.message_id) == MessageStatus.DELIVERED
    print("[判定] 重新提交后沿同一 message_id 正常完成一次")
