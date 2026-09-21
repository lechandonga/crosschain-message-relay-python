"""多处理者并发：最终状态等同串行，无半更新；积压限流不无界占用内存。"""
import threading

import pytest

from relay.config import RelayConfig
from relay.errors import BackpressureError
from relay.message import MessageStatus
from tests.conftest import make_msg


def test_concurrent_handlers_same_message_equivalent_to_serial(env):
    a, b, ab = env["A"], env["B"], env["AB"]
    m = make_msg(nonce=0, amount=250)
    ab.submit(m)
    a.mine_empty(2)
    ab.observe()

    results: list[MessageStatus] = []
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()  # 最大化同时进入临界区的概率
        results.append(ab.deliver_one(m.message_id))

    print("\n[输入] 8 个处理者线程同时处理同一消息")
    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    rec = ab.get_record(m.message_id)
    print(f"[判定] 最终状态={rec.status.value} attempts={rec.attempts} "
          f"目标链入账={b.balance('carol')} delivered集合大小={len(b.delivered)}")
    assert rec.status == MessageStatus.DELIVERED
    assert b.balance("carol") == 250          # 只入账一次
    assert len(b.delivered) == 1
    # 全部调用都收敛到同一结论
    assert set(results) == {MessageStatus.DELIVERED}
    total = (a.spendable_total() + a.escrow_total()
             + b.spendable_total() + b.escrow_total())
    assert total == 2_000_000


def test_concurrent_different_messages_all_deliver_once(env):
    a, b, ab = env["A"], env["B"], env["AB"]
    msgs = [make_msg(nonce=i, amount=10) for i in range(20)]
    for m in msgs:
        ab.submit(m)
    a.mine_empty(2)
    ab.observe()

    errors: list[Exception] = []

    def worker(m):
        try:
            ab.deliver_one(m.message_id)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    print("\n[输入] 20 条消息各由独立线程并发投递（同发送方，受顺序门控）")
    threads = [threading.Thread(target=worker, args=(m,)) for m in msgs]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    # 乱序到达的消息被暂缓；在无故障、已最终的前提下，多轮处理必然全部有序完成
    for _ in range(20):
        if len(b.delivered) == 20:
            break
        for m in msgs:
            ab.deliver_one(m.message_id)
    assert len(b.delivered) == 20
    assert b.balance("carol") == 200
    print(f"[判定] 经顺序门控多轮收敛，20 条全部生效一次，carol="
          f"{b.balance('carol')}（期望200）")
    total = (a.spendable_total() + a.escrow_total()
             + b.spendable_total() + b.escrow_total())
    assert total == 2_000_000


def test_backpressure_caps_inflight(env, fake_clock):
    a, b = env["A"], env["B"]
    from relay.relayer import Relayer
    small = RelayConfig(max_pending=3)
    ab = Relayer(a, b, small, sleep=fake_clock.sleep, clock=fake_clock.time)
    print("\n[输入] max_pending=3，连续提交 4 条未终结消息")
    for i in range(3):
        ab.submit(make_msg(nonce=i, amount=1))
    with pytest.raises(BackpressureError) as ei:
        ab.submit(make_msg(nonce=3, amount=1))
    print(f"[判定] 第4条被限流: {ei.value}")
    # 终结一条后腾出容量
    a.mine_empty(2)
    ab.run_once()
    ab.submit(make_msg(nonce=3, amount=1))
    print("[判定] 有消息终结后可继续提交，积压不会无界增长")
    assert len(ab.status()["counts"]) >= 1
