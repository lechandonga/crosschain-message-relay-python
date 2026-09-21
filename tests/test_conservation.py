"""任何一次跨链转移（含失败/回滚/并发）后，两链总量必须守恒。"""
from tests.conftest import make_msg

INITIAL_TOTAL = 2_000_000


def total_of(a, b):
    return (a.spendable_total() + a.escrow_total()
            + b.spendable_total() + b.escrow_total())


def test_bidirectional_multi_transfer_conservation(env):
    a, b, ab, ba = env["A"], env["B"], env["AB"], env["BA"]
    print("\n[输入] A->B 3 笔（alice 发起），B->A 2 笔（bob 发起）")
    amts_a = [100, 250, 70]
    amts_b = [300, 90]
    for i, amt in enumerate(amts_a):
        ab.submit(make_msg(nonce=i, amount=amt, recipient="carol"))
    for i, amt in enumerate(amts_b):
        ba.submit(make_msg(source="B", dest="A", sender="bob",
                           recipient="dave", amount=amt, nonce=i))
    a.mine_empty(2)
    b.mine_empty(2)
    ab.run_once()
    ba.run_once()

    assert b.balance("carol") == sum(amts_a)
    assert a.balance("dave") == sum(amts_b)
    t = total_of(a, b)
    print(f"[判定] A spendable={a.spendable_total()} escrow={a.escrow_total()} | "
          f"B spendable={b.spendable_total()} escrow={b.escrow_total()} | "
          f"合计={t}")
    assert t == INITIAL_TOTAL


def test_conservation_after_mixed_failures(env):
    a, b, ab = env["A"], env["B"], env["AB"]
    print("\n[输入] 1 笔成功、1 笔永久拒绝、1 笔暂时性故障后成功、1 笔回滚")
    m0 = make_msg(nonce=0, amount=100)
    m1 = make_msg(nonce=1, amount=200)
    m2 = make_msg(nonce=2, amount=300)
    m3 = make_msg(nonce=3, amount=400)
    ab.submit(m0); a.mine_empty(2); ab.run_once()

    ab.submit(m1); a.mine_empty(2)
    b.fault_mode = "permanent"; b.fault_remaining = 1
    ab.run_once()

    ab.submit(m2); a.mine_empty(2)
    b.fault_mode = "transient"; b.fault_remaining = 1
    ab.run_once()
    ab.run_once()  # 退避后成功

    ab.submit(m3)  # 不推进最终性，直接回滚
    a.reorg(1)
    ab.observe()

    assert b.balance("carol") == 400           # 100 + 300
    assert a.balance("alice") == INITIAL_TOTAL // 2 - 400  # 永久拒绝/回滚均退款
    t = total_of(a, b)
    print(f"[判定] 混合异常后系统总量={t}（期望 {INITIAL_TOTAL}）")
    assert t == INITIAL_TOTAL
