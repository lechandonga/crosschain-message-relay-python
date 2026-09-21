"""重复投递、重放、伪造证明必须被拒绝，且拒绝原因彼此可区分。"""
from dataclasses import replace

import pytest

from relay.errors import (
    DuplicateDeliveryError,
    ForgedProofError,
    NotFinalError,
    ReplayDetectedError,
)
from relay.message import MessageStatus
from tests.conftest import make_msg


def _finalize_and_deliver(env, m):
    a, ab = env["A"], env["AB"]
    ab.submit(m)
    a.mine_empty(2)
    ab.run_once()
    assert ab.message_status(m.message_id) == MessageStatus.DELIVERED


def test_duplicate_delivery_rejected_with_distinct_reason(env):
    b = env["B"]
    m = make_msg(nonce=0, amount=100)
    _finalize_and_deliver(env, m)
    proof = env["A"].generate_proof(m.message_id)

    print("\n[输入] 对已生效消息再次调用目标链 deliver")
    with pytest.raises(DuplicateDeliveryError) as ei:
        b.deliver(proof)
    print(f"[判定] DuplicateDeliveryError: {ei.value}")
    assert b.balance("carol") == 100  # 没有重复入账


def test_replay_after_reorg_is_replay_not_forgery(env):
    a, b, ab = env["A"], env["B"], env["AB"]
    m = make_msg(nonce=0, amount=100)
    ab.submit(m)
    old_proof = a.generate_proof(m.message_id)
    print("\n[输入] 重组回滚高度1（未最终），攻击者持旧证明投递")
    discarded = a.reorg(1)
    print(f"[判定] 被丢弃消息 {[d[:8] for d in discarded]}，托管原路退回 alice")
    assert a.balance("alice") == 1_000_000

    with pytest.raises(ReplayDetectedError) as ei:
        b.deliver(old_proof)
    print(f"[判定] ReplayDetectedError（区别于伪造）: {ei.value}")
    ab.observe()
    assert ab.message_status(m.message_id) == MessageStatus.ROLLED_BACK

    # 在替代链上重新提交同一消息：新证明可以正常生效，且全系统只生效一次
    ab.submit(m)
    a.mine_empty(2)
    ab.run_once()
    assert b.balance("carol") == 100
    assert len(b.delivered) == 1


def test_forged_proof_variants_are_rejected(env):
    a, b = env["A"], env["B"]
    m = make_msg(nonce=0, amount=100)
    ab = env["AB"]
    ab.submit(m)
    a.mine_empty(2)
    good = a.generate_proof(m.message_id)

    # 1) 编造不存在的区块哈希（不在已丢弃集合）
    fake_hash = "f" * 32
    p1 = replace(good, block_hash=fake_hash, proof_hash="")
    p1 = replace(p1, proof_hash=p1.commitment())
    print("\n[输入] 伪造证明：替换为不存在的区块哈希")
    with pytest.raises(ForgedProofError) as ei:
        b.deliver(p1)
    print(f"[判定] ForgedProofError: {ei.value}")

    # 2) 直接篡改 proof_hash
    p2 = replace(good, proof_hash="deadbeef")
    print("[输入] 伪造证明：直接篡改 proof_hash")
    with pytest.raises(ForgedProofError):
        b.deliver(p2)
    print("[判定] 自洽性校验失败 -> ForgedProofError")

    # 3) 越界高度
    p3 = replace(good, block_height=999, proof_hash="")
    p3 = replace(p3, proof_hash=p3.commitment())
    print("[输入] 伪造证明：越界区块高度 999")
    with pytest.raises(ForgedProofError):
        b.deliver(p3)
    print("[判定] 高度越界 -> ForgedProofError")

    assert b.balance("carol") == 0  # 任何伪造都未生效


def test_replay_old_height_reoccupied_by_new_branch(env):
    """重组深度恰好使同高度被替代链占据时，旧证明仍是 Replay 而非 Forged。"""
    a, b, ab = env["A"], env["B"], env["AB"]
    m = make_msg(nonce=0, amount=60)
    ab.submit(m)
    a.mine_empty(1)  # tip=2，消息在高度1，仍未最终
    old_proof = a.generate_proof(m.message_id)
    print("\n[输入] 回滚2个区块，同高度1被替代链重新占据后持旧证明投递")
    a.reorg(2)
    # 新链高度1已是不同哈希（空块）
    with pytest.raises(ReplayDetectedError):
        b.deliver(old_proof)
    print("[判定] 旧位置哈希命中历史位置索引 -> ReplayDetectedError")


def test_non_final_proof_rejected(env):
    a, b, ab = env["A"], env["B"], env["AB"]
    m = make_msg(nonce=0)
    ab.submit(m)
    proof = a.generate_proof(m.message_id)
    print("\n[输入] 区块 tip=1 未达最终性深度 2 时投递")
    with pytest.raises(NotFinalError) as ei:
        b.deliver(proof)
    print(f"[判定] NotFinalError: {ei.value}")
    assert b.balance("carol") == 0
