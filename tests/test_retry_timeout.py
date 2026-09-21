"""目标链拒绝 / 暂时性故障 / 超时必须被区分；退避有上限；重试不重复扣款。"""

from relay.message import MessageStatus
from tests.conftest import make_msg


def _submit_finalized(env, nonce, amount=100, sender="alice", recipient="carol"):
    a, ab = env["A"], env["AB"]
    m = make_msg(nonce=nonce, amount=amount, sender=sender, recipient=recipient)
    ab.submit(m)
    a.mine_empty(2)
    ab.observe()
    return m


def test_transient_then_timeout_classified_and_retried_with_backoff(env, fake_clock):
    b, ab = env["B"], env["AB"]
    m = _submit_finalized(env, nonce=0)
    print("\n[输入] 注入：暂时性故障x2、超时x1，随后恢复")
    b.fault_mode = "transient"
    b.fault_remaining = 2
    # 第1次：transient
    ab.deliver_one(m.message_id)
    # 第2次：transient（虚钟走完退避）
    ab.deliver_one(m.message_id)
    b.fault_mode = "timeout"
    b.fault_remaining = 1
    # 第3次：timeout
    ab.deliver_one(m.message_id)
    # 第4次：故障已耗尽 -> 成功
    ab.deliver_one(m.message_id)

    rec = ab.get_record(m.message_id)
    print(f"[判定] attempts={rec.attempts} transient={rec.transient_failures} "
          f"timeout={rec.timeout_failures} status={rec.status.value}")
    assert rec.status == MessageStatus.DELIVERED
    assert rec.transient_failures == 2
    assert rec.timeout_failures == 1
    assert b.balance("carol") == 100  # 恰好入账一次
    # 退避序列 10ms,20ms,40ms（确定性虚钟累加）
    print(f"[判定] 虚钟累计退避={fake_clock.now*1000:.0f}ms（期望 70ms）")
    assert fake_clock.now == 0.07


def test_retry_budget_exhausted_marks_failed_and_no_double_spend(env):
    a, b, ab = env["A"], env["B"], env["AB"]
    m = _submit_finalized(env, nonce=0, amount=500)
    print("\n[输入] 持续暂时性故障，max_retries=4")
    b.fault_mode = "transient"
    b.fault_remaining = 100
    statuses = [ab.deliver_one(m.message_id) for _ in range(6)]
    rec = ab.get_record(m.message_id)
    print(f"[判定] attempts={rec.attempts} 最终状态={rec.status.value} 原因={rec.last_error}")
    assert rec.status == MessageStatus.FAILED
    assert rec.attempts == 5  # 1 次初始 + 4 次重试
    # 重试耗尽：托管原路退回发送方，不丢失、不锁死、不重复扣款
    assert a.escrow_balance(m.message_id) == 0
    assert a.balance("alice") == 1_000_000
    assert b.balance("carol") == 0
    assert statuses[-1] == MessageStatus.FAILED


def test_permanent_rejection_is_not_retried(env):
    b, ab = env["B"], env["AB"]
    m = _submit_finalized(env, nonce=0)
    print("\n[输入] 注入永久拒绝")
    b.fault_mode = "permanent"
    b.fault_remaining = 5
    status = ab.deliver_one(m.message_id)
    rec = ab.get_record(m.message_id)
    print(f"[判定] status={status.value} attempts={rec.attempts}（永久失败不重试）")
    assert status == MessageStatus.FAILED
    assert rec.attempts == 1
    assert "RejectedByChainError" in (rec.last_error or "")
    # 永久拒绝立即退款
    assert env["A"].balance("alice") == 1_000_000
    assert env["A"].escrow_balance(m.message_id) == 0


def test_failed_message_deliver_one_is_idempotent_terminal(env):
    b, ab = env["B"], env["AB"]
    m = _submit_finalized(env, nonce=0)
    b.fault_mode = "permanent"
    b.fault_remaining = 10
    ab.deliver_one(m.message_id)
    b.fault_mode = None
    again = ab.deliver_one(m.message_id)
    print(f"\n[判定] 故障清除后再次处理终态消息 -> {again.value}（不复活）")
    assert again == MessageStatus.FAILED
