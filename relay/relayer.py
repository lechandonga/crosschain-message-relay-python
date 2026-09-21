r"""观察-证明-投递管线：最终性、顺序、重试退避、并发幂等与背压。

并发模型
--------
- SimulatedChain 的锁保证账本原子；本类 RLock 保护观察索引与状态字典；
  每条消息有独立锁，保证同一消息的投递临界区串行。
- 多个处理者并发处理同一消息时，结论与串行一致：状态沿
  SUBMITTED -> OBSERVED -> FINALIZED -> DELIVERED/FAILED 单向推进，
  目标链 deliver 本身幂等，重复尝试不会重复扣款。
- 同一发送方按 (区块高度, nonce) 有序处理；缺号暂缓（不阻塞其他发送方）。
- 积压达到 max_pending 时 submit 直接抛 BackpressureError，拒绝无界增长。

重试
----
暂时性故障(Transient)与超时(Timeout)分别计数；退避
backoff = min(base * 2**attempts, max)，可由注入的 sleep/clock 确定性驱动。
永久错误（伪造/永久拒绝）不重试直接 FAILED；重试耗尽同样 FAILED。
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional

from .chain import SimulatedChain
from .config import RelayConfig
from .errors import (
    BackpressureError,
    ChainTimeoutError,
    DuplicateDeliveryError,
    ForgedProofError,
    NotFinalError,
    RejectedByChainError,
    ReplayDetectedError,
    TransientChainError,
)
from .message import TERMINAL_STATUSES, CrossChainMessage, MessageStatus
from .proof import Proof

log = logging.getLogger("relay.relayer")


@dataclass
class MessageRecord:
    message: CrossChainMessage
    status: MessageStatus
    observed_height: Optional[int] = None
    observed_block_hash: Optional[str] = None
    finalized: bool = False
    proof: Optional[Proof] = None
    attempts: int = 0
    transient_failures: int = 0
    timeout_failures: int = 0
    last_error: Optional[str] = None
    not_before: float = 0.0  # 退避结束时刻（注入时钟域）


class Relayer:
    def __init__(self, source: SimulatedChain, target: SimulatedChain,
                 config: RelayConfig,
                 sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic) -> None:
        config.validate()
        self.source = source
        self.target = target
        self.config = config
        self._sleep = sleep
        self._clock = clock
        self._lock = threading.RLock()
        self._msg_locks: dict[str, threading.Lock] = {}
        self._records: dict[str, MessageRecord] = {}
        # 每个发送方：已观察到的最大连续 nonce（含），从 -1 开始
        self._sender_next_nonce: dict[str, int] = {}
        self._timeout_total = 0
        self._transient_total = 0

    # ---------------- 提交（源链托管扣款 + 背压） ----------------
    def submit(self, message: CrossChainMessage, *, _enforce_backpressure: bool = True):
        # 背压仅对“全新且尚未记录”的消息生效
        with self._lock:
            is_new = message.message_id not in self._records
            if _enforce_backpressure and is_new:
                inflight = sum(
                    1 for r in self._records.values()
                    if r.status not in TERMINAL_STATUSES
                    and r.status != MessageStatus.ROLLED_BACK
                )
                if inflight >= self.config.max_pending:
                    raise BackpressureError(
                        f"在途消息 {inflight} 已达上限 {self.config.max_pending}，"
                        "拒绝继续提交（背压限流）"
                    )
        # 始终由源链裁决：重复 message_id / nonce 复用在这里被明确拒绝；
        # 重组后同一消息重新提交也必须重新入块成功，才会刷新中继状态。
        height, block_hash = self.source.submit_message(message)
        with self._lock:
            if is_new:
                self._records[message.message_id] = MessageRecord(
                    message=message, status=MessageStatus.SUBMITTED,
                    observed_height=height, observed_block_hash=block_hash,
                )
                self._msg_locks[message.message_id] = threading.Lock()
            else:
                # 重组后同一消息在替代链重新提交
                rec = self._records[message.message_id]
                rec.status = MessageStatus.SUBMITTED
                rec.observed_height = height
                rec.observed_block_hash = block_hash
                rec.last_error = None
                rec.not_before = 0.0
        log.info(
            "[submit] id=%s sender=%s nonce=%d amount=%d height=%d 判定=已提交并托管扣款",
            message.message_id[:12], message.sender, message.nonce,
            message.amount, height,
        )
        return height, block_hash

    # ---------------- 观察（最终性推进 + 重组处置） ----------------
    def observe(self) -> list[str]:
        changed: list[str] = []
        with self._lock:
            records = list(self._records.values())
        for rec in records:
            with self._get_lock(rec.message.message_id):
                result = self._refresh_one(rec)
                if result:
                    changed.append(rec.message.message_id)
        log.info("[observe] 本轮变更消息数=%d ids=%s",
                 len(changed), [x[:12] for x in changed])
        return changed

    def _refresh_one(self, rec: MessageRecord) -> bool:
        msg = rec.message
        found = self.source.find_message(msg.message_id)
        if found is None:
            # 已不在规范链上：区块被重组丢弃，状态置 ROLLED_BACK（确定、可查询）
            if rec.status != MessageStatus.ROLLED_BACK:
                rec.status = MessageStatus.ROLLED_BACK
                rec.finalized = False
                rec.proof = None
                rec.last_error = "源链重组：消息所在区块被丢弃，托管已原路退回"
                log.info("[observe] id=%s 判定=重组回滚 -> rolled_back",
                         msg.message_id[:12])
                return True
            return False
        height, index, _ = found
        block = self.source.get_block(height)
        assert block is not None
        if rec.observed_height is None:
            rec.observed_height = height
            rec.observed_block_hash = block.block_hash
            if rec.status in (TERMINAL_STATUSES | {MessageStatus.ROLLED_BACK}):
                rec.status = MessageStatus.OBSERVED
        elif rec.observed_height != height or \
                rec.observed_block_hash != block.block_hash:
            # 替代链上重新包含：刷新观察锚点
            rec.observed_height = height
            rec.observed_block_hash = block.block_hash
            rec.finalized = False
            rec.proof = None
            rec.status = MessageStatus.OBSERVED
            rec.last_error = None
            rec.not_before = 0.0
            log.info("[observe] id=%s 判定=替代链重新包含 height=%d",
                     msg.message_id[:12], height)
            return True
        final = self.source.is_final(height)
        if final and not rec.finalized:
            rec.finalized = True
            rec.status = MessageStatus.FINALIZED
            log.info("[observe] id=%s 判定=达到最终性 height=%d",
                     msg.message_id[:12], height)
            return True
        if not final and rec.status == MessageStatus.SUBMITTED:
            rec.status = MessageStatus.OBSERVED
            return True
        return False

    # ---------------- 投递单条（顺序门控 + 重试退避） ----------------
    def deliver_one(self, message_id: str) -> MessageStatus:
        rec = self._require_record(message_id)
        with self._get_lock(message_id):
            if rec.status in TERMINAL_STATUSES:
                log.info("[deliver] id=%s 判定=已是终态 %s，直接返回（幂等）",
                         message_id[:12], rec.status.value)
                return rec.status
            self._refresh_one(rec)
            if rec.status == MessageStatus.ROLLED_BACK:
                log.info("[deliver] id=%s 判定=已回滚，暂不投递",
                         message_id[:12])
                return rec.status
            if not rec.finalized:
                log.info("[deliver] id=%s 判定=未达最终性，保持 %s",
                         message_id[:12], rec.status.value)
                return rec.status

            # 顺序门控：同一发送方必须按 nonce 连续投递
            if not self._order_ok(rec):
                reason = (f"发送方 {rec.message.sender} 存在更小 nonce "
                          f"尚未最终/投递，nonce={rec.message.nonce} 暂缓")
                rec.last_error = reason
                log.info("[deliver] id=%s 判定=乱序暂缓 (%s)",
                         message_id[:12], reason)
                return rec.status

            # 退避窗口
            now = self._clock()
            if now < rec.not_before:
                wait = rec.not_before - now
                log.info("[deliver] id=%s 判定=退避中，等待 %.1fms",
                         message_id[:12], wait * 1000)
                self._sleep(wait)
            return self._attempt_delivery(rec)

    def _order_ok(self, rec: MessageRecord) -> bool:
        msg = rec.message
        expected = self._sender_next_nonce.get(msg.sender, 0)
        return msg.nonce == expected

    def _attempt_delivery(self, rec: MessageRecord) -> MessageStatus:
        msg = rec.message
        rec.attempts += 1
        proof = self.source.generate_proof(msg.message_id)
        rec.proof = proof
        try:
            self.target.deliver(proof)
        except DuplicateDeliveryError:
            # 并发场景下对端已生效：收敛为 DELIVERED，不视为失败、不扣款
            rec.status = MessageStatus.DELIVERED
            log.info("[deliver] id=%s 判定=目标链已存在生效记录，收敛为 delivered",
                     msg.message_id[:12])
            self._advance_nonce(msg.sender, msg.nonce)
            return rec.status
        except (ReplayDetectedError, ForgedProofError,
                RejectedByChainError) as e:
            # 永久失败：中止转账，托管原路退回（由链层 aborted 标记保证不重复结算）
            refund = self.source.release_escrow(msg.message_id)
            rec.status = MessageStatus.FAILED
            rec.last_error = f"{type(e).__name__}: {e}（已退回托管 {refund}）"
            log.warning("[deliver] id=%s 判定=永久失败 %s，托管退回=%d（不重试）",
                        msg.message_id[:12], type(e).__name__, refund)
            # FAILED 同样是终态，占住 nonce 并推进水位，不永久阻塞后续消息
            self._advance_nonce(msg.sender, msg.nonce)
            return rec.status
        except (TransientChainError, ChainTimeoutError) as e:
            if isinstance(e, ChainTimeoutError):
                rec.timeout_failures += 1
                self._timeout_total += 1
                kind = "超时"
            else:
                rec.transient_failures += 1
                self._transient_total += 1
                kind = "暂时性故障"
            if rec.attempts > self.config.max_retries:
                refund = self.source.release_escrow(msg.message_id)
                rec.status = MessageStatus.FAILED
                rec.last_error = (
                    f"{type(e).__name__}: {e}；重试 {rec.attempts - 1} 次后耗尽上限，"
                    f"已退回托管 {refund}"
                )
                log.warning("[deliver] id=%s 判定=重试上限耗尽 -> failed，托管退回=%d",
                            msg.message_id[:12], refund)
                self._advance_nonce(msg.sender, msg.nonce)
                return rec.status
            delay_ms = min(
                self.config.backoff_base_ms * 2 ** (rec.attempts - 1),
                self.config.backoff_max_ms,
            )
            rec.not_before = self._clock() + delay_ms / 1000.0
            rec.last_error = f"{type(e).__name__}: {e}"
            log.info("[deliver] id=%s 判定=%s（第%d次），退避 %dms 后可重试",
                     msg.message_id[:12], kind, rec.attempts, delay_ms)
            return rec.status
        except NotFinalError as e:
            # 源链可能在证明生成后被推进判定反转：回到未最终观察
            rec.finalized = False
            rec.status = MessageStatus.OBSERVED
            rec.last_error = f"NotFinalError: {e}"
            return rec.status
        else:
            rec.status = MessageStatus.DELIVERED
            rec.last_error = None
            log.info("[deliver] id=%s 判定=投递成功 sender=%s nonce=%d amount=%d",
                     msg.message_id[:12], msg.sender, msg.nonce, msg.amount)
            self._advance_nonce(msg.sender, msg.nonce)
            return rec.status

    def _advance_nonce(self, sender: str, nonce: int) -> None:
        cur = self._sender_next_nonce.get(sender, 0)
        if nonce == cur:
            self._sender_next_nonce[sender] = cur + 1

    # ---------------- 一轮完整处理（按发送方分组有序） ----------------
    def run_once(self) -> None:
        self.observe()
        with self._lock:
            pending = [
                r for r in self._records.values()
                if r.status not in TERMINAL_STATUSES
                and r.status != MessageStatus.ROLLED_BACK
                and r.finalized
            ]
            by_sender: dict[str, list[MessageRecord]] = {}
            for r in pending:
                by_sender.setdefault(r.message.sender, []).append(r)
        for sender, group in by_sender.items():
            group.sort(key=lambda r: r.message.nonce)
            for rec in group:
                self.deliver_one(rec.message.message_id)

    # ---------------- 查询 ----------------
    def message_status(self, message_id: str) -> Optional[MessageStatus]:
        with self._lock:
            rec = self._records.get(message_id)
            return rec.status if rec else None

    def get_record(self, message_id: str) -> Optional[MessageRecord]:
        with self._lock:
            return self._records.get(message_id)

    def status(self) -> dict:
        with self._lock:
            counts: dict[str, int] = {}
            for rec in self._records.values():
                counts[rec.status.value] = counts.get(rec.status.value, 0) + 1
            return {
                "source": self.source.config.name,
                "target": self.target.config.name,
                "counts": counts,
                "known_messages": len(self._records),
                "sender_next_nonce": dict(self._sender_next_nonce),
                "transient_total": self._transient_total,
                "timeout_total": self._timeout_total,
            }

    # ---------------- 内部 ----------------
    def _require_record(self, message_id: str) -> MessageRecord:
        with self._lock:
            rec = self._records.get(message_id)
            if rec is None:
                raise KeyError(f"中继未知消息: {message_id[:12]}")
            return rec

    def _get_lock(self, message_id: str) -> threading.Lock:
        with self._lock:
            lk = self._msg_locks.get(message_id)
            if lk is None:
                lk = threading.Lock()
                self._msg_locks[message_id] = lk
            return lk
