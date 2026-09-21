"""本地模拟链：区块账本、消息提交、最终性、重组、证明校验与目标投递。

守恒模型
--------
- 提交跨链消息时：源链从发送方余额扣款，金额进入「托管(escrow)」，
  仍计入系统总量，不可再花费。
- 目标链投递成功时：原子完成「源链释放托管 + 目标链给接收方入账」。
- 区块被重组丢弃时：对应托管金额原路退回发送方。
因此任何时刻：所有链 spendable 余额之和 + 所有链 escrow 之和 = 铸币总量。

原子性
------
跨链结算需要同时持有两条链的锁，始终按链名字典序加锁，
保证不会半更新，也不会死锁。
"""
from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass, field
from typing import Optional

from .config import ChainConfig
from .errors import (
    ChainTimeoutError,
    DuplicateDeliveryError,
    ForgedProofError,
    NotFinalError,
    RejectedByChainError,
    RelayError,
    ReplayDetectedError,
    TransientChainError,
)
from .message import CrossChainMessage
from .proof import Proof


@dataclass
class Block:
    height: int
    block_hash: str
    parent_hash: str
    branch: int
    message_ids: list[str] = field(default_factory=list)
    message_map: dict[str, CrossChainMessage] = field(default_factory=dict)


class SimulatedChain:
    """线程安全的本地账本链。"""

    def __init__(self, config: ChainConfig) -> None:
        config.validate()
        self.config = config
        self.lock = threading.RLock()
        self.peer: Optional["SimulatedChain"] = None
        self.blocks: list[Block] = []
        self.ledger: dict[str, int] = {}
        self.escrow: dict[str, int] = {}
        self.escrow_owner: dict[str, str] = {}
        self.delivered: set[str] = set()
        self.delivered_order: list[str] = []  # 保留实际生效次序
        self.delivered_source: dict[str, str] = {}
        self._message_index: dict[str, int] = {}
        self._used_nonces: dict[tuple[str, str, int], str] = {}  # (sender,dest,nonce)->message_id
        self._discarded_hashes: set[str] = set()
        # (height, block_hash) 中曾属于被丢弃链的位置：保证同高度被新哈希
        # 占据后，持旧证明仍判为「重放」而不是误判为「伪造」
        self._old_positions: set[tuple[int, str]] = set()
        self._branch = 0
        # 故障注入（测试用）：None | "transient" | "timeout" | "permanent"
        self.fault_mode: Optional[str] = None
        self.fault_remaining: int = 0
        genesis = Block(height=0, block_hash=_hash("genesis", "", 0, 0, []),
                        parent_hash="", branch=0)
        self.blocks.append(genesis)

    # ---------- 连接对端链（用于跨链证明校验） ----------
    def connect(self, peer: "SimulatedChain") -> None:
        with _ordered_locks(self, peer):
            self.peer = peer
            peer.peer = self

    # ---------- 基础账本 ----------
    @property
    def tip_height(self) -> int:
        with self.lock:
            return self.blocks[-1].height

    def is_final(self, height: int) -> bool:
        with self.lock:
            return self.blocks[-1].height - height >= self.config.finality_depth

    def mint(self, account: str, amount: int) -> None:
        if amount <= 0:
            raise ValueError("mint 金额必须 > 0")
        with self.lock:
            self.ledger[account] = self.ledger.get(account, 0) + amount

    def balance(self, account: str) -> int:
        with self.lock:
            return self.ledger.get(account, 0)

    def escrow_balance(self, message_id: str) -> int:
        with self.lock:
            return self.escrow.get(message_id, 0)

    def release_escrow(self, message_id: str) -> int:
        """中止转账时将托管原路退回发送方；返回退款金额（幂等：无托管返回0）。"""
        with self.lock:
            amount = self.escrow.pop(message_id, 0)
            owner = self.escrow_owner.pop(message_id, None)
            if amount and owner is not None:
                self.ledger[owner] = self.ledger.get(owner, 0) + amount
            return amount

    def spendable_total(self) -> int:
        with self.lock:
            return sum(self.ledger.values())

    def escrow_total(self) -> int:
        with self.lock:
            return sum(self.escrow.values())

    # ---------- 出块 ----------
    def _mine_block(self, messages: list[CrossChainMessage]) -> Block:
        parent = self.blocks[-1]
        ids = [m.message_id for m in messages]
        h = _hash("block", parent.block_hash, parent.height + 1,
                  self._branch, ids)
        block = Block(
            height=parent.height + 1,
            block_hash=h,
            parent_hash=parent.block_hash,
            branch=self._branch,
            message_ids=ids,
            message_map={m.message_id: m for m in messages},
        )
        self.blocks.append(block)
        for i, mid in enumerate(ids):
            self._message_index[mid] = block.height
        return block

    def mine_empty(self, count: int = 1) -> None:
        """推进链高度但不包含消息（用于推动最终性）。"""
        with self.lock:
            for _ in range(count):
                self._mine_block([])

    # ---------- 消息提交（源链托管扣款） ----------
    def submit_message(self, message: CrossChainMessage) -> tuple[int, str]:
        if message.source_chain != self.config.name:
            raise RejectedByChainError(
                f"消息源链 {message.source_chain} 与本链 {self.config.name} 不一致"
            )
        with self.lock:
            if message.message_id in self._message_index:
                raise DuplicateDeliveryError(
                    f"消息 {message.message_id[:12]} 已在源链提交过"
                )
            nonce_key = (message.sender, message.dest_chain, message.nonce)
            existing = self._used_nonces.get(nonce_key)
            if existing is not None and existing != message.message_id:
                raise RejectedByChainError(
                    f"发送方 {message.sender} 到 {message.dest_chain} 的 "
                    f"nonce={message.nonce} 已被消息 {existing[:12]} 使用，"
                    "拒绝同序号不同内容的重放式提交"
                )
            balance = self.ledger.get(message.sender, 0)
            if balance < message.amount:
                raise RejectedByChainError(
                    f"发送方 {message.sender} 余额不足: {balance} < {message.amount}"
                )
            self.ledger[message.sender] = balance - message.amount
            self.escrow[message.message_id] = message.amount
            self.escrow_owner[message.message_id] = message.sender
            self._used_nonces[nonce_key] = message.message_id
            block = self._mine_block([message])
            return block.height, block.block_hash

    def get_block(self, height: int) -> Optional[Block]:
        with self.lock:
            if 0 <= height < len(self.blocks):
                return self.blocks[height]
            return None

    def contains(self, message_id: str) -> bool:
        with self.lock:
            return message_id in self._message_index

    def find_message(self, message_id: str) -> Optional[tuple[int, int, CrossChainMessage]]:
        """返回 (height, index, message)，不存在返回 None。"""
        with self.lock:
            height = self._message_index.get(message_id)
            if height is None:
                return None
            block = self.blocks[height]
            index = block.message_ids.index(message_id)
            return height, index, block.message_map[message_id]

    # ---------- 证明生成与校验 ----------
    def generate_proof(self, message_id: str) -> Proof:
        found = self.find_message(message_id)
        if found is None:
            raise LookupError(f"消息 {message_id[:12]} 不在链 {self.config.name} 上")
        height, index, _ = found
        with self.lock:
            return Proof.generate(message_id, height,
                                  self.blocks[height].block_hash, index)

    def verify_proof(self, proof: Proof, require_final: bool = True) -> CrossChainMessage:
        with self.lock:
            if not proof.is_internally_consistent():
                raise ForgedProofError(
                    f"证明自身承诺被篡改: {proof.message_id[:12]}"
                )
            if not (0 <= proof.block_height < len(self.blocks)):
                raise ForgedProofError(
                    f"证明高度 {proof.block_height} 超出链高度 "
                    f"{len(self.blocks) - 1}"
                )
            block = self.blocks[proof.block_height]
            if block.block_hash != proof.block_hash:
                if (proof.block_height, proof.block_hash) in self._old_positions:
                    raise ReplayDetectedError(
                        f"证明来自被重组取代的旧区块: height={proof.block_height}"
                    )
                raise ForgedProofError(
                    f"区块哈希与规范链不一致(疑似伪造): height={proof.block_height}"
                )
            if (proof.message_index >= len(block.message_ids)
                    or block.message_ids[proof.message_index] != proof.message_id):
                raise ForgedProofError(
                    f"消息序号/归属与区块内容不一致: height={proof.block_height} "
                    f"index={proof.message_index}"
                )
            message = block.message_map[proof.message_id]
            if require_final and not self.is_final(proof.block_height):
                raise NotFinalError(
                    f"区块 {proof.block_height} 未达最终性深度 "
                    f"{self.config.finality_depth}（当前 tip="
                    f"{self.blocks[-1].height}）"
                )
            return message

    # ---------- 重组 / 回滚 ----------
    def reorg(self, depth: int) -> list[str]:
        """丢弃最后 depth 个区块并以替代空链接续；返回被丢弃的 message_id。

        规则（确定且可复现）：
        - 已最终区块不可回滚，违反时抛错（最终性是硬边界）；
        - 被丢弃消息的托管金额原路退回发送方；
        - 旧区块哈希记入 _discarded_hashes，之后持旧证明投递判为重放。
        """
        with self.lock:
            if depth <= 0:
                raise ValueError("reorg 深度必须 > 0")
            removable = len(self.blocks) - 1
            if depth > removable:
                raise RelayError(f"回滚深度 {depth} 超过可回滚区块数 {removable}")
            oldest = self.blocks[-depth].height
            if self.is_final(oldest):
                raise RelayError(
                    f"拒绝重组：高度 {oldest} 的区块已达到最终性"
                )
            discarded: list[str] = []
            for block in self.blocks[-depth:]:
                self._discarded_hashes.add(block.block_hash)
                self._old_positions.add((block.height, block.block_hash))
                for mid, msg in block.message_map.items():
                    discarded.append(mid)
                    amount = self.escrow.pop(mid, 0)
                    owner = self.escrow_owner.pop(mid, msg.sender)
                    if amount:
                        self.ledger[owner] = self.ledger.get(owner, 0) + amount
                    del self._message_index[mid]
                    self._used_nonces.pop(
                        (msg.sender, msg.dest_chain, msg.nonce), None
                    )
            self.blocks = self.blocks[:-depth]
            self._branch += 1
            for _ in range(depth):
                self._mine_block([])
            return discarded

    # ---------- 目标链投递（跨链原子结算） ----------
    def deliver(self, proof: Proof) -> CrossChainMessage:
        """依据源链证明在本链投递；返回生效的消息。

        异常严格区分：ReplayDetected / ForgedProof / NotFinal /
        DuplicateDelivery / Rejected(永久) / Transient / Timeout。
        """
        if self.peer is None:
            raise RelayError(f"链 {self.config.name} 尚未连接对端链")
        first, second = _ordered(self, self.peer)
        with first.lock, second.lock:
            source: SimulatedChain = self.peer
            message = source.verify_proof(proof, require_final=True)
            if message.dest_chain != self.config.name:
                raise RejectedByChainError(
                    f"目标链不匹配: 证明指向 {message.dest_chain}，"
                    f"投递到 {self.config.name}"
                )
            if proof.message_id in self.delivered:
                raise DuplicateDeliveryError(
                    f"消息 {proof.message_id[:12]} 已在目标链生效，拒绝重复投递"
                )
            if self.fault_mode is not None and self.fault_remaining > 0:
                mode = self.fault_mode
                self.fault_remaining -= 1
                if mode == "transient":
                    raise TransientChainError("目标链暂时不可用（注入故障）")
                if mode == "timeout":
                    raise ChainTimeoutError("目标链处理超时（注入故障）")
                raise RejectedByChainError("目标链永久拒绝（注入故障）")
            # 原子结算：源链释放托管，目标链入账
            amount = source.escrow.get(message.message_id)
            if amount is None:
                # 托管已因永久失败/重试耗尽被退回：转账已中止，不能再到账
                raise RejectedByChainError(
                    f"源链托管已释放（转账已中止或被重组）: "
                    f"{message.message_id[:12]}"
                )
            del source.escrow[message.message_id]
            del source.escrow_owner[message.message_id]
            self.ledger[message.recipient] = (
                self.ledger.get(message.recipient, 0) + amount
            )
            self.delivered.add(message.message_id)
            self.delivered_order.append(message.message_id)
            self.delivered_source[message.message_id] = source.config.name
            return message

    # ---------- 状态快照 ----------
    def snapshot(self) -> dict:
        with self.lock:
            return {
                "name": self.config.name,
                "tip_height": self.blocks[-1].height,
                "branch": self._branch,
                "finality_depth": self.config.finality_depth,
                "ledger": dict(self.ledger),
                "escrow": dict(self.escrow),
                "delivered": sorted(self.delivered),
                "spendable_total": sum(self.ledger.values()),
                "escrow_total": sum(self.escrow.values()),
            }


def _hash(tag: str, parent: str, height: int, branch: int,
          message_ids: list[str]) -> str:
    material = "|".join([tag, parent, str(height), str(branch),
                         ",".join(message_ids)])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]


def _ordered(a: "SimulatedChain", b: "SimulatedChain"):
    if a.config.name <= b.config.name:
        return a, b
    return b, a


def _ordered_locks(a: "SimulatedChain", b: "SimulatedChain"):
    first, second = _ordered(a, b)
    return _MultiLock(first.lock, second.lock)


class _MultiLock:
    def __init__(self, *locks: threading.RLock) -> None:
        self.locks = locks

    def __enter__(self):
        for lock in self.locks:
            lock.acquire()
        return self

    def __exit__(self, *exc):
        for lock in reversed(self.locks):
            lock.release()
