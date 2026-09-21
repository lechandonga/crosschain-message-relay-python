"""消息与生命周期状态类型。"""
from __future__ import annotations

import enum
import hashlib
from dataclasses import dataclass


class MessageStatus(enum.Enum):
    SUBMITTED = "submitted"      # 已提交到源链区块
    OBSERVED = "observed"        # 中继已观察，但区块未最终
    FINALIZED = "finalized"      # 源链已最终
    DELIVERED = "delivered"      # 目标链已生效（终态）
    FAILED = "failed"            # 永久失败（终态：永久拒绝/伪造/重试耗尽）
    ROLLED_BACK = "rolled_back"  # 所在区块被重组丢弃；若重新提交可再次被观察


TERMINAL_STATUSES = frozenset({MessageStatus.DELIVERED, MessageStatus.FAILED})


@dataclass(frozen=True)
class CrossChainMessage:
    source_chain: str
    dest_chain: str
    sender: str
    recipient: str
    amount: int                  # 最小单位整数，保证守恒计算精确
    nonce: int                   # 同一发送方严格递增
    payload: str = ""

    def __post_init__(self) -> None:
        if not self.source_chain or not self.dest_chain:
            raise ValueError("source_chain / dest_chain 不能为空")
        if self.source_chain == self.dest_chain:
            raise ValueError("跨链消息的源链与目标链必须不同")
        if not self.sender or not self.recipient:
            raise ValueError("sender / recipient 不能为空")
        if self.amount <= 0:
            raise ValueError(f"amount 必须 > 0，实际: {self.amount}")
        if self.nonce < 0:
            raise ValueError(f"nonce 必须 >= 0，实际: {self.nonce}")

    @property
    def message_id(self) -> str:
        # 全字段参与承诺：发送方/nonce 决定顺序，金额/接收方决定内容
        material = "|".join([
            self.source_chain, self.dest_chain, self.sender, self.recipient,
            str(self.amount), str(self.nonce), self.payload,
        ])
        return hashlib.sha256(material.encode("utf-8")).hexdigest()
