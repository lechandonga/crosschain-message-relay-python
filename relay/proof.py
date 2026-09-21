"""证明数据结构与生成/校验（本地哈希承诺，无外部依赖）。

证明绑定四要素：消息 ID、区块高度、规范区块哈希、消息在区块内的序号。
- 伪造：区块哈希或序号与源链规范链不一致；
- 重放：区块哈希属于被重组取代的旧链；
这两类在校验入口依据状态分别抛出不同错误。
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass


@dataclass(frozen=True)
class Proof:
    message_id: str
    block_height: int
    block_hash: str  # 源链规范区块哈希（重组后旧哈希即可判定为重放）
    message_index: int
    proof_hash: str  # 对 (message_id, height, block_hash, index) 的承诺

    @classmethod
    def generate(cls, message_id: str, block_height: int, block_hash: str,
                 message_index: int) -> "Proof":
        proof = cls(
            message_id=message_id,
            block_height=block_height,
            block_hash=block_hash,
            message_index=message_index,
            proof_hash="",
        )
        return cls(
            message_id=message_id,
            block_height=block_height,
            block_hash=block_hash,
            message_index=message_index,
            proof_hash=proof.commitment(),
        )

    def commitment(self) -> str:
        material = f"{self.message_id}|{self.block_height}|{self.block_hash}|{self.message_index}"
        return hashlib.sha256(material.encode("utf-8")).hexdigest()

    def is_internally_consistent(self) -> bool:
        """自身承诺是否自洽（防篡改）。"""
        return self.commitment() == self.proof_hash
