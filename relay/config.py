"""关键运行参数配置：确定默认值 + 启动时显式校验。"""
from __future__ import annotations

from dataclasses import dataclass

from .errors import ConfigError


@dataclass(frozen=True)
class ChainConfig:
    name: str
    finality_depth: int = 2  # 区块进入规范链后需再追加 N 个区块才算最终

    def validate(self) -> None:
        if not self.name:
            raise ConfigError("链名称不能为空")
        if self.finality_depth < 1:
            raise ConfigError(
                f"链 {self.name} 的 finality_depth 必须 >= 1，实际: {self.finality_depth}"
            )


@dataclass(frozen=True)
class RelayConfig:
    chain_a: ChainConfig = ChainConfig("A")
    chain_b: ChainConfig = ChainConfig("B")
    max_retries: int = 5            # 单条消息最多重试次数
    backoff_base_ms: int = 50       # 退避基数：base * 2^attempt
    backoff_max_ms: int = 2000      # 退避上限
    delivery_timeout_ms: int = 1000 # 单次投递超时
    max_pending: int = 128          # 在途积压上限（背压）
    retry_fail_after: bool = True   # 超过重试上限后置为终态失败

    def validate(self) -> None:
        """启动前显式校验；任何非法值都抛 ConfigError，绝不静默采用默认。"""
        if not self.chain_a.name or not self.chain_b.name:
            raise ConfigError("链名称不能为空")
        if self.chain_a.name == self.chain_b.name:
            raise ConfigError("两条链名称必须不同")
        self.chain_a.validate()
        self.chain_b.validate()
        if self.max_retries < 0:
            raise ConfigError(f"max_retries 必须 >= 0，实际: {self.max_retries}")
        if self.backoff_base_ms <= 0:
            raise ConfigError(f"backoff_base_ms 必须 > 0，实际: {self.backoff_base_ms}")
        if self.backoff_max_ms < self.backoff_base_ms:
            raise ConfigError(
                f"backoff_max_ms({self.backoff_max_ms}) 不能小于 "
                f"backoff_base_ms({self.backoff_base_ms})"
            )
        if self.delivery_timeout_ms <= 0:
            raise ConfigError(
                f"delivery_timeout_ms 必须 > 0，实际: {self.delivery_timeout_ms}"
            )
        if self.max_pending < 1:
            raise ConfigError(f"max_pending 必须 >= 1，实际: {self.max_pending}")


def default_config() -> RelayConfig:
    return RelayConfig()
