"""错误类型：每类异常路径都有彼此可区分的拒绝原因。"""


class RelayError(Exception):
    """所有中继相关错误的基类。"""


class ConfigError(RelayError):
    """非法配置：必须被明确拒绝，而不是静默忽略。"""


class DuplicateDeliveryError(RelayError):
    """目标链已存在同一 message_id 的生效记录，拒绝重复投递。"""


class ReplayDetectedError(RelayError):
    """检测到重放：证明来自已失效的旧区块（被重组取代）。"""


class ForgedProofError(RelayError):
    """伪造证明：区块哈希不匹配或消息未包含在该区块中。"""


class NotFinalError(RelayError):
    """消息所在区块尚未达到最终性深度，禁止投递。"""


class OutOfOrderError(RelayError):
    """同一发送方乱序消息（nonce 不连续），暂缓投递。"""


class StaleMessageError(RelayError):
    """过期消息：已被重组回滚且不再存在于源链规范链上。"""


class RejectedByChainError(RelayError):
    """目标链永久性拒绝（如余额不足、账户非法），不应重试。"""


class TransientChainError(RelayError):
    """暂时性故障：可重试。"""


class ChainTimeoutError(RelayError):
    """目标链处理超时：可重试，与暂时性故障分别统计。"""


class BackpressureError(RelayError):
    """积压超过上限：按策略限流，拒绝继续入队。"""
