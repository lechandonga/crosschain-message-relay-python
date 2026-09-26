# 架构与消息生命周期

本服务完全本地运行：两条模拟链 `A` 与 `B`、一个中继状态库、一个有界工作池、一个
确定性虚拟时钟。不连接任何外部链节点，不使用真实凭据或付费服务。

## 组件

| 模块 | 职责 |
| --- | --- |
| `relay/chain.py` | 哈希链接区块、本地账本、出块、最终性确认数、目标投递、重组、故障注入 |
| `relay/crypto.py` | HMAC-SHA256 本地证明签发/校验（无真实凭据） |
| `relay/proof.py` | 真实性、最终性、回滚、通道顺序/重放判定 |
| `relay/store.py` | 每消息记录、通道 nonce 游标、尝试审计（锁保护，无半更新） |
| `relayer.py` | 观察 → 证明/最终性/顺序校验 → 投递（退避重试，每次尝试前复查源）→ 终态结算 → 重组对账 |
| `relay/runtime.py` | 有界队列 + 固定数量 worker + 背压 + 统计 |
| `relay/clock.py` | 虚拟时钟与虚拟时间事件循环（时间只跳到下一定时器） |
| `relay/config.py` | 带确定默认值与严格校验的配置 |
| `relay/api.py` / `main.py` | HTTP 接口与本地世界装配 |

## 资金模型（守恒）

每条链独立守恒自己的代币总量；跨两侧“发送方+接收方”合计在任意重组前后
也保持一致（同一条消息最终只有一次资金效果）：

- 源链提交：`sender -= amount`，`escrow += amount`（锁定）。
- 目标链投递：`escrow -= amount`，`receiver += amount`（投递会产出一个
  “投递区块”，区块内容记录消息 id）。
- 源链重组：被移除的提交反向，`escrow -> sender` 自动退款。
- 目标链重组：被移除的投递反向，`receiver -> escrow` 恢复，消息重新可投递。
- 中继结算（产出结算区块、按消息 id 幂等、可被重组反向、由对账重建）：
  - 永久失败 / 永久过期：`refund_locked`，`escrow -> sender`，锁定资金
    有确定归宿、不悬空。
  - **已投递后源侧被回滚**：目标入账保留；源侧那笔自动退款由
    `reclaim_refund` 收回，`sender -> escrow`，于是发送方既已扣款、
    接收方保留入账，跨两侧合计与重组前一致，绝不凭空多出一笔。

任何故障（瞬时/超时/永久拒绝）都在**任何余额变动之前**抛出，因此重试不会
重复扣款；每次跨链转移后两侧账本各自总量不变。

## 消息生命周期（RelayRecord.status）

```
observed ──(证明有效且达到最终性)──> finalized ──(投递成功)──> delivered
   │                                   │
   │                                   ├─ target_rejected/retries_exhausted -> failed(终态)
   │                                   │      └─ refund_locked 退款 + 越过 nonce
   │                                   └─ stale 永久过期 -> expired(终态)
   │                                          └─ refund_locked 退款 + 越过 nonce
   └─(源链重组使包含失效)──────────────> source_reorged(终态)
                                          ├─ 目标投递保留: reclaim_refund 收回自动退款
                                          └─ 无目标投递: 保留链层自动退款

delivered ──(目标链重组回滚投递)──────> target_reorged ──(重新投递一次)──> delivered
```

- `observed`：已在源链规范链上看到，但未达最终性。
- `finalized`：通过真实性 + 最终性 + 顺序校验，可投递。
- `delivered`：已在目标链规范链上生效（每个消息至多一次）。
- `source_reorged`：源链包含被重组掉（终态）。
  - 目标投递仍在：记录保留 `delivered_height`，源侧自动退款被
    `reclaim_refund` 收回，`note` 写明目标投递与收回结算高度，
    `not redeliverable`，绝不静默丢弃或双重生效。
  - 目标投递不在：发送方保留链层退款，同样终态、不可再投递。
- `target_reorged`：目标投递被回滚，可重新投递且仍至多生效一次。
- `failed` / `expired`：终态；锁定资金已 `refund_locked` 退回发送方，
  nonce 已消费，拒绝原因与归宿可查询，不可再投递。

## 拒绝原因码（彼此可区分，绝不静默忽略）

| reason | 含义 | retryable | HTTP |
| --- | --- | --- | --- |
| `forged_proof` | 证明签名/字段被篡改或证明对应其他消息 | 否 | 422 |
| `duplicate_delivery` | 同一消息已在目标生效 | 否 | 409 |
| `replay_detected` | 携带有效证明但复用了已消费 nonce | 否 | 409 |
| `not_finalized` | 源确认数不足 | 是 | 425 |
| `reorg_rollback` | 源包含不再规范（重组） | 否 | 410 |
| `stale_nonce` | nonce 缺口等待（可重试）或超出过期窗口（终态） | 视情况 | 409 |
| `target_rejected` | 目标链永久拒绝（如账户被禁/托管金不足） | 否 | 422 |
| `transient_failure` | 目标暂时性故障 | 是 | 503 |
| `timeout` | 目标在期限内未响应 | 是 | 504 |
| `retries_exhausted` | 重试次数用尽 | 否 | 503 |
| `backpressure` | 有界队列已满 | 是 | 503 |
| `invalid_config` | 配置非法 | 否 | 400 |

> 区分要点：**伪造**是证明本身不真实；**重放**是证明真实但 nonce 已被消费；
> **重复投递**是同一条已生效消息再次提交。三者 reason 不同。

## 重试与退避

- 退避：`min(backoff_cap_ms, base_backoff_ms * 2**(attempt-1))`。
- 默认 `max_attempts=4`（1 次初试 + 3 次重试），用尽进入 `failed`。
- 永久拒绝（`target_rejected`）不进入重试循环。
- 所有退避都在虚拟时钟上等待，耗时为 0 真实时间，但虚拟时长精确可复现。

## 顺序规则（每通道，按发送方所在链 + channel）

- nonce 严格递增；游标 = 下一个期望 nonce。
- `nonce < 期望`：`replay_detected`。
- `nonce == 期望`：按序处理。
- `nonce > 期望`：存在缺口；在 `stale_window` 个源区块内等待（可重试），
  超过窗口仍未补齐则判为永久过期（`expired`）。
- 一条消息进入终态即**消费其 nonce**：规范投递成功、永久失败（`failed`）、
  永久过期（`expired`），以及源被回滚但目标投递保留（`source_reorged`）。
  因此一条消息永久失败/过期不会把同通道后续消息一起堵死。
- 重组后游标由“已终结 nonce 集合”确定性重建，不跳号、不复用；重复对账无
  额外影响。
