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
| `relayer.py` | 观察 → 证明/最终性/顺序校验 → 投递（退避重试） → 重组对账 |
| `relay/runtime.py` | 有界队列 + 固定数量 worker + 背压 + 统计 |
| `relay/clock.py` | 虚拟时钟与虚拟时间事件循环（时间只跳到下一定时器） |
| `relay/config.py` | 带确定默认值与严格校验的配置 |
| `relay/api.py` / `main.py` | HTTP 接口与本地世界装配 |

## 资金模型（守恒）

每条链独立守恒自己的代币总量。每一笔源链发送的托管锁最终落在且只落在
三种归宿之一（`send_disposition` 可查）：

- `locked`：在途。源链提交时 `sender -= amount`，`escrow += amount`。
- `settled`：目标投递生效（`escrow -= amount`，`receiver += amount`，
  投递产出记录消息 id 的“投递区块”），源侧托管锁保留作为这笔兑付的背书；
  此后源链重组**不再退款**。
- `refunded`：托管锁释放回发送方，恰好一次。触发时机：源链重组回滚了
  未投递的提交；或消息进入 `failed`/`expired` 终态。

推论（异常路径的收敛规则）：

- 源链重组：被移除的提交若仍 `locked` 则退款；若已 `settled`（投递生效）
  则保留托管锁；若已 `refunded` 则不重复退款。若重组先于投递确认造成了
  退款，`reconcile()` 会恰好追回一次（`compensated`），保证同一消息两侧
  合计只有一次资金效果。
- 目标链重组：被移除的投递反向（`receiver -> escrow`），消息重新可投递。
- 两侧先后（或交错）回滚：投递已被目标侧回滚、随后源侧包含也被移除时，
  该消息已不可能再次投递；此前为兑付而保留的 `settled` 锁成为“无兑付背书”
  的孤儿锁，由 `release_unbacked_send()` 恰好退还一次给发送方（最终归宿
  `refunded`），资金不会无限期悬空。先源后目、先目后源两种顺序收敛到同一
  终态与同一资金归宿。
- 永久失败/过期：仍锁定的资金恰好退款一次，nonce 被消费，通道继续；之后
  即便源链再发生重组，`failed`/`expired` 的状态、说明与资金归宿都保持不变
  （只影响该消息本身，与源重组结论互不覆盖）。

任何故障（瞬时/超时/永久拒绝）都在**任何余额变动之前**抛出，因此重试不会
重复扣款；每次跨链转移后两侧账本各自总量不变。

## 消息生命周期（RelayRecord.status）

```
observed ──(证明有效且达到最终性)──> finalized ──(投递成功)──> delivered
   │                                   │
   │                                   ├─ target_rejected/retries_exhausted -> failed(终态)
   │                                   └─ stale 永久过期 -> expired(终态)
   └─(源链重组使包含失效)──────────────> source_reorged(终态)

delivered ──(目标链重组回滚投递)──────> target_reorged ──(重新投递一次)──> delivered
                                             │
                                             └─(源包含随后也被回滚)──> source_reorged(终态, 退款发送方)
```

退避等待中的尝试与重组对账交错时：每轮尝试在“投递提交”前重新核对源包含
与记录终态；若源包含已被重组并完成对账，等待中的尝试立即以
`reorg_rollback` 中止，不会产生新的目标入账，也不会用“投递成功”覆盖已经
确定的 `source_reorged` 结论。

- `observed`：已在源链规范链上看到，但未达最终性。
- `finalized`：通过真实性 + 最终性 + 顺序校验，可投递。
- `delivered`：已在目标链规范链上生效（每个消息至多一次）。
- `source_reorged`：源链包含被重组掉（终态，不可再次投递，再投递得
  `reorg_rollback`）。若目标投递仍然存在，记录保留 `delivered_height`，
  源侧托管锁保留（或已被 `compensated` 追回），`note` 明确说明“目标投递
  保留”，发送方不会再获退款——两侧合计仅一次资金效果；若目标投递也已
  不存在（未投递、或先目标回滚后源回滚），记录保留回滚历史与
  `delivered_height`，孤儿托管锁恰好退还发送方，`note` 写明资金去向。
- `target_reorged`：目标投递被回滚但源包含仍在，可重新投递且仍至多生效
  一次；若此后源包含也被回滚，则收敛为 `source_reorged` 并退款发送方。
- `failed`：终态（`target_rejected` / `retries_exhausted`，note 以原因开头，
  彼此及与暂时性故障均可区分）；锁定资金已恰好退款一次，nonce 已消费，
  不阻塞同通道后续消息，后续源重组不改变该终态。
- `expired`：终态（缺口超窗未补齐）；资金同样退款，与 `failed` 状态可区分，
  后续源重组同样不改变该终态。

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
  超过窗口仍未补齐则判为永久过期。
- 永久失败/过期的消息**消费**其 nonce（游标越过它，通道不卡住）；被源重组
  移除的消息**不消费** nonce（序号可重新提交）。
- 重组后游标由“已消费”的 nonce 集合确定性重建，不跳号、不复用。

## 交错时序如何收敛（串行等价）

两层锁共同保证“投递、重试、目标回滚、源侧对账”任意交错都等价于某个合理
的串行执行，同一消息只产生一次资金效果、不存在半更新记录：

- 每消息一把 asyncio 锁：同一消息的多个处理者串行；
- 每消息一把仅覆盖“资金 + 记录/游标提交段”的线程锁
  （`_commit_locks`）：退避 `sleep` 不持锁，所以同步的
  `reconcile()`/`/reorg`（运行在另一个线程）可以在重试等待期间完成对账，
  但永远不会与“核对链状态 → 移动资金 → 写记录”的提交段交错。

收敛后的终态只取决于两侧规范链事实，与到达顺序无关：

| 源包含 | 目标投递 | 收敛终态 | 资金归宿 |
| --- | --- | --- | --- |
| 在 | 在 | `delivered` | 托管保留（`settled`），接收方已到账 |
| 在 | 不在 | `target_reorged` | 目标资金已收回托管，可恰好重投一次 |
| 不在 | 在 | `source_reorged` | 投递保留，托管保留/补偿追回，不可再投 |
| 不在 | 不在 | `source_reorged` | 托管锁（含孤儿 `settled` 锁）恰好退还发送方，不可再投 |

`failed`/`expired` 是针对当下消息的独立结论，即使随后源包含消失也保持原状。

重复执行 `reconcile()` 不再移动资金、不改状态与说明（`affected` 为空、
note 字节级稳定）；补偿历史（`compensated`）持久保留，故说明不会在第二次
对账时漂移。同通道后续消息的处理能力不受这些终态影响：`failed`/`expired`
消费 nonce，`source_reorged` 释放 nonce 供重新提交。

### 通过查询结果确认收敛

- `GET /messages/{id}`：`status` 为 `source_reorged`/`target_reorged`，
  `note` 写明资金归宿（`refunded <amount> to <sender>` /
  `escrow lock retained` / `compensated` / `not redeliverable`），
  `delivered_height` 保留回滚历史，`attempts` 保留重试审计。
- `GET /chains`：`escrow` 余额、各账户余额与 `total_supply` 可核对每链守恒；
  `send_disposition(id)`（链层）给出 `locked`/`settled`/`refunded`。
- `POST /reconcile` 收敛后重复调用返回 `{"affected": []}`。
- `GET /messages` 的 `cursors` 确认通道游标已确定性重建、后续 nonce 可继续。
