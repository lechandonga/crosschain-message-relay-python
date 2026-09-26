# 最终性与回滚规则

## 确认与最终性

- 区块通过 `parent_hash` 哈希链接；消息进入某一高度的区块。
- 确认数 `confirmations(h) = head_height - h + 1`（含该区块本身）。
- 仅当 `confirmations(source_height) >= finality_depth`（默认 3）才允许投递。
- 未达最终性返回 `not_finalized`（可重试），**不会**投递，也不会推进 nonce。

## 重组（reorg）语义

`POST /reorg {"chain","depth"}` 用一条全新分叉替换链尾 `depth` 个区块，
并立即触发 `await relayer.reconcile()`。状态由确定性规则重建，结果可复现。

**资金归宿总则**：无论源侧重组多深、何时发生，同一条消息最终只保留一次
资金效果，且“发送方+接收方”跨两侧的持有合计与重组前一致。已发生的目标
入账不会被抹掉或覆盖；源链层在重组时自动退给发送方的那笔钱，若目标入账
仍然成立，会由中继发出一笔 `reclaim_refund` 结算（产出结算区块、记录在
`/chains` 的 `settlements` 中）从发送方收回托管。

1. **已投递，随后源链包含被回滚（目标投递仍规范）**：记录 -> `source_reorged`
   （终态，`delivered_height` 保留）。
   - 目标侧入账**保留不动**；源侧自动退款被 `reclaim_refund` 收回 `escrow`。
   - 结果：发送方没有拿回钱、接收方保留入账，跨两侧合计 = 重组前；不会凭空多出金额。
   - `note` 写明目标投递高度、收回结算高度、`not redeliverable`；再次投递得到
     `reorg_rollback`（410）。
2. **未投递，源链包含被回滚**：记录 -> `source_reorged`（终态）。
   - 源侧 `escrow` 的自动退款发给发送方（发送方拿回，接收方从未入账），
     合计仍 = 重组前；无 `reclaim_refund`；不可再投递。
3. **源仍规范，但目标投递被回滚**：记录 -> `target_reorged`。
   - 目标侧资金已从接收方恢复到 `escrow`。
   - 消息重新可投递；再次 `relay_once` 恰好生效一次，之后重复提交得到
     `duplicate_delivery`。
4. **永久失败 / 过期后结算区块本身又被回滚**：中继对账重新发出
   `refund_locked`，资金重新退回发送方——资金不会悬在托管里，记录终态不变。
5. **源与目标都规范**：维持 `delivered`，幂等。

### 结算（settlement）是什么

中继层为锁定资金发放的、与提交/投递同构的确定性链上动作，均产出区块、
按消息 id 幂等、可被后续重组反向、再由对账重建：

| kind | 方向 | 触发 |
| --- | --- | --- |
| `refund_locked` | `escrow -> sender` | 目标永久拒绝 / 重试耗尽 / nonce 永久过期 |
| `reclaim_refund` | `sender -> escrow` | 已投递消息源侧被回滚、目标入账保留时 |

在 `GET /chains[/{chain}]` 的 `settlements` 字段可查到每笔结算的
`message_id / kind / sender / amount / height`。

### 确定性与可复现

- 重组深度相同、重组前状态相同，则对账后状态、note、游标、结算完全一致。
- 重复执行 `reconcile()` 不再产生变化（幂等，返回 `[]`）；已在链上的
  规范结算会被复用，绝不重复搬动资金。
- 结算区块被更深的重组移除时，下一次对账按相同规则重新收敛到同一归宿。
- 通道 nonce 游标在每次对账后从“已终结（规范投递 / 永久失败 / 过期，
  或源被回滚但目标投递保留）”的 nonce 集合重算：
  `游标 = 1 + 最大的“从最小值起连续已终结”nonce`。

### 深度边界

- 重组深度必须为正且不超过当前链高（genesis 高度 0 不可回滚），否则
  链层抛出 `ValueError`。
- 只有被移除区块中“记录在区块内容里”的投递/结算会回滚；更早深度的不受
  影响（浅重组保留更早的投递与结算）。

## 永久失败与顺序收敛

单条消息的永久失败**只影响它自己**，不卡住通道、不让资金悬空：

- `target_rejected`（目标永久拒绝，不重试）与 `retries_exhausted`
  （退避重试达到硬上限）、以及 `stale_nonce` 超窗（终态 `expired`）彼此
  可区分：reason 码、`retryable`、记录 `status`/`note` 都不同。
- 终态处理在同一消息锁内一步完成：源侧 `refund_locked` 把锁定资金退给
  发送方（确定归宿）、记录置 `failed`/`expired`、通道游标越过该 nonce。
- 同通道序号更靠后的消息随后可正常投递（示例见
  `tests/test_settlement.py::test_target_rejected_refunds_and_channel_continues`）。
- 终态消息再次 `relay_once` 得到稳定的终态拒绝（failed -> relay_error，
  expired -> stale_nonce），不会重复退款（结算按消息 id 幂等）。

## 并发一致性

- 每个消息 id 持有独立 asyncio 锁：不同消息可并发，同一消息严格串行。
- `reconcile()` 持有一把全局对账锁，并按存储顺序依次获取每条受影响消息
  的消息锁；因此“投递 / 退避重试 / 对账”任意交错（含同一消息被多个处理者
  同时处理）都等价于某个串行执行：无半更新、无重复资金变动、重复对账无副作用。
- 关键防线：每次目标投递尝试（含每次退避睡眠醒来之后）都会在消息锁内重新
  核对源包含是否仍规范。若源侧已在退避期间被回滚并完成对账，醒来的重试会
  以 `reorg_rollback` 中止，绝不会再对一条源侧已回滚的消息在目标侧补投。
- 多个处理者竞争同一消息，最终状态与串行执行一致：恰好 1 次 `delivered`，
  其余为 `duplicate_delivery`，余额只变动一次，审计中只有一条成功 attempt。
- nonce 游标推进、终态结算与记录状态在同一消息锁内完成，外部永远观察不到
  “已投递/已退款但游标或记录未更新”之类的半更新状态。

## 积压与限流

- 待处理队列容量固定为 `queue_capacity`（默认 256），满则 `submit` 抛
  `backpressure`，内存不会无界增长。
- worker 数量固定为 `max_inflight`（默认 8），即并发上限。
- 同一已排队 id 的重复提交被合并（返回 False 而非重复入队）。
- 被背压拒绝的消息稍后重试即可，投递仍保证恰好一次。

