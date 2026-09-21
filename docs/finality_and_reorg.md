# 最终性与回滚规则

## 确认与最终性

- 区块通过 `parent_hash` 哈希链接；消息进入某一高度的区块。
- 确认数 `confirmations(h) = head_height - h + 1`（含该区块本身）。
- 仅当 `confirmations(source_height) >= finality_depth`（默认 3）才允许投递。
- 未达最终性返回 `not_finalized`（可重试），**不会**投递，也不会推进 nonce。

## 重组（reorg）语义

`POST /reorg {"chain","depth"}` 用一条全新分叉替换链尾 `depth` 个区块，
并立即触发 `relayer.reconcile()`。状态由确定性规则重建，结果可复现：

1. **源链包含被回滚**：记录 -> `source_reorged`（终态）。
   - 源侧 `escrow` 自动退还给原发送方，源链总量守恒。
   - 若该消息此前已投递且目标投递仍然规范，目标侧资金保持不动，记录的
     `delivered_height` 保留，`note` 明确写出“目标投递保留”。该消息不再
     允许重新投递（源已不存在），但状态对调用方完全可见、可解释。
2. **源仍规范，但目标投递被回滚**：记录 -> `target_reorged`。
   - 目标侧资金已从接收方恢复到 `escrow`。
   - 消息重新可投递；再次 `relay_once` 恰好生效一次，之后重复提交得到
     `duplicate_delivery`。
3. **源与目标都规范**：维持 `delivered`，幂等。

### 确定性与可复现

- 重组深度相同、重组前状态相同，则对账后状态、note、游标完全一致。
- 重复执行 `reconcile()` 不再产生变化（幂等）。
- 通道 nonce 游标在每次对账后从规范投递集合重算：
  `游标 = 1 + 最大的“从最小值起连续已投递”nonce`。

### 深度边界

- 重组深度必须为正且不超过当前链高（genesis 高度 0 不可回滚），否则
  链层抛出 `ValueError`。
- 只有被移除区块中“记录在区块内容里”的投递会回滚；更早深度的投递不受影响
  （浅重组保留更早投递）。

## 并发一致性

- 每个消息 id 持有独立 asyncio 锁：不同消息可并发，同一消息严格串行。
- 多个处理者竞争同一消息，最终状态与串行执行一致：恰好 1 次
  `delivered`，其余为 `duplicate_delivery`，余额只变动一次，审计中只有
  一条成功 attempt。
- nonce 游标推进与记录置为 `delivered` 在同一消息锁内完成，外部永远观察
  不到“已投递但游标未推进”之类的半更新状态。

## 积压与限流

- 待处理队列容量固定为 `queue_capacity`（默认 256），满则 `submit` 抛
  `backpressure`，内存不会无界增长。
- worker 数量固定为 `max_inflight`（默认 8），即并发上限。
- 同一已排队 id 的重复提交被合并（返回 False 而非重复入队）。
- 被背压拒绝的消息稍后重试即可，投递仍保证恰好一次。
