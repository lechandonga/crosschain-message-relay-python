# 跨链消息中继服务（本地双模拟链 · Python）

一个**零外部依赖即可在本地运行**的跨链消息中继：两条本地模拟链 `A`、`B`
承载消息的「提交 → 观察 → 证明 → 最终性确认 → 目标链投递」全生命周期，
账本完全由内存中的本地账本表示，不连接任何外部链节点、真实凭据或付费服务。

设计目标：在**重放、回滚、超时、永久拒绝、并发、积压**等异常路径下，
结论一致、拒绝原因可区分、状态可查询、资金总量守恒。

---

## 1. 快速开始

```bash
# 依赖仅 fastapi/uvicorn（开发期含 pytest）
uv sync

# 启动 HTTP 服务（默认已为 A:alice / B:bob 预置 1,000,000 余额）
uv run uvicorn main:app --reload
# 浏览器打开 http://127.0.0.1:8000/docs 可交互调用
```

一笔 A→B 跨链转账的最小流程：

```bash
# 1) A 链铸币
curl -X POST localhost:8000/chains/A/mint -d '{"account":"alice","amount":1000}' -H 'Content-Type: application/json'
# 2) 提交跨链消息（源链立即托管扣款并出块）
curl -X POST localhost:8000/messages -H 'Content-Type: application/json' \
  -d '{"source":"A","dest":"B","sender":"alice","recipient":"carol","amount":100,"nonce":0}'
# 3) 中继观察一轮（区块高度=1，未达最终性深度 2，不会投递）
curl -X POST localhost:8000/relayer/A/B/run
# 4) 源链再出 2 个空块，使消息所在区块达到最终性
curl -X POST localhost:8000/chains/A/mine -d '{"count":2}' -H 'Content-Type: application/json'
# 5) 再跑一轮：生成证明、校验、目标链入账
curl -X POST localhost:8000/relayer/A/B/run
# 6) 查询消息与总量守恒
curl localhost:8000/messages/<message_id>
curl localhost:8000/conservation
```

---

## 2. 消息生命周期

```
SUBMITTED ──observe──▶ OBSERVED ──达到最终性──▶ FINALIZED ──投递成功──▶ DELIVERED(终态)
                              │                       │
                              │                       ├─ 永久拒绝/伪造/重放 ─▶ FAILED(终态, 托管退回)
                              │                       └─ 暂时性故障/超时 ──退避重试──▶ 成功 或 重试耗尽→FAILED
                              └─ 所在区块被重组丢弃 ──▶ ROLLED_BACK ──重新提交──▶ SUBMITTED …
```

- `message_id = sha256(源链|目标链|发送方|接收方|金额|nonce|payload)`，确定性、可复现。
- 状态只沿上述方向推进；终态（`DELIVERED`/`FAILED`）**幂等不复活**。
- 同一 `(发送方, 目标链, nonce)` 只能绑定一个 message_id，**同 nonce 不同内容
  在源链提交时即被拒绝**（防止重放式替换金额/接收方）。

### 资金模型（为什么永远守恒）

| 时点 | 源链 | 目标链 |
|---|---|---|
| 提交消息 | 发送方扣款，金额进入**托管 escrow**（不可再花） | — |
| 投递成功（跨链原子结算） | 托管销毁/释放 | 接收方入账 |
| 永久失败 / 重试耗尽 | 托管**原路退回**发送方 | — |
| 区块被重组丢弃 | 托管**原路退回**发送方 | — |

任意时刻恒等式：

```
Σ 所有链 spendable 余额 + Σ 所有链 escrow 余额 == 累计铸币总量
```

`GET /conservation` 可随时查询；所有测试均断言该等式。

---

## 3. 最终性与回滚规则

- 每条链有 `finality_depth`（默认 2）：区块 `h` 满足
  `tip - h >= finality_depth` 才算最终；**未最终的消息禁止投递**
  （报 `NotFinalError`，可区分原因）。
- `POST /chains/{name}/reorg {"depth": n}` 可确定性地丢弃最后 `n` 个区块，
  改接一条「替代空链」（分支号 +1）：
  - **已最终区块不可回滚**——触及最终区块的重组被明确拒绝（最终性是硬边界）；
  - 被丢弃消息：托管原路退款、消息从规范链索引移除、旧区块哈希记入
    「已丢弃集合」，中继记录置为 `ROLLED_BACK`，**不静默丢弃**；
  - 之后持**旧区块哈希**的证明投递时报 `ReplayDetectedError`；
  - 消息若在替代链重新提交并最终化，可沿**同一 message_id 正常生效一次**。
- 已 `DELIVERED` 的消息是目标链上的既成事实，后续只回滚源链非最终空块
  不会撤销它（结论确定、可复现）。

### 证明与三类拒绝的区分

证明绑定 `(message_id, block_height, block_hash, message_index)` 并有自洽哈希：

| 情形 | 错误类型 |
|---|---|
| 目标链已有同一 message_id 生效 | `DuplicateDeliveryError` |
| 证明区块哈希属于被重组取代的旧链 | `ReplayDetectedError` |
| 哈希不匹配/序号越界/证明被篡改/高度越界 | `ForgedProofError` |
| 区块未达最终性 | `NotFinalError` |
| 目标链永久拒绝（注入 permanent、目标链不匹配、托管已释放等） | `RejectedByChainError` |
| 目标链暂时不可用 | `TransientChainError`（可重试） |
| 目标链处理超时 | `ChainTimeoutError`（可重试，单独计数） |

---

## 4. 重试、退避与顺序

- **暂时性故障与超时分别计数**（记录中 `transient_failures` /
  `timeout_failures` 可查询）；永久错误**不重试**直接 FAILED。
- 指数退避（测试由虚拟时钟确定性驱动，不真实睡眠）：

  `delay_ms = min(backoff_base_ms * 2**(attempt-1), backoff_max_ms)`
  默认 `50ms, 100, 200, 400, …, 上限 2000ms`。
- 重试上限 `max_retries`（默认 5 次尝试）；耗尽后置 FAILED 并退回托管，
  **重试不会造成重复入账**（目标链投递幂等 + message_id 唯一约束）。
- **顺序规则**：同一发送方严格按 nonce 连续处理，缺号时高 nonce 消息暂缓
  （记录 `last_error` 说明依据），但**不阻塞其他发送方**；
  `DELIVERED` 与 `FAILED` 都占住 nonce 并推进水位，
  `ROLLED_BACK` 不推进（消息可能重新出现）。

## 5. 并发与背压

- 账本临界区由链锁保护；跨链结算按链名字典序加锁，避免死锁，
  源链释放托管与目标链入账**原子完成，无半更新**。
- 中继对每条消息有独立锁；多个处理者并发处理同一消息时，
  最终状态与串行执行一致，`delivered` 集合只增加一次，余额只变动一次
  （`tests/test_concurrency.py` 用 8 线程屏障并发验证）。
- 积压超过 `max_pending`（默认 128）时新提交抛 `BackpressureError`，
  显式限流而非无界占用内存。

---

## 6. 配置参数（确定默认值，非法值启动即拒绝）

| 参数 | 默认 | 含义 |
|---|---|---|
| `chain_a/chain_b.finality_depth` | 2 | 各自最终性深度（>=1） |
| `max_retries` | 5 | 单条消息最大尝试次数（>=0） |
| `backoff_base_ms` | 50 | 退避基数（>0） |
| `backoff_max_ms` | 2000 | 退避上限（>=base） |
| `delivery_timeout_ms` | 1000 | 单次投递超时预算（>0） |
| `max_pending` | 128 | 在途积压上限（>=1） |

环境变量覆盖：`RELAY_FINALITY_DEPTH`、`RELAY_MAX_RETRIES`、
`RELAY_BACKOFF_BASE_MS`、`RELAY_BACKOFF_MAX_MS`、
`RELAY_DELIVERY_TIMEOUT_MS`、`RELAY_MAX_PENDING`。
任何非法组合都会抛 `ConfigError`，**绝不静默回落到默认值**。

## 7. HTTP 接口

| 方法路径 | 作用 |
|---|---|
| `GET /health` | 健康检查 |
| `POST /chains/{name}/mint` | 铸币（测试/初始化账本） |
| `POST /chains/{name}/mine` | 推进 N 个空块（推动最终性） |
| `POST /chains/{name}/reorg` | 确定性重组最后 N 个区块 |
| `GET  /chains/{name}/state` | 区块高度、分支、账本、托管、已投递集合 |
| `POST /messages` | 提交跨链消息 |
| `GET  /messages/{id}` | 消息状态、尝试次数、分类失败计数、最后判定依据 |
| `POST /relayer/{src}/{dst}/run` | 跑一轮 observe + 有序投递 |
| `GET  /relayer/status` | 两个方向中继的状态汇总 |
| `GET  /conservation` | 两链 spendable+escrow 总量守恒检查 |

错误统一返回 `4xx/409`，body 为
`{"detail":{"error_type":"<异常类名>","detail":"<中文判定依据>"}}`，
拒绝原因彼此可区分。故障注入（测试用）：直接设置
`chain.fault_mode = "transient"|"timeout"|"permanent"` 与
`chain.fault_remaining = N`。

---

## 8. 本地验证方法

```bash
uv run pytest -s
```

测试以异常路径为主，单测日志打印输入与判定依据：

| 测试文件 | 覆盖场景 |
|---|---|
| `test_happy_path.py` | 正向生命周期与守恒 |
| `test_dedup_replay.py` | 重复投递 / 重组后重放 / 多种伪造证明 / 未最终，拒绝原因两两可区分 |
| `test_reorg.py` | 最终区块不可回滚、已投递不受影响、观察后回滚退款再提交、跨运行可复现 |
| `test_retry_timeout.py` | 暂时性故障与超时分类、指数退避、重试上限、永久失败不重试、终态幂等 |
| `test_timeout_path.py` | 超时单独计数、退避时长精确断言（虚拟时钟） |
| `test_ordering.py` | 同发送方乱序暂缓、补齐后严格按序生效、不同发送方不互相阻塞、过期判定一致 |
| `test_concurrency.py` | 8 线程并发同一消息、20 消息并发、背压上限 |
| `test_conservation.py` | 双向多笔转账、成功/永久失败/暂态/回滚混合后的总量守恒 |
| `test_config_api.py` | 非法配置拒绝、nonce 复用拒绝、HTTP 状态查询与可区分错误类型 |
