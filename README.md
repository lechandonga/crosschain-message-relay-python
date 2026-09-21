# 本地跨链消息中继服务（Python）

一个完全本地、可运行、确定性可复现的跨链消息中继：在两条模拟链
**A / B** 之间完成消息的**提交 → 证明校验 → 最终性确认 → 目标投递**，
并对重放、回滚、超时、并发、积压等异常路径给出**彼此可区分、可解释**的结论。

不依赖外部链节点、真实凭据或付费服务；链上状态就是本地账本。

## 快速开始

```bash
uv sync
uv run pytest                       # 54 个测试：重放/回滚/超时/并发/守恒/背压
uv run uvicorn main:app --port 8077  # 启动本地服务
```

打开另一个终端：

```bash
curl -s localhost:8077/health
```

完整手工流程见 [`docs/local_verification.md`](docs/local_verification.md)。

## 关键保证

- **恰好一次生效**：同一消息在目标链最多生效一次。重复投递
  (`duplicate_delivery`)、nonce 重放 (`replay_detected`)、伪造证明
  (`forged_proof`) 被区分为不同 reason，绝不静默忽略或覆盖结果。
- **最终性**：源确认数达到 `finality_depth`（默认 3）才投递；未达到返回
  `not_finalized`。
- **重组/回滚确定性**：源回滚 -> `source_reorged`（退款，终态）；目标回滚 ->
  `target_reorged`（资金恢复托管，可重投且仍只生效一次）。规则确定、可复现、
  幂等，不静默丢弃。见 [`docs/finality_and_reorg.md`](docs/finality_and_reorg.md)。
- **失败与重试**：目标永久拒绝、暂时性故障、超时被区分；指数退避
  `base*2^(n-1)`（上限 `backoff_cap_ms`），重试次数有硬上限；失败不扣款，
  重试不重复扣款，两侧账本总量守恒。
- **顺序一致**：每通道 nonce 严格递增；缺口在过期窗口内等待，超窗判永久过期；
  重组后游标确定性重建。
- **并发等价串行**：每消息独立锁，多个处理者竞争同一消息最终结果与串行一致，
  无半更新状态。
- **有界积压**：队列容量固定，满则显式 `backpressure`，不无限占用内存；
  重复入队被合并。
- **可配置且强校验**：关键参数都有确定默认值，非法配置启动即拒绝，运行状态
  可经 `GET /runtime`、`GET /messages` 查询。

## 可配置参数（默认值）

| 环境变量 | 默认 | 含义 |
| --- | --- | --- |
| `RELAY_FINALITY_DEPTH` | 3 | 投递所需源确认数 |
| `RELAY_MAX_ATTEMPTS` | 4 | 初试 + 重试上限 |
| `RELAY_BASE_BACKOFF_MS` | 50 | 退避基数 |
| `RELAY_BACKOFF_CAP_MS` | 1000 | 退避上限 |
| `RELAY_TIMEOUT_MS` | 200 | 目标调用期限 |
| `RELAY_STALE_WINDOW` | 8 | nonce 缺口过期窗口（区块） |
| `RELAY_QUEUE_CAPACITY` | 256 | 有界待处理队列容量 |
| `RELAY_MAX_INFLIGHT` | 8 | 并发 worker 上限 |
| `RELAY_ESCROW_INITIAL` | 1000000 | 目标桥托管初始资金 |

非法值（0、负数、cap<base、未知键）都会以 `invalid_config` 明确拒绝。

## 主要 HTTP 接口

| 方法与路径 | 作用 |
| --- | --- |
| `GET /health` | 状态、时钟、当前配置 |
| `GET /chains` / `/chains/{A|B}` | 账本、托管金、区块头、消息、投递 |
| `POST /chains/{chain}/submit` | 提交跨链消息（下一次出块纳入） |
| `POST /mine` | 出块 |
| `POST /reorg` | 重组并触发对账 |
| `POST /faults` | 注入瞬时故障/超时/永久封禁 |
| `POST /scan` | 观察规范链消息入库 |
| `POST /relay/{id}` | 校验并投递（含退避重试） |
| `POST /reconcile` | 按规范链状态重建记录与游标 |
| `GET /messages[?status=]` | 记录、游标、审计尝试 |
| `POST /runtime/scan-enqueue` `/runtime/join` | 扫描并入工作池 / 等待排空 |
| `GET /runtime` | 队列、在处理、按 reason 分类的统计与背压计数 |

## 文档

- [`docs/architecture.md`](docs/architecture.md)：组件、资金模型、消息生命周期、
  拒绝原因码、重试与顺序规则。
- [`docs/finality_and_reorg.md`](docs/finality_and_reorg.md)：最终性、重组/回滚、
  并发一致性、限流规则。
- [`docs/local_verification.md`](docs/local_verification.md)：本地测试与手工复现
  各异常路径的方法。

## 目录

```
relay/
  chain.py      模拟链：区块、账本、最终性、投递、重组、故障注入
  crypto.py     本地 HMAC 证明签发/校验
  proof.py      真实性/最终性/回滚/顺序判定
  store.py      并发安全的记录、游标与审计
  relayer.py    中继管线与重组对账
  runtime.py    有界工作池、背压、统计
  clock.py      虚拟时钟与虚拟时间事件循环
  config.py     配置与校验
  api.py        HTTP 接口
main.py         本地世界装配与服务入口
tests/          以异常路径与守恒为主的测试
```
