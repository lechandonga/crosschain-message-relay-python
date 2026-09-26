# 本地验证方法

无需任何外部链、凭据或付费服务。以下命令均可离线运行。

## 准备

```bash
uv sync                 # 安装依赖（fastapi/uvicorn/pytest/pytest-asyncio）
```

## 运行测试

```bash
uv run pytest                       # 全部测试
uv run pytest -q -k replay          # 只跑重放/伪造/重复判定
uv run pytest -q -k reorg           # 回滚/重组对账
uv run pytest -q -k settlement      # 投递后源回滚/永久失败/交错收敛（本版新增）
uv run pytest -q -k "retry or timeout"
uv run pytest -q -k concurrency     # 同消息并发串行一致性
uv run pytest -q -k conservation    # 双向/多次/失败转移守恒
uv run pytest -q -k backpressure    # 有界队列与限流
```

新增的三类异常时序收敛用例集中在 `tests/test_settlement.py`：

- 已投递后源侧被回滚：
  `test_delivered_then_source_reorg_keeps_single_effect`、
  `..._is_idempotent_at_any_depth`（含结算区块再被回滚）、
  `test_source_reorg_before_delivery_refunds_sender`。
- 永久失败后同通道继续：
  `test_target_rejected_refunds_and_channel_continues`、
  `test_retries_exhausted_refunds_and_channel_continues`、
  `test_failed_refund_reissued_after_settlement_reorg`。
- 投递/重试/对账交错（含同消息多处理者）：
  `test_reorg_during_backoff_never_late_delivers`、
  `test_concurrent_handlers_with_reorg_match_serial_run`、
  `test_reconcile_interleaving_with_inflight_delivery_is_stable`。

测试使用虚拟时间事件循环：重试退避、超时**零真实耗时**但虚拟时长精确，
因此每次运行结果一致。单测通过 `log_case(...)` 打印“输入 + 判定依据”。

## 启动本地服务

```bash
uv run uvicorn main:app --port 8077
```

健康检查：`GET /health`。运行参数可通过 `RELAY_*` 环境变量覆盖，例如：

```bash
RELAY_FINALITY_DEPTH=5 RELAY_MAX_ATTEMPTS=2 \
  uv run uvicorn main:app --port 8077
```

非法覆盖（如 `RELAY_MAX_ATTEMPTS=0`）在启动时以 `invalid_config` 明确拒绝。

## 一次完整的手工流程（curl）

```bash
# 1) 在 A 提交（下一次 mine 才入块）
curl -s -X POST localhost:8077/chains/A/submit -H 'Content-Type: application/json' \
  -d '{"chain":"A","channel":"ch","sender":"alice","receiver":"bob","amount":120,"nonce":1}'

# 2) 挖 3 个区块达到最终性
curl -s -X POST localhost:8077/mine -H 'Content-Type: application/json' \
  -d '{"chain":"A","count":3}'

# 3) 观察 -> 中继投递
curl -s -X POST localhost:8077/scan
curl -s -X POST localhost:8077/relay/A-msg-1
# 再次投递 -> 409 duplicate_delivery

# 查询状态
curl -s localhost:8077/chains | python3 -m json.tool
curl -s localhost:8077/messages | python3 -m json.tool
curl -s localhost:8077/runtime | python3 -m json.tool
```

## 复现异常路径

```bash
# 暂时性故障 2 次（第 3 次成功，观察退避 50+100ms 虚拟时间）
curl -s -X POST localhost:8077/faults -H 'Content-Type: application/json' \
  -d '{"chain":"B","message_id":"A-msg-1","transient":2}'

# 超时
curl -s -X POST localhost:8077/faults -H 'Content-Type: application/json' \
  -d '{"chain":"B","message_id":"A-msg-1","timeout":1}'

# 永久封禁接收方（target_rejected，不重试）
curl -s -X POST localhost:8077/faults -H 'Content-Type: application/json' \
  -d '{"chain":"B","message_id":"A-msg-1","reject_receiver":"bob"}'

# 源链回滚（depth=链尾高度），随后 GET /messages/A-msg-1 见 source_reorged
curl -s -X POST localhost:8077/reorg -H 'Content-Type: application/json' \
  -d '{"chain":"A","depth":3}'
```

### 场景一：已投递后源侧回滚（资金只生效一次）

```bash
# 投递 nonce 1 成功（bob 入账 120）
curl -s -X POST localhost:8077/chains/A/submit -H 'Content-Type: application/json' \
  -d '{"chain":"A","channel":"ch","sender":"alice","receiver":"bob","amount":120,"nonce":1}'
curl -s -X POST localhost:8077/mine -H 'Content-Type: application/json' -d '{"chain":"A","count":3}'
curl -s -X POST localhost:8077/scan
curl -s -X POST localhost:8077/relay/A-msg-1

# 回滚整条 A 链（源包含消失，目标投递仍在 B）
DEPTH=$(curl -s localhost:8077/chains/A | python3 -c 'import sys,json;print(json.load(sys.stdin)["head_height"])')
curl -s -X POST localhost:8077/reorg -H 'Content-Type: application/json' \
  -d "{\"chain\":\"A\",\"depth\":$DEPTH}"

# 结论核对：
curl -s localhost:8077/messages/A-msg-1 | python3 -m json.tool
#   status=source_reorged，delivered_height 保留，note 含 "retained"/"reclaimed"/
#   "not redeliverable"；GET /chains/A 的 settlements 中有一笔 reclaim_refund
# 再次投递 -> 410 reorg_rollback；bob 保留 120，alice 没有拿回 120（无中生有被消除）
```

### 场景二：永久失败不堵通道、资金退回

```bash
# 在目标链 B 永久封禁 ghost（message_id 为必填，封禁按 receiver 生效，占位即可）
curl -s -X POST localhost:8077/faults -H 'Content-Type: application/json' \
  -d '{"chain":"B","message_id":"*","reject_receiver":"ghost"}'
# nonce 1 发到 ghost -> 422 target_rejected
curl -s -X POST localhost:8077/chains/A/submit -H 'Content-Type: application/json' \
  -d '{"chain":"A","channel":"ch","sender":"alice","receiver":"ghost","amount":100,"nonce":1}'
curl -s -X POST localhost:8077/mine -H 'Content-Type: application/json' -d '{"chain":"A","count":3}'
curl -s -X POST localhost:8077/scan
curl -s -X POST localhost:8077/relay/A-msg-1     # 422 target_rejected
# nonce 2 发到 bob -> 200 delivered（通道没有被 nonce 1 卡住）
curl -s -X POST localhost:8077/chains/A/submit -H 'Content-Type: application/json' \
  -d '{"chain":"A","channel":"ch","sender":"alice","receiver":"bob","amount":200,"nonce":2}'
curl -s -X POST localhost:8077/mine -H 'Content-Type: application/json' -d '{"chain":"A","count":3}'
curl -s -X POST localhost:8077/scan
curl -s -X POST localhost:8077/relay/A-msg-2
# 核对：GET /messages?status=failed 的 note 含退款结算高度；
#       GET /chains/A 的 settlements 中有一笔 refund_locked(100 -> alice)；
#       GET /messages 的 cursors 为 {"A:ch": 3}
```

## 判定结论在哪里看

- 每条消息：`GET /messages/{id}`，含 `status`、`note`、`delivered_height`、
  全部 `attempts`（每次尝试的结果原因与时间）。终态消息的 `note` 明确写出
  资金归宿（退款/收回结算高度）、是否可再投递（`not redeliverable`）。
- 汇总：`GET /messages?status=delivered|failed|expired|source_reorged|target_reorged`。
- 资金结算：`GET /chains[/{A|B}]` 的 `settlements` 数组列出每笔链上结算
  （`kind` = `refund_locked` / `reclaim_refund`，及 `sender/amount/height`）；
  同响应里的 `escrow` 与各账户余额可直接核对守恒。
- 通道游标：`GET /messages` 的 `cursors`（形如 `{"A:ch": 3}`）。
- 运行时：`GET /runtime`，含队列大小、在处理数、按 reason 分类的错误计数、
  背压次数。
- 错误响应体统一为：
  `{"ok":false,"reason":...,"retryable":...,"detail":...}`。
- 终态 reason 区分：`target_rejected`（永久拒绝，不重试）、
  `retries_exhausted`（重试耗尽）、`stale_nonce` 且 `retryable=false`
  （永久过期）、`transient_failure`/`timeout`（暂时性，可重试）、
  `reorg_rollback`（源回滚终态）、`duplicate_delivery`/`replay_detected`
  （重复/重放）。
