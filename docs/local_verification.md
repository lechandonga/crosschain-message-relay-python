# 本地验证方法

无需任何外部链、凭据或付费服务。以下命令均可离线运行。

## 准备

```bash
uv sync                 # 安装依赖（fastapi/uvicorn/pytest/pytest-asyncio）
```

## 运行测试

```bash
uv run pytest                       # 全部测试（重放/回滚/超时/并发/守恒/背压/终态结算）
uv run pytest -q -k replay          # 只跑重放/伪造/重复判定
uv run pytest -q -k reorg           # 回滚/重组对账
uv run pytest -q -k "retry or timeout"
uv run pytest -q -k concurrency     # 同消息并发串行一致性
uv run pytest -q -k conservation    # 双向/多次/失败转移守恒
uv run pytest -q -k backpressure    # 有界队列与限流
uv run pytest -q -k settlement      # 终态结算：单次资金效果/通道活性/交错一致
```

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

## 复现终态结算场景

```bash
# A. 已投递后源侧被回滚 -> 只保留一次资金效果
#    依次：submit -> mine 3 -> scan -> relay/A-msg-1（delivered）
#    记下 GET /chains 中 alice / escrow(A) / bob / escrow(B) 四个余额，
#    然后回滚源链：
curl -s -X POST localhost:8077/reorg -H 'Content-Type: application/json' \
  -d '{"chain":"A","depth":3}'
#    再次 GET /chains：四个余额与重组前完全一致（alice 未获退款，
#    escrow(A) 保留托管锁）；GET /messages/A-msg-1 为 source_reorged，
#    note 含 “target delivery ... stands; escrow lock retained”，
#    delivered_height 保留；再 POST /relay/A-msg-1 -> 410 reorg_rollback；
#    再 POST /reconcile -> {"affected": []}（幂等，无额外资金变动）。

# B. 永久失败后同通道继续处理
curl -s -X POST localhost:8077/faults -H 'Content-Type: application/json' \
  -d '{"chain":"B","message_id":"A-msg-1","reject_receiver":"ghost"}'
#    relay nonce=1（receiver=ghost）-> 422 target_rejected，记录 failed，
#    note 为 “target_rejected; refunded <amount> to alice”；
#    GET /messages 可见游标已越过 nonce 1；再提交 nonce=2 并 relay ->
#    正常 delivered；alice 的退款只发生一次。

# C. 交错/并发：同一消息并发投递、重试退避中执行 reconcile、
#    重组后多个处理者竞争——均有自动化用例（pytest -k settlement），
#    日志打印输入与判定依据，可逐条对照。
```

## 判定结论在哪里看

- 每条消息：`GET /messages/{id}`，含 `status`、`note`、全部 `attempts`
  （每次尝试的结果原因与时间）。终态消息的 `note` 同时写明资金归宿
  （`refunded <amount> to <sender>` / `escrow lock retained` /
  `compensated`）。
- 汇总：`GET /messages?status=delivered|failed|source_reorged|...`。
- 资金：`GET /chains` 给出各链 `escrow`、总量与规范投递清单；两侧总量
  始终各自守恒，异常路径收敛后账面合计与事前一致。
- 运行时：`GET /runtime`，含队列大小、在处理数、按 reason 分类的错误计数、
  背压次数。
- 错误响应体统一为：
  `{"ok":false,"reason":...,"retryable":...,"detail":...}`。
