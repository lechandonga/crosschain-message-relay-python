"""非法配置必须明确拒绝；同 nonce 不同内容必须拒绝；运行状态可经接口查询。"""
import pytest
from fastapi.testclient import TestClient

from relay.app import build_context
from relay.config import ChainConfig, RelayConfig
from relay.errors import ConfigError, RejectedByChainError
from tests.conftest import make_msg


@pytest.mark.parametrize("kwargs", [
    {"max_retries": -1},
    {"backoff_base_ms": 0},
    {"backoff_max_ms": 5, "backoff_base_ms": 50},
    {"delivery_timeout_ms": 0},
    {"max_pending": 0},
])
def test_invalid_config_rejected(kwargs):
    print(f"\n[输入] 非法配置 {kwargs}")
    with pytest.raises(ConfigError) as ei:
        RelayConfig(**kwargs).validate()
    print(f"[判定] ConfigError: {ei.value}")


def test_duplicate_chain_names_rejected():
    with pytest.raises(ConfigError):
        cfg = RelayConfig(chain_a=ChainConfig("X"), chain_b=ChainConfig("X"))
        cfg.validate()


def test_same_nonce_different_content_rejected(env):
    ab = env["AB"]
    ab.submit(make_msg(nonce=0, amount=100, payload="v1"))
    m_evil = make_msg(nonce=0, amount=99999, payload="v2")
    print("\n[输入] 同发送方同 nonce=0 但金额/内容不同的第二条消息")
    with pytest.raises(RejectedByChainError) as ei:
        ab.submit(m_evil)
    print(f"[判定] {type(ei.value).__name__}: {ei.value}")
    # 完全相同的消息由 message_id 去重路径拒绝（重复提交）
    with pytest.raises(Exception):
        ab.submit(make_msg(nonce=0, amount=100, payload="v1"))


def test_api_state_queryable_end_to_end():
    ctx = build_context(seed_balances={("A", "alice"): 1000})
    c = TestClient(ctx.app)
    r = c.post("/messages", json={
        "source": "A", "dest": "B", "sender": "alice",
        "recipient": "carol", "amount": 200, "nonce": 0,
    })
    mid = r.json()["message_id"]
    assert c.get(f"/messages/{mid}").json()["status"] == "submitted"
    c.post("/relayer/A/B/run")
    assert c.get(f"/messages/{mid}").json()["status"] == "observed"
    c.post("/chains/A/mine", json={"count": 2})
    c.post("/relayer/A/B/run")
    detail = c.get(f"/messages/{mid}").json()
    print(f"\n[判定] 接口查询消息 status={detail['status']} attempts={detail['attempts']}")
    assert detail["status"] == "delivered"
    state = c.get("/chains/B/state").json()
    assert state["ledger"]["carol"] == 200
    status = c.get("/relayer/status").json()
    print(f"[判定] 中继状态: {status}")
    assert "A->B" in status
    cons = c.get("/conservation").json()
    print(f"[判定] 守恒接口: {cons}")
    assert cons["system_total"] == 1000


def test_api_returns_distinguishable_error_types():
    ctx = build_context(seed_balances={("A", "alice"): 1000})
    c = TestClient(ctx.app)
    r1 = c.post("/messages", json={
        "source": "A", "dest": "B", "sender": "alice",
        "recipient": "carol", "amount": 2000, "nonce": 0,
    })
    print(f"\n[判定] 余额不足 HTTP {r1.status_code}: {r1.json()['detail']}")
    assert r1.json()["detail"]["error_type"] == "RejectedByChainError"
    r2 = c.post("/messages", json={
        "source": "A", "dest": "Z", "sender": "alice",
        "recipient": "carol", "amount": 1, "nonce": 0,
    })
    assert r2.json()["detail"]["error_type"] == "UnknownDirection"
