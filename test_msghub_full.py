#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""msgHub 全方位 E2E 测试"""
import json
import os
import threading
import time
import urllib.error
import urllib.request
import websocket

WS_URL = os.environ.get("Z_LLM_MSGHUB_WS_URL", "ws://localhost:8080/z-llm-msghub/msghub")
BASE_HTTP = os.environ.get("Z_LLM_MSGHUB_HTTP_BASE", "http://localhost:8080/z-llm-msghub")
PASS = 0
FAIL = 0
SKIP = 0


def ok(msg):
    global PASS
    PASS += 1
    print(f"  ✅ {msg}")


def fail(msg):
    global FAIL
    FAIL += 1
    print(f"  ❌ {msg}")


def skip(msg):
    global SKIP
    SKIP += 1
    print(f"  ⏭️ {msg}")


def http_get(path):
    try:
        resp = urllib.request.urlopen(f"{BASE_HTTP}{path}", timeout=5)
        return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception as e:
        return -1, str(e)


def http_delete(path):
    try:
        req = urllib.request.Request(f"{BASE_HTTP}{path}", method="DELETE")
        resp = urllib.request.urlopen(req, timeout=5)
        return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception as e:
        return -1, str(e)


def ws_connect(agent_id, agent_type="AGENT"):
    """连接 + 注册，返回 ws 对象"""
    ws = websocket.create_connection(WS_URL, timeout=10)
    reg = {
        "topic": "hub.register",
        "senderId": agent_id,
        "payload": {"name": agent_id, "type": agent_type},
        "timestamp": int(time.time() * 1000)
    }
    ws.send(json.dumps(reg))
    ack = ws.recv()
    resp = json.loads(ack)
    if "hub.register" in resp.get("topic", "") and resp.get("content") == "ok":
        return ws
    return None


def send_msg(ws, topic, sender_id, content="{}"):
    msg = {
        "topic": topic,
        "senderId": sender_id,
        "content": content,
        "timestamp": int(time.time() * 1000)
    }
    ws.send(json.dumps(msg))


def recv_msg(ws, timeout=5):
    ws.settimeout(timeout)
    try:
        return json.loads(ws.recv())
    except:
        return None


# ========== 测试用例 ==========

def test_http_endpoints():
    """1. HTTP 端点全覆盖"""
    print("\n{'='*60}")
    print("  1. HTTP 端点全覆盖")
    print("{'='*60}")

    # Swagger（返回 HTML，不解析 JSON）
    try:
        resp = urllib.request.urlopen(f"{BASE_HTTP}/doc.html", timeout=5)
        if resp.status == 200:
            ok("Swagger doc.html → 200")
        else:
            fail(f"Swagger doc.html → {resp.status}")
    except Exception as e:
        fail(f"Swagger doc.html → {e}")

    # Actuator health
    code, data = http_get("/actuator/health")
    if code == 200 and data.get("status") == "UP":
        ok("Actuator health → UP")
    else:
        fail(f"Actuator health → {code} {data}")

    # Actuator info
    code, _ = http_get("/actuator/info")
    if code == 200:
        ok("Actuator info → 200")
    else:
        fail(f"Actuator info → {code}")

    # Actuator env
    code, data = http_get("/actuator/env")
    if code == 200 and "propertySources" in data:
        ok("Actuator env → 200")
    else:
        fail(f"Actuator env → {code}")

    # Actuator metrics
    code, data = http_get("/actuator/metrics")
    if code == 200 and "names" in data:
        ok("Actuator metrics → 200")
    else:
        fail(f"Actuator metrics → {code}")

    # Monitor: overview
    code, data = http_get("/msghub/api/overview")
    if code == 200 and data.get("serverStatus") == "RUNNING":
        ok(f"Monitor overview → RUNNING (agents={data.get('onlineAgents')})")
    else:
        fail(f"Monitor overview → {code}")

    # Monitor: config
    code, data = http_get("/msghub/api/config")
    if code == 200 and data.get("enabled") == True:
        ok(f"Monitor config → enabled, ws.port={data.get('websocket', {}).get('port')}")
    else:
        fail(f"Monitor config → {code}")

    # Monitor: connections
    code, data = http_get("/msghub/api/connections")
    if code == 200 and isinstance(data, list):
        ok(f"Monitor connections → {len(data)} 个连接")
    else:
        fail(f"Monitor connections → {code}")

    # Monitor: stats/type
    code, data = http_get("/msghub/api/stats/type")
    if code == 200 and isinstance(data, dict):
        ok(f"Monitor stats/type → {data}")
    else:
        fail(f"Monitor stats/type → {code}")

    # 404 测试
    code, _ = http_get("/msghub/api/nonexistent")
    if code == 404:
        ok("不存在端点 → 404")
    else:
        fail(f"不存在端点 → {code}")


def test_ws_basic():
    """2. WebSocket 基础流程"""
    print("\n{'='*60}")
    print("  2. WebSocket 基础流程")
    print("{'='*60}")

    ws = ws_connect("agent-basic", "AGENT")
    if ws:
        ok("Agent agent-basic 注册成功")
    else:
        fail("agent-basic 注册失败")
        return

    # 心跳
    send_msg(ws, "hub.heartbeat", "agent-basic", "{}")
    time.sleep(0.5)
    ok("心跳发送成功")

    # 验证监控显示在线
    code, data = http_get("/msghub/api/overview")
    if data and data.get("onlineAgents", 0) >= 1:
        ok(f"监控显示在线 agents={data.get('onlineAgents')}")
    else:
        fail(f"监控在线数异常: {data}")

    # 验证 connections 列表
    code, data = http_get("/msghub/api/connections")
    found = any(c.get("agentId") == "agent-basic" for c in data) if data else False
    if found:
        conn = [c for c in data if c.get("agentId") == "agent-basic"][0]
        ok(f"connections 列表含 agent-basic (type={conn.get('type')}, remoteAddr={conn.get('remoteAddr')})")
    else:
        fail("connections 列表未找到 agent-basic")

    ws.close()
    time.sleep(1)

    # 验证断开后清理
    code, data = http_get("/msghub/api/connections")
    found = any(c.get("agentId") == "agent-basic" for c in data) if data else False
    if not found:
        ok("断开后 connections 列表已清理")
    else:
        fail("断开后 connections 列表仍有 agent-basic")


def test_ws_routing():
    """3. 消息路由（多 Agent + 多类型）"""
    print("\n{'='*60}")
    print("  3. 消息路由（多 Agent + 多类型）")
    print("{'='*60}")

    agents = {}
    agent_specs = [
        ("agent-A", "AGENT"),
        ("agent-B", "AGENT"),
        ("observer-1", "OBSERVER"),
        ("ft-1", "FT"),
    ]

    for agent_id, agent_type in agent_specs:
        ws = ws_connect(agent_id, agent_type)
        if ws:
            agents[agent_id] = ws
            ok(f"{agent_id} ({agent_type}) 注册成功")
        else:
            fail(f"{agent_id} ({agent_type}) 注册失败")

    # 验证 stats/type
    code, data = http_get("/msghub/api/stats/type")
    expected = {"AGENT": 2, "OBSERVER": 1, "FT": 1}
    if data == expected:
        ok(f"stats/type 正确: {data}")
    else:
        fail(f"stats/type 期望 {expected} 实际 {data}")

    # A 发消息，B/observer/ft 都应收到
    send_msg(agents["agent-A"], "agent.message", "agent-A", json.dumps({"msg": "broadcast test"}))
    time.sleep(1)

    for agent_id in ["agent-B", "observer-1", "ft-1"]:
        msg = recv_msg(agents[agent_id], timeout=3)
        if msg and "agent.message" in msg.get("topic", ""):
            ok(f"{agent_id} 收到 A 的消息")
        else:
            fail(f"{agent_id} 未收到消息: {msg}")

    # A 不应收到回环
    msg = recv_msg(agents["agent-A"], timeout=2)
    if msg is None:
        ok("Agent-A 无回环")
    else:
        fail(f"Agent-A 收到回环: {msg}")

    # B 回复消息
    send_msg(agents["agent-B"], "agent.message", "agent-B", json.dumps({"msg": "reply from B"}))
    time.sleep(1)

    # A 应收到 B 的回复
    msg = recv_msg(agents["agent-A"], timeout=3)
    if msg and "agent.message" in msg.get("topic", ""):
        ok("Agent-A 收到 B 的回复")
    else:
        fail(f"Agent-A 未收到 B 的回复: {msg}")

    # B 不应收到自己的
    msg = recv_msg(agents["agent-B"], timeout=2)
    if msg is None:
        ok("Agent-B 无回环")
    else:
        fail(f"Agent-B 收到回环: {msg}")

    # 验证 routedMessages 计数
    code, data = http_get("/msghub/api/overview")
    routed = int(data.get("routedMessages", 0)) if data else 0
    if routed >= 2:
        ok(f"routedMessages={routed} (>=2)")
    else:
        fail(f"routedMessages={routed} (期望 >=2)")

    for ws in agents.values():
        ws.close()
    time.sleep(1)


def test_kick_agent():
    """4. 手动踢出 Agent"""
    print("\n{'='*60}")
    print("  4. 手动踢出 Agent (DELETE API)")
    print("{'='*60}")

    ws = ws_connect("agent-kick", "AGENT")
    if ws:
        ok("agent-kick 注册成功")
    else:
        fail("agent-kick 注册失败")
        return

    # 踢出
    code, data = http_delete("/msghub/api/connections/agent-kick")
    if code == 200 and data.get("success") == True:
        ok(f"DELETE kick → success: {data.get('message')}")
    else:
        fail(f"DELETE kick → {code} {data}")

    # 验证已踢出
    time.sleep(1)
    code, data = http_get("/msghub/api/connections")
    found = any(c.get("agentId") == "agent-kick" for c in data) if data else False
    if not found:
        ok("踢出后 connections 不含 agent-kick")
    else:
        fail("踢出后 connections 仍有 agent-kick")

    # 再踢一次（不存在的）
    code, data = http_delete("/msghub/api/connections/agent-nonexistent")
    if code == 200 and data.get("success") == False:
        ok(f"踢出不存在的 Agent → success=false: {data.get('message')}")
    else:
        fail(f"踢出不存在的 Agent → {code} {data}")


def test_invalid_messages():
    """5. 非法消息处理"""
    print("\n{'='*60}")
    print("  5. 非法消息处理")
    print("{'='*60}")

    ws = websocket.create_connection(WS_URL, timeout=10)
    ws = ws_connect("agent-robust", "AGENT") if ws else None
    if not ws:
        fail("agent-robust 注册失败")
        return

    # 发非法 JSON
    ws.send("not a json")
    time.sleep(0.5)
    ok("发送非法 JSON → 服务未崩溃")

    # 发空 topic
    send_msg(ws, "", "agent-robust", "{}")
    time.sleep(0.5)
    ok("发送空 topic → 服务未崩溃")

    # 发大消息
    big_content = json.dumps({"msg": "x" * 10000})
    send_msg(ws, "agent.message", "agent-robust", big_content)
    time.sleep(0.5)
    ok("发送大消息(10KB) → 服务未崩溃")

    # 验证仍在线
    code, data = http_get("/msghub/api/connections")
    found = any(c.get("agentId") == "agent-robust" for c in data) if data else False
    if found:
        ok("非法消息后 agent-robust 仍在线")
    else:
        fail("非法消息后 agent-robust 已掉线")

    ws.close()


def test_reconnect():
    """6. 断线重连"""
    print("\n{'='*60}")
    print("  6. 断线重连")
    print("{'='*60}")

    # 第一次连接
    ws1 = ws_connect("agent-reconnect", "AGENT")
    if ws1:
        ok("首次连接成功")
        ws1.close()
        time.sleep(1)

        # 用相同 ID 重连
        ws2 = ws_connect("agent-reconnect", "AGENT")
        if ws2:
            ok("相同 agentId 重连成功")

            # 验证只有一个实例
            code, data = http_get("/msghub/api/connections")
            count = sum(1 for c in data if c.get("agentId") == "agent-reconnect") if data else 0
            if count == 1:
                ok(f"重连后 connections 中只有 1 个 agent-reconnect")
            else:
                fail(f"重连后 connections 中有 {count} 个 agent-reconnect（应只有1个）")

            ws2.close()
        else:
            fail("重连失败")
    else:
        fail("首次连接失败")


def test_summary():
    """汇总"""
    print("\n" + "=" * 60)
    print(f"  测试汇总: ✅{PASS} ❌{FAIL} ⏭️{SKIP}")
    print("=" * 60)


if __name__ == "__main__":
    print("=" * 60)
    print("  msgHub 全方位 E2E 测试")
    print(f"  目标: {WS_URL}")
    print("=" * 60)

    test_http_endpoints()
    test_ws_basic()
    test_ws_routing()
    test_kick_agent()
    test_invalid_messages()
    test_reconnect()
    test_summary()
