#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""msgHub WebSocket E2E 测试（简化版，长超时）"""
import json
import os
import time
import urllib.request
import websocket

WS_URL = os.environ.get("Z_LLM_MSGHUB_WS_URL", "ws://localhost:8080/z-llm-msghub/msghub")
BASE_HTTP = os.environ.get("Z_LLM_MSGHUB_HTTP_BASE", "http://localhost:8080/z-llm-msghub")


def test():
    print("=" * 60)
    print("  msgHub E2E 测试")
    print("=" * 60)

    # 测试1: WebSocket 连接 + 注册 + 心跳
    print("\n=== 测试1: WebSocket 连接 + 注册 ===")
    print(f"  URL: {WS_URL}")
    try:
        ws = websocket.create_connection(WS_URL, timeout=10)
        print("  ✅ WebSocket 连接成功")

        register_msg = {
            "topic": "hub.register",
            "senderId": "test-agent-1",
            "content": json.dumps({"name": "Test Agent 1", "type": "AGENT"}),
            "timestamp": int(time.time() * 1000)
        }
        ws.send(json.dumps(register_msg))
        print(f"  SEND register: {register_msg['senderId']}")

        ws.settimeout(10)
        try:
            result = ws.recv()
            print(f"  RECV: {result}")
            resp = json.loads(result)
            if "hub.register" in resp.get("topic", ""):
                print("  ✅ 注册成功，收到 ACK")
            else:
                print(f"  ⚠️ 收到非预期响应: {resp.get('topic')}")
        except Exception as e:
            print(f"  ❌ 等待 ACK 超时: {e}")
    except Exception as e:
        print(f"  ❌ WebSocket 连接失败: {e}")
        return

    # 测试2: 心跳
    print("\n=== 测试2: 心跳保活 ===")
    heartbeat_msg = {
        "topic": "hub.heartbeat",
        "senderId": "test-agent-1",
        "content": "{}",
        "timestamp": int(time.time() * 1000)
    }
    ws.send(json.dumps(heartbeat_msg))
    print("  SEND heartbeat")
    time.sleep(1)
    print("  ✅ 心跳发送成功")

    # 测试3: 双 Agent 消息路由
    print("\n=== 测试3: 双 Agent 消息路由 ===")
    client_b = None
    try:
        client_b = websocket.create_connection(WS_URL, timeout=10)
        register_b = {
            "topic": "hub.register",
            "senderId": "agent-B",
            "content": json.dumps({"name": "Agent B", "type": "AGENT"}),
            "timestamp": int(time.time() * 1000)
        }
        client_b.send(json.dumps(register_b))
        ack_b = client_b.recv()
        print(f"  Agent-B 注册: {ack_b[:120]}")

        # A 发消息
        print("\n  Agent-A 发送 agent.message...")
        route_msg = {
            "topic": "agent.message",
            "senderId": "test-agent-1",
            "content": json.dumps({"msg": "Hello from A!"}),
            "timestamp": int(time.time() * 1000)
        }
        ws.send(json.dumps(route_msg))

        # B 收消息
        client_b.settimeout(5)
        try:
            recv_b = client_b.recv()
            print(f"  Agent-B 收到: {recv_b[:200]}")
            resp_b = json.loads(recv_b)
            if "agent.message" in resp_b.get("topic", ""):
                print("  ✅ 消息路由成功: A → B")
            else:
                print(f"  ⚠️ 收到非预期 topic: {resp_b.get('topic')}")
        except Exception as e:
            print(f"  ❌ Agent-B 未收到消息: {e}")

        # A 不应收到回环
        ws.settimeout(2)
        try:
            recv_a = ws.recv()
            print(f"  ⚠️ Agent-A 收到回环: {recv_a[:100]}")
        except:
            print("  ✅ Agent-A 未收到回环（无回环）")

    except Exception as e:
        print(f"  ❌ 路由测试失败: {e}")
    finally:
        if client_b:
            client_b.close()

    # 测试4: 验证监控 API
    print("\n=== 测试4: 监控 API（连接后） ===")
    try:
        resp = urllib.request.urlopen(f"{BASE_HTTP}/msghub/api/overview", timeout=5)
        data = json.loads(resp.read())
        print(f"  overview: {json.dumps(data, indent=2)}")
        resp2 = urllib.request.urlopen(f"{BASE_HTTP}/msghub/api/stats/type", timeout=5)
        data2 = json.loads(resp2.read())
        print(f"  stats/type: {json.dumps(data2, indent=2)}")
        resp3 = urllib.request.urlopen(f"{BASE_HTTP}/msghub/api/connections", timeout=5)
        data3 = json.loads(resp3.read())
        print(f"  connections: {json.dumps(data3, indent=2)[:300]}")
    except Exception as e:
        print(f"  ❌ 监控 API 查询失败: {e}")

    ws.close()
    print("\n" + "=" * 60)
    print("  测试完成")
    print("=" * 60)


if __name__ == "__main__":
    test()
