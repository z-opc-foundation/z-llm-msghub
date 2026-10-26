package com.zifang.z.team.msg.server;

import com.zifang.util.json.JsonUtil;
import com.zifang.z.team.msg.common.protocol.AgentPayload;
import com.zifang.z.team.msg.common.protocol.HubMessage;
import com.zifang.z.team.msg.common.protocol.Topics;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * MsgHub WebSocket 路由语义测试（替代已废弃的 Netty TCP E2E）。
 * <p>
 * 原 MsgHubE2ETest / MsgHubTcpStressTest 测的是已被 Spring WebSocket 替代的
 * Netty TCP 链路（MessageHubServer 已改为状态追踪器），直接起 TCP 端口必然
 * Connection refused。这里用 fake WebSocketSession 直接驱动
 * {@link MsgHubWebSocketHandler}，保真复刻原 E2E 的核心路由语义：
 * <pre>
 *   1. 同 topic + sessionId 订阅者互转，发送方不收自己的消息
 *   2. 不同 sessionId 互不干扰
 *   3. 断开连接 → ConnectionManager 自动清理
 *   4. hub.heartbeat 不产生副作用异常
 * </pre>
 */
public class MsgHubWebSocketRouteTest {

    private ConnectionManager connManager;
    private MsgHubWebSocketHandler handler;

    @BeforeEach
    public void setUp() {
        connManager = new ConnectionManager();
        handler = new MsgHubWebSocketHandler(connManager);
    }

    @Test
    public void testMessageForwarding() throws Exception {
        List<String> inboxA = new CopyOnWriteArrayList<>();
        List<String> inboxB = new CopyOnWriteArrayList<>();
        WebSocketSession wsA = connect("chan-A", inboxA);
        WebSocketSession wsB = connect("chan-B", inboxB);

        subscribe(wsA, Topics.AGENT_MSG, "task-001");
        subscribe(wsB, Topics.AGENT_MSG, "task-001");

        AgentPayload payload = AgentPayload.of("agent-A", "hello from A", "signal");
        HubMessage msg = HubMessage.of(Topics.AGENT_MSG, "task-001", JsonUtil.toJson(payload));
        handler.handleTextMessage(wsA, new TextMessage(JsonUtil.toJson(msg)));

        Assertions.assertEquals(inboxB.size(), 1, "B should receive the message");
        HubMessage receivedMsg = JsonUtil.fromJson(inboxB.get(0), HubMessage.class);
        Assertions.assertEquals(receivedMsg.getTopic(), Topics.AGENT_MSG);
        Assertions.assertEquals(receivedMsg.getSessionId(), "task-001");
        AgentPayload receivedPayload = JsonUtil.fromJson(receivedMsg.getPayload(), AgentPayload.class);
        Assertions.assertEquals(receivedPayload.getFrom(), "agent-A");
        Assertions.assertEquals(receivedPayload.getContent(), "hello from A");

        Assertions.assertEquals(inboxA.size(), 0, "A should not receive its own published message");
    }

    @Test
    public void testSessionIsolation() throws Exception {
        List<String> inboxA = new CopyOnWriteArrayList<>();
        List<String> inboxB = new CopyOnWriteArrayList<>();
        WebSocketSession wsA = connect("iso-A", inboxA);
        WebSocketSession wsB = connect("iso-B", inboxB);

        subscribe(wsA, Topics.AGENT_MSG, "session-1");
        subscribe(wsB, Topics.AGENT_MSG, "session-2");

        HubMessage msg1 = HubMessage.of(Topics.AGENT_MSG, "session-1",
                JsonUtil.toJson(AgentPayload.of("agent-A", "msg for session-1", "signal")));
        handler.handleTextMessage(wsA, new TextMessage(JsonUtil.toJson(msg1)));

        Assertions.assertEquals(inboxB.size(), 0, "B should NOT receive message from a different session");

        HubMessage msg2 = HubMessage.of(Topics.AGENT_MSG, "session-2",
                JsonUtil.toJson(AgentPayload.of("agent-A", "msg for session-2", "signal")));
        handler.handleTextMessage(wsA, new TextMessage(JsonUtil.toJson(msg2)));

        Assertions.assertEquals(inboxB.size(), 1, "B should receive message for session-2");
        Assertions.assertEquals(JsonUtil.fromJson(inboxB.get(0), HubMessage.class).getSessionId(), "session-2");
    }

    @Test
    public void testDisconnectCleanup() throws Exception {
        List<String> inbox = new CopyOnWriteArrayList<>();
        WebSocketSession ws = connect("cleanup-1", inbox);
        subscribe(ws, Topics.AGENT_MSG, "cleanup-test");

        Assertions.assertEquals(connManager.getOnlineCount(), 1);

        handler.afterConnectionClosed(ws, org.springframework.web.socket.CloseStatus.NORMAL);
        Assertions.assertEquals(connManager.getOnlineCount(), 0,
                "ConnectionManager should clean up after disconnect");
    }

    @Test
    public void testHeartbeatNoSideEffect() throws Exception {
        List<String> inbox = new CopyOnWriteArrayList<>();
        WebSocketSession ws = connect("hb-1", inbox);
        subscribe(ws, Topics.AGENT_MSG, "hb-test");

        HubMessage hb = HubMessage.of(Topics.HUB_HEARTBEAT, "hb-test", "{}");
        handler.handleTextMessage(ws, new TextMessage(JsonUtil.toJson(hb)));

        Assertions.assertEquals(connManager.getOnlineCount(), 1, "heartbeat should keep the connection");
        Assertions.assertEquals(inbox.size(), 0, "heartbeat should not route any message");
    }

    // ========== 工具方法 ==========

    private WebSocketSession connect(String id, List<String> inbox) {
        WebSocketSession ws = fakeWsSession(id, inbox);
        handler.afterConnectionEstablished(ws);
        return ws;
    }

    private void subscribe(WebSocketSession ws, String topic, String sessionId) throws Exception {
        com.zifang.util.json.model.JsonObject subInfo = new com.zifang.util.json.model.JsonObject();
        subInfo.put("topic", topic);
        subInfo.put("sessionId", sessionId);
        HubMessage msg = HubMessage.of(Topics.HUB_SUBSCRIBE, sessionId, JsonUtil.toJson(subInfo));
        handler.handleTextMessage(ws, new TextMessage(JsonUtil.toJson(msg)));
    }

    /** fake Spring WebSocketSession：getId 返回指定 id，sendMessage 捕获进 inbox。 */
    private static WebSocketSession fakeWsSession(String id, List<String> inbox) {
        return (WebSocketSession) Proxy.newProxyInstance(
                MsgHubWebSocketRouteTest.class.getClassLoader(),
                new Class<?>[]{WebSocketSession.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getId":
                            return id;
                        case "sendMessage":
                            inbox.add(((TextMessage) args[0]).getPayload());
                            return null;
                        case "isOpen":
                            return true;
                        default:
                            Class<?> rt = method.getReturnType();
                            if (rt == boolean.class) {
                                return false;
                            }
                            if (rt == int.class) {
                                return 0;
                            }
                            if (rt == long.class) {
                                return 0L;
                            }
                            return null;
                    }
                });
    }
}
