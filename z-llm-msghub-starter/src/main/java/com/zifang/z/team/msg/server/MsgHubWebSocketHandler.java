package com.zifang.z.team.msg.server;

import com.zifang.util.json.JsonUtil;
import com.zifang.util.json.model.JsonObject;
import com.zifang.z.team.msg.common.protocol.HubMessage;
import com.zifang.z.team.msg.common.protocol.MsgJson;
import com.zifang.z.team.msg.common.protocol.Topics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * msgHub WebSocket 消息处理核心（Spring 侧）
 * <p>
 * 处理 WebSocket 文本消息（JSON）：
 * - hub.subscribe: 客户端订阅 topic + sessionId
 * - hub.heartbeat: 心跳
 * - 其他 topic: 路由转发（按 topic + sessionId 匹配订阅）
 * <p>
 * 替代原 Netty MsgHubHandler，逻辑完全一致，仅传输层从 TCP 切换为 WebSocket。
 */
public class MsgHubWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LogManager.getLogger(MsgHubWebSocketHandler.class);

    private final ConnectionManager connManager;

    public MsgHubWebSocketHandler(ConnectionManager connManager) {
        this.connManager = connManager;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String channelId = session.getId();
        connManager.onConnect(channelId, new WebSocketHubSession(session));
        log.debug("WebSocket connection established: {}", session.getRemoteAddress());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        String text = message.getPayload();
        String channelId = session.getId();

        HubMessage msg;
        try {
            msg = MsgJson.fromJson(text, HubMessage.class);
        } catch (Exception e) {
            log.warn("无法解析消息: {}", text.length() > 200 ? text.substring(0, 200) : text, e);
            return;
        }

        if (msg.getTopic() == null || msg.getTopic().isEmpty()) {
            log.warn("消息缺少 topic，忽略");
            return;
        }

        switch (msg.getTopic()) {
            case Topics.HUB_SUBSCRIBE:
                handleSubscribe(channelId, msg);
                break;
            case Topics.HUB_HEARTBEAT:
                connManager.refreshHeartbeat(channelId);
                break;
            default:
                connManager.route(msg, channelId);
                break;
        }
    }

    private void handleSubscribe(String channelId, HubMessage msg) {
        try {
            JsonObject subInfo = JsonUtil.parseObject(msg.getPayload());
            String topic = subInfo.getString("topic");
            String sessionId = subInfo.getString("sessionId");

            if (topic == null || sessionId == null) {
                log.warn("订阅消息缺少 topic 或 sessionId，忽略");
                return;
            }

            connManager.addSubscription(channelId, topic, sessionId);
            log.info("Subscription added: channel={}, topic={}, sessionId={}", channelId, topic, sessionId);
        } catch (Exception e) {
            log.warn("解析订阅消息失败: {}", e.getMessage());
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        String channelId = session.getId();
        connManager.onDisconnect(channelId);
        log.debug("WebSocket connection closed: {}, status: {}", session.getRemoteAddress(), status);
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        String channelId = session.getId();
        log.error("WebSocket transport error from {}: {}", session.getRemoteAddress(), exception.getMessage());
        connManager.onDisconnect(channelId);
    }
}
