package com.zifang.z.team.msg.routing;

import com.zifang.z.team.msg.common.protocol.HubMessage;

import java.util.function.Consumer;

/**
 * 订阅记录
 * <p>
 * 按 topic + sessionId 双维度匹配消息。
 */
public final class HubSubscription {

    final String topic;
    final String sessionId;
    final Consumer<HubMessage> handler;

    public HubSubscription(String topic, String sessionId, Consumer<HubMessage> handler) {
        this.topic = topic != null ? topic : "";
        this.sessionId = sessionId != null ? sessionId : "";
        this.handler = handler;
    }

    /**
     * 匹配消息（topic + sessionId 双维度精确匹配）
     */
    public boolean matches(HubMessage message) {
        return topic.equals(message.getTopic()) && sessionId.equals(message.getSessionId());
    }
}
