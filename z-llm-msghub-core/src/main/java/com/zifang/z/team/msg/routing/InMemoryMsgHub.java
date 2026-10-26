package com.zifang.z.team.msg.routing;

import com.zifang.util.core.lang.RandomUtil;
import com.zifang.z.team.msg.common.protocol.HubMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * 内存消息路由实现
 * <p>
 * 按 topic + sessionId 双维度精确匹配，本地进程内 pub/sub。
 */
public class InMemoryMsgHub implements MsgHub {

    private final String hubId;
    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();
    private final List<HubMessage> messageLog = new CopyOnWriteArrayList<>();

    public InMemoryMsgHub(String hubId) {
        this.hubId = hubId != null ? hubId
                : RandomUtil.uuidShort(8);
    }

    public InMemoryMsgHub() {
        this(null);
    }

    // ========== 订阅 / 发布 ==========

    @Override
    public void subscribe(String topic, String sessionId, Consumer<HubMessage> handler) {
        subscriptions.add(new Subscription(topic, sessionId, handler));
    }

    @Override
    public void publish(HubMessage message) {
        messageLog.add(message);
        dispatch(message);
    }

    private void dispatch(HubMessage message) {
        for (Subscription sub : subscriptions) {
            if (sub.topic.equals(message.getTopic()) && sub.sessionId.equals(message.getSessionId())) {
                try {
                    sub.handler.accept(message);
                } catch (RuntimeException ignored) {
                    // 单条订阅失败不阻断其余投递
                }
            }
        }
    }

    @Override
    public void destroy() {
        subscriptions.clear();
        messageLog.clear();
    }

    // ========== 访问器 ==========

    public String getHubId() {
        return hubId;
    }

    public int getSubscriptionCount() {
        return subscriptions.size();
    }

    public int getMessageLogCount() {
        return messageLog.size();
    }

    public List<HubMessage> getMessageLog() {
        return new ArrayList<>(messageLog);
    }

    // ========== 内部类 ==========

    private static class Subscription {
        final String topic;
        final String sessionId;
        final Consumer<HubMessage> handler;

        Subscription(String topic, String sessionId, Consumer<HubMessage> handler) {
            this.topic = topic != null ? topic : "";
            this.sessionId = sessionId != null ? sessionId : "";
            this.handler = handler;
        }
    }
}
