package com.zifang.z.team.msg.routing;

import com.zifang.z.team.msg.common.protocol.HubMessage;

import java.util.function.Consumer;

/**
 * 消息路由接口（内存模式）
 * <p>
 * 本地进程内 pub/sub 路由，按 topic + sessionId 双维度匹配。
 * 不依赖网络，用于单 JVM 内的消息分发。
 */
public interface MsgHub {

    /**
     * 订阅 topic + sessionId 的消息
     *
     * @param topic     消息 topic
     * @param sessionId 会话 ID
     * @param handler   消息回调
     */
    void subscribe(String topic, String sessionId, Consumer<HubMessage> handler);

    /**
     * 发布消息（本地 dispatch）
     *
     * @param message 消息体
     */
    void publish(HubMessage message);

    /**
     * 销毁：清除订阅和消息日志
     */
    void destroy();
}
