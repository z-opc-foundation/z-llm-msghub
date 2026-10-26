package com.zifang.z.team.msg.client;

import com.zifang.z.team.msg.common.protocol.AgentPayload;
import com.zifang.z.team.msg.common.protocol.HubMessage;

import java.util.function.Consumer;

/**
 * msgHub 客户端接口（哑管道）
 * <p>
 * Agent 侧通过此接口连接 msgHub Server，实现消息收发。
 * 不绑定身份、无 register、无 type。
 * 会话隔离由 subscribe/publish 的 topic + sessionId 参数承担。
 * <p>
 * 使用方式：
 * <pre>
 *   MsgHubClient client = MsgHubClientFactory.remote("msghub:9091");
 *   client.connect();
 *   client.subscribe(Topics.AGENT_MSG, "task-001", msg -> { ... });
 *   client.publish(Topics.AGENT_MSG, "task-001",
 *       AgentPayload.of("agent-A", "hello", "signal"));
 * </pre>
 */
public interface MsgHubClient {

    /**
     * 连接 msgHub Server（阻塞直到连接成功或失败）
     */
    void connect();

    /**
     * 订阅 topic + sessionId 的消息
     *
     * @param topic     消息 topic（如 Topics.AGENT_MSG）
     * @param sessionId 会话 ID（必填，不支持 null）
     * @param handler   消息回调
     */
    void subscribe(String topic, String sessionId, Consumer<HubMessage> handler);

    /**
     * 发布消息
     *
     * @param topic     消息 topic
     * @param sessionId 会话 ID
     * @param payload   固定结构的 Payload（如 AgentPayload）
     */
    void publish(String topic, String sessionId, AgentPayload payload);

    /**
     * 是否已连接
     */
    boolean isConnected();

    /**
     * 关闭客户端（释放所有资源）
     */
    void close();
}
