package com.zifang.z.team.msg.common.protocol;

/**
 * 消息 Topic 常量
 * <p>
 * 当前收敛为唯一业务 topic: AgentMsg。
 * 每个 Topic 绑定唯一的 Payload 类型（如 AGENT_MSG → AgentPayload）。
 * 未来可扩展新 Topic，需同步定义对应 Payload 类。
 */
public final class Topics {

    /**
     * 业务 topic：Agent 间消息
     */
    public static final String AGENT_MSG = "AgentMsg";

    /**
     * 内部控制：订阅命令（client → server）
     */
    public static final String HUB_SUBSCRIBE = "hub.subscribe";

    /**
     * 内部控制：心跳（client → server）
     */
    public static final String HUB_HEARTBEAT = "hub.heartbeat";

    private Topics() {
    }
}
