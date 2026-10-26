package com.zifang.z.team.msg.common.protocol;

import com.zifang.util.core.lang.RandomUtil;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 统一消息协议体
 * <p>
 * 作为 msgHub 的网络传输消息，同时也是内存路由的消息体。
 * 放在 msg-common 模块，供 msg-core（server）和 msg-client（agent）共享。
 * <p>
 * payload 为 JSON 字符串，按 topic 绑定的 Payload 类型反序列化
 * （如 Topics.AGENT_MSG → AgentPayload）。
 */
public class HubMessage {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private String messageId;
    private String sessionId;
    private String topic;
    private String payload;
    private String createdAt;

    public HubMessage() {
        this.messageId = RandomUtil.uuidCompact();
        this.payload = "";
        this.createdAt = LocalDateTime.now().format(FORMATTER);
    }

    // ========== 快捷构造 ==========

    public static HubMessage of(String topic, String sessionId, String payload) {
        HubMessage msg = new HubMessage();
        msg.topic = topic != null ? topic : "";
        msg.sessionId = sessionId != null ? sessionId : "";
        msg.payload = payload != null ? payload : "";
        return msg;
    }

    // ========== Getters / Setters ==========

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId != null ? messageId : "";
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId != null ? sessionId : "";
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic != null ? topic : "";
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload != null ? payload : "";
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt != null ? createdAt : LocalDateTime.now().format(FORMATTER);
    }

    /**
     * 获取 createdAt 作为 LocalDateTime 对象
     */
    public LocalDateTime getCreatedAtAsLocalDateTime() {
        if (createdAt == null || createdAt.isEmpty()) {
            return LocalDateTime.now();
        }
        try {
            return LocalDateTime.parse(createdAt, FORMATTER);
        } catch (Exception e) {
            return LocalDateTime.now();
        }
    }

    /**
     * 设置 createdAt 从 LocalDateTime 对象
     */
    public void setCreatedAtFromLocalDateTime(LocalDateTime dateTime) {
        this.createdAt = dateTime != null ? dateTime.format(FORMATTER) : LocalDateTime.now().format(FORMATTER);
    }

    // ========== 工具 ==========

    public HubMessage copy() {
        HubMessage c = new HubMessage();
        c.messageId = this.messageId;
        c.sessionId = this.sessionId;
        c.topic = this.topic;
        c.payload = this.payload;
        c.createdAt = this.createdAt;
        return c;
    }

    @Override
    public String toString() {
        return "HubMessage{topic='" + topic + "', sessionId='" + sessionId + "'}";
    }
}
