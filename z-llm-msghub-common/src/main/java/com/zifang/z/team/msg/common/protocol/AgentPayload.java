package com.zifang.z.team.msg.common.protocol;

/**
 * AgentPayload — AgentMsg topic 的固定 Payload 类型
 * <p>
 * 每个 Topic 绑定唯一 Payload 类型。Topics.AGENT_MSG → AgentPayload。
 * 接收方依据 topic 反序列化为对应 Payload 类。
 */
public class AgentPayload {

    /**
     * 谁发的
     */
    private String from;

    /**
     * 发什么
     */
    private String content;

    /**
     * 做什么（signal / completed / error / ...）
     */
    private String action;

    public AgentPayload() {
    }

    public static AgentPayload of(String from, String content, String action) {
        AgentPayload p = new AgentPayload();
        p.from = from != null ? from : "";
        p.content = content != null ? content : "";
        p.action = action != null ? action : "";
        return p;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from != null ? from : "";
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content != null ? content : "";
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action != null ? action : "";
    }

    @Override
    public String toString() {
        return "AgentPayload{from='" + from + "', content='" + content + "', action='" + action + "'}";
    }
}
