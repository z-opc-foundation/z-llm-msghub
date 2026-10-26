package com.zifang.z.team.msg.session;

/**
 * HubSession — 统一会话抽象
 * <p>
 * 无论底层是 Netty Channel 还是 Spring WebSocketSession，
 * ConnectionManager 只通过此接口操作连接。
 */
public interface HubSession {

    /**
     * 会话唯一 ID
     */
    String getId();

    /**
     * 发送文本消息
     */
    void sendText(String text);

    /**
     * 连接是否活跃
     */
    boolean isActive();

    /**
     * 关闭连接
     */
    void close();

    /**
     * 远端地址（日志用）
     */
    String getRemoteAddr();
}
