package com.zifang.z.team.msg.server;

import com.zifang.z.team.msg.session.HubSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

/**
 * Spring WebSocketSession → HubSession 适配器
 * <p>
 * 替代 NettyHubSession，ConnectionManager 通过此接口操作 WebSocket 连接。
 */
public class WebSocketHubSession implements HubSession {

    private static final Logger log = LogManager.getLogger(WebSocketHubSession.class);

    private final WebSocketSession session;

    public WebSocketHubSession(WebSocketSession session) {
        this.session = session;
    }

    @Override
    public String getId() {
        return session.getId();
    }

    @Override
    public void sendText(String text) {
        try {
            synchronized (session) {
                if (session.isOpen()) {
                    session.sendMessage(new TextMessage(text));
                }
            }
        } catch (Exception e) {
            log.warn("WebSocket send failed: {}", e.getMessage());
        }
    }

    @Override
    public boolean isActive() {
        return session.isOpen();
    }

    @Override
    public void close() {
        try {
            session.close();
        } catch (Exception ignored) {
        }
    }

    @Override
    public String getRemoteAddr() {
        try {
            return session.getRemoteAddress() != null ? session.getRemoteAddress().toString() : "unknown";
        } catch (Exception e) {
            return "unknown";
        }
    }

    public WebSocketSession getSession() {
        return session;
    }
}
