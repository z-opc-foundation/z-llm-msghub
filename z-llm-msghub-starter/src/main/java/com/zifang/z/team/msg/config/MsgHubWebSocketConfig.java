package com.zifang.z.team.msg.config;

import com.zifang.z.team.msg.server.ConnectionManager;
import com.zifang.z.team.msg.server.MsgHubWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * MsgHub WebSocket 端点注册
 * <p>
 * 在 Spring Boot HTTP 端口上注册 WebSocket handler，
 * 路径：/msghub/ws
 * <p>
 * 与 z-team 后端的 TeamWebSocketConfig 完全相同的模式，
 * nginx 天然支持转发（Upgrade 机制）。
 */
@Configuration
@EnableWebSocket
public class MsgHubWebSocketConfig implements WebSocketConfigurer {

    private final ConnectionManager connManager;

    public MsgHubWebSocketConfig(ConnectionManager connManager) {
        this.connManager = connManager;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(new MsgHubWebSocketHandler(connManager), "/msghub/ws")
                .setAllowedOrigins("*");
    }
}
