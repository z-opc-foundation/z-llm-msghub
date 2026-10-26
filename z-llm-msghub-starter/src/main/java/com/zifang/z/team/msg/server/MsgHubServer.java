package com.zifang.z.team.msg.server;

import com.zifang.z.team.msg.config.MsgHubProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * MsgHub Server 状态追踪器
 * <p>
 * 原 Netty TCP Server 已替换为 Spring WebSocket（MsgHubWebSocketConfig），
 * 此类仅保留 running 标志供 HealthIndicator / MonitorController 查询。
 */
public class MsgHubServer {

    private static final Logger log = LogManager.getLogger(MsgHubServer.class);

    private final MsgHubProperties props;
    private volatile boolean running = false;

    public MsgHubServer(MsgHubProperties props, ConnectionManager connManager) {
        this.props = props;
    }

    public void start() {
        running = true;
        log.info("msgHub WebSocket endpoint ready at /msghub/ws (Spring Boot HTTP port)");
    }

    public void shutdown() {
        running = false;
        log.info("msgHub WebSocket endpoint stopped.");
    }

    public boolean isRunning() {
        return running;
    }
}
