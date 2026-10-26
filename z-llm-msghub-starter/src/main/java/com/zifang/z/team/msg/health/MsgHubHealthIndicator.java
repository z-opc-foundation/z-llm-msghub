package com.zifang.z.team.msg.health;

import com.zifang.z.team.msg.server.ConnectionManager;
import com.zifang.z.team.msg.server.MsgHubServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * msgHub 自定义健康指标
 * <p>
 * Spring Boot Actuator /actuator/health 会聚合此指标。
 * 当 Netty Server 未运行时标记为 DOWN。
 */
@Component
public class MsgHubHealthIndicator implements HealthIndicator {

    @Autowired
    private MsgHubServer msgHubServer;
    @Autowired
    private ConnectionManager connManager;

    @Override
    public Health health() {
        Health.Builder builder;
        if (msgHubServer.isRunning()) {
            builder = Health.up();
        } else {
            builder = Health.down();
        }
        builder.withDetail("websocketPort", msgHubServer.isRunning() ? "listening" : "stopped");
        builder.withDetail("onlineAgents", connManager.getOnlineCount());
        builder.withDetail("routedMessages", connManager.getRoutedCount());
        return builder.build();
    }
}
