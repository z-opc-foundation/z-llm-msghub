package com.zifang.z.team.msg;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * msgHub 独立部署入口
 * <p>
 * 启动后暴露两个端口：
 * - HTTP (默认 9090): 监控面板 + Actuator 健康检查
 * - WebSocket (默认 9091): Agent / Observer / z-team 的消息长连接
 */
@SpringBootApplication
public class MsgHubApplication {

    public static void main(String[] args) {
        SpringApplication.run(MsgHubApplication.class, args);
    }
}
