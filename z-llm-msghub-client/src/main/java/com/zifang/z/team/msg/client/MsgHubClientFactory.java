package com.zifang.z.team.msg.client;

/**
 * msgHub 客户端工厂
 * <p>
 * 使用方式：
 * <pre>
 *   MsgHubClient client = MsgHubClientFactory.remote("ws://msghub:9090/msghub/ws");
 * </pre>
 */
public final class MsgHubClientFactory {

    private MsgHubClientFactory() {
    }

    /**
     * 创建远端 WebSocket 客户端
     *
     * @param url WebSocket 地址，如 ws://host:port/path
     */
    public static MsgHubClient remote(String url) {
        return new RemoteMsgHubClient(url);
    }

    /**
     * 创建远端 WebSocket 客户端（自定义参数）
     *
     * @param url                      WebSocket 地址，如 ws://host:port/path
     * @param heartbeatIntervalSeconds 心跳间隔（秒）
     * @param maxReconnectDelaySeconds 最大重连间隔（秒）
     */
    public static MsgHubClient remote(String url, int heartbeatIntervalSeconds, int maxReconnectDelaySeconds) {
        return new RemoteMsgHubClient(url, heartbeatIntervalSeconds, maxReconnectDelaySeconds);
    }
}
