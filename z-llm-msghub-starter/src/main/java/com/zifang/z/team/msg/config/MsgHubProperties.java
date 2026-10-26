package com.zifang.z.team.msg.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * msgHub 配置属性
 */
@ConfigurationProperties(prefix = "msghub")
public class MsgHubProperties {

    /**
     * 是否启用 msgHub 服务端
     */
    private boolean enabled = true;

    private WebSocket websocket = new WebSocket();
    private Persistence persistence = new Persistence();
    private Route route = new Route();
    private Monitor monitor = new Monitor();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public WebSocket getWebsocket() {
        return websocket;
    }

    public void setWebsocket(WebSocket websocket) {
        this.websocket = websocket;
    }

    public Persistence getPersistence() {
        return persistence;
    }

    public void setPersistence(Persistence persistence) {
        this.persistence = persistence;
    }

    public Route getRoute() {
        return route;
    }

    public void setRoute(Route route) {
        this.route = route;
    }

    public Monitor getMonitor() {
        return monitor;
    }

    public void setMonitor(Monitor monitor) {
        this.monitor = monitor;
    }

    public static class WebSocket {
        private int port = 9091;
        private int bossThreads = 1;
        /**
         * 0 = CPU * 2
         */
        private int workerThreads = 0;
        /**
         * 空闲超时（秒），超时后断开连接
         */
        private int idleTimeoutSeconds = 15;
        /**
         * WebSocket 路径
         */
        private String path = "/msghub";

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public int getBossThreads() {
            return bossThreads;
        }

        public void setBossThreads(int bossThreads) {
            this.bossThreads = bossThreads;
        }

        public int getWorkerThreads() {
            return workerThreads;
        }

        public void setWorkerThreads(int workerThreads) {
            this.workerThreads = workerThreads;
        }

        public int getIdleTimeoutSeconds() {
            return idleTimeoutSeconds;
        }

        public void setIdleTimeoutSeconds(int idleTimeoutSeconds) {
            this.idleTimeoutSeconds = idleTimeoutSeconds;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }
    }

    public static class Persistence {
        /**
         * 留空则按 z-team.workspace 自动解析为 {workspaceRoot}/msgHub/
         */
        private String dataDir = "";
        /**
         * 快照间隔（秒）
         */
        private int snapshotIntervalSeconds = 30;
        /**
         * 路由表变更时立即写文件
         */
        private boolean flushOnChange = true;
        /**
         * 是否启用消息磁盘持久化（重启后消息不丢失）
         */
        private boolean persistEnabled = true;
        /**
         * 定期刷盘间隔（秒），0=不自动刷盘
         */
        private int flushIntervalSeconds = 5;

        public String getDataDir() {
            return dataDir;
        }

        public void setDataDir(String dataDir) {
            this.dataDir = dataDir;
        }

        public int getSnapshotIntervalSeconds() {
            return snapshotIntervalSeconds;
        }

        public void setSnapshotIntervalSeconds(int v) {
            this.snapshotIntervalSeconds = v;
        }

        public boolean isFlushOnChange() {
            return flushOnChange;
        }

        public void setFlushOnChange(boolean flushOnChange) {
            this.flushOnChange = flushOnChange;
        }

        public boolean isPersistEnabled() {
            return persistEnabled;
        }

        public void setPersistEnabled(boolean persistEnabled) {
            this.persistEnabled = persistEnabled;
        }

        public int getFlushIntervalSeconds() {
            return flushIntervalSeconds;
        }

        public void setFlushIntervalSeconds(int flushIntervalSeconds) {
            this.flushIntervalSeconds = flushIntervalSeconds;
        }
    }

    public static class Route {
        private boolean wildcardEnabled = true;

        public boolean isWildcardEnabled() {
            return wildcardEnabled;
        }

        public void setWildcardEnabled(boolean wildcardEnabled) {
            this.wildcardEnabled = wildcardEnabled;
        }
    }

    /**
     * 监控配置
     */
    public static class Monitor {
        /**
         * 消息环形缓冲区大小（保留最近 N 条消息供 admin 查看）
         */
        private int messageLogSize = 1000;

        public int getMessageLogSize() {
            return messageLogSize;
        }

        public void setMessageLogSize(int messageLogSize) {
            this.messageLogSize = messageLogSize;
        }
    }
}
