package com.zifang.z.team.msg.config;

import com.zifang.z.team.msg.persistence.JsonRouteStore;
import com.zifang.z.team.msg.persistence.RouteStore;
import com.zifang.z.team.msg.server.ConnectionManager;
import com.zifang.z.team.msg.server.MsgHubServer;
import com.zifang.z.team.msg.store.IndexedMessageStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * msgHub 自动装配
 * <p>
 * Spring Boot 启动时：
 * 1. 创建 RouteStore（文件持久化）
 * 2. 创建 IndexedMessageStore（消息磁盘持久化，Kafka 式 .log + .idx）
 * 3. 创建 ConnectionManager（连接管理 + 路由引擎 + 消息落盘）
 * 4. WebSocket 端点由 MsgHubWebSocketConfig 自动注册到 Spring Boot HTTP 端口
 * <p>
 * Spring Boot 关闭时：
 * 5. 停止刷盘调度
 * 6. flush + close IndexedMessageStore
 */
@Configuration
@EnableConfigurationProperties(MsgHubProperties.class)
@ConditionalOnProperty(name = "msghub.enabled", havingValue = "true", matchIfMissing = true)
public class MsgHubAutoConfiguration {

    private static final Logger log = LogManager.getLogger(MsgHubAutoConfiguration.class);

    /**
     * 工作区环境：local → ~/z_team，prod → Nacos base-dir
     */
    @Value("${z-team.workspace.env:prod}")
    private String workspaceEnv;

    /**
     * 工作区自定义根路径（需配合 override=true 生效，与 evolution 一致）
     */
    @Value("${z-team.workspace.base-dir:}")
    private String workspaceBaseDir;

    @Value("${z-team.workspace.override:false}")
    private boolean workspaceOverride;

    private MsgHubServer msgHubServer;
    private IndexedMessageStore messageStore;
    private ScheduledExecutorService flushScheduler;

    /**
     * 解析工作区根路径（与 EvolutionWorkspaceProperties 完全一致）。
     * <ul>
     *   <li>env=local → ~/z_team</li>
     *   <li>env=prod + override=true → Nacos 配置的 base-dir</li>
     *   <li>env=prod + override=false → ~/z_team（默认）</li>
     * </ul>
     */
    private String resolveWorkspaceRoot() {
        if ("local".equals(workspaceEnv)) {
            return System.getProperty("user.home") + "/z_team";
        }
        if (workspaceOverride && workspaceBaseDir != null && !workspaceBaseDir.trim().isEmpty()) {
            return workspaceBaseDir.trim();
        }
        return System.getProperty("user.home") + "/z_team";
    }

    /**
     * 解析持久化数据目录：{workspaceRoot}/msgHub/
     * <p>
     * 与 evolution 模块对齐，data 挂在 z-team workspace 下，
     * 与 agents/ groups/ knowledge/ pipeline/ 同级。
     */
    private String resolveDataDir(MsgHubProperties props) {
        String configured = props.getPersistence().getDataDir();
        // 显式配置了非空路径时直接使用
        if (configured != null && !configured.trim().isEmpty()) {
            log.info("[MsgHub] explicit dataDir = {}", configured);
            return configured;
        }
        // 按 workspace 解析：{workspaceRoot}/msgHub/
        String dir = resolveWorkspaceRoot() + "/msgHub";
        log.info("[MsgHub] workspace env={}, dataDir = {}", workspaceEnv, dir);
        return dir;
    }

    @Bean
    @ConditionalOnMissingBean
    public RouteStore routeStore(MsgHubProperties props) {
        return new JsonRouteStore(resolveDataDir(props));
    }

    /**
     * 创建 IndexedMessageStore（消息磁盘持久化）
     * <p>
     * 启用后，所有经过 ConnectionManager.route() 的消息都会顺序追加到磁盘，
     * 重启后可从 .log 恢复，消息不丢失。
     * <p>
     * 创建失败时返回 null，ConnectionManager 降级为纯内存路由（不持久化）。
     */
    private IndexedMessageStore createMessageStore(MsgHubProperties props) {
        if (!props.getPersistence().isPersistEnabled()) {
            log.info("[MsgHub] persistEnabled=false, message persistence disabled");
            return null;
        }
        String dataDir = resolveDataDir(props);
        try {
            IndexedMessageStore store = new IndexedMessageStore(dataDir);
            log.info("[MsgHub] IndexedMessageStore initialized at {}", dataDir);
            return store;
        } catch (Exception e) {
            log.error("[MsgHub] Failed to initialize IndexedMessageStore at {}, " +
                    "messages will NOT persist on restart: {}", dataDir, e.getMessage(), e);
            return null;
        }
    }

    @Bean
    @ConditionalOnMissingBean
    public ConnectionManager connectionManager(MsgHubProperties props) {
        // 创建磁盘持久化存储
        IndexedMessageStore store = createMessageStore(props);
        this.messageStore = store;

        ConnectionManager connMgr = new ConnectionManager(
                props.getMonitor().getMessageLogSize(), store);

        // 定期刷盘（确保 page cache 数据写入磁盘，防 kill -9 丢失）
        if (store != null && props.getPersistence().getFlushIntervalSeconds() > 0) {
            int interval = props.getPersistence().getFlushIntervalSeconds();
            flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "msghub-flush");
                t.setDaemon(true);
                return t;
            });
            flushScheduler.scheduleAtFixedRate(() -> {
                try {
                    store.flush();
                } catch (Exception e) {
                    log.warn("[MsgHub] periodic flush failed: {}", e.getMessage());
                }
            }, interval, interval, TimeUnit.SECONDS);
            log.info("[MsgHub] periodic flush scheduled every {}s", interval);
        }

        return connMgr;
    }

    @Bean
    public MsgHubServer msgHubServer(MsgHubProperties props, ConnectionManager connManager) {
        msgHubServer = new MsgHubServer(props, connManager);
        msgHubServer.start();
        return msgHubServer;
    }

    @PreDestroy
    public void destroy() {
        if (flushScheduler != null) {
            flushScheduler.shutdownNow();
        }
        if (msgHubServer != null) {
            msgHubServer.shutdown();
        }
        if (messageStore != null) {
            log.info("[MsgHub] flushing and closing IndexedMessageStore...");
            try {
                messageStore.flush();
                messageStore.close();
            } catch (Exception e) {
                log.warn("[MsgHub] IndexedMessageStore close failed: {}", e.getMessage());
            }
        }
    }
}
