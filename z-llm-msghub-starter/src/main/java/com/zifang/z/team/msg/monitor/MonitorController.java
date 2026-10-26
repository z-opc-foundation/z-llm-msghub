package com.zifang.z.team.msg.monitor;

import com.zifang.util.core.meta.page.PageResult;
import com.zifang.util.json.JsonUtil;
import com.zifang.util.json.model.JsonObject;
import com.zifang.z.team.msg.config.MsgHubProperties;
import com.zifang.z.team.msg.server.ConnectionManager;
import com.zifang.z.team.msg.server.MsgHubServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * msgHub 监控面板 REST API
 * <p>
 * 提供连接列表、消息流水、会话统计、配置查看、手动踢出等运维接口。
 */
@RestController
@RequestMapping("/msghub/api")
public class MonitorController {

    @Autowired
    private ConnectionManager connManager;
    @Autowired
    private MsgHubServer msgHubServer;
    @Autowired
    private MsgHubProperties props;

    /**
     * 概览仪表盘
     */
    @GetMapping("/overview")
    public Map<String, Object> overview() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("serverStatus", msgHubServer.isRunning() ? "RUNNING" : "STOPPED");
        m.put("wsPath", "/msghub/ws");
        m.put("onlineConnections", connManager.getOnlineCount());
        m.put("routedMessages", connManager.getRoutedCount());
        m.put("totalMessages", connManager.getTotalMessagesWritten());
        m.put("sessionCount", connManager.getSessionCounts().size());
        m.put("timestamp", System.currentTimeMillis());
        return m;
    }

    /**
     * 在线连接列表（含订阅详情）
     */
    @GetMapping("/connections")
    public List<Map<String, Object>> connections() {
        List<Map<String, Object>> list = new ArrayList<>();
        for (ConnectionManager.ConnectionInfo info : connManager.getConnections()) {
            list.add(buildConnectionDetail(info));
        }
        return list;
    }

    /**
     * 连接详情
     */
    @GetMapping("/connections/{channelId}")
    public Map<String, Object> connectionDetail(@PathVariable String channelId) {
        ConnectionManager.ConnectionInfo info = connManager.getConnection(channelId);
        if (info == null) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("error", "connection not found: " + channelId);
            return err;
        }
        return buildConnectionDetail(info);
    }

    /**
     * 踢出连接
     */
    @DeleteMapping("/connections/{channelId}")
    public Map<String, Object> kickConnection(@PathVariable String channelId) {
        boolean ok = connManager.kick(channelId);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("kicked", ok);
        m.put("channelId", channelId);
        return m;
    }

    /**
     * 消息流水分页查询（可按 sessionId 过滤）。
     * <p>
     * page=1 为最新一页，页内消息按时间正序（从旧到新）。
     *
     * @param sessionId 可选，过滤指定会话
     * @param page      页码，从 1 开始，默认 1
     * @param pageSize  每页条数，默认 20
     */
    @GetMapping("/messages")
    public Map<String, Object> messages(
            @RequestParam(required = false) String sessionId,
            @RequestParam(required = false, defaultValue = "1") int page,
            @RequestParam(required = false, defaultValue = "20") int pageSize) {
        if (page < 1) page = 1;
        if (pageSize < 1) pageSize = 20;
        if (pageSize > 500) pageSize = 500;

        PageResult<ConnectionManager.MessageEntry> pageResult = connManager.getMessagesPage(sessionId, page, pageSize);

        List<Map<String, Object>> list = new ArrayList<>();
        for (ConnectionManager.MessageEntry e : pageResult.getRecords()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("timestamp", e.timestamp);
            m.put("messageId", e.messageId);
            m.put("topic", e.topic);
            m.put("sessionId", e.sessionId);
            m.put("fromChannel", e.fromChannelId);
            m.put("fromRemoteAddr", e.fromRemoteAddr);
            // 解析 payload
            Map<String, Object> payload = parsePayload(e.payload);
            m.put("payload", payload);
            list.add(m);
        }

        long total = pageResult.getTotal();
        long size = pageResult.getPageSize();
        long current = pageResult.getPageNum();
        long totalPages = size > 0 ? (total + size - 1) / size : 0;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("list", list);
        result.put("total", total);
        result.put("page", current);
        result.put("pageSize", size);
        result.put("totalPages", totalPages);
        result.put("hasNext", current < totalPages);
        result.put("hasPrevious", current > 1);
        return result;
    }

    /**
     * 会话列表（sessionId + 消息数）
     */
    @GetMapping("/sessions")
    public List<Map<String, Object>> sessions() {
        Map<String, Long> counts = connManager.getSessionCounts();
        List<Map<String, Object>> list = new ArrayList<>();
        for (Map.Entry<String, Long> entry : counts.entrySet()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("sessionId", entry.getKey());
            m.put("messageCount", entry.getValue());
            list.add(m);
        }
        list.sort((a, b) -> Long.compare((long) b.get("messageCount"), (long) a.get("messageCount")));
        return list;
    }

    /**
     * 按 action 统计消息数
     */
    @GetMapping("/stats/action")
    public List<Map<String, Object>> statsByAction() {
        Map<String, Long> counts = connManager.getActionCounts();
        List<Map<String, Object>> list = new ArrayList<>();
        for (Map.Entry<String, Long> entry : counts.entrySet()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("action", entry.getKey());
            m.put("count", entry.getValue());
            list.add(m);
        }
        list.sort((a, b) -> Long.compare((long) b.get("count"), (long) a.get("count")));
        return list;
    }

    /**
     * 当前配置
     */
    @GetMapping("/config")
    public MsgHubProperties config() {
        return props;
    }

    // ========== 内部方法 ==========

    private Map<String, Object> buildConnectionDetail(ConnectionManager.ConnectionInfo info) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("channelId", info.channelId);
        m.put("remoteAddr", info.getRemoteAddr());
        m.put("connectedAt", info.connectedAt);
        m.put("lastActive", info.lastActive);
        m.put("idleSeconds", (System.currentTimeMillis() - info.lastActive) / 1000);
        m.put("subscriptionCount", info.subscriptions.size());
        m.put("subscriptions", info.getSubscriptionDetails());
        return m;
    }

    private Map<String, Object> parsePayload(String payloadJson) {
        if (payloadJson == null || payloadJson.isEmpty()) {
            return Collections.emptyMap();
        }
        try {
            JsonObject obj = JsonUtil.parseObject(payloadJson);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("from", obj.getString("from"));
            m.put("action", obj.getString("action"));
            m.put("content", obj.getString("content"));
            return m;
        } catch (Exception e) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("raw", payloadJson);
            return m;
        }
    }
}
