package com.zifang.z.team.msg.server;

import com.zifang.util.core.meta.page.PageResult;
import com.zifang.util.json.JsonUtil;
import com.zifang.util.json.model.JsonObject;
import com.zifang.z.team.msg.common.protocol.HubMessage;
import com.zifang.z.team.msg.session.HubSession;
import com.zifang.z.team.msg.store.MessageStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * 连接管理器
 * <p>
 * 维护所有活跃的 TCP 连接及其订阅关系。
 * 核心职责：
 * 1. 连接生命周期管理（连接 / 断开 / 心跳）
 * 2. 消息路由转发（按 topic + sessionId 双维度匹配）
 * 3. 消息环形缓冲（最近 N 条，供 admin 排查）
 */
public class ConnectionManager {

    private static final Logger log = LogManager.getLogger(ConnectionManager.class);

    /**
     * channelId → 连接信息
     */
    private final Map<String, ConnectionInfo> connections = new ConcurrentHashMap<>();

    /**
     * 消息路由计数
     */
    private final AtomicLong routedCount = new AtomicLong(0);

    /**
     * 消息环形缓冲区大小
     */
    private final int messageLogSize;

    /**
     * 消息环形缓冲区（线程安全）
     */
    private final MessageRingBuffer messageBuffer;

    /**
     * 磁盘持久化存储（nullable，为 null 时不落盘）
     */
    private final MessageStore messageStore;

    public ConnectionManager() {
        this(1000, null);
    }

    public ConnectionManager(int messageLogSize) {
        this(messageLogSize, null);
    }

    /**
     * @param messageLogSize 环形缓冲区大小
     * @param messageStore   磁盘持久化存储（nullable）
     */
    public ConnectionManager(int messageLogSize, MessageStore messageStore) {
        this.messageLogSize = Math.max(100, messageLogSize);
        this.messageBuffer = new MessageRingBuffer(this.messageLogSize);
        this.messageStore = messageStore;
    }

    // ========== 消息环形缓冲 ==========

    /**
     * 连接建立时注册
     */
    public void onConnect(String channelId, HubSession session) {
        connections.put(channelId, new ConnectionInfo(channelId, session));
    }

    /**
     * 连接断开时清理
     */
    public void onDisconnect(String channelId) {
        connections.remove(channelId);
    }

    // ========== 连接信息 ==========

    /**
     * 添加订阅
     */
    public void addSubscription(String channelId, String topic, String sessionId) {
        ConnectionInfo info = connections.get(channelId);
        if (info != null) {
            info.subscriptions.add(new Subscription(topic, sessionId));
        }
    }

    /**
     * 刷新心跳
     */
    public void refreshHeartbeat(String channelId) {
        ConnectionInfo info = connections.get(channelId);
        if (info != null) {
            info.lastActive = System.currentTimeMillis();
        }
    }

    // ========== 连接管理 ==========

    /**
     * 路由消息到匹配的连接
     * <p>
     * 按 topic + sessionId 双维度精确匹配，投递给除发送方外所有匹配的连接。
     */
    public void route(HubMessage msg, String fromChannelId) {
        routedCount.incrementAndGet();

        // 持久化到磁盘（重启后可恢复，先于路由投递以确保消息不丢）
        if (messageStore != null) {
            try {
                messageStore.append(msg);
            } catch (Exception e) {
                log.warn("[MsgHub] message store append failed, message may be lost on restart: {}", e.getMessage());
            }
        }

        // 记录消息到环形缓冲
        ConnectionInfo fromConn = fromChannelId != null ? connections.get(fromChannelId) : null;
        String fromRemoteAddr = fromConn != null ? fromConn.getRemoteAddr() : "";
        messageBuffer.put(new MessageEntry(msg, fromChannelId, fromRemoteAddr));

        String json = JsonUtil.toJson(msg);

        for (Map.Entry<String, ConnectionInfo> entry : connections.entrySet()) {
            // 跳过发送方
            if (entry.getKey().equals(fromChannelId)) {
                continue;
            }

            ConnectionInfo info = entry.getValue();
            for (Subscription sub : info.subscriptions) {
                if (sub.topic.equals(msg.getTopic()) && sub.sessionId.equals(msg.getSessionId())) {
                    if (info.session.isActive()) {
                        try {
                            info.session.sendText(json);
                        } catch (Exception e) {
                            // 发送失败，忽略
                        }
                    }
                    break; // 每个连接只投递一次
                }
            }
        }
    }

    public int getOnlineCount() {
        return connections.size();
    }

    public long getRoutedCount() {
        return routedCount.get();
    }

    public Collection<ConnectionInfo> getConnections() {
        return connections.values();
    }

    // ========== 消息路由 ==========

    public ConnectionInfo getConnection(String channelId) {
        return connections.get(channelId);
    }

    // ========== 查询 ==========

    /**
     * 踢出指定连接
     */
    public boolean kick(String channelId) {
        ConnectionInfo info = connections.remove(channelId);
        if (info != null && info.session.isActive()) {
            try {
                info.session.close();
            } catch (Exception ignored) {
            }
            return true;
        }
        return false;
    }

    /**
     * 获取最近消息（可按 sessionId 过滤）
     */
    public List<MessageEntry> getRecentMessages(String sessionId, int limit) {
        return messageBuffer.getRecent(sessionId, Math.min(limit, messageLogSize));
    }

    /**
     * 分页查询消息流水（可按 sessionId 过滤）。
     * <p>
     * 优先从磁盘存储查询（全量数据），磁盘存储不可用时 fallback 到内存环形缓冲区。
     * page=1 为最新一页，页内消息按时间正序（从旧到新）。
     *
     * @param sessionId 可选，null 或空 = 不过滤
     * @param page      页码，从 1 开始
     * @param pageSize  每页条数
     */
    public PageResult<MessageEntry> getMessagesPage(String sessionId, int page, int pageSize) {
        if (messageStore != null) {
            // 优先用磁盘存储分页（全量数据）
            PageResult<HubMessage> storePage = messageStore.findPage(sessionId, page, pageSize);
            List<MessageEntry> entries = new ArrayList<>();
            for (HubMessage msg : storePage.getRecords()) {
                // 磁盘存储不保存 fromChannelId / fromRemoteAddr，置空
                entries.add(new MessageEntry(msg, "", ""));
            }
            return new PageResult<>(entries, storePage.getTotal(), page, pageSize);
        }
        // fallback 到内存缓冲区分页
        return messageBuffer.getPage(sessionId, page, pageSize);
    }

    /**
     * 获取所有 sessionId 及消息数
     */
    public Map<String, Long> getSessionCounts() {
        return messageBuffer.getSessionCounts();
    }

    /**
     * 按 AgentPayload.action 统计消息数
     */
    public Map<String, Long> getActionCounts() {
        return messageBuffer.getActionCounts();
    }

    /**
     * 获取已写入消息总数（含已淘汰的）
     */
    public long getTotalMessagesWritten() {
        return messageBuffer.getTotalWritten();
    }

    /**
     * 最近消息快照条目，供 admin 查看。
     */
    public static class MessageEntry {
        public final long timestamp;
        public final String messageId;
        public final String topic;
        public final String sessionId;
        public final String payload;
        public final String fromChannelId;
        public final String fromRemoteAddr;

        public MessageEntry(HubMessage msg, String fromChannelId, String fromRemoteAddr) {
            this.timestamp = System.currentTimeMillis();
            this.messageId = msg.getMessageId();
            this.topic = msg.getTopic();
            this.sessionId = msg.getSessionId();
            this.payload = msg.getPayload();
            this.fromChannelId = fromChannelId != null ? fromChannelId : "";
            this.fromRemoteAddr = fromRemoteAddr != null ? fromRemoteAddr : "";
        }
    }

    /**
     * 线程安全环形缓冲区，存储最近 N 条消息。
     */
    static class MessageRingBuffer {
        private final AtomicReferenceArray<MessageEntry> buffer;
        private final int capacity;
        private final AtomicLong writeIndex = new AtomicLong(0);
        private final AtomicLong totalWritten = new AtomicLong(0);

        MessageRingBuffer(int capacity) {
            this.capacity = capacity;
            this.buffer = new AtomicReferenceArray<>(capacity);
        }

        void put(MessageEntry entry) {
            int idx = (int) (writeIndex.getAndIncrement() % capacity);
            buffer.set(idx, entry);
            totalWritten.incrementAndGet();
        }

        /**
         * 获取最近的消息，可按 sessionId 过滤。
         *
         * @param sessionIdFilter null 或空 = 不过滤
         * @param limit           最大返回条数
         */
        List<MessageEntry> getRecent(String sessionIdFilter, int limit) {
            int total = (int) Math.min(totalWritten.get(), capacity);
            long currentWrite = writeIndex.get();
            List<MessageEntry> result = new ArrayList<>();
            for (int i = 0; i < total && result.size() < limit; i++) {
                int idx = (int) ((currentWrite - 1 - i) % capacity);
                if (idx < 0) idx += capacity;
                MessageEntry entry = buffer.get(idx);
                if (entry == null) continue;
                if (sessionIdFilter != null && !sessionIdFilter.isEmpty()
                        && !sessionIdFilter.equals(entry.sessionId)) {
                    continue;
                }
                result.add(entry);
            }
            return result;
        }

        /**
         * 分页查询消息（可按 sessionId 过滤）。
         * <p>
         * page=1 为最新一页，页内消息按时间正序（从旧到新）。
         *
         * @param sessionIdFilter null 或空 = 不过滤
         * @param page            页码，从 1 开始
         * @param pageSize        每页条数
         */
        PageResult<MessageEntry> getPage(String sessionIdFilter, int page, int pageSize) {
            int total = (int) Math.min(totalWritten.get(), capacity);
            long currentWrite = writeIndex.get();

            // 从最新到最旧收集所有匹配的消息
            List<MessageEntry> allDesc = new ArrayList<>();
            for (int i = 0; i < total; i++) {
                int idx = (int) ((currentWrite - 1 - i) % capacity);
                if (idx < 0) idx += capacity;
                MessageEntry entry = buffer.get(idx);
                if (entry == null) continue;
                if (sessionIdFilter != null && !sessionIdFilter.isEmpty()
                        && !sessionIdFilter.equals(entry.sessionId)) {
                    continue;
                }
                allDesc.add(entry);
            }

            long totalMatched = allDesc.size();
            int totalPages = pageSize > 0 ? (int) ((totalMatched + pageSize - 1) / pageSize) : 0;
            if (totalMatched == 0 || page > totalPages) {
                return new PageResult<>(Collections.<MessageEntry>emptyList(), 0, page, pageSize);
            }

            // page=1 对应最新的一页（allDesc 的前 pageSize 条）
            int endIdx = Math.min(pageSize - 1, (int) totalMatched - 1);
            int startIdx = 0;
            if (page > 1) {
                endIdx = (int) totalMatched - 1 - (page - 1) * pageSize;
                startIdx = Math.max(0, endIdx - pageSize + 1);
            }
            if (endIdx < 0) {
                return new PageResult<>(Collections.<MessageEntry>emptyList(), 0, page, pageSize);
            }

            // allDesc 是从新到旧，取出当前页后反转为正序（从旧到新）
            List<MessageEntry> pageData = new ArrayList<>();
            for (int i = endIdx; i >= startIdx; i--) {
                pageData.add(allDesc.get(i));
            }
            return new PageResult<>(pageData, totalMatched, page, pageSize);
        }

        /**
         * 收集所有出现过的 sessionId 及其消息数
         */
        Map<String, Long> getSessionCounts() {
            Map<String, Long> counts = new LinkedHashMap<>();
            int total = (int) Math.min(totalWritten.get(), capacity);
            for (int i = 0; i < total; i++) {
                MessageEntry entry = buffer.get(i);
                if (entry != null && entry.sessionId != null && !entry.sessionId.isEmpty()) {
                    counts.merge(entry.sessionId, 1L, Long::sum);
                }
            }
            return counts;
        }

        /**
         * 按 AgentPayload.action 统计消息数
         */
        Map<String, Long> getActionCounts() {
            Map<String, Long> counts = new LinkedHashMap<>();
            int total = (int) Math.min(totalWritten.get(), capacity);
            for (int i = 0; i < total; i++) {
                MessageEntry entry = buffer.get(i);
                if (entry == null || entry.payload == null || entry.payload.isEmpty()) {
                    continue;
                }
                String action = "unknown";
                try {
                    JsonObject payload = JsonUtil.parseObject(entry.payload);
                    if (payload != null && payload.containsKey("action")) {
                        action = payload.getString("action");
                    }
                } catch (Exception ignored) {
                    // 非 JSON payload，归类为 unknown
                }
                counts.merge(action, 1L, Long::sum);
            }
            return counts;
        }

        long getTotalWritten() {
            return totalWritten.get();
        }
    }

    public static class ConnectionInfo {
        public final String channelId;
        public final HubSession session;
        public final Set<Subscription> subscriptions = ConcurrentHashMap.newKeySet();
        public final long connectedAt;
        public volatile long lastActive;

        public ConnectionInfo(String channelId, HubSession session) {
            this.channelId = channelId;
            this.session = session;
            this.connectedAt = System.currentTimeMillis();
            this.lastActive = this.connectedAt;
        }

        public String getRemoteAddr() {
            return session.getRemoteAddr();
        }

        public List<Map<String, String>> getSubscriptionDetails() {
            List<Map<String, String>> list = new ArrayList<>();
            for (Subscription sub : subscriptions) {
                Map<String, String> m = new LinkedHashMap<>();
                m.put("topic", sub.topic);
                m.put("sessionId", sub.sessionId);
                list.add(m);
            }
            return list;
        }
    }

    public static class Subscription {
        public final String topic;
        public final String sessionId;

        public Subscription(String topic, String sessionId) {
            this.topic = topic;
            this.sessionId = sessionId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Subscription that = (Subscription) o;
            return topic.equals(that.topic) && sessionId.equals(that.sessionId);
        }

        @Override
        public int hashCode() {
            return topic.hashCode() * 31 + sessionId.hashCode();
        }
    }
}
