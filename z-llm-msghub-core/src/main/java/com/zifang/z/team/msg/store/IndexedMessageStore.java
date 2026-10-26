package com.zifang.z.team.msg.store;

import com.zifang.util.core.meta.page.PageResult;
import com.zifang.z.team.msg.common.protocol.HubMessage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 索引消息存储引擎
 * <p>
 * 融合 Kafka + RocketMQ 核心思想：
 * <ul>
 *   <li><b>Kafka CommitLog</b>：所有消息按顺序追加写入 segment .log 文件</li>
 *   <li><b>Kafka SparseIndex</b>：每个 segment 维护 .idx 稀疏索引，二分查找定位</li>
 *   <li><b>RocketMQ ConsumeQueue</b>：内存维护 topic → offset 列表映射，支持按 topic 快速查找</li>
 *   <li><b>Segment Rolling</b>：当 .log 达到阈值自动切分新 segment</li>
 * </ul>
 *
 * <h3>文件布局</h3>
 * <pre>
 * {dataDir}/messages/
 * ├── 00000000000000000000.log     # segment 0: 顺序消息日志
 * ├── 00000000000000000000.idx     # segment 0: 稀疏索引
 * ├── 00000000000000100000.log     # segment 1 (base offset = 100000)
 * ├── 00000000000000100000.idx
 * └── ...
 * </pre>
 *
 * <h3>性能特征</h3>
 * <ul>
 *   <li>写入：O(1) 顺序追加，无随机写，利用 OS page cache</li>
 *   <li>查找 by offset：O(log N) 二分查找 .idx + 少量顺序扫描</li>
 *   <li>查找 by topic：O(1) 内存索引查 offset 列表 + O(log N) 逐条读</li>
 *   <li>恢复：重启后扫描 .log 重建 topic 内存索引</li>
 * </ul>
 */
public class IndexedMessageStore implements MessageStore, Closeable {

    private final File dataDir;
    private final long segmentMaxBytes;
    private final int indexIntervalBytes;
    private final long indexMaxBytes;

    /**
     * 所有 segment，按 baseOffset 排序
     */
    private final List<LogSegment> segments = new ArrayList<>();
    /**
     * 全局 offset 计数器
     */
    private final AtomicLong nextOffset = new AtomicLong(0);
    /**
     * Topic 内存索引（RocketMQ ConsumeQueue 思路）
     * topic → offset 列表（按 offset 升序）
     */
    private final Map<String, List<Long>> topicOffsets = new ConcurrentHashMap<>();
    /**
     * 统计：总消息数
     */
    private final AtomicLong totalMessages = new AtomicLong(0);
    /**
     * 当前活跃 segment（写入用）
     */
    private LogSegment activeSegment;

    /**
     * @param dataDir            数据目录
     * @param segmentMaxBytes    单个 segment 最大字节数（默认 64MB）
     * @param indexIntervalBytes 稀疏索引间隔字节（默认 4KB）
     * @param indexMaxBytes      索引文件最大字节数（默认 10MB）
     */
    public IndexedMessageStore(String dataDir, long segmentMaxBytes,
                               int indexIntervalBytes, long indexMaxBytes) throws IOException {
        this.dataDir = new File(dataDir, "messages");
        this.segmentMaxBytes = segmentMaxBytes;
        this.indexIntervalBytes = indexIntervalBytes;
        this.indexMaxBytes = indexMaxBytes;

        if (!this.dataDir.exists()) {
            this.dataDir.mkdirs();
        }

        // 恢复已有 segments
        recover();
    }

    /**
     * 简化构造（使用默认配置）
     */
    public IndexedMessageStore(String dataDir) throws IOException {
        this(dataDir, 64 * 1024 * 1024L, 4096, 10 * 1024 * 1024L);
    }

    // ========== 写入 ==========

    @Override
    public synchronized long append(HubMessage message) {
        long offset = nextOffset.getAndIncrement();

        // 检查是否需要滚动新 segment
        if (activeSegment == null || activeSegment.isFull()) {
            rollNewSegment(offset);
        }

        // 写入消息
        long position = activeSegment.append(offset, message);
        if (position < 0) {
            // 当前 segment 写不下，滚动后重试
            rollNewSegment(offset);
            position = activeSegment.append(offset, message);
            if (position < 0) {
                throw new RuntimeException("Failed to append message, segment full after roll");
            }
        }

        // 更新 topic 内存索引
        topicOffsets.computeIfAbsent(message.getTopic(), k -> Collections.synchronizedList(new ArrayList<>()))
                .add(offset);

        totalMessages.incrementAndGet();
        return offset;
    }

    // ========== 查找 ==========

    @Override
    public synchronized HubMessage findByOffset(long offset) {
        // 1. 找到包含该 offset 的 segment（二分查找）
        LogSegment segment = findSegment(offset);
        if (segment == null) {
            return null;
        }
        // 2. 在 segment 内查找
        return segment.findByOffset(offset);
    }

    @Override
    public List<HubMessage> findByTopic(String topic, int maxCount) {
        List<Long> offsets = topicOffsets.get(topic);
        if (offsets == null || offsets.isEmpty()) {
            return new ArrayList<>();
        }

        // 取最近 maxCount 条
        int start = Math.max(0, offsets.size() - maxCount);
        List<HubMessage> result = new ArrayList<>(maxCount);
        for (int i = start; i < offsets.size(); i++) {
            HubMessage msg = findByOffset(offsets.get(i));
            if (msg != null) {
                result.add(msg);
            }
        }
        return result;
    }

    @Override
    public List<HubMessage> findByTopicAndSession(String topic, String sessionId, int maxCount) {
        List<Long> offsets = topicOffsets.get(topic);
        if (offsets == null || offsets.isEmpty()) {
            return new ArrayList<>();
        }

        List<HubMessage> result = new ArrayList<>();
        // 从最新的开始倒序扫描
        for (int i = offsets.size() - 1; i >= 0 && result.size() < maxCount; i--) {
            HubMessage msg = findByOffset(offsets.get(i));
            if (msg != null && sessionId.equals(msg.getSessionId())) {
                result.add(msg);
            }
        }
        // 反转为升序
        Collections.reverse(result);
        return result;
    }

    /**
     * 分页查询消息流水。
     * <p>
     * 消息按 offset 升序（写入时间正序），分页从最新一页开始：page=1 为最新一页。
     *
     * @param sessionId 可选，null 或空 = 不过滤
     * @param page      页码，从 1 开始
     * @param pageSize  每页条数
     */
    @Override
    public PageResult<HubMessage> findPage(String sessionId, int page, int pageSize) {
        long maxOffset = getMaxOffset();
        if (maxOffset < 0 || page < 1 || pageSize < 1) {
            return new PageResult<>(Collections.<HubMessage>emptyList(), 0, page, pageSize);
        }

        boolean hasSessionFilter = sessionId != null && !sessionId.isEmpty();

        if (!hasSessionFilter) {
            // 无 sessionId 过滤：offset 连续递增，直接按范围分页
            long total = maxOffset + 1;
            int totalPages = (int) ((total + pageSize - 1) / pageSize);
            if (page > totalPages) {
                return new PageResult<>(Collections.<HubMessage>emptyList(), 0, page, pageSize);
            }

            // page=1 对应最新的一页（offset 最大的那批）
            long endOffset = maxOffset - (long) (page - 1) * pageSize;
            long startOffset = Math.max(0, endOffset - pageSize + 1);

            List<HubMessage> messages = new ArrayList<>();
            for (long offset = startOffset; offset <= endOffset; offset++) {
                HubMessage msg = findByOffset(offset);
                if (msg != null) {
                    messages.add(msg);
                }
            }
            return new PageResult<>(messages, total, page, pageSize);
        } else {
            // 有 sessionId 过滤：扫描所有 segment 获取匹配的 offset 列表
            // 利用 LogSegment.scanMessageMetas() 顺序扫描，比逐条 findByOffset 高效
            List<Long> matchedOffsets = new ArrayList<>();
            for (LogSegment seg : segments) {
                List<LogSegment.MessageMeta> metas = seg.scanMessageMetas();
                for (LogSegment.MessageMeta meta : metas) {
                    if (sessionId.equals(meta.sessionId)) {
                        matchedOffsets.add(meta.offset);
                    }
                }
            }
            // matchedOffsets 已按 offset 升序（segment 顺序 + segment 内顺序）

            long total = matchedOffsets.size();
            int totalPages = (int) ((total + pageSize - 1) / pageSize);
            if (total == 0 || page > totalPages) {
                return new PageResult<>(Collections.<HubMessage>emptyList(), 0, page, pageSize);
            }

            // page=1 对应最新的一页
            int endIdx = matchedOffsets.size() - 1 - (page - 1) * pageSize;
            int startIdx = Math.max(0, endIdx - pageSize + 1);

            List<HubMessage> messages = new ArrayList<>();
            for (int i = startIdx; i <= endIdx; i++) {
                HubMessage msg = findByOffset(matchedOffsets.get(i));
                if (msg != null) {
                    messages.add(msg);
                }
            }
            return new PageResult<>(messages, total, page, pageSize);
        }
    }

    // ========== 状态查询 ==========

    @Override
    public long getMaxOffset() {
        return nextOffset.get() - 1;
    }

    @Override
    public StoreStats getStats() {
        long totalLogSize = 0;
        for (LogSegment seg : segments) {
            totalLogSize += seg.getWritePosition();
        }
        return new StoreStats(
                totalMessages.get(),
                getMaxOffset(),
                segments.size(),
                totalLogSize,
                topicOffsets.size()
        );
    }

    public int getTopicCount() {
        return topicOffsets.size();
    }

    public int getTopicMessageCount(String topic) {
        List<Long> offsets = topicOffsets.get(topic);
        return offsets != null ? offsets.size() : 0;
    }

    // ========== 内部方法 ==========

    /**
     * 滚动创建新 segment
     */
    private void rollNewSegment(long baseOffset) {
        try {
            if (activeSegment != null) {
                activeSegment.flush();
            }
            LogSegment seg = new LogSegment(dataDir, baseOffset, segmentMaxBytes,
                    indexIntervalBytes, indexMaxBytes);
            segments.add(seg);
            activeSegment = seg;
        } catch (IOException e) {
            throw new RuntimeException("Failed to create new segment at offset " + baseOffset, e);
        }
    }

    /**
     * 二分查找包含指定 offset 的 segment
     */
    private LogSegment findSegment(long offset) {
        if (segments.isEmpty()) {
            return null;
        }
        // segments 按 baseOffset 升序
        int low = 0;
        int high = segments.size() - 1;
        int result = -1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            LogSegment seg = segments.get(mid);
            if (seg.getBaseOffset() <= offset) {
                result = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        if (result < 0) {
            return null;
        }
        LogSegment seg = segments.get(result);
        return seg.contains(offset) ? seg : null;
    }

    /**
     * 启动时恢复已有 segments 和 topic 索引
     */
    private void recover() throws IOException {
        File[] logFiles = dataDir.listFiles((dir, name) -> name.endsWith(".log"));
        if (logFiles == null || logFiles.length == 0) {
            return;
        }

        // 按 baseOffset 排序
        List<Long> baseOffsets = new ArrayList<>();
        for (File f : logFiles) {
            String name = f.getName().replace(".log", "");
            baseOffsets.add(Long.parseLong(name));
        }
        Collections.sort(baseOffsets);

        // 加载每个 segment
        for (Long baseOffset : baseOffsets) {
            LogSegment seg = new LogSegment(dataDir, baseOffset, segmentMaxBytes,
                    indexIntervalBytes, indexMaxBytes);
            segments.add(seg);
            activeSegment = seg;
        }

        // 重建 topic 内存索引
        for (LogSegment seg : segments) {
            List<LogSegment.MessageMeta> metas = seg.scanMessageMetas();
            for (LogSegment.MessageMeta meta : metas) {
                topicOffsets.computeIfAbsent(meta.topic, k -> Collections.synchronizedList(new ArrayList<>()))
                        .add(meta.offset);
                totalMessages.incrementAndGet();
            }
        }

        // 恢复 nextOffset
        if (!segments.isEmpty()) {
            nextOffset.set(segments.get(segments.size() - 1).getMaxOffset() + 1);
        }
    }

    /**
     * 刷盘
     */
    public void flush() {
        for (LogSegment seg : segments) {
            seg.flush();
        }
    }

    @Override
    public void close() {
        flush();
        for (LogSegment seg : segments) {
            try {
                seg.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }
}
