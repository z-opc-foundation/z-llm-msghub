package com.zifang.z.team.msg.store;

import com.zifang.util.core.meta.page.PageResult;
import com.zifang.z.team.msg.common.protocol.HubMessage;

import java.util.List;

/**
 * 消息存储引擎接口
 * <p>
 * 参考 Kafka / RocketMQ 的核心存储设计：
 * <ul>
 *   <li>Kafka: 顺序追加写入 .log + 稀疏索引 .index，按 offset 查找</li>
 *   <li>RocketMQ: CommitLog 统一写入 + ConsumeQueue 按 topic 建索引</li>
 * </ul>
 * <p>
 * 本接口融合两者：
 * <ul>
 *   <li>写入：所有消息顺序追加到 segment .log 文件（纯磁盘顺序写，性能最高）</li>
 *   <li>索引：每 N 条消息或 N 字节记录一条稀疏索引到 .idx 文件</li>
 *   <li>查找 by offset：二分查找 .idx → 定位 segment 和物理位置 → 随机读</li>
 *   <li>查找 by topic：内存维护 topic → offset 列表映射，再走 offset 查找</li>
 * </ul>
 */
public interface MessageStore {

    /**
     * 追加写入一条消息（顺序写磁盘）
     *
     * @param message 消息体
     * @return 分配的全局 offset（从 0 递增）
     */
    long append(HubMessage message);

    /**
     * 按 offset 精确查找单条消息
     * <p>
     * 流程：二分查找 segment → 二分查找 .idx → 随机读 .log
     *
     * @param offset 消息 offset
     * @return 消息体，找不到返回 null
     */
    HubMessage findByOffset(long offset);

    /**
     * 按 topic 查找最近的消息
     * <p>
     * 流程：从内存索引找到 topic 的 offset 列表 → 倒序取 maxCount 个 offset → 逐个 findByOffset
     *
     * @param topic    消息 topic
     * @param maxCount 最多返回条数
     * @return 消息列表（按 offset 升序）
     */
    List<HubMessage> findByTopic(String topic, int maxCount);

    /**
     * 按 topic + sessionId 查找消息
     *
     * @param topic     消息 topic
     * @param sessionId 会话 ID
     * @param maxCount  最多返回条数
     * @return 消息列表
     */
    List<HubMessage> findByTopicAndSession(String topic, String sessionId, int maxCount);

    /**
     * 分页查询消息流水（可按 sessionId 过滤）。
     * <p>
     * 消息按 offset 升序排列（写入顺序），分页从最新一页开始往前翻。
     *
     * @param sessionId 可选，null 或空 = 不过滤
     * @param page      页码，从 1 开始
     * @param pageSize  每页条数
     * @return 分页结果
     */
    PageResult<HubMessage> findPage(String sessionId, int page, int pageSize);

    /**
     * 获取当前已写入的最大 offset
     */
    long getMaxOffset();

    /**
     * 获取存储统计信息
     */
    StoreStats getStats();

    /**
     * 刷盘 + 关闭
     */
    void close();

    // ========== 统计 DTO ==========

    class StoreStats {
        public final long totalMessages;
        public final long maxOffset;
        public final int segmentCount;
        public final long totalLogSizeBytes;
        public final int topicCount;

        public StoreStats(long totalMessages, long maxOffset, int segmentCount,
                          long totalLogSizeBytes, int topicCount) {
            this.totalMessages = totalMessages;
            this.maxOffset = maxOffset;
            this.segmentCount = segmentCount;
            this.totalLogSizeBytes = totalLogSizeBytes;
            this.topicCount = topicCount;
        }

        @Override
        public String toString() {
            return "StoreStats{" +
                    "totalMessages=" + totalMessages +
                    ", maxOffset=" + maxOffset +
                    ", segments=" + segmentCount +
                    ", logSize=" + (totalLogSizeBytes / 1024 / 1024) + "MB" +
                    ", topics=" + topicCount +
                    '}';
        }
    }
}
