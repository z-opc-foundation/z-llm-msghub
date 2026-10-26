package com.zifang.z.team.msg.store;

import com.zifang.util.json.JsonUtil;
import com.zifang.z.team.msg.common.protocol.HubMessage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 日志分段（Log Segment）
 * <p>
 * 参考 Kafka LogSegment 设计：
 * <ul>
 *   <li>一个 segment = 一个 .log 文件 + 一个 .idx 稀疏索引文件</li>
 *   <li>消息按顺序追加到 .log，每条消息包含 header（offset + timestamp + topic）</li>
 *   <li>每 indexIntervalBytes 字节，在 .idx 中记录一条稀疏索引</li>
 *   <li>当 .log 达到 segmentMaxBytes 时，滚动创建新 segment</li>
 * </ul>
 *
 * <h3>日志记录二进制格式</h3>
 * <pre>
 * ┌──────────────┬──────────────┬───────────────┬───────────────┐
 * │ totalLength  │   offset     │  timestamp    │  topicLen     │
 * │   (4 bytes)  │  (8 bytes)   │  (8 bytes)    │  (2 bytes)    │
 * ├──────────────┴──────────────┴───────────────┼───────────────┤
 * │                  topicBytes                  │ sessionIdLen  │
 * │                (topicLen bytes)              │   (2 bytes)    │
 * ├──────────────────────────────────────────────┼───────────────┤
 * │                sessionIdBytes                │   jsonLen      │
 * │            (sessionIdLen bytes)              │   (4 bytes)    │
 * ├──────────────────────────────────────────────┴───────────────┤
 * │                     jsonBytes (fastjson)                     │
 * │                    (jsonLen bytes)                           │
 * └──────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h3>稀疏索引条目（.idx 文件）</h3>
 * <pre>
 * ┌──────────────┬──────────────┬──────────────┐
 * │   offset     │   position   │  timestamp   │
 * │  (8 bytes)   │  (8 bytes)   │  (8 bytes)   │
 * └──────────────┴──────────────┴──────────────┘
 *   每条 24 bytes，按 offset 升序
 * </pre>
 */
public class LogSegment implements Closeable {

    /**
     * 日志文件
     */
    private final File logFile;
    /**
     * 稀疏索引
     */
    private final SparseIndex index;
    /**
     * 日志 RandomAccessFile
     */
    private final RandomAccessFile logRaf;
    private final FileChannel logChannel;
    /**
     * 写入缓冲区（mmap）
     */
    private final MappedByteBuffer writeBuffer;

    /**
     * segment 的 base offset（第一条消息的 offset）
     */
    private final long baseOffset;
    /**
     * 稀疏索引间隔（字节）
     */
    private final int indexIntervalBytes;
    /**
     * segment 最大字节数
     */
    private final long segmentMaxBytes;
    /**
     * 当前写入位置
     */
    private volatile long writePosition = 0;
    /**
     * 当前 segment 的最大 offset（最后一条消息的 offset）
     */
    private volatile long maxOffset = -1;
    /**
     * 消息计数
     */
    private volatile int messageCount = 0;
    /**
     * 上次建索引时的写入位置
     */
    private long lastIndexedPosition = 0;

    public LogSegment(File dir, long baseOffset, long segmentMaxBytes,
                      int indexIntervalBytes, long indexMaxBytes) throws IOException {
        this.baseOffset = baseOffset;
        this.segmentMaxBytes = segmentMaxBytes;
        this.indexIntervalBytes = indexIntervalBytes;

        String prefix = String.format("%020d", baseOffset);
        this.logFile = new File(dir, prefix + ".log");
        File idxFile = new File(dir, prefix + ".idx");

        boolean exists = logFile.exists();
        this.logRaf = new RandomAccessFile(logFile, "rw");
        this.logChannel = logRaf.getChannel();

        // 预分配 mmap 空间
        this.writeBuffer = logChannel.map(FileChannel.MapMode.READ_WRITE, 0, segmentMaxBytes);

        this.index = new SparseIndex(idxFile, indexMaxBytes);

        if (exists) {
            recover();
        }
    }

    // ========== 写入 ==========

    /**
     * 追加一条消息到 segment
     *
     * @param offset  全局 offset
     * @param message 消息体
     * @return 写入的物理位置，如果 segment 已满返回 -1
     */
    public synchronized long append(long offset, HubMessage message) {
        byte[] topicBytes = getBytes(message.getTopic());
        byte[] sessionBytes = getBytes(message.getSessionId());
        String json = JsonUtil.toJson(message);
        byte[] jsonBytes = getBytes(json);

        // header: 4 + 8 + 8 + 2 + topicLen + 2 + sessionLen + 4 = 28 + topicLen + sessionLen
        int totalLen = 28 + topicBytes.length + sessionBytes.length + jsonBytes.length;

        if (writePosition + totalLen > segmentMaxBytes) {
            return -1; // segment 已满
        }

        int pos = (int) writePosition;
        writeBuffer.putInt(pos, totalLen);
        writeBuffer.putLong(pos + 4, offset);
        long timestamp = System.currentTimeMillis();
        writeBuffer.putLong(pos + 12, timestamp);
        writeBuffer.putShort(pos + 20, (short) topicBytes.length);
        ((Buffer) writeBuffer).position(pos + 22);
        writeBuffer.put(topicBytes);
        writeBuffer.putShort((short) sessionBytes.length);
        writeBuffer.put(sessionBytes);
        writeBuffer.putInt(jsonBytes.length);
        writeBuffer.put(jsonBytes);

        writePosition += totalLen;
        messageCount++;
        maxOffset = offset;

        // 稀疏索引：每 indexIntervalBytes 字节记录一条
        if (writePosition - lastIndexedPosition >= indexIntervalBytes) {
            index.add(offset, pos, timestamp);
            lastIndexedPosition = writePosition;
        }

        return pos;
    }

    // ========== 读取 ==========

    /**
     * 按 offset 查找消息
     * <p>
     * 流程：二分查找 .idx → 定位物理位置 → 顺序扫描直到找到精确 offset
     *
     * @param targetOffset 目标 offset
     * @return 消息体，找不到返回 null
     */
    public synchronized HubMessage findByOffset(long targetOffset) {
        if (targetOffset < baseOffset || targetOffset > maxOffset) {
            return null;
        }

        // 1. 二分查找稀疏索引，找到 <= targetOffset 的最近物理位置
        SparseIndex.IndexLookupResult lookup = index.lookup(targetOffset);
        int scanPos;
        if (lookup != null) {
            scanPos = (int) lookup.position;
        } else {
            scanPos = 0;
        }

        // 2. 从 scanPos 开始顺序扫描，直到找到精确 offset
        while (scanPos < writePosition) {
            int totalLen = writeBuffer.getInt(scanPos);
            long offset = writeBuffer.getLong(scanPos + 4);
            if (offset == targetOffset) {
                return readMessage(scanPos, totalLen);
            }
            if (offset > targetOffset) {
                break; // 超过了，找不到
            }
            scanPos += totalLen;
        }

        return null;
    }

    /**
     * 扫描 segment 中所有消息的 header 信息（用于 topic 索引重建）
     */
    public synchronized List<MessageMeta> scanMessageMetas() {
        List<MessageMeta> metas = new ArrayList<>();
        int pos = 0;
        while (pos < writePosition) {
            int totalLen = writeBuffer.getInt(pos);
            long offset = writeBuffer.getLong(pos + 4);
            long timestamp = writeBuffer.getLong(pos + 12);
            short topicLen = writeBuffer.getShort(pos + 20);
            byte[] topicBytes = new byte[topicLen];
            ((Buffer) writeBuffer).position(pos + 22);
            writeBuffer.get(topicBytes);
            String topic = new String(topicBytes, StandardCharsets.UTF_8);

            short sessionLen = writeBuffer.getShort();
            byte[] sessionBytes = new byte[sessionLen];
            writeBuffer.get(sessionBytes);
            String sessionId = new String(sessionBytes, StandardCharsets.UTF_8);

            metas.add(new MessageMeta(offset, pos, timestamp, topic, sessionId));
            pos += totalLen;
        }
        return metas;
    }

    /**
     * 从指定物理位置读取一条消息
     */
    private HubMessage readMessage(int pos, int totalLen) {
        // skip header: totalLen(4) + offset(8) + timestamp(8) + topicLen(2) = 22
        short topicLen = writeBuffer.getShort(pos + 20);
        int topicStart = pos + 22;
        int sessionStart = topicStart + topicLen;
        short sessionLen = writeBuffer.getShort(sessionStart);
        int jsonStart = sessionStart + sessionLen + 2;
        int jsonLen = writeBuffer.getInt(jsonStart);
        int jsonByteStart = jsonStart + 4;

        byte[] jsonBytes = new byte[jsonLen];
        ((Buffer) writeBuffer).position(jsonByteStart);
        writeBuffer.get(jsonBytes);
        String json = new String(jsonBytes, StandardCharsets.UTF_8);

        return JsonUtil.fromJson(json, HubMessage.class);
    }

    // ========== 恢复 ==========

    /**
     * 从已有 .log 文件恢复状态
     * <p>
     * 由于 .log 文件是预分配的（mmap），文件大小 = segmentMaxBytes，
     * 不能用文件大小判断实际写入量。
     * 恢复方式：从头扫描，遇到 totalLen <= 0 视为未写入空间。
     */
    private void recover() throws IOException {
        List<MessageMeta> metas = new ArrayList<>();
        int pos = 0;

        while (pos + 4 <= segmentMaxBytes) {
            int totalLen = writeBuffer.getInt(pos);
            if (totalLen <= 0 || pos + totalLen > segmentMaxBytes) {
                break; // 未写入空间或越界
            }

            long offset = writeBuffer.getLong(pos + 4);
            long timestamp = writeBuffer.getLong(pos + 12);
            short topicLen = writeBuffer.getShort(pos + 20);
            if (topicLen < 0 || pos + 22 + topicLen > segmentMaxBytes) {
                break;
            }
            byte[] topicBytes = new byte[topicLen];
            ((Buffer) writeBuffer).position(pos + 22);
            writeBuffer.get(topicBytes);
            String topic = new String(topicBytes, StandardCharsets.UTF_8);

            short sessionLen = writeBuffer.getShort();
            if (sessionLen < 0) {
                break;
            }
            byte[] sessionBytes = new byte[sessionLen];
            writeBuffer.get(sessionBytes);
            String sessionId = new String(sessionBytes, StandardCharsets.UTF_8);

            metas.add(new MessageMeta(offset, pos, timestamp, topic, sessionId));
            pos += totalLen;
        }

        writePosition = pos;
        messageCount = metas.size();
        if (!metas.isEmpty()) {
            maxOffset = metas.get(metas.size() - 1).offset;
        }
        lastIndexedPosition = writePosition;
    }

    // ========== 状态查询 ==========

    public boolean isFull() {
        return writePosition >= segmentMaxBytes;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public long getWritePosition() {
        return writePosition;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public boolean contains(long offset) {
        return offset >= baseOffset && offset <= maxOffset;
    }

    /**
     * 刷盘
     */
    public void flush() {
        writeBuffer.force();
    }

    @Override
    public synchronized void close() throws IOException {
        flush();
        index.close();
        logChannel.close();
        logRaf.close();
    }

    // ========== 工具 ==========

    private byte[] getBytes(String str) {
        if (str == null || str.isEmpty()) {
            return new byte[0];
        }
        return str.getBytes(StandardCharsets.UTF_8);
    }

    // ========== DTO ==========

    public static class MessageMeta {
        public final long offset;
        public final int position;
        public final long timestamp;
        public final String topic;
        public final String sessionId;

        public MessageMeta(long offset, int position, long timestamp, String topic, String sessionId) {
            this.offset = offset;
            this.position = position;
            this.timestamp = timestamp;
            this.topic = topic;
            this.sessionId = sessionId;
        }
    }
}
