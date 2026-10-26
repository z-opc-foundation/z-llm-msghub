package com.zifang.z.team.msg.store;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * 稀疏索引（Sparse Index）
 * <p>
 * 参考 Kafka .index 文件设计：
 * <ul>
 *   <li>每条索引固定 24 bytes: [8B offset][8B position][8B timestamp]</li>
 *   <li>稀疏策略：每 indexIntervalBytes 字节的消息数据记录一条索引</li>
 *   <li>查找：二分查找 offset → 返回最近的 position（可能需要顺序扫描少量消息）</li>
 * </ul>
 * <p>
 * 文件格式：连续的 24B 条目，按 offset 升序排列。
 */
public class SparseIndex implements Closeable {

    /**
     * 每条索引条目大小：offset(8) + position(8) + timestamp(8) = 24 bytes
     */
    public static final int ENTRY_SIZE = 24;

    private final File indexFile;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    /**
     * 文件最大容量（条目数）
     */
    private final int maxEntries;
    /**
     * 写入缓冲区
     */
    private MappedByteBuffer writeBuffer;
    /**
     * 当前已写入的条目数
     */
    private volatile int entryCount = 0;

    /**
     * @param indexFile 索引文件
     * @param maxBytes  索引文件最大字节数
     */
    public SparseIndex(File indexFile, long maxBytes) throws IOException {
        this.indexFile = indexFile;
        this.maxEntries = (int) (maxBytes / ENTRY_SIZE);
        boolean exists = indexFile.exists();

        this.raf = new RandomAccessFile(indexFile, "rw");
        this.channel = raf.getChannel();

        // 预分配文件空间
        long fileSize = maxEntries * ENTRY_SIZE;
        this.writeBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);

        if (exists) {
            // 加载已有索引，统计有效条目数
            this.entryCount = countValidEntries();
        }
    }

    /**
     * 添加一条索引
     *
     * @param offset    消息全局 offset
     * @param position  消息在 .log 文件中的物理位置
     * @param timestamp 消息时间戳
     */
    public synchronized void add(long offset, long position, long timestamp) {
        if (entryCount >= maxEntries) {
            // 索引文件已满，需要滚动新 segment
            return;
        }
        int pos = entryCount * ENTRY_SIZE;
        writeBuffer.putLong(pos, offset);
        writeBuffer.putLong(pos + 8, position);
        writeBuffer.putLong(pos + 16, timestamp);
        entryCount++;
    }

    /**
     * 二分查找：找到 <= targetOffset 的最近索引条目
     * <p>
     * 返回该条目对应的 .log 文件物理位置。
     * 调用方从这个位置开始顺序扫描，直到找到精确 offset。
     *
     * @param targetOffset 目标 offset
     * @return 查找结果，包含 position 和 timestamp；找不到返回 null
     */
    public IndexLookupResult lookup(long targetOffset) {
        if (entryCount == 0) {
            return null;
        }

        // 二分查找：找到 <= targetOffset 的最后一个条目
        int low = 0;
        int high = entryCount - 1;
        int result = -1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midOffset = writeBuffer.getLong(mid * ENTRY_SIZE);
            if (midOffset <= targetOffset) {
                result = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        if (result < 0) {
            // targetOffset 比所有索引条目都小，从头开始扫
            return new IndexLookupResult(0L, 0L);
        }

        int pos = result * ENTRY_SIZE;
        long offset = writeBuffer.getLong(pos);
        long position = writeBuffer.getLong(pos + 8);
        long timestamp = writeBuffer.getLong(pos + 16);
        return new IndexLookupResult(position, timestamp);
    }

    /**
     * 获取所有索引条目（用于 topic 索引重建）
     */
    public synchronized List<IndexEntry> getAllEntries() {
        List<IndexEntry> entries = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            int pos = i * ENTRY_SIZE;
            entries.add(new IndexEntry(
                    writeBuffer.getLong(pos),
                    writeBuffer.getLong(pos + 8),
                    writeBuffer.getLong(pos + 16)
            ));
        }
        return entries;
    }

    public int getEntryCount() {
        return entryCount;
    }

    /**
     * 统计已有文件中的有效条目数
     * 从文件头开始扫描，直到遇到 offset == 0 且 position == 0 的空条目
     */
    private int countValidEntries() {
        int count = 0;
        for (int i = 0; i < maxEntries; i++) {
            // 只做绝对读，避免 duplicate()：MappedByteBuffer 的 covariant duplicate()
            // 在 JDK 9 才加入，按 Buffer 声明编译会在 Java 8 运行时抛 NoSuchMethodError。
            long offset = writeBuffer.getLong(i * ENTRY_SIZE);
            if (offset == 0 && i > 0) {
                break;
            }
            count++;
        }
        return count > 0 ? count : 0;
    }

    @Override
    public synchronized void close() throws IOException {
        if (writeBuffer != null) {
            writeBuffer.force();
        }
        channel.close();
        raf.close();
    }

    // ========== 结果 DTO ==========

    public static class IndexLookupResult {
        public final long position;
        public final long timestamp;

        public IndexLookupResult(long position, long timestamp) {
            this.position = position;
            this.timestamp = timestamp;
        }
    }

    public static class IndexEntry {
        public final long offset;
        public final long position;
        public final long timestamp;

        public IndexEntry(long offset, long position, long timestamp) {
            this.offset = offset;
            this.position = position;
            this.timestamp = timestamp;
        }
    }
}
