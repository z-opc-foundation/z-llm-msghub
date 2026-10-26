package com.zifang.z.team.msg.store;

import com.zifang.util.json.JsonUtil;
import com.zifang.z.team.msg.common.protocol.AgentPayload;
import com.zifang.z.team.msg.common.protocol.HubMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

/**
 * IndexedMessageStore 单元测试
 * <p>
 * 覆盖场景：
 * 1. 基本写入 + 按 offset 查找
 * 2. 按 topic 查找
 * 3. 按 topic + sessionId 查找
 * 4. 多 topic 混合写入
 * 5. 稀疏索引验证
 * 6. Segment 滚动
 * 7. 重启恢复
 * 8. 统计信息
 *
 * <p><b>2026-10-04 由 TestNG 迁到 JUnit 5。</b>原先整类用 TestNG 注解，而
 * surefire 3.0.0-M5 选的是 JUnitPlatformProvider（根 pom 供的
 * junit-vintage-engine 跑 JUnit 4，不跑 TestNG）⇒ 这 12 个用例一次都没执行过，
 * 构建照样报 {@code Tests run: 0} + SUCCESS。
 *
 * <p>迁移要点：TestNG 的 {@code @Test(dependsOnMethods=...)} 在 JUnit 5 没有
 * 对应机制，改用 {@code @TestMethodOrder(OrderAnnotation.class)} + {@code @Order}
 * 显式排序，顺序严格照搬原依赖图：
 * <pre>
 *   testAppendAndFindByOffset → testAppendMultiple
 *     ├→ testFindByOffsetNotFound / testFindByTopic
 *     ├→ testFindByTopicMaxCountLargerThanSize / testFindByTopicNotExist
 *     └→ testMultiTopicMixed → testFindByTopicAndSession → testSegmentRolling
 *        → testSparseIndexLookup → testStoreStats → testRecovery
 * </pre>
 * 那 4 个叶子只读、都期望 {@code test.topic} 恰好 11 条，而唯一给 test.topic
 * 追加的是 testAppendMultiple，故它们彼此之间、以及与 testMultiTopicMixed 之间
 * 顺序无关——这正是原依赖图允许它们并列的原因，排序里保持并列即可。
 *
 * <p>另外 {@code @BeforeClass/@AfterClass} 在 JUnit 5 对应
 * {@code @BeforeAll/@AfterAll}，二者<b>要求方法为 static</b>，故共享的
 * tempDir/store 字段一并改为 static。
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class IndexedMessageStoreTest {

    private static File tempDir;
    private static IndexedMessageStore store;

    @BeforeAll
    public static void setUp() throws Exception {
        tempDir = Files.createTempDirectory("msghub-test").toFile();
        store = new IndexedMessageStore(tempDir.getAbsolutePath(),
                1024, 128, 4096);
    }

    @AfterAll
    public static void tearDown() {
        if (store != null) {
            store.close();
        }
        deleteRecursively(tempDir);
    }

    private HubMessage createMsg(String topic, String sessionId, String content) {
        AgentPayload payload = AgentPayload.of("agent-1", content, "signal");
        return HubMessage.of(topic, sessionId, JsonUtil.toJson(payload));
    }

    @Test
    @Order(1)
    public void testAppendAndFindByOffset() {
        HubMessage msg = createMsg("test.topic", "session-1", "hello world");
        long offset = store.append(msg);

        Assertions.assertEquals(offset, 0L);

        HubMessage found = store.findByOffset(offset);
        Assertions.assertNotNull(found);
        Assertions.assertEquals(found.getTopic(), "test.topic");
        Assertions.assertEquals(found.getSessionId(), "session-1");
        AgentPayload payload = JsonUtil.fromJson(found.getPayload(), AgentPayload.class);
        Assertions.assertEquals(payload.getContent(), "hello world");
    }

    @Test
    @Order(2)
    public void testAppendMultiple() {
        for (int i = 1; i <= 10; i++) {
            HubMessage msg = createMsg("test.topic", "session-1", "msg-" + i);
            long offset = store.append(msg);
            Assertions.assertEquals(offset, (long) i);
        }

        for (int i = 0; i <= 10; i++) {
            HubMessage found = store.findByOffset(i);
            Assertions.assertNotNull(found, "offset " + i + " not found");
        }
    }

    @Test
    @Order(3)
    public void testFindByOffsetNotFound() {
        HubMessage found = store.findByOffset(99999);
        Assertions.assertNull(found);
    }

    @Test
    @Order(4)
    public void testFindByTopic() {
        List<HubMessage> msgs = store.findByTopic("test.topic", 5);
        Assertions.assertEquals(msgs.size(), 5);
    }

    @Test
    @Order(5)
    public void testFindByTopicMaxCountLargerThanSize() {
        List<HubMessage> msgs = store.findByTopic("test.topic", 100);
        Assertions.assertEquals(msgs.size(), 11);
    }

    @Test
    @Order(6)
    public void testFindByTopicNotExist() {
        List<HubMessage> msgs = store.findByTopic("not.exist", 10);
        Assertions.assertTrue(msgs.isEmpty());
    }

    @Test
    @Order(7)
    public void testMultiTopicMixed() {
        store.append(createMsg("agent.message", "session-1", "a1"));
        store.append(createMsg("team.message", "session-1", "t1"));
        store.append(createMsg("task.create", "session-1", "t2"));

        Assertions.assertEquals(store.getTopicMessageCount("agent.message"), 1);
        Assertions.assertEquals(store.getTopicMessageCount("team.message"), 1);
        Assertions.assertEquals(store.getTopicMessageCount("task.create"), 1);
        Assertions.assertEquals(store.getTopicMessageCount("test.topic"), 11);
    }

    @Test
    @Order(8)
    public void testFindByTopicAndSession() {
        for (int i = 0; i < 5; i++) {
            store.append(createMsg("session.topic", "session-A", "s-" + i));
        }
        for (int i = 0; i < 3; i++) {
            store.append(createMsg("session.topic", "session-B", "s-B-" + i));
        }

        List<HubMessage> msgsA = store.findByTopicAndSession("session.topic", "session-A", 10);
        Assertions.assertEquals(msgsA.size(), 5);

        List<HubMessage> msgsB = store.findByTopicAndSession("session.topic", "session-B", 10);
        Assertions.assertEquals(msgsB.size(), 3);

        List<HubMessage> msgsC = store.findByTopicAndSession("session.topic", "session-C", 10);
        Assertions.assertTrue(msgsC.isEmpty());
    }

    @Test
    @Order(9)
    public void testSegmentRolling() {
        long beforeSegments = store.getStats().segmentCount;

        for (int i = 0; i < 100; i++) {
            store.append(createMsg("roll.test", "session-1", "content-" + i));
        }

        long afterSegments = store.getStats().segmentCount;
        Assertions.assertTrue(afterSegments > beforeSegments,
                "segment count should increase: before=" + beforeSegments + ", after=" + afterSegments);

        List<HubMessage> all = store.findByTopic("roll.test", 100);
        Assertions.assertEquals(all.size(), 100);
    }

    @Test
    @Order(10)
    public void testSparseIndexLookup() {
        for (int i = 0; i < 50; i++) {
            store.append(createMsg("sparse.test", "session-1", "content-" + i));
        }

        long maxOffset = store.getMaxOffset();
        for (int i = 0; i < 50; i++) {
            HubMessage found = store.findByOffset(maxOffset - i);
            Assertions.assertNotNull(found, "offset " + (maxOffset - i) + " should be found");
            Assertions.assertEquals(found.getTopic(), "sparse.test");
        }
    }

    @Test
    @Order(11)
    public void testStoreStats() {
        MessageStore.StoreStats stats = store.getStats();
        Assertions.assertTrue(stats.totalMessages > 0);
        Assertions.assertTrue(stats.maxOffset >= 0);
        Assertions.assertTrue(stats.segmentCount > 0);
        Assertions.assertTrue(stats.totalLogSizeBytes > 0);
        Assertions.assertTrue(stats.topicCount > 0);
    }

    @Test
    @Order(12)
    public void testRecovery() throws Exception {
        long beforeMaxOffset = store.getMaxOffset();
        long beforeTotalMessages = store.getStats().totalMessages;
        int beforeTopicCount = store.getTopicCount();

        store.close();

        store = new IndexedMessageStore(tempDir.getAbsolutePath(), 1024, 128, 4096);

        long afterMaxOffset = store.getMaxOffset();
        long afterTotalMessages = store.getStats().totalMessages;
        int afterTopicCount = store.getTopicCount();

        Assertions.assertEquals(afterMaxOffset, beforeMaxOffset);
        Assertions.assertEquals(afterTotalMessages, beforeTotalMessages);
        Assertions.assertEquals(afterTopicCount, beforeTopicCount);

        HubMessage found = store.findByOffset(0);
        Assertions.assertNotNull(found);
        AgentPayload payload = JsonUtil.fromJson(found.getPayload(), AgentPayload.class);
        Assertions.assertEquals(payload.getContent(), "hello world");
    }

    private static void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }
}
