package com.zifang.z.team.msg.stress;

import com.zifang.util.json.JsonUtil;
import com.zifang.z.team.msg.common.protocol.AgentPayload;
import com.zifang.z.team.msg.common.protocol.HubMessage;
import com.zifang.z.team.msg.common.protocol.Topics;
import com.zifang.z.team.msg.routing.InMemoryMsgHub;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * InMemoryMsgHub 压力测试
 * <p>
 * 测试维度：
 * 1. 单线程大批量消息吞吐（10K msgs × 10 subs）
 * 2. 多线程并发发布（4 threads × 2500 msgs × 10 subs）
 * 3. 多 session 隔离下的吞吐（100 sessions × 100 msgs）
 * 4. 延迟分布（1000 msgs, p50/p99）
 * <p>
 * 每个 @Test 方法独立使用新的 hub 实例，避免 messageLog 跨测试累积。
 */
public class InMemoryMsgHubStressTest {

    private InMemoryMsgHub hub;

    @BeforeEach
    public void setUp() {
        hub = new InMemoryMsgHub("stress-" + System.nanoTime());
    }

    @AfterEach
    public void tearDown() {
        if (hub != null) {
            hub.destroy();
        }
    }

    // ========== 吞吐量测试 ==========

    /**
     * 测试1：单线程 10K 消息 × 10 订阅者 → 吞吐量
     * <p>
     * 预构建所有消息（排除 JSON 序列化开销），纯粹测路由 + 分发性能。
     */
    @Test
    public void testSingleThreadThroughput() {
        int subCount = 10;
        int msgCount = 10_000;

        // 订阅
        AtomicLong[] counters = new AtomicLong[subCount];
        for (int i = 0; i < subCount; i++) {
            counters[i] = new AtomicLong(0);
            final AtomicLong c = counters[i];
            hub.subscribe(Topics.AGENT_MSG, "session-1", msg -> c.incrementAndGet());
        }

        // 预构建消息
        List<HubMessage> messages = new ArrayList<>(msgCount);
        for (int i = 0; i < msgCount; i++) {
            AgentPayload payload = AgentPayload.of("sender", "msg-" + i, "signal");
            messages.add(HubMessage.of(Topics.AGENT_MSG, "session-1", JsonUtil.toJson(payload)));
        }

        // 压测
        long start = System.nanoTime();
        for (HubMessage msg : messages) {
            hub.publish(msg);
        }
        long elapsed = System.nanoTime() - start;

        // 验证
        for (int i = 0; i < subCount; i++) {
            Assertions.assertEquals(counters[i].get(), msgCount,
                    "Subscriber " + i + " should receive all " + msgCount + " messages");
        }

        // 输出
        double elapsedMs = elapsed / 1_000_000.0;
        double throughput = msgCount / (elapsedMs / 1000.0);
        double deliveriesPerSec = throughput * subCount;
        System.out.printf("[InMemory-Single] %d msgs × %d subs → %.1f ms, %.0f msg/s, %.0f deliveries/s%n",
                msgCount, subCount, elapsedMs, throughput, deliveriesPerSec);

        Assertions.assertTrue(throughput > 5000,
                "Throughput should be > 5000 msg/s, got " + String.format("%.0f", throughput));
    }

    /**
     * 测试2：4 线程并发发布 × 2500 消息 × 10 订阅者
     */
    @Test
    public void testConcurrentPublish() throws Exception {
        int threadCount = 4;
        int msgsPerThread = 2500;
        int subCount = 10;

        AtomicLong totalReceived = new AtomicLong(0);
        for (int i = 0; i < subCount; i++) {
            hub.subscribe(Topics.AGENT_MSG, "session-concurrent", msg -> totalReceived.incrementAndGet());
        }

        ExecutorService exec = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        long start = System.nanoTime();
        for (int t = 0; t < threadCount; t++) {
            final int tid = t;
            exec.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < msgsPerThread; i++) {
                        AgentPayload payload = AgentPayload.of("thread-" + tid, "msg-" + i, "signal");
                        HubMessage msg = HubMessage.of(Topics.AGENT_MSG, "session-concurrent",
                                JsonUtil.toJson(payload));
                        hub.publish(msg);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean done = doneLatch.await(30, TimeUnit.SECONDS);
        long elapsed = System.nanoTime() - start;
        exec.shutdown();

        Assertions.assertTrue(done, "Concurrent publish should complete within 30s");

        long expected = (long) threadCount * msgsPerThread * subCount;
        Assertions.assertEquals(totalReceived.get(), expected,
                "Total deliveries should be " + expected);

        double elapsedMs = elapsed / 1_000_000.0;
        double totalMsgs = (long) threadCount * msgsPerThread;
        double throughput = totalMsgs / (elapsedMs / 1000.0);
        System.out.printf("[InMemory-Concurrent] %d threads × %d msgs × %d subs → %.1f ms, %.0f msg/s, %.0f deliveries/s%n",
                threadCount, msgsPerThread, subCount, elapsedMs, throughput, throughput * subCount);

        Assertions.assertTrue(throughput > 3000,
                "Concurrent throughput should be > 3000 msg/s, got " + String.format("%.0f", throughput));
    }

    /**
     * 测试3：100 个 session，每个 session 1 个订阅者，每个发 100 条
     * <p>
     * 验证多 session 场景下的路由匹配效率。
     */
    @Test
    public void testMultiSessionThroughput() {
        int sessionCount = 100;
        int msgsPerSession = 100;

        AtomicLong totalReceived = new AtomicLong(0);
        for (int s = 0; s < sessionCount; s++) {
            hub.subscribe(Topics.AGENT_MSG, "session-" + s, msg -> totalReceived.incrementAndGet());
        }

        long start = System.nanoTime();
        for (int s = 0; s < sessionCount; s++) {
            String sessionId = "session-" + s;
            for (int i = 0; i < msgsPerSession; i++) {
                AgentPayload payload = AgentPayload.of("sender", "msg-" + i, "signal");
                HubMessage msg = HubMessage.of(Topics.AGENT_MSG, sessionId, JsonUtil.toJson(payload));
                hub.publish(msg);
            }
        }
        long elapsed = System.nanoTime() - start;

        long expected = (long) sessionCount * msgsPerSession;
        Assertions.assertEquals(totalReceived.get(), expected);

        double elapsedMs = elapsed / 1_000_000.0;
        double throughput = expected / (elapsedMs / 1000.0);
        System.out.printf("[InMemory-MultiSession] %d sessions × %d msgs → %.1f ms, %.0f msg/s%n",
                sessionCount, msgsPerSession, elapsedMs, throughput);

        Assertions.assertTrue(throughput > 5000,
                "Multi-session throughput should be > 5000 msg/s, got " + String.format("%.0f", throughput));
    }

    /**
     * 测试4：延迟分布
     * <p>
     * 1000 条消息，记录每条从 publish() 到 handler 回调完成的时间。
     * 内存路由是同步的，所以延迟 = publish() 方法耗时。
     */
    @Test
    public void testLatencyDistribution() {
        int msgCount = 1000;
        long[] latencies = new long[msgCount]; // 纳秒

        hub.subscribe(Topics.AGENT_MSG, "session-lat", msg -> {
            // handler 不做任何事，延迟由 publish() 同步返回后测量
        });

        for (int i = 0; i < msgCount; i++) {
            AgentPayload payload = AgentPayload.of("sender", "msg-" + i, "signal");
            HubMessage msg = HubMessage.of(Topics.AGENT_MSG, "session-lat", JsonUtil.toJson(payload));

            long start = System.nanoTime();
            hub.publish(msg);
            long elapsed = System.nanoTime() - start;
            latencies[i] = elapsed;
        }

        Arrays.sort(latencies);

        long p50 = latencies[msgCount / 2];
        long p99 = latencies[(int) (msgCount * 0.99)];

        System.out.printf("[InMemory-Latency] p50=%.3fμs, p99=%.3fμs, max=%.3fμs%n",
                p50 / 1000.0, p99 / 1000.0, latencies[msgCount - 1] / 1000.0);

        // p99 应小于 1ms (1,000,000 ns)
        Assertions.assertTrue(p99 < 1_000_000,
                "p99 latency should be < 1ms, got " + (p99 / 1000.0) + "μs");
    }
}
