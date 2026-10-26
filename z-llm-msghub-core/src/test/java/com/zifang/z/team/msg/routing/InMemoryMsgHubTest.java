package com.zifang.z.team.msg.routing;

import com.zifang.util.json.JsonUtil;
import com.zifang.z.team.msg.common.protocol.AgentPayload;
import com.zifang.z.team.msg.common.protocol.HubMessage;
import com.zifang.z.team.msg.common.protocol.Topics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * InMemoryMsgHub 单元测试
 * <p>
 * 测试用例覆盖：
 * - publish / subscribe / dispatch
 * - topic + sessionId 双维度精确匹配
 * - session 隔离
 * - 异常隔离
 * - destroy
 */
public class InMemoryMsgHubTest {

    private InMemoryMsgHub hub;

    @BeforeEach
    public void setUp() {
        hub = new InMemoryMsgHub("test-hub");
    }

    // ========== publish ==========

    @Test
    public void testPublishReturnsMessageWithId() {
        HubMessage msg = HubMessage.of(Topics.AGENT_MSG, "session-1", "{}");
        hub.publish(msg);
        assertNotNull(msg.getMessageId());
        assertFalse(msg.getMessageId().isEmpty());
    }

    // ========== subscribe & dispatch ==========

    @Test
    public void testSubscribeReceivesMatchingMessage() {
        List<HubMessage> received = new ArrayList<>();
        hub.subscribe(Topics.AGENT_MSG, "session-1", msg -> received.add(msg));

        HubMessage msg = HubMessage.of(Topics.AGENT_MSG, "session-1", "{}");
        hub.publish(msg);

        assertEquals(received.size(), 1);
    }

    @Test
    public void testSubscribeDifferentTopicDoesNotReceive() {
        List<HubMessage> received = new ArrayList<>();
        hub.subscribe(Topics.AGENT_MSG, "session-1", msg -> received.add(msg));

        hub.publish(HubMessage.of("OtherTopic", "session-1", "{}"));

        assertEquals(received.size(), 0);
    }

    @Test
    public void testSubscribeDifferentSessionDoesNotReceive() {
        List<HubMessage> received = new ArrayList<>();
        hub.subscribe(Topics.AGENT_MSG, "session-1", msg -> received.add(msg));

        hub.publish(HubMessage.of(Topics.AGENT_MSG, "session-2", "{}"));

        assertEquals(received.size(), 0);
    }

    @Test
    public void testMultipleSubscriptionsSameTopicAndSession() {
        AtomicInteger count1 = new AtomicInteger(0);
        AtomicInteger count2 = new AtomicInteger(0);
        hub.subscribe(Topics.AGENT_MSG, "session-1", msg -> count1.incrementAndGet());
        hub.subscribe(Topics.AGENT_MSG, "session-1", msg -> count2.incrementAndGet());

        hub.publish(HubMessage.of(Topics.AGENT_MSG, "session-1", "{}"));

        assertEquals(count1.get(), 1);
        assertEquals(count2.get(), 1);
    }

    @Test
    public void testHandlerExceptionDoesNotBlockOtherSubscribers() {
        AtomicBoolean secondCalled = new AtomicBoolean(false);
        hub.subscribe(Topics.AGENT_MSG, "session-1", msg -> {
            throw new RuntimeException("boom");
        });
        hub.subscribe(Topics.AGENT_MSG, "session-1", msg -> secondCalled.set(true));

        hub.publish(HubMessage.of(Topics.AGENT_MSG, "session-1", "{}"));

        assertTrue(secondCalled.get());
    }

    // ========== AgentPayload 集成 ==========

    @Test
    public void testPublishWithAgentPayload() {
        AtomicReference<HubMessage> received = new AtomicReference<>();
        hub.subscribe(Topics.AGENT_MSG, "task-001", msg -> received.set(msg));

        AgentPayload payload = AgentPayload.of("agent-A", "hello", "signal");
        hub.publish(HubMessage.of(Topics.AGENT_MSG, "task-001", JsonUtil.toJson(payload)));

        assertNotNull(received.get());
        AgentPayload parsed = JsonUtil.fromJson(received.get().getPayload(), AgentPayload.class);
        assertEquals(parsed.getFrom(), "agent-A");
        assertEquals(parsed.getContent(), "hello");
        assertEquals(parsed.getAction(), "signal");
    }

    // ========== session 隔离 ==========

    @Test
    public void testSessionIsolation() {
        List<HubMessage> session1Received = new ArrayList<>();
        List<HubMessage> session2Received = new ArrayList<>();

        hub.subscribe(Topics.AGENT_MSG, "session-1", msg -> session1Received.add(msg));
        hub.subscribe(Topics.AGENT_MSG, "session-2", msg -> session2Received.add(msg));

        hub.publish(HubMessage.of(Topics.AGENT_MSG, "session-1", "{}"));
        hub.publish(HubMessage.of(Topics.AGENT_MSG, "session-2", "{}"));

        assertEquals(session1Received.size(), 1);
        assertEquals(session2Received.size(), 1);
    }

    // ========== destroy ==========

    @Test
    public void testDestroyClearsSubscriptions() {
        hub.subscribe(Topics.AGENT_MSG, "session-1", msg -> {
        });
        assertEquals(hub.getSubscriptionCount(), 1);

        hub.destroy();

        assertEquals(hub.getSubscriptionCount(), 0);
    }

    @Test
    public void testDestroyClearsMessageLog() {
        hub.publish(HubMessage.of(Topics.AGENT_MSG, "session-1", "{}"));
        assertEquals(hub.getMessageLogCount(), 1);

        hub.destroy();

        assertEquals(hub.getMessageLogCount(), 0);
    }

    // ========== messageLog ==========

    @Test
    public void testPublishAppendsToMessageLog() {
        hub.publish(HubMessage.of(Topics.AGENT_MSG, "session-1", "{}"));
        hub.publish(HubMessage.of(Topics.AGENT_MSG, "session-2", "{}"));

        assertEquals(hub.getMessageLogCount(), 2);
    }

    // ========== MsgHubFactory ==========

    @Test
    public void testFactoryDefaultReturnsInMemoryMsgHub() {
        MsgHub h = com.zifang.z.team.msg.factory.MsgHubFactory.create("test");
        assertNotNull(h);
        assertTrue(h instanceof InMemoryMsgHub);
    }
}
