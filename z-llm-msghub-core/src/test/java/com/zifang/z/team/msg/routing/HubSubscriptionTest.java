package com.zifang.z.team.msg.routing;

import com.zifang.z.team.msg.common.protocol.HubMessage;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class HubSubscriptionTest {

    private final Consumer<HubMessage> noopHandler = msg -> {
    };

    @Test
    void matchesWhenTopicAndSessionIdBothMatch() {
        HubSubscription sub = new HubSubscription("AgentMsg", "session-1", noopHandler);
        HubMessage msg = HubMessage.of("AgentMsg", "session-1", "{}");
        assertTrue(sub.matches(msg));
    }

    @Test
    void matchesWhenTopicDoesNotMatch() {
        HubSubscription sub = new HubSubscription("AgentMsg", "session-1", noopHandler);
        HubMessage msg = HubMessage.of("OtherTopic", "session-1", "{}");
        assertFalse(sub.matches(msg));
    }

    @Test
    void matchesWhenSessionIdDoesNotMatch() {
        HubSubscription sub = new HubSubscription("AgentMsg", "session-1", noopHandler);
        HubMessage msg = HubMessage.of("AgentMsg", "session-2", "{}");
        assertFalse(sub.matches(msg));
    }

    @Test
    void nullTopicInConstructorDefaultsToEmpty() {
        HubSubscription sub = new HubSubscription(null, "session-1", noopHandler);
        assertEquals("", sub.topic);
    }

    @Test
    void nullSessionIdInConstructorDefaultsToEmpty() {
        HubSubscription sub = new HubSubscription("AgentMsg", null, noopHandler);
        assertEquals("", sub.sessionId);
    }

    @Test
    void matchesWithEmptyTopicAgainstMessageWithEmptyTopic() {
        HubSubscription sub = new HubSubscription(null, "session-1", noopHandler);
        HubMessage msg = HubMessage.of(null, "session-1", "{}");
        assertTrue(sub.matches(msg));
    }
}
