package com.zifang.z.team.msg.common.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HubMessageTest {

    @Test
    void defaultConstructorHasNonEmptyMessageId() {
        HubMessage msg = new HubMessage();
        assertNotNull(msg.getMessageId());
        assertFalse(msg.getMessageId().isEmpty());
    }

    @Test
    void defaultConstructorHasEmptyPayload() {
        HubMessage msg = new HubMessage();
        assertEquals("", msg.getPayload());
    }

    @Test
    void ofWithAllParams() {
        HubMessage msg = HubMessage.of("AgentMsg", "session-1", "hello");
        assertEquals("AgentMsg", msg.getTopic());
        assertEquals("session-1", msg.getSessionId());
        assertEquals("hello", msg.getPayload());
    }

    @Test
    void ofWithNullParamsReturnsEmptyStrings() {
        HubMessage msg = HubMessage.of(null, null, null);
        assertEquals("", msg.getTopic());
        assertEquals("", msg.getSessionId());
        assertEquals("", msg.getPayload());
    }

    @Test
    void settersWithNullReturnEmptyStrings() {
        HubMessage msg = new HubMessage();
        msg.setTopic(null);
        msg.setSessionId(null);
        msg.setPayload(null);
        msg.setMessageId(null);
        assertEquals("", msg.getTopic());
        assertEquals("", msg.getSessionId());
        assertEquals("", msg.getPayload());
        assertEquals("", msg.getMessageId());
    }

    @Test
    void copyPreservesAllFields() {
        HubMessage original = HubMessage.of("AgentMsg", "session-1", "hello");
        HubMessage copy = original.copy();

        assertEquals(original.getMessageId(), copy.getMessageId());
        assertEquals(original.getTopic(), copy.getTopic());
        assertEquals(original.getSessionId(), copy.getSessionId());
        assertEquals(original.getPayload(), copy.getPayload());
        assertEquals(original.getCreatedAt(), copy.getCreatedAt());
    }

    @Test
    void copyIsDifferentInstance() {
        HubMessage original = HubMessage.of("AgentMsg", "session-1", "hello");
        HubMessage copy = original.copy();

        assertNotSame(original, copy);
    }

    @Test
    void toStringContainsTopicAndSessionId() {
        HubMessage msg = HubMessage.of("AgentMsg", "session-1", "hello");
        String str = msg.toString();
        assertTrue(str.contains("AgentMsg"));
        assertTrue(str.contains("session-1"));
    }
}
