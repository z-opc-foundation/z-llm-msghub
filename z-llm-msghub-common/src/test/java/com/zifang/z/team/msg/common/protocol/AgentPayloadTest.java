package com.zifang.z.team.msg.common.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AgentPayloadTest {

    @Test
    void ofWithAllParams() {
        AgentPayload payload = AgentPayload.of("agent-A", "hello", "signal");
        assertEquals("agent-A", payload.getFrom());
        assertEquals("hello", payload.getContent());
        assertEquals("signal", payload.getAction());
    }

    @Test
    void ofWithNullParamsReturnsEmptyStrings() {
        AgentPayload payload = AgentPayload.of(null, null, null);
        assertEquals("", payload.getFrom());
        assertEquals("", payload.getContent());
        assertEquals("", payload.getAction());
    }

    @Test
    void settersWithNullReturnEmptyStrings() {
        AgentPayload payload = new AgentPayload();
        payload.setFrom(null);
        payload.setContent(null);
        payload.setAction(null);
        assertEquals("", payload.getFrom());
        assertEquals("", payload.getContent());
        assertEquals("", payload.getAction());
    }

    @Test
    void toStringContainsKeyFields() {
        AgentPayload payload = AgentPayload.of("agent-A", "hello", "signal");
        String str = payload.toString();
        assertTrue(str.contains("agent-A"));
        assertTrue(str.contains("hello"));
        assertTrue(str.contains("signal"));
    }
}
