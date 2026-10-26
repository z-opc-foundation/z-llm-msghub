package com.zifang.z.team.msg.common.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MsgJsonTest {

    /**
     * 线上真实形态：整帧 pretty-print，payload 字符串值里带裸换行（不合 RFC 8259）。
     */
    private static final String RAW_NEWLINE_FRAME = "{\n"
            + "  \"messageId\": \"b2f900ca0e3942cdb02f78a43f25fc57\",\n"
            + "  \"sessionId\": \"2100376666305081345\",\n"
            + "  \"topic\": \"AgentMsg\",\n"
            + "  \"payload\": \"{\n"
            + "    \\\"from\\\": \\\"79a8989a\\\",\n"
            + "    \\\"content\\\": \\\"hello\\\"\n"
            + "  }\"\n"
            + "}";

    @Test
    void parsesWellFormedFrame() {
        HubMessage msg = MsgJson.fromJson(
                "{\"topic\":\"AgentMsg\",\"sessionId\":\"s1\",\"payload\":\"p\"}", HubMessage.class);
        assertEquals("AgentMsg", msg.getTopic());
        assertEquals("s1", msg.getSessionId());
        assertEquals("p", msg.getPayload());
    }

    @Test
    void fallsBackForRawNewlineFrameAndNestedPayload() {
        HubMessage msg = MsgJson.fromJson(RAW_NEWLINE_FRAME, HubMessage.class);
        assertEquals("b2f900ca0e3942cdb02f78a43f25fc57", msg.getMessageId());
        assertEquals("2100376666305081345", msg.getSessionId());
        assertEquals("AgentMsg", msg.getTopic());

        AgentPayload payload = MsgJson.fromJson(msg.getPayload(), AgentPayload.class);
        assertEquals("79a8989a", payload.getFrom());
        assertEquals("hello", payload.getContent());
    }

    @Test
    void throwsForUnrecoverableText() {
        assertThrows(Exception.class, () -> MsgJson.fromJson("{ this is not json", HubMessage.class));
    }
}
