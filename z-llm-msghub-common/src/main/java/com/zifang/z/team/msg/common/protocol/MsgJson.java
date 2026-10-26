package com.zifang.z.team.msg.common.protocol;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.zifang.util.json.JsonUtil;

/**
 * msgHub 报文解析（z-util 优先，宽松 Jackson 兜底）。
 * <p>
 * 客户端（本地 Agent / 桌面端）常把 pretty-print 的 JSON 直接拼进 payload 字符串，
 * 这类帧语义完整、但字符串值里带裸换行，不合 RFC 8259；z-util 的 JSON DSL 会抛
 * {@code Rule 'json' failed to match}，导致整帧在入口处被静默丢弃。
 * 兜底 mapper 放开 ALLOW_UNESCAPED_CONTROL_CHARS 再解一次。
 */
public final class MsgJson {

    private static final ObjectMapper TOLERANT_MAPPER = JsonMapper.builder()
            .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private MsgJson() {
    }

    public static <T> T fromJson(String text, Class<T> type) {
        try {
            T value = JsonUtil.fromJson(text, type);
            if (value != null) {
                return value;
            }
        } catch (RuntimeException ignored) {
            // 落到宽松解析
        }
        try {
            return TOLERANT_MAPPER.readValue(text, type);
        } catch (Exception e) {
            throw new IllegalStateException("msgHub 报文解析失败: " + type.getSimpleName(), e);
        }
    }
}
