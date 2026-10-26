package com.zifang.z.team.msg.server;

import com.zifang.util.json.JsonUtil;
import com.zifang.util.json.model.JsonObject;
import com.zifang.z.team.msg.common.protocol.HubMessage;
import com.zifang.z.team.msg.common.protocol.MsgJson;
import com.zifang.z.team.msg.common.protocol.Topics;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * msgHub 消息处理核心（Netty 侧）
 * <p>
 * 处理 TCP 文本消息（JSON）：
 * - hub.subscribe: 客户端订阅 topic + sessionId
 * - hub.heartbeat: 心跳
 * - 其他 topic: 路由转发（按 topic + sessionId 匹配订阅）
 */
public class MsgHubHandler extends SimpleChannelInboundHandler<String> {

    private static final Logger log = LogManager.getLogger(MsgHubHandler.class);

    private final ConnectionManager connManager;

    public MsgHubHandler(ConnectionManager connManager) {
        this.connManager = connManager;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String text) {
        HubMessage msg;
        try {
            msg = MsgJson.fromJson(text, HubMessage.class);
        } catch (Exception e) {
            log.warn("无法解析消息: {}", text.length() > 200 ? text.substring(0, 200) : text, e);
            return;
        }

        if (msg.getTopic() == null || msg.getTopic().isEmpty()) {
            log.warn("消息缺少 topic，忽略");
            return;
        }

        String channelId = ctx.channel().id().asLongText();

        switch (msg.getTopic()) {
            case Topics.HUB_SUBSCRIBE:
                handleSubscribe(channelId, msg);
                break;
            case Topics.HUB_HEARTBEAT:
                connManager.refreshHeartbeat(channelId);
                break;
            default:
                connManager.route(msg, channelId);
                break;
        }
    }

    private void handleSubscribe(String channelId, HubMessage msg) {
        try {
            JsonObject subInfo = JsonUtil.parseObject(msg.getPayload());
            String topic = subInfo.getString("topic");
            String sessionId = subInfo.getString("sessionId");

            if (topic == null || sessionId == null) {
                log.warn("订阅消息缺少 topic 或 sessionId，忽略");
                return;
            }

            connManager.addSubscription(channelId, topic, sessionId);
            log.info("Subscription added: channel={}, topic={}, sessionId={}", channelId, topic, sessionId);
        } catch (Exception e) {
            log.warn("解析订阅消息失败: {}", e.getMessage());
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        String channelId = ctx.channel().id().asLongText();
        connManager.onConnect(channelId, new NettyHubSession(ctx.channel()));
        log.debug("Connection established: {}", ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        String channelId = ctx.channel().id().asLongText();
        connManager.onDisconnect(channelId);
        log.debug("Connection closed: {}", ctx.channel().remoteAddress());
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            log.info("Idle timeout, disconnecting: {}", ctx.channel().remoteAddress());
            connManager.onDisconnect(ctx.channel().id().asLongText());
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Handler exception from {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}
