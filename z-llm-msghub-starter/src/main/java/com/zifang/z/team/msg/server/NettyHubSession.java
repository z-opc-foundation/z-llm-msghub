package com.zifang.z.team.msg.server;

import com.zifang.z.team.msg.session.HubSession;
import io.netty.channel.Channel;

import java.net.InetSocketAddress;

/**
 * Netty Channel → HubSession 适配器
 */
public class NettyHubSession implements HubSession {

    private final Channel channel;

    public NettyHubSession(Channel channel) {
        this.channel = channel;
    }

    @Override
    public String getId() {
        return channel.id().asLongText();
    }

    @Override
    public void sendText(String text) {
        channel.writeAndFlush(text);
    }

    @Override
    public boolean isActive() {
        return channel.isActive();
    }

    @Override
    public void close() {
        channel.close();
    }

    @Override
    public String getRemoteAddr() {
        if (channel.remoteAddress() instanceof InetSocketAddress) {
            return ((InetSocketAddress) channel.remoteAddress()).toString();
        }
        return channel.remoteAddress().toString();
    }

    public Channel getChannel() {
        return channel;
    }
}
