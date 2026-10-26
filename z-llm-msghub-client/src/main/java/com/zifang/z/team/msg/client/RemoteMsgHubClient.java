package com.zifang.z.team.msg.client;

import com.zifang.util.json.JsonUtil;
import com.zifang.util.json.model.JsonObject;
import com.zifang.z.team.msg.common.protocol.AgentPayload;
import com.zifang.z.team.msg.common.protocol.HubMessage;
import com.zifang.z.team.msg.common.protocol.MsgJson;
import com.zifang.z.team.msg.common.protocol.Topics;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Netty WebSocket 客户端实现
 * <p>
 * 协议：WebSocket TextFrame + UTF-8 JSON payload
 * <p>
 * 特性：
 * - 自动重连（指数退避，最大间隔 30s）
 * - 自动心跳（默认 10s 间隔）
 * - 断连恢复后自动重新订阅
 * - 可通过 nginx HTTP 反向代理连接（Upgrade 机制）
 */
public class RemoteMsgHubClient implements MsgHubClient {

    private static final Logger log = LogManager.getLogger(RemoteMsgHubClient.class);

    private final String url;
    private final String host;
    private final int port;
    private final int heartbeatIntervalSeconds;
    private final int maxReconnectDelaySeconds;
    // State
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    // 订阅
    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();
    // 重连
    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);
    // Netty
    private NioEventLoopGroup group;
    private Channel channel;
    private ScheduledExecutorService reconnectExecutor;

    // 心跳
    private ScheduledFuture<?> heartbeatFuture;

    // 连接状态回调
    private Runnable onConnectCallback;
    private Runnable onDisconnectCallback;

    /**
     * @param url WebSocket 地址，如 ws://host:port/path
     */
    public RemoteMsgHubClient(String url) {
        this(url, 5, 30);
    }

    /**
     * @param url                      WebSocket 地址，如 ws://host:port/path
     * @param heartbeatIntervalSeconds 心跳间隔（秒）
     * @param maxReconnectDelaySeconds 最大重连间隔（秒）
     */
    public RemoteMsgHubClient(String url, int heartbeatIntervalSeconds, int maxReconnectDelaySeconds) {
        this.url = url;
        URI uri = URI.create(url);
        this.host = uri.getHost();
        this.port = uri.getPort() > 0 ? uri.getPort() : 80;
        this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
        this.maxReconnectDelaySeconds = maxReconnectDelaySeconds;
    }

    /**
     * 设置连接成功回调（每次重连成功都会触发）
     */
    public void onConnect(Runnable callback) {
        this.onConnectCallback = callback;
    }

    /**
     * 设置断开回调
     */
    public void onDisconnect(Runnable callback) {
        this.onDisconnectCallback = callback;
    }

    // ========== MsgHubClient 接口实现 ==========

    @Override
    public void connect() {
        group = new NioEventLoopGroup(1);
        reconnectExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "msghub-reconnect");
            t.setDaemon(true);
            return t;
        });

        doConnect();
    }

    @Override
    public void subscribe(String topic, String sessionId, Consumer<HubMessage> handler) {
        subscriptions.add(new Subscription(topic, sessionId, handler));

        // 如果已连接，立即发送订阅命令
        if (isConnected()) {
            sendSubscribe(topic, sessionId);
        }
    }

    @Override
    public void publish(String topic, String sessionId, AgentPayload payload) {
        String payloadJson = JsonUtil.toJson(payload != null ? payload : new AgentPayload());
        HubMessage msg = HubMessage.of(topic, sessionId, payloadJson);
        sendText(JsonUtil.toJsonPretty(msg));
    }

    @Override
    public boolean isConnected() {
        return connected.get() && channel != null && channel.isActive();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (heartbeatFuture != null) {
                heartbeatFuture.cancel(false);
                heartbeatFuture = null;
            }

            if (reconnectExecutor != null) {
                reconnectExecutor.shutdownNow();
            }

            if (channel != null) {
                channel.close();
            }

            if (group != null) {
                group.shutdownGracefully();
            }

            connected.set(false);
        }
    }

    // ========== 内部实现 ==========

    private void doConnect() {
        if (closed.get()) {
            return;
        }

        URI uri = URI.create(url);
        WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                uri, WebSocketVersion.V13, null, false, new DefaultHttpHeaders());

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new HttpClientCodec());
                        p.addLast(new HttpObjectAggregator(8192));
                        // 读空闲 20s 触发重连，写空闲 0（不检测），全空闲 0
                        p.addLast(new IdleStateHandler(20, 0, 0, TimeUnit.SECONDS));
                        p.addLast(new WebSocketClientProtocolHandler(handshaker));
                        p.addLast(new ClientHandler());
                    }
                });

        ChannelFuture future = bootstrap.connect(host, port);
        future.addListener((ChannelFutureListener) f -> {
            if (!f.isSuccess()) {
                scheduleReconnect();
            }
        });
    }

    private void scheduleReconnect() {
        if (closed.get()) {
            return;
        }

        connected.set(false);
        if (onDisconnectCallback != null) {
            try {
                onDisconnectCallback.run();
            } catch (Exception ignored) {
            }
        }

        int attempt = reconnectAttempts.incrementAndGet();
        int delay = Math.min(maxReconnectDelaySeconds, (int) Math.pow(2, attempt - 1));

        log.info("[MsgHubClient] Reconnecting in {}s (attempt {})", delay, attempt);

        reconnectExecutor.schedule(this::doConnect, delay, TimeUnit.SECONDS);
    }

    private void startHeartbeat() {
        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(false);
        }

        heartbeatFuture = channel.eventLoop().scheduleAtFixedRate(() -> {
            if (connected.get() && channel.isActive()) {
                // 1. 发送 WebSocket 协议层 Ping 帧（保活穿透网关）
                channel.writeAndFlush(new PingWebSocketFrame());
                // 2. 发送应用层心跳（服务端 IdleStateHandler 检测读活跃）
                HubMessage heartbeat = HubMessage.of(Topics.HUB_HEARTBEAT, "", "");
                sendText(JsonUtil.toJson(heartbeat));
                log.debug("[MsgHubClient] heartbeat sent (interval={}s)", heartbeatIntervalSeconds);
            }
        }, heartbeatIntervalSeconds, heartbeatIntervalSeconds, TimeUnit.SECONDS);
    }

    private void onHandshakeComplete(Channel ch) {
        channel = ch;
        connected.set(true);
        reconnectAttempts.set(0);

        // 重新发送所有订阅
        for (Subscription sub : subscriptions) {
            sendSubscribe(sub.topic, sub.sessionId);
        }

        // 启动心跳
        startHeartbeat();

        // 回调
        if (onConnectCallback != null) {
            try {
                onConnectCallback.run();
            } catch (Exception ignored) {
            }
        }
    }

    private void sendSubscribe(String topic, String sessionId) {
        JsonObject subInfo = new JsonObject();
        subInfo.put("topic", topic);
        subInfo.put("sessionId", sessionId);
        HubMessage msg = HubMessage.of(Topics.HUB_SUBSCRIBE, sessionId, JsonUtil.toJson(subInfo));
        sendText(JsonUtil.toJson(msg));
    }

    private void sendText(String text) {
        if (isConnected()) {
            channel.writeAndFlush(new TextWebSocketFrame(text));
        }
    }

    private void dispatchMessage(HubMessage msg) {
        for (Subscription sub : subscriptions) {
            if (sub.topic.equals(msg.getTopic()) && sub.sessionId.equals(msg.getSessionId())) {
                try {
                    sub.handler.accept(msg);
                } catch (Exception e) {
                    log.warn("[MsgHubClient] Handler error: {}", e.getMessage());
                }
            }
        }
    }

    // ========== 内部类 ==========

    private static class Subscription {
        final String topic;
        final String sessionId;
        final Consumer<HubMessage> handler;

        Subscription(String topic, String sessionId, Consumer<HubMessage> handler) {
            this.topic = topic;
            this.sessionId = sessionId;
            this.handler = handler;
        }
    }

    private class ClientHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
            if (frame instanceof TextWebSocketFrame) {
                String text = ((TextWebSocketFrame) frame).text();
                try {
                    HubMessage hubMsg = MsgJson.fromJson(text, HubMessage.class);
                    if (hubMsg != null && hubMsg.getTopic() != null) {
                        // 忽略心跳回声
                        if (!Topics.HUB_HEARTBEAT.equals(hubMsg.getTopic())) {
                            dispatchMessage(hubMsg);
                        }
                    }
                } catch (Exception e) {
                    log.warn("[MsgHubClient] Parse error: {}", e.getMessage());
                }
            }
            // Pong/Ping/Close 帧由 WebSocketClientProtocolHandler 自动处理
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                onHandshakeComplete(ctx.channel());
            } else if (evt instanceof IdleStateEvent) {
                // 读空闲 = 服务端可能已断开，主动关闭触发重连
                log.warn("[MsgHubClient] Read idle detected, closing to trigger reconnect");
                ctx.close();
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (!closed.get()) {
                log.warn("[MsgHubClient] Connection lost, will reconnect");
                scheduleReconnect();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("[MsgHubClient] Error: {}", cause.getMessage());
            ctx.close();
        }
    }
}
