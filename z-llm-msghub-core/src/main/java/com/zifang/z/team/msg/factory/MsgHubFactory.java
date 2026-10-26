package com.zifang.z.team.msg.factory;

import com.zifang.z.team.msg.routing.MsgHub;

/**
 * MsgHub 工厂
 * <p>
 * 统一入口，通过切换 Provider 实现内存/远端模式切换。
 * 默认使用 InMemoryMsgHubProvider（内存模式）。
 */
public final class MsgHubFactory {

    private static MsgHubProvider provider = new InMemoryMsgHubProvider();

    private MsgHubFactory() {
    }

    /**
     * 创建 MsgHub 实例
     */
    public static MsgHub create(String hubId) {
        return provider.create(hubId);
    }

    /**
     * 创建默认 MsgHub 实例
     */
    public static MsgHub create() {
        return provider.create(null);
    }

    /**
     * 获取当前 Provider
     */
    public static MsgHubProvider getProvider() {
        return provider;
    }

    /**
     * 切换 Provider
     */
    public static void setProvider(MsgHubProvider p) {
        if (p != null) {
            provider = p;
        }
    }
}
