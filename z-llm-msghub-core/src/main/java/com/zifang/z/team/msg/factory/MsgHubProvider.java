package com.zifang.z.team.msg.factory;

import com.zifang.z.team.msg.routing.MsgHub;

/**
 * MsgHub Provider 接口
 * <p>
 * 实现此接口来提供不同的 MsgHub 实现。
 */
public interface MsgHubProvider {

    MsgHub create(String hubId);
}
