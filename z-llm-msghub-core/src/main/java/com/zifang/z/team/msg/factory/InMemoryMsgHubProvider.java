package com.zifang.z.team.msg.factory;

import com.zifang.z.team.msg.routing.InMemoryMsgHub;
import com.zifang.z.team.msg.routing.MsgHub;

/**
 * 内存 MsgHub Provider（默认实现）
 */
public class InMemoryMsgHubProvider implements MsgHubProvider {

    @Override
    public MsgHub create(String hubId) {
        return new InMemoryMsgHub(hubId);
    }
}
