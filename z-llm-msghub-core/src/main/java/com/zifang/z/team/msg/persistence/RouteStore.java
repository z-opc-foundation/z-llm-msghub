package com.zifang.z.team.msg.persistence;

import java.util.List;

/**
 * 路由表持久化接口
 * <p>
 * 用于 msgHub-starter 独立部署时，将订阅信息快照到本地文件。
 * 当前阶段（内存模式）不使用，为远端部署预留。
 */
public interface RouteStore {

    /**
     * 保存订阅列表
     */
    void saveSubscriptions(List<SubscriptionSnapshot> subscriptions);

    /**
     * 加载订阅列表
     */
    List<SubscriptionSnapshot> loadSubscriptions();

    // ========== 快照 DTO ==========

    class SubscriptionSnapshot {
        public String topic;
        public String sessionId;

        public SubscriptionSnapshot() {
        }

        public SubscriptionSnapshot(String topic, String sessionId) {
            this.topic = topic;
            this.sessionId = sessionId;
        }
    }
}
