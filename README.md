# z-llm-msghub

Agent 间消息总线，轻量级 pub/sub 系统。

## 为什么不使用标准 MQ

| 维度   | 标准 MQ (Kafka/RabbitMQ/RocketMQ)           | z-llm-msghub           |
|------|-------------------------------------------|----------------------|
| 部署   | 需要独立集群（Zookeeper/Kafka Broker/NameServer） | 随应用进程启动，无额外组件        |
| 运维   | 需专职运维，topic/broker/分区管理复杂                 | 零运维，Spring Boot 启动即用 |
| 延迟   | 毫秒级（跨进程序列化 + 网络跳转）                        | 微秒级（单 TCP 长连接直连）     |
| 依赖   | 重度依赖（ZK、Broker、客户端库）                      | 仅 Netty + fastjson   |
| 适用场景 | 高吞吐、跨服务、持久化队列                             | Agent 协作、实时信号、会话内通信  |

### 核心理由

1. **场景不需要 MQ 的核心能力**
    - Agent 间通信是低频、低延迟、会话隔离的实时信号，不需要 MQ 的高吞吐、分区、消费组等能力
    - 消息量小（每秒数十条），不需要 Kafka 的百万级 TPS

2. **部署成本**
    - Agent 运行环境可能受限（内网/边缘节点），引入 MQ 集群不可行
    - NFS/PV 共享文件即可解决多实例问题，不需要 MQ 的分布式协调

3. **会话隔离是第一需求**
    - 每个 task 有独立的 sessionId，消息只在同 session 内流转
    - 标准 MQ 的 topic 粒度太粗，按 task 动态创建 topic 会导致 topic 爆炸
    - 本方案用 `topic + sessionId` 双维度路由，topic 固定，session 动态

4. **协议极简**
    - 4 字节长度前缀 + UTF-8 JSON，无 MQ 的协议层开销
    - Agent 端只需一个 TCP 连接，不需要 MQ 客户端的连接池/消费者线程

## 架构

```
┌──────────────────────────────────────────────────┐
│                  msg-starter                     │
│  ┌─────────────┐  ┌──────────────────────────┐  │
│  │ MsgHubServer│  │   ConnectionManager      │  │
│  │ (Netty TCP) │──│   topic+sessionId 路由   │  │
│  │  :9091      │  │                          │  │
│  └─────────────┘  └──────────────────────────┘  │
│  ┌─────────────────────────────────────────────┐ │
│  │ IndexedMessageStore (Kafka式存储)           │ │
│  │ .log 顺序追加 + .idx 稀疏索引               │ │
│  └─────────────────────────────────────────────┘ │
│  数据目录: ${MSGHUB_DATA_DIR} (NFS/PV 共享)      │
└──────────────────────────────────────────────────┘
         ↑ TCP 长连接 (4字节长度前缀 + JSON)
    ┌────┴────┐         ┌────┴────┐
    │msg-client│        │msg-client│
    │ Agent-A  │        │ Observer │
    └─────────┘         └─────────┘
```

### 模块结构

```
z-llm-msghub/
├── msg-common    — 共享协议（HubMessage, Topics, AgentPayload）
├── msg-core      — 内存路由 + 文件存储（IndexedMessageStore）
├── msg-client    — Agent 侧 SDK（MsgHubClient）
└── msg-starter   — Spring Boot + Netty Server
```

### 通信协议

- **传输层**：Netty TCP 长连接
- **帧格式**：4 字节长度前缀 + UTF-8 JSON
- **断连处理**：指数退避自动重连 + 心跳保活 + 断连后自动重新订阅

## 使用方式

### 1. 启动 Server

msg-starter 随 Spring Boot 应用启动，监听 TCP 端口：

```yaml
# application.yaml
msghub:
  enabled: true
  websocket:
    port: 9091
    idle-timeout-seconds: 90
  persistence:
    data-dir: ${MSGHUB_DATA_DIR:/data/msghub}
```

### 2. Agent 侧使用 Client SDK

```java
// 引入 msg-client 依赖后

// 创建客户端（每个 Agent 一个实例）
MsgHubClient client = MsgHubClientFactory.remote("msghub:9091");
client.connect();

// 订阅消息（topic + sessionId 必填）
client.subscribe(Topics.AGENT_MSG, "task-001", msg -> {
    AgentPayload payload = JSON.parseObject(msg.getPayload(), AgentPayload.class);
    System.out.println(payload.getFrom() + ": " + payload.getContent());
});

// 发布消息
client.publish(Topics.AGENT_MSG, "task-001",
    AgentPayload.of("agent-A", "hello", "signal"));

// 关闭
client.close();
```

### 3. Observer 使用

```java
MsgHubClient observer = MsgHubClientFactory.remote("msghub:9091");
observer.connect();

// Observer 也必须指定 sessionId，不支持全局监听
observer.subscribe(Topics.AGENT_MSG, "task-001", msg -> {
    AgentPayload payload = JSON.parseObject(msg.getPayload(), AgentPayload.class);
    // 观察指定会话内的所有消息
});

observer.close();
```

## 消息协议

### HubMessage

```json
{
  "messageId": "a1b2c3d4e5f6",
  "sessionId": "task-001",
  "topic": "AgentMsg",
  "payload": "{\"from\":\"agent-A\",\"content\":\"hello\",\"action\":\"signal\"}",
  "createdAt": "2026-07-02T10:30:00"
}
```

| 字段        | 说明                                  |
|-----------|-------------------------------------|
| messageId | 消息唯一 ID（UUID）                       |
| sessionId | 会话 ID（路由维度）                         |
| topic     | 消息类型（当前固定 `AgentMsg`，可扩展）           |
| payload   | JSON 字符串，按 topic 绑定的 Payload 类型反序列化 |
| createdAt | 创建时间                                |

### AgentPayload

`Topics.AGENT_MSG` 绑定的 Payload 类型：

| 字段      | 说明                                     |
|---------|----------------------------------------|
| from    | 发送者标识                                  |
| content | 消息内容                                   |
| action  | 语义动作（signal / completed / error / ...） |

### Topic 与 Payload 绑定

每个 Topic 绑定唯一 Payload 类型，接收方按 topic 反序列化：

```
Topics.AGENT_MSG  →  AgentPayload
Topics.XXX（未来）  →  XxxPayload（配套新增）
```

### 路由规则

消息投递按 `topic + sessionId` 双维度精确匹配：

- 发布方发布 `topic=AgentMsg, sessionId=task-001`
- 服务端找到所有订阅了 `(AgentMsg, task-001)` 的连接
- 投递给除发送方外所有匹配的连接
- 不同 sessionId 的订阅者不会收到

### 内部控制消息

| Topic           | 方向              | 说明                                 |
|-----------------|-----------------|------------------------------------|
| `hub.subscribe` | client → server | 订阅命令，payload 含目标 topic + sessionId |
| `hub.heartbeat` | client → server | 心跳保活                               |

## 存储模型

IndexedMessageStore 采用 Kafka 式设计：

```
${MSGHUB_DATA_DIR}/messages/
├── 00000000000000000000.log    # segment: 顺序追加
├── 00000000000000000000.idx    # sparse index: 二分查找
├── 00000000000000100000.log    # segment 滚动
└── 00000000000000100000.idx
```

- **写入**：O(1) 顺序追加，mmap 直写，OS page cache
- **查找 by offset**：O(log N) 二分查找 .idx + 顺序扫描
- **查找 by topic+session**：内存 topic→offset 映射 + 逐条 sessionId 过滤
- **恢复**：重启后扫描 .log 重建内存索引
- **Segment 滚动**：默认 64MB 切分新 segment
- **稀疏索引**：默认每 4KB 一条索引

### 分布式方案

多实例共享 `${MSGHUB_DATA_DIR}`（NFS/PV 挂载），不引入 Redis 或独立 MQ 集群。

## 测试

### 编译 & 运行

```bash
# 编译全部模块
mvn clean compile -pl z-llm-msghub/z-llm-msghub-common,z-llm-msghub/z-llm-msghub-core,z-llm-msghub/z-llm-msghub-client,z-llm-msghub/z-llm-msghub-starter

# 运行 core 测试（30 个：单元测试 26 + 压力测试 4）
mvn test -pl z-llm-msghub/z-llm-msghub-core

# 运行 starter 测试（8 个：E2E 4 + TCP 压力测试 4）
mvn test -pl z-llm-msghub/z-llm-msghub-starter
```

### 测试矩阵

| 层级  | 模块          | 测试类                      | 测试数 | 说明                   |
|-----|-------------|--------------------------|-----|----------------------|
| 单元  | msg-core    | InMemoryMsgHubTest       | 7   | 路由匹配、session 隔离、异常处理 |
| 单元  | msg-core    | IndexedMessageStoreTest  | 11  | 存储追加、索引、滚动、恢复        |
| 单元  | msg-core    | JsonRouteStoreTest       | 8   | 路由表持久化、加载            |
| 压力  | msg-core    | InMemoryMsgHubStressTest | 4   | 吞吐、并发、多 session、延迟   |
| E2E | msg-starter | MsgHubE2ETest            | 4   | TCP 转发、隔离、断连、计数      |
| 压力  | msg-starter | MsgHubTcpStressTest      | 4   | TCP 吞吐、扇出、并发、延迟      |

### 压力测试报告

#### 内存路由层（InMemoryMsgHub）

| 测试场景               | 消息数    | 耗时     | 吞吐量               | 断言阈值      |
|--------------------|--------|--------|-------------------|-----------|
| 单线程 10K×10subs     | 10,000 | 21.7ms | **460,304 msg/s** | > 5,000   |
| 并发 4线程×2500×10subs | 10,000 | 85.1ms | **117,506 msg/s** | > 3,000   |
| 多Session 100×100   | 10,000 | 67.0ms | **149,183 msg/s** | > 5,000   |
| 延迟 p50/p99         | 1,000  | -      | 0.3μs / 3.7μs     | p99 < 1ms |

- 单线程分发 10K 消息给 10 个订阅者，总投递量 10 万次，耗时 22ms
- 并发 4 线程同时发布，CopyOnWriteArrayList 读端零开销
- 多 session 场景验证路由匹配效率（100 session × 100 msg）

#### TCP 端到端层（Netty）

| 测试场景       | 消息数   | 耗时    | 吞吐量                                  | 断言阈值        |
|------------|-------|-------|--------------------------------------|-------------|
| 1→1 吞吐     | 5,000 | 210ms | **23,805 msg/s**                     | > 500       |
| 1→10 扇出    | 1,000 | 108ms | **9,269 msg/s** (92.7K deliveries/s) | > 200       |
| 5→1 并发     | 5,000 | 146ms | **34,161 msg/s**                     | > 500       |
| 延迟 p50/p99 | 100   | -     | 1.3ms / 3.9ms                        | p99 < 100ms |

- 完整链路：Client → TCP → Server(反序列化→路由→序列化) → TCP → Client
- 1→10 扇出：单条消息经服务端路由分发至 10 个接收方，总投递 1 万次
- 5→1 并发：5 个发送者同时 publish，验证服务端并发处理能力
- 延迟 p99 < 4ms，满足 Agent 实时协作需求

### 结论

- **内存路由**：单线程 46 万 msg/s，延迟亚微秒级，无瓶颈
- **TCP 端到端**：2.4 万 msg/s，延迟 p99 < 4ms，满足 Agent 间通信需求
- **无需优化**：所有指标远超断言阈值

## 部署

### Dev 环境部署

通过 Matrix 发布平台触发 dev 环境构建+部署，直接执行脚本：

```bash
bash _DEVELOP/_SKILL_CRAFT/deploy-msg-dev.sh
```

脚本内容即调用 Matrix API，关键参数：

| 参数           | 值                                | 说明        |
|--------------|----------------------------------|-----------|
| masterBranch | `master`                         | 发布分支      |
| envCodes     | `base-dev`                       | 目标环境（dev） |
| deployModel  | `online`                         | 在线部署      |
| pipelineCode | `z-llm-msghub_dev_defaultPipeline` | dev 环境流水线 |
| buildType    | `beServerBuild`                  | 后端服务构建    |
