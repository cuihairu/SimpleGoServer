---
layout: doc
title: 术语表
---

# 术语表

本术语表使用[架构分层](/zh/architecture)定义的抽象层；该页有反向链接。

| 术语 | 英文 | 定义 | 证据 |
| --- | --- | --- | --- |
| 应用生命周期 | Application lifecycle | 把 CLI/YAML 输入装配成可运行进程，并负责启动、信号处理和关闭。 | `http-service/main.go:29`, `cmd/server/main.go:48`, `cmd/cli/client.go:39` |
| HTTP 键值服务 | HTTP KV service | 独立的标准库 HTTP 进程，提供 put/get/delete 和健康路由。 | `http-service/main.go:29`, `http-service/handler.go:14`, `http-service/service.go:53` |
| TCP Reactor 服务 | TCP Reactor server | 独立示例进程，接受 TCP 连接、分发给 Worker 并提供自定义帧协议。 | `cmd/server/main.go:48`, `pkg/reactor/reactor.go:42`, `pkg/reactor/reactor.go:92` |
| 传输接入 | Transport access | `net/http` 或 Reactor 拥有的 accept/read/write 入口；不判断领域有效性。 | `http-service/handler.go:14`, `pkg/reactor/reactor.go:105`, `pkg/reactor/worker.go:206` |
| Worker 组 | Worker group | 管理 Worker 注册、连接注册表访问，并经 channel 分发到选中的 Worker。 | `pkg/reactor/worker.go:40`, `pkg/reactor/worker.go:52`, `pkg/reactor/worker.go:91` |
| Worker | Worker | 一个拥有连接队列和活动连接数的事件循环；其 goroutine 驱动连接 pipeline。 | `pkg/reactor/worker.go:18`, `pkg/reactor/worker.go:179`, `pkg/reactor/worker.go:206` |
| 连接注册表 | Connection registry | 跟踪打开连接，并把注册与关闭清扫的竞态串行化，确保 handler 不躲过清理。 | `pkg/reactor/registry.go:12`, `pkg/reactor/registry.go:33`, `pkg/reactor/registry.go:62` |
| 消息管线 | Message pipeline | 每连接的有序 handler 链，向内传播 active/read/exception/inactive，向外传播 write。 | `pkg/handler/handler.go:52`, `internal/handler/linked_pipeline.go:90`, `internal/handler/node_context.go:103` |
| 帧编解码器 | Frame codec | pipeline handler，把连接字节流转成逻辑帧、把逻辑帧转回字节，包括流重组。 | `pkg/proto/protocol.go:765`, `pkg/proto/protocol.go:812`, `pkg/proto/protocol.go:842` |
| 帧协议 | Frame protocol | 带 10 字节头 + JSON payload 的版本化契约，提供请求响应、发布订阅、保活和优雅关闭。 | `pkg/proto/frame.go:33`, `pkg/proto/protocol.go:32`, `pkg/proto/protocol.go:41` |
| 协议处理器 | Protocol handler | 服务端有状态语义层，处理 HELLO、请求、订阅、主题重放和发布。 | `pkg/proto/protocol.go:76`, `pkg/proto/protocol.go:285`, `pkg/proto/protocol.go:644` |
| 协议会话 | Protocol session | 一个逻辑客户端的服务端记忆：当前连接、主题和 last-seen；可超越传输连接存活。 | `pkg/proto/protocol.go:148`, `pkg/proto/protocol.go:345`, `pkg/proto/protocol.go:406` |
| 主题重放缓存 | Topic replay cache | 按主题和总字节有界的已编码发布记忆，用于恢复会话补齐消息。 | `pkg/proto/protocol.go:124`, `pkg/proto/protocol.go:133`, `pkg/proto/protocol.go:460` |
| 协议客户端 | Protocol client | 客户端拥有一个 TCP 连接、pending 请求 channel、流分片、保活和关闭。 | `pkg/proto/client.go:81`, `pkg/proto/client.go:266`, `pkg/proto/client.go:225` |
| 可恢复客户端 | Resilient client | 包装协议客户端，断连后恢复令牌/订阅并同步重放游标。 | `pkg/proto/resilient.go:56`, `pkg/proto/resilient.go:367`, `pkg/proto/resilient.go:385` |
| 业务规则 | Business rules | HTTP 专属有效性和结果规则：键语法、单个 JSON 值、大小上限和 sentinel error。 | `http-service/service.go:41`, `http-service/service.go:53`, `http-service/service.go:68` |
| 存储缝 | Store seam | HTTP Service 背后的存储接口；生产实现只有进程内 map。 | `http-service/store.go:19`, `http-service/store.go:29`, `http-service/store.go:38` |
| 均衡器 | Balancer | 已注册后端的通用选择策略；Reactor 默认按实时负载自适应选择。 | `pkg/balancer.go:19`, `pkg/reactor/worker.go:52`, `internal/balancer/adaptive.go:42` |
| 自适应均衡器 | Adaptive balancer | 选择负载最低 Worker，每个负载值只读一次，并在同分间随机分流量。 | `internal/balancer/adaptive.go:26`, `internal/balancer/adaptive.go:42`, `pkg/reactor/worker.go:263` |
| 异步执行器 | Async executor | 固定 worker 池通过无缓冲 channel 路由 action 和返回 Future，用 context 停止。 | `pkg/executor.go:20`, `internal/executor/async_executor.go:38`, `internal/executor/async_executor.go:85` |
| 异步 Future | Async future | 一次性任务结果，带完成/取消视图和超时拒绝；取消不覆盖已完成结果。 | `internal/executor/async_future.go:12`, `internal/executor/async_future.go:33`, `internal/executor/async_future.go:75` |
| Payload 池 | Payload pool | 按大小分级的 frame/payload 回收，明确所有权、清零和幂等 release。 | `pkg/proto/pool.go:10`, `pkg/proto/pool.go:104`, `pkg/proto/pool.go:133` |
| 优雅关闭 | Graceful shutdown | 进程专属清理：HTTP 限时排空；Reactor 关监听、连接排空、强关和停止 Worker。 | `http-service/main.go:72`, `pkg/reactor/reactor.go:156`, `pkg/reactor/reactor.go:177` |
| 信号监听 | Signal listener | OS 信号桥；TCP 使用共享 Graceful，HTTP 使用 `signal.NotifyContext`。 | `pkg/utils/graceful.go:22`, `pkg/utils/graceful.go:45`, `http-service/main.go:55` |
| 事件监听器 | Event listener | Reactor 的生命周期/错误/连接观察者；默认日志，构造时注入。 | `pkg/event/listener.go:5`, `pkg/reactor/reactor.go:42`, `internal/handler/event_listener.go:17` |
| 配置绑定 | Configuration binding | YAML/flag/环境变量到运行行为的映射；HTTP 直接使用 flag。 | `config.example.yml:4`, `cmd/server/main.go:107`, `http-service/main.go:30` |
