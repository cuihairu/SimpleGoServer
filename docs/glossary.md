---
layout: doc
title: Glossary
---

# Glossary

This glossary uses the abstract layers defined in [Architecture and layers](/architecture), which links back here.

| Term | English | Definition | Evidence |
| --- | --- | --- | --- |
| 应用生命周期 | Application lifecycle | Turns CLI/YAML input into a runnable process and owns its startup, signal handling and shutdown. | `http-service/main.go:29`, `cmd/server/main.go:48`, `cmd/cli/client.go:39` |
| HTTP 键值服务 | HTTP KV service | The standalone standard-library HTTP process exposing put/get/delete and health routes. | `http-service/main.go:29`, `http-service/handler.go:14`, `http-service/service.go:53` |
| TCP Reactor 服务 | TCP Reactor server | The standalone demo process that accepts TCP connections, distributes them to workers and serves the custom frame protocol. | `cmd/server/main.go:48`, `pkg/reactor/reactor.go:42`, `pkg/reactor/reactor.go:92` |
| 传输接入 | Transport access | The accept/read/write entry owned by either `net/http` or the Reactor; it does not decide domain validity. | `http-service/handler.go:14`, `pkg/reactor/reactor.go:105`, `pkg/reactor/worker.go:206` |
| Worker 组 | Worker group | Owns worker registration, connection registry access and channel-based dispatch to the selected worker. | `pkg/reactor/worker.go:40`, `pkg/reactor/worker.go:52`, `pkg/reactor/worker.go:91` |
| Worker | Worker | One event loop owning its connection queue and active-connection count; its goroutines drive connection pipelines. | `pkg/reactor/worker.go:18`, `pkg/reactor/worker.go:179`, `pkg/reactor/worker.go:206` |
| 连接注册表 | Connection registry | Tracks open connections and closes registration races with the shutdown sweep so no handler can hide from cleanup. | `pkg/reactor/registry.go:12`, `pkg/reactor/registry.go:33`, `pkg/reactor/registry.go:62` |
| 消息管线 | Message pipeline | Ordered per-connection handler chain propagating active/read/exception/inactive inbound and write outbound. | `pkg/handler/handler.go:52`, `internal/handler/linked_pipeline.go:90`, `internal/handler/node_context.go:103` |
| 帧编解码器 | Frame codec | Pipeline handler converting a connection byte stream to logical frames and logical frames back to bytes, including stream assembly. | `pkg/proto/protocol.go:765`, `pkg/proto/protocol.go:812`, `pkg/proto/protocol.go:842` |
| 帧协议 | Frame protocol | The versioned 10-byte-header/JSON-payload contract with request/response, pub/sub, keepalive and graceful-close features. | `pkg/proto/frame.go:33`, `pkg/proto/protocol.go:32`, `pkg/proto/protocol.go:41` |
| 协议处理器 | Protocol handler | Server-side stateful semantic layer handling HELLO, requests, subscriptions, topic replay and publishing. | `pkg/proto/protocol.go:76`, `pkg/proto/protocol.go:285`, `pkg/proto/protocol.go:644` |
| 协议会话 | Protocol session | Server memory for one logical client: current connection, topics and last-seen time; it can outlive the transport. | `pkg/proto/protocol.go:148`, `pkg/proto/protocol.go:345`, `pkg/proto/protocol.go:406` |
| 主题重放缓存 | Topic replay cache | Bounded per-topic and total-byte memory of encoded publishes used to catch up resumed sessions. | `pkg/proto/protocol.go:124`, `pkg/proto/protocol.go:133`, `pkg/proto/protocol.go:460` |
| 协议客户端 | Protocol client | Client ownership of one TCP connection, pending request channels, stream fragmentation, keepalive and close. | `pkg/proto/client.go:81`, `pkg/proto/client.go:266`, `pkg/proto/client.go:225` |
| 可恢复客户端 | Resilient client | Wraps a protocol client to restore token/subscriptions and replay cursors after connection loss. | `pkg/proto/resilient.go:56`, `pkg/proto/resilient.go:367`, `pkg/proto/resilient.go:385` |
| 业务规则 | Business rules | HTTP-only validity and result mapping: key syntax, single JSON value, size limit and sentinel errors. | `http-service/service.go:41`, `http-service/service.go:53`, `http-service/service.go:68` |
| 存储缝 | Store seam | Storage interface behind HTTP Service; the only production implementation is an in-memory map. | `http-service/store.go:19`, `http-service/store.go:29`, `http-service/store.go:38` |
| 均衡器 | Balancer | Generic selection policy for registered backends; the Reactor defaults to adaptive selection by live load. | `pkg/balancer.go:19`, `pkg/reactor/worker.go:52`, `internal/balancer/adaptive.go:42` |
| 自适应均衡器 | Adaptive balancer | Chooses the least-loaded worker, reads each load value once, and randomly shares traffic among ties. | `internal/balancer/adaptive.go:26`, `internal/balancer/adaptive.go:42`, `pkg/reactor/worker.go:263` |
| 异步执行器 | Async executor | Fixed worker pool routing actions and returned futures through unbuffered channels, with context-based stop. | `internal/executor/contract.go:20`, `internal/executor/async_executor.go:38`, `internal/executor/async_executor.go:85` |
| 异步 Future | Async future | One-shot task result with done/cancel views, timeout rejection and cancellation that does not overwrite a completed result. | `internal/executor/async_future.go:12`, `internal/executor/async_future.go:33`, `internal/executor/async_future.go:75` |
| Payload 池 | Payload pool | Size-classed frame/payload recycling with explicit ownership, zeroing and idempotent release. | `pkg/proto/pool.go:10`, `pkg/proto/pool.go:104`, `pkg/proto/pool.go:133` |
| 优雅关闭 | Graceful shutdown | Process-specific cleanup: HTTP deadline drain; Reactor listener close, connection drain, force close and worker stop. | `http-service/main.go:72`, `pkg/reactor/reactor.go:156`, `pkg/reactor/reactor.go:177` |
| 信号监听 | Signal listener | OS signal bridge; TCP uses the shared Graceful listener while HTTP uses `signal.NotifyContext`. | `pkg/utils/graceful.go:22`, `pkg/utils/graceful.go:45`, `http-service/main.go:55` |
| 事件监听器 | Event listener | Lifecycle/error/connect observer for Reactor; defaults to logging and is injected at construction. | `pkg/event/listener.go:5`, `pkg/reactor/reactor.go:42`, `internal/handler/event_listener.go:17` |
| 配置绑定 | Configuration binding | Runtime input map from YAML/flags/environment to server behavior; HTTP uses flags directly. | `config.example.yml:4`, `cmd/server/main.go:107`, `http-service/main.go:30` |
