---
layout: doc
title: 架构分层
---

# 架构分层

本分析对应 HEAD `e3dd2e1`。仓库有两个可运行进程形态：HTTP 键值服务是一个二进制（`go run ./http-service`）；TCP Reactor 示例是另一个（`go run ./cmd/server`），配套协议客户端（`go run ./cmd/cli`）。二者互不调用，依赖方向指向接口缝。

逐条术语定义和源码锚点见[术语表](/zh/glossary)。该页也有反向链接。

## 抽象层

1. **应用生命周期**把命令行和 YAML 输入装配成一个可运行服务，并负责信号到收尾的行为。HTTP 二进制在 `http-service/main.go:40` 装配 `Service(MemoryStore)` 和 `http.Server`；TCP 二进制在 `cmd/server/main.go:15` 把 Viper 键和命令行 flag 转成 `ServerOptions`，启动 Reactor 后等待信号；协议 CLI 在 `cmd/cli/client.go:39` 选择普通或可恢复客户端，并运行请求/watch 生命周期。协议服务端 schema 在 `config.example.yml:4`，flag 绑定在 `cmd/server/main.go:86`；HTTP flag 在 `http-service/main.go:30`。
2. **传输接入**拥有进出进程的字节。Reactor accept 循环在 `pkg/reactor/reactor.go:92` 把连接交给 Worker；Worker 队列负责分发，并由连接 goroutine 驱动 pipeline（`pkg/reactor/worker.go:206`）。HTTP 进程则由 `net/http` 接收请求，在 `http-service/handler.go:14` 做路由和状态码映射。
3. **消息管线**按顺序传播连接事件。接口链在 `pkg/handler/handler.go:52`；链表管线在 `internal/handler/linked_pipeline.go:90` 插入 head/tail 节点并从 head 向 tail 传播入站，`NodeContext` 在 `internal/handler/node_context.go:103` 实现入站跳过当前节点、出站向前传播。Reactor 默认错误监听器和未处理异常关闭连接的位置分别在 `internal/handler/event_listener.go:17`、`internal/handler/tail_header.go:25`。
4. **线协议**是 TCP 专属的有状态契约。`FrameCodec` 在 `pkg/proto/protocol.go:765` 编解码帧；`ProtocolHandler` 在 `pkg/proto/protocol.go:76`、`pkg/proto/protocol.go:285` 完成 HELLO 协商、请求处理、订阅跟踪、重放和发布。客户端对应物在 `pkg/proto/client.go:266` 维护 pending 调用、保活和优雅关闭；可恢复客户端在 `pkg/proto/resilient.go:56` 跨重连保存会话令牌和重放游标。
5. **业务语义**按进程形态分开。HTTP 服务在 `http-service/service.go:41` 校验键、单个 JSON 值，并把缺失/非法数据映射成业务错误；TCP 示例把 action 执行交给 `cmd/server/demo/demo.go:27` 配置的 `RequestHandler` 回调。HTTP 存储在 `http-service/store.go:19` 只作为 map 接缝，生产路径没有 Redis、MySQL、远程数据库或专属持久化文件参与。
6. **分发策略**为每个接入连接选择 Worker。`WorkerGroup` 在 `pkg/reactor/worker.go:52` 默认使用自适应均衡器；自适应算法在 `internal/balancer/adaptive.go:42` 每个后端只读一次实时负载并随机打散同分。轮询、随机、IP 哈希、最小连接和加权变体实现同一个泛型接口（`pkg/balancer.go:19`）；`Worker.Load()` 在 `pkg/reactor/worker.go:263` 提供自适应输入。
7. **运行时资源控制**提供并发原语、执行 Future 和缓冲所有权。异步执行器在 `internal/executor/async_executor.go:85` 通过无缓冲 channel 分发任务和 Future；Future 在 `internal/executor/async_future.go:33` 暴露完成/取消/超时解析；协议 payload 池在 `pkg/proto/pool.go:133` 按容量分级回收缓冲；优雅信号和通用 option/契约包在 `pkg/utils/graceful.go:45`、`pkg/options.go:8`、`pkg/balancer.go:19`。

## 概念落位

| 概念 | 实现 | 配置 schema | 专属协议文件 | 文案/交互面 |
| --- | --- | --- | --- | --- |
| HTTP KV 进程 | `http-service/main.go`、`handler.go`、`service.go`、`store.go` | 命令行 flag：`http-service/main.go:30` | 不适用 | 错误 JSON 和状态码映射在 `http-service/handler.go:24` |
| TCP 示例进程 | `cmd/server/main.go`、`pkg/reactor/*`、`pkg/proto/*` | `config.example.yml:4`；Viper 绑定在 `cmd/server/main.go:86` | `pkg/proto/protocol.go` | CLI 用法与帮助在 `cmd/cli/client.go:40` |
| 协议会话/恢复 | 服务端 `ProtocolHandler` 在 `pkg/proto/protocol.go:80`；客户端在 `pkg/proto/resilient.go:56` | `server.strictHello` 在 `config.example.yml:12` | 同左 | CLI flag 和 watch 文案在 `cmd/cli/client.go:39` |
| 分发 | `WorkerGroup` 在 `pkg/reactor/worker.go:40`；均衡器在 `internal/balancer` | `server.multicore`、`server.numWorkers` 在 `config.example.yml:8` | 无 | 无 |
| 异步执行/缓冲 | `internal/executor/*`、`pkg/proto/pool.go` | 无 | 无 | 无 |

TCP 协议没有独立文案包：操作说明在 CLI flag 中，HTTP API 文案由 `http-service/handler.go` 的处理器生成。

## 为什么这么拆

1. **生命周期与接入/读取分开，因为两个独立进程形态需要不同收尾契约。** HTTP 进程在 `http-service/main.go:64` 等信号并用可配置 deadline 调 `http.Server.Shutdown`；Reactor 在 `pkg/reactor/reactor.go:177` 关监听器、排空活动连接、强制关闭残留连接、停止 Worker，并在 handler 未退出时报告泄漏。复用一个生命周期类型会把信号策略和不同排空阶段耦合。
2. **传输接入与 pipeline 分开，因为两种形态本就没有共享连接抽象。** Reactor 在 `pkg/reactor/reactor.go:105` 拥有 `net.Listener`、accept 重试和 Worker 分发；HTTP 处理器在 `http-service/handler.go:71` 接收 `*http.Request` 并通过 `http.ResponseWriter` 写回。统一传输层必须抽象两者，却不会减少任一真实路径。
3. **pipeline 与协议分开，因为 Reactor 接收可插拔 initializer 和 handler，而不是硬编码协议语义。** `NewReactor` 在 `pkg/reactor/reactor.go:42` 接收 `handler.PipelineInitializer`；`echoInitializer` 在 `cmd/server/demo/demo.go:33` 只装帧编解码和协议处理器；传播方向独立实现在 `internal/handler/node_context.go:103`。这是真实扩展缝，不是目录装饰。
4. **线协议拆成编解码、服务端状态和客户端恢复，因为各状态机拥有不兼容的记忆。** 帧边界和流重组隔离在 `pkg/proto/protocol.go:765`；服务端会话、主题序列和按字节预算的重放缓存由一把 RWMutex 保护（`pkg/proto/protocol.go:84`）；pending 响应和保活状态在客户端（`pkg/proto/client.go:85`）；重连令牌和游标在 `ResilientClient`（`pkg/proto/resilient.go:56`）。合并会把进程级与连接级所有权放进一个对象。
5. **业务语义与协议/HTTP 封装分开，因为两个二进制的有效性规则不同。** HTTP 在 `http-service/service.go:41` 校验可打印 ASCII 键、单个 JSON 值和配置大小上限；TCP 示例在 `cmd/server/demo/demo.go:27` 接受任意 action 回调，只封装 JSON 字节。共享业务层会发明两个进程都不拥有的规则。
6. **分发策略单独成层，因为调度依赖 Worker 实时计数，并且可在不改分发逻辑的情况下注入。** `WorkerGroup` 在 `pkg/reactor/worker.go:66` 默认自适应选择，分发在 `pkg/reactor/worker.go:91` 调 `balancer.Next`，`Worker` 在 `pkg/reactor/worker.go:263` 报告自己的活动连接数。其他均衡器通过同一泛型契约注册/注销（`internal/balancer/adaptive.go:85`）。
7. **资源控制单独成层，因为 channel/Future 执行与线缓冲复用是横切关注点，且有明确所有权规则。** 执行器在 `internal/executor/async_executor.go:71` 处理取消后的丢弃，并在共享 worker 循环中执行任务/Future；payload 池在 `pkg/proto/pool.go:157` 清零并回收缓冲以避免旧数据复用。这些策略可被协议/运行时代码使用，但本身不是协议语义。

## 模块/服务清单与调用通道

| 模块/服务 | 状态存储 | 调用方 → 通道 → 被调方 | 输入 | 生命周期 |
| --- | --- | --- | --- | --- |
| HTTP KV 应用 | `MemoryStore`：进程内 map + `sync.RWMutex`（`http-service/store.go:29`） | curl/HTTP → `net/http` → handler → 直接方法 → Service → 直接方法 → Store | flag：`http-service/main.go:30` | 绑定、信号、限时排空：`http-service/main.go:55` |
| TCP 示例应用 | 无领域持久化；拥有 Reactor 和共享协议处理器 | OS 信号 → `utils.Graceful` → 直接方法 → Reactor；配置 → Viper → flags/options | YAML + flag：`config.example.yml:4`、`cmd/server/main.go:86` | 构建 Reactor、运行 accept、分级 shutdown：`cmd/server/main.go:15` |
| Reactor | listener、context/Worker channel、`ConnectionRegistry` map（`pkg/reactor/registry.go:12`） | 接入连接 → 直接方法 → `WorkerGroup.Dispatch`；分发 → channel → Worker；Worker → pipeline 调用 | `ServerOptions`：`pkg/reactor/options.go:8` | 构造/绑定：`pkg/reactor/reactor.go:42`；分级 shutdown：`pkg/reactor/reactor.go:177` |
| 消息管线 | 有序节点和每连接 attachment 在内存（`internal/handler/node_context.go:19`） | Worker → 直接方法 → `Pipeline.FireRead`；handler context 传播 next/prev | initializer 函数：`pkg/reactor/worker.go:283` | 每连接创建；关闭后移除：`pkg/reactor/worker.go:295` |
| 协议服务端状态 | 订阅/会话/主题序列与缓存 map，TTL/计数 atomic（`pkg/proto/protocol.go:84`） | FrameCodec → pipeline context → ProtocolHandler；应用 → 直接 `Publish` | wire frame + 业务回调 | handler 和 janitor 关闭：`pkg/proto/protocol.go:191` |
| 协议客户端/可恢复客户端 | pending map、连接/流状态、令牌/订阅/游标均在内存（`pkg/proto/client.go:85`、`pkg/proto/resilient.go:56`） | CLI → 直接方法 → Client；Client → TCP；read loop 按 stream ID 匹配响应 | CLI flag：`cmd/cli/client.go:40` | 拨号/关闭：`pkg/proto/client.go:72`；重连 watch：`pkg/proto/resilient.go:385` |
| 均衡器 | 各实现内由互斥锁保护的后端切片（`internal/balancer/adaptive.go:26`） | WorkerGroup → 泛型接口直接调用 → 选中 Worker | 后端注册；`Worker.Load` | 随 Reactor 生命周期注册 |
| 异步执行器 | 无缓冲 task/future channel 和 context（`internal/executor/async_executor.go:38`） | 调用方 → channel → worker goroutine → task/future | 泛型 action/future | `Stop`/`Shutdown` 取消：`internal/executor/async_executor.go:63` |

## 目录/分类不合理处

- `pkg/balancer.go` 是泛型契约文件，唯一具体实现在 `internal/balancer`；`Balancer` 接口在 `pkg/balancer.go:19` 导出并被 reactor worker 消费（`pkg/reactor/worker.go:91`），故保持公共。executor 契约（`Executor`/`Future`/`Action`/`ActionWithReturn`）没有生产调用方——唯一公共消费方 `handler.ExecutorHandler` 是零实现、仅断言不调度的休眠接口，已删除——故契约现与实现同层：`internal/executor/contract.go:20`，具体代码在 `internal/executor/async_executor.go:38`。
- `internal/handler/head_handler.go` 在 `internal/handler/head_handler.go:6` 导入并实现 `pkg/handler` 接口，而 `pkg/reactor` 也导入 internal pipeline。公共 Reactor API 因此依赖私有实现包；把具体 pipeline 移到 `pkg/handler`，或暴露更窄的注入契约，可对齐访问边界。
- `cmd/server/main.go` 现在只含进程配置装配；协议示例 action 与其 pipeline initializer 位于 `cmd/server/demo` 包（`cmd/server/demo/demo.go:27`、`cmd/server/demo/demo.go:33`）。`cmd/cli/client.go` 仍混合解析、执行和 watch 交互。
