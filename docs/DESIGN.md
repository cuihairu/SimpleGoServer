# 设计文档：架构、数据流与取舍

本文回答"这套系统是怎么设计的、为什么这么设计"。每个关键决策都给出：
**备选方案是什么、为什么放弃**。机制原理与基础知识见 [NOTES.md](NOTES.md)，
线上字节格式见 [Proto.md](Proto.md)，宏观论证（为什么事件驱动、为什么长度
前缀、为什么不用 net/http）见 [Analysis.md](Analysis.md)，性能数据见
[Benchmark.md](Benchmark.md)——四份文档互不重复，本文聚焦**实现层的结构与
取舍**。

---

## 0. 总览：两道题，一套方法论

本仓两道面试题共享同一套设计方法论，面试时可以先立框架再分述：

| 维度 | 题一：Reactor + 自定义协议 | 题二：http-service |
| --- | --- | --- |
| 分层 | 传输(reactor) / 成帧(codec) / 语义(protocol) / 业务(handler) | 传输(handler) / 业务(service) / 存储(store) |
| 并发单位 | 每连接一个 goroutine，worker 分发 | net/http 内建每连接一 goroutine |
| 生命周期 | accept → dispatch → serve → 分级关闭 | listen → serve → 信号 → drain |
| 错误策略 | 三分类（网络/协议/应用）+ 分级关闭 | 哨兵错误 → 统一状态码映射 |
| 资源防线 | MaxFrameSize/MaxStreamSize/缓存预算/空闲回收 | 四超时/请求体上限/优雅关闭超时 |

核心原则一句话：**越靠近传输的错误越果断（断连），越靠近应用的错误越宽容
（回错误响应）；所有无界的东西（队列、缓存、输入、等待）都必须有界**。

---

## 1. 题目一：主从 Reactor + 自定义帧协议

### 1.1 架构与模块划分

```text
cmd/server, cmd/cli, examples/          装配层：配置、信号、把组件接起来
        │ 依赖
        ▼
pkg/reactor          核心引擎：accept 循环、WorkerGroup、连接注册表、分级关闭
pkg/proto            协议栈：帧编解码、流式分片、语义层（请求/订阅/会话）、客户端
internal/handler     pipeline 实现：双向链表、节点上下文、头尾哨兵
internal/balancer    分发策略族：七种 Balancer 实现 + 契约测试
internal/executor    任务执行器：future、worker 池
pkg/                 抽象接口层：Balancer / Executor / Options / handler / event / channels
        ▲ 依赖
internal/* 实现 pkg 的接口（依赖方向：internal → pkg，永不反向）
```

**为什么这样切**：

- **`pkg` 放接口、`internal` 放实现，依赖单向**。备选是"包按功能切、互相
  引用"——放弃：reactor 需要负载均衡但不应认识具体策略，`pkg.Balancer[T]`
  泛型接口钉住这个缝之后，新增策略（如一致性哈希）零改动核心循环，且
  `internal` 在 Go 语义下对外不可导入，实现细节天然不外泄。
- **`pkg/channels`（NIO 词汇：Selector/Channel/SelectKeys）与 `pkg/event`
  （Listener/Loop/Group）是预留的接口词汇层**，Reactor 通过实现
  `event.Group` 接入。备选是"只留实际用到的接口"——保留这层的理由是
  它把"事件模型该怎么说话"固定下来（Netty 的接口形态），未来换 epoll 式
  selector 不需要改调用方词汇；代价是几个薄文件，测试全覆盖所以不是死代码。
- **协议栈独立成 `pkg/proto` 而不是散在 handler 里**。成帧（字节↔帧）与
  语义（帧↔业务）是两层不同的演变轴：换 JSON 为 protobuf 只动语义层，加帧
  类型不改编解码。业务函数只见到 `RequestHandler func(action string, data []byte)`。

### 1.2 连接生命周期：控制流走读

一条连接从生到死的完整路径（行号为主干锚点）：

```text
net.Listen(url 解析出的 scheme://host)                 reactor.go:60
  └─ Run(): accept 循环                                  reactor.go:92
       ├─ Accept 失败 → stopping? 退出 : ErrClosed? 退出 : 退避 5ms→1s 重试
       └─ workers.Dispatch(conn)                         worker.go:90
            ├─ balancer.Next(remoteAddr)   ← AdaptiveBalancer 按 Worker.Load 选
            └─ worker.AddConn(conn)
                 └─ newCh <- conn          (cap=100，满则 accept 阻塞 = 背压)
worker loop (每 worker 一个 goroutine, 可 LockOSThread)   worker.go:194
  └─ 收到 conn：registry.Add
       │    ├─ 接受 → handlerStarted + count++   ← 三件事在 loop 上做，
       │    │                                      而不是在 handler goroutine
       │    └─ 拒绝（CloseAll 已清扫）→ 自关 + closePending + return（注册闸门，§1.5）
       └─ go handleConnection(lc)          (每连接一个 goroutine)
            ├─ NewPipeline + pipelineInitializer          worker.go:258
            ├─ pipeline.FireActive()
            └─ for { ctx.Done? 退出 : idle? SetReadDeadline : pipeline.FireRead(lc) }
                 └─ FrameCodec.HandleRead → DecodeStreamed   (阻塞读一逻辑帧)
                      └─ ProtocolHandler.HandleRead → 按帧类型分发
                           ├─ REQUEST → 业务函数 → ctx.Write(RESPONSE)
                           ├─ SUBSCRIBE/UNSUBSCRIBE → 维护订阅表 → ack
                           ├─ PING → PONG；CLOSE → ack + 断开
                           └─ HELLO → 版本协商 + 会话 + 补发
退出路径（任一触发）：
  - 对端 EOF / 协议错误 → codec 关连接 → lifecycleConn.closed=true → 循环 return
  - 空闲超时 → ReadDeadline 到期 → codec 收 ErrDeadlineExceeded → 关连接
  - 服务关闭 → Registry.CloseAll / ctx.Done → 同上
defer 清理（顺序固定）：Close → registry.Remove → FireInactive → count-- → handlerFinished
```

**关键取舍**：

1. **注册动作在 worker loop 上做，不在 handler goroutine 里做**
   （worker.go:201-211 注释）。理由是可见性同步：`AwaitDone`（active 计数）、
   drain 轮询（TotalCount）与 `Registry.CloseAll` 三个观察者必须看到同一
   份连接账本——若在 handler goroutine 里注册，关闭路径可能观察到
   "active 有它、registry 没它"的三种说法打架。备选"handler 里注册"少一次
   loop 交接，但把关闭正确性押在运气上。但"在 loop 上做"只保证注册点
   唯一，不保证注册与清扫的先后——`newCh` 里排队中的连接对三者皆不可见，
   select 双臂就绪（conn 与 ctx.Done）掷币选中 conn 臂时，注册会落在
   CloseAll 之后，连接从此无人可关（实测由 `-cpu=1` 复现，见 NOTES §17）。
   所以注册点唯一还不够，还需要注册**闸门**：`Add` 与 `CloseAll` 同锁
   串行化，清扫后 `Add` 返回拒绝，worker 自行关连接且不启动 handler
   （取舍 #36）。
2. **`newCh` 容量 100、满则阻塞 accept**（worker.go:163 注释）。这是结构性
   背压：worker 处理不过来时，压力沿 Dispatch → accept 循环回传，accept
   停下，内核 backlog 接管，客户端看到连接变慢而不是服务端 OOM。备选
   "无界队列"——放弃，等于把内存上限交给对端；备选"满了就拒"——丢弃
   已 accept 的连接同样合法，但阻塞更简单且不丢已建立连接。
3. **空闲回收用 `SetReadDeadline` 而不是每连接一个 timer**
   （worker.go:290-295）。deadline 挂在阻塞的 Read 上，到点自然醒，不需要
   额外的 timer 对象与回调路径；且"任何收到的帧都重置 deadline"天然把心跳
   计入活跃判据。备选"心跳计数 + 超时检查"——需要额外状态且和 codec 的
   阻塞读打架。
4. **accept 失败退避 5ms→1s 封顶**（reactor.go:124-135），镜像 net/http 的
   accept 循环行为：fd 耗尽这类瞬态失败不该变成错误日志风暴。
5. **`lifecycleConn` 包装让读循环能区分"handler 关的"与"继续读"**
   （worker.go:244-255）。`Close` 用 `atomic.Bool` 做幂等：对端 EOF、协议
   错误、发布剔除、服务关闭四条路径都会关连接，重复 Close 必须无害。

### 1.3 关键数据结构

#### Frame 与帧头（`pkg/proto/frame.go`）

```go
FrameHeader{ FrameType uint8; Flags uint8; StreamId uint32; Length int32 }  // 10B 定长
Frame{ Header FrameHeader; Payload []byte }
```

- **字段序 `[type][flags][streamId][length]` 与 HTTP/2/Dubbo 同构**，论证
  与对照表见 Analysis.md §5。`Length` 用 `int32`：解码时 `length < 0` 检查
  （codec.go:50）防的是把高位置 1 的 4 字节读成负数绕过 `> MaxFrameSize`
  比较——只比上界会漏掉负数。
- **StreamId 一物三用**：请求/响应配对；PING/PONG 配对；PUBLISH 复用为全局
  单调序号（天然游标，见会话一节）。备选"再加一个 seq 字段"——多 4 字节且
  与 StreamId 冗余。
- **Flags 只定义了 bit0（FlagMore）**，其余位预留（压缩/加密/控制语义），
  与 HTTP/2 flags 的演进方式一致。

#### ProtocolHandler 的五张表（`pkg/proto/protocol.go:80-119`）

```go
subs       map[topic]map[net.Conn]struct{}    // 订阅表：topic → 成员集
sessions   map[token]*session                 // 会话表：token → 会话
connTokens map[net.Conn]string                // 反向索引：conn → token
topicCache map[topic][]cachedPublish          // 补发缓存：topic → 已编码帧
// + cacheBytes（全局字节账本）、seq（发布序号）、pings/pongs（心跳计数）
```

每个结构都有"为什么"：

- **一张 RWMutex 管五张表**，而不是每表一把锁。这些表之间有跨表不变量
  （"订阅表非空 ⟹ 缅存必须保留"，`dropTopicCacheLocked` 依赖同时读 subs
  和 sessions），分表加锁就要定义锁序，多锁是死锁面的代名词。写少读多
  （订阅/发布远低于请求），RWMutex 读并发足够。
- **`connTokens` 反向索引**：SUBSCRIBE 帧到来时要找"这条连接的会话"来记账，
  没有反向索引就得扫全表。它还有一个**零成本复用**：`connTokens` 里有记录
  ⟺ 该连接已完成握手——严格模式判定握手与否直接查这张表
  （protocol.go:281-289），不另设一个可能与事实漂移的布尔标记。
- **`session.lastSeen` 是 `atomic.Int64`（UnixNano）**：任何入站帧在**读锁**
  下就能刷新它（`touchSession`），不必升级写锁——活着的会话无论挂多久都
  不会被清扫回收，只有真正静默的才过期。
- **补发缓存双预算**：每 topic 最多 64 条（`topicCacheSize`）**并且**全局
  最多 64 MiB（`defaultCacheBudget`）。条数上限≠内存上限：topic 数无界且
  单帧可达 1 MiB。字节预算超限时**整 topic 逐出**（占用最大者优先）——
  部分逐出会让被挤压的 topic 补发序列正好断在中间，等于坏掉它的补发承诺；
  整 topic 拿掉则该 topic 的 best-effort 语义仍是自洽的"有缺口"。
- **缓存条目存"已编码帧"**（`cachedPublish.buf`）而不是原始 payload：
  补发路径 `replayTo` 直接 `conn.Write(buf)`，零编码开销。

#### Worker / WorkerGroup / ConnectionRegistry（`pkg/reactor/`）

- **`WorkerGroup.active` 用原子计数轮询，不用 WaitGroup**
  （worker.go:44-49 注释）：连接 handler 随连接到来**动态**启动，WaitGroup
  的 `Add` 与 `Wait` 并发是未定义行为；原子计数 + 10ms 轮询没有这条规则，
  且 `AwaitDone` 只在关闭时跑一次，轮询成本可忽略。
- **`ConnectionRegistry` 是关闭能力的地基**：能枚举、能 `CloseAll`，才能把
  "排空→强关"分成两段（见 1.5）。`CloseAll` 之后换新 map，剩余条目清零，
  并落 `closed` 闸门——此后 `Add` 返回 false，把"注册晚于清扫"的窗口
  压成确定性的拒绝（见 1.5 与决策 #36）。
- **Worker 同时实现 `Count()`（连接数）与 `Load()`（负载值）**：前者是
  `CountBackend` 契约（最少连接策略用），后者是 `LoadAware` 契约（自适应
  策略用），两个策略族共用同一个数据源。`SetCount` 故意是空实现——活跃
  计数由 handleConnection 维护，绝不允许均衡器改写。

#### pipeline：双向链表 + 哨兵（`internal/handler/`）

```text
head(HeadHandler) ⇄ [FrameCodec] ⇄ [ProtocolHandler] ⇄ tail(TailHeader)
   入站 Fire* 从 head 向 tail 传播；出站 Write 从当前节点向 head 找 outbound handler
```

- **为什么是链表不是切片**：pipeline 是**每连接**的运行时结构，节点在握手
  阶段可能动态增删（`AddFirst/AddLast/AddHandler`），链表增删 O(1) 且节点
  指针稳定——`NodeContext` 持有 prev/next，`ctx.Write` 能从任意节点向 head
  走。备选切片——中间插入 O(n) 且索引会漂移。
- **构造时一次性做类型断言缓存**（node_context.go:38-43）：每个 handler 实现
  哪些行为（Active/Inbound/Outbound/Exception/Inactive/Executor）在入链时
  断言一次存字段，事件传播时零反射零断言，只判 nil。这是 Netty
  `AbstractChannelHandlerContext` 的同款手法。
- **传播跳过自身**（"skip self"）：`ctx.HandleRead` 从 `next` 开始找，
  `ctx.Write` 从 `prev` 开始找——handler 通过 context 触发的事件不会递归
  回自己。这是 Netty 语义的直接移植，备选"包含自身"会造成无限递归。
- **HeadHandler 是单例且写失败 panic**（head_handler.go:31-50）：它支持
  `[]byte`/`[][]byte`/`Buffer`/`io.WriterTo`/`io.Reader` 五种出站消息形态，
  `AssertWriteLength` 在写错误时 panic。**panic 在这里是设计**：出站写失败
  意味着传输层死亡，panic 沿 handleConnection 的 recover → `FireException`
  → 统一收尾路径走（而不是在每层写代码处 if err），把"传输死亡"从普通
  错误流里显式分离。备选返回 error——出站接口 `HandleWrite` 无返回值
  （与 Netty 一致），错误只能经异常通道传播。
- **TailHeader 是异常兜底**：异常一路无人处理到达 tail，说明 pipeline 装配
  缺了 ExceptionHandler——打印指引文案并关连接，绝不静默吞掉。

#### Client：单连接多路复用（`pkg/proto/client.go`）

```go
pending map[uint32]chan *Response   // streamId → 等待者（cap 1 的缓冲通道）
```

- **分配 ID + 注册 pending 在同一个临界区**（roundTrip，client.go:237-241）：
  若分两步，两个并发的 Call 可能拿到相邻 ID 却以相反顺序注册，读循环按 ID
  回投就会张冠李戴。单临界区让"ID 的诞生"与"等待者的登记"原子。
- **等待通道 cap=1**：读循环 `resolvePending` 发送后不再关心有没有人在等
  （超时者已走，迟到响应发进缓冲然后被丢弃），读循环永不因等待者离开而
  阻塞——**解耦方向是刻意的**：慢的是调用方，读循环必须永远快。
- **单帧写在锁外**（client.go:253-257 注释）：注册完 pending 就放锁再写
  socket，一个慢 socket 不会挡住别的调用者注册；**分片写在锁内**：同一
  请求的分片必须背靠背上线，否则并发请求的帧会插进分片序列中间（流式
  传输的完整性约束，见 Proto.md）。
- **迟到响应直接丢弃**（`resolvePending` 的 `!ok` 分支）：超时后从 pending
  删除，迟到的 RESPONSE 查不到等待者就扔——不需要取消机制。

#### ResilientClient：重连与补发（`pkg/proto/resilient.go`）

- **游标在用户回调之前更新**（`recordCursor` 在 `onEvent` 包装的最前面）：
  用户 handler panic 或阻塞都不能丢游标——游标是补发正确性的根。
- **`swap` 在锁内重查 closed**（resilient.go:326-331 注释）：重连拨号与
  `Close()` 竞态时，事后安装新连接会泄漏一整对活连接（客户端读循环 +
  服务端 handler 都挂着）——这正是 goleak 抓到的签名，修法是把"安装"
  变成锁内的检查-安装原子动作。

### 1.4 并发模型：goroutine 与锁的清单

面试时最常被追问"你的 goroutine 都有哪些、锁都有哪些、会不会泄漏"。全表：

**goroutine 清单**（各自的生命周期与退出条件）：

| goroutine | 数量 | 退出条件 |
| --- | --- | --- |
| accept 循环（`Reactor.Run`） | 1 | ctx.Done / listener 关闭 / ErrClosed |
| worker loop | NumWorkers（≤NumCPU） | worker ctx.Done + `closePending` 清残留队列 |
| 连接 handler | 每连接 1 | 连接关（EOF/错误/deadline）或服务关闭；defer 全量清理 |
| session janitor | 每进程 1 | `ProtocolHandler.Close()` 关 closed 通道 |
| 客户端 readLoop | 每客户端 1 | 读错误 → failPending → Close |
| KeepAlive 循环 | 每客户端 ≤1 | stop() / 客户端关闭 / ping 失败自杀 |
| resilient watch | 每实例 1 | Close；连接断开只触发重连，不退出 |

每张表都有 goleak 在包级 `TestMain` 兜底：任何一条泄漏，测试进程直接失败。

**锁清单**（保护什么、为什么是它）：

| 锁 | 保护 | 选择理由 |
| --- | --- | --- |
| `ProtocolHandler.mu` (RWMutex) | 五张表 + cacheBytes | 跨表不变量要求单锁；读多写少 |
| `ConnectionRegistry.mu` (RWMutex) | 连接集合 | Add/Remove 高频，CloseAll/Len 低频 |
| `AdaptiveBalancer.rwMutex` | backends 切片 | 注册/注销少，Next 并发多 |
| `Client.mu` | nextID + pending | 临界区必须覆盖"分配+登记"整体 |
| `ResilientClient.mu` | client/token/订阅/游标 | 同上，swap 的原子性 |
| `NodeContext.attachMu` | 每节点附件 | handler 可能从不同 goroutine 回调设置附件 |
| `MemoryStore.mu`（题二） | items map | 见 §2.3 |

**原子计数**：pings/pongs/seq、session.lastSeen、Worker.count、
WorkerGroup.active、lifecycleConn.closed、requireHello、client.version——
全是"单值、无跨字段不变量"的场景，原子比锁便宜且无死锁面。

**channel 交接点**：`newCh`（accept→worker，有界=背压）、`futureCh/tasks`
（提交者→执行器，无缓冲=直 Handoff）、pending 通道（读循环→调用方，cap1
= 解耦）、errCh（题二，buffer 1 = 关闭路径不丢监听错误）。

### 1.5 错误处理与分级关闭

**三分类处置表**（Proto.md 有线上契约视角，这里是实现视角）：

| 类别 | 例子 | 处置 | 代码锚点 |
| --- | --- | --- | --- |
| 网络错误 | EOF、reset、写失败 | 关连接，业务无感 | codec HandleRead / Publish 剔除 |
| 协议错误 | 超限帧、非 envelope、分片交错、重复 HELLO | 关连接 + 记录（字节流已不可信） | `ctx.Close(fmt.Errorf("proto: ..."))` |
| 应用错误 | 未知 action、业务返回 error | 错误 RESPONSE，**连接保持** | handleRequestFrame |

**panic 策略分三层**：业务 handler 的 panic 由 handleConnection 的 recover
捕获 → `FireException` → pipeline 收尾（单连接故障不波及进程）；出站写
失败由 HeadHandler 主动 panic 走同一通道；pipelineInitializer 失败是装配
编程错误——撤销注册后**故意 panic**（worker.go:257-269），让它变成看得见
的启动期崩溃而不是静默的连接黑洞。

**分级关闭五段**（`ShutdownWithTimeout`，reactor.go:177-207）：

```text
1. stopping=true + listener.Close()      停止接收（accept 循环见 ErrClosed 静默退出）
2. drain：轮询 TotalCount==0，上限 10s   给在途连接自然结束的机会
3. Registry.CloseAll()                   强关残余 → 阻塞在 Read 的 goroutine 全部醒来；
                                         落 closed 闸门，此后 Add 一律被拒
4. cancel + workers.Stop + AwaitDone(5s) 停 worker 循环，等 handler 返回
5. OnShutdown                             通知观察者
```

- `stopOnce` 保证重复调用无害；第 4 步超时会**dump 全部 goroutine 栈**
  （reactor.go:199-202）——handler 卡死是泄漏，操作者需要看到卡在哪。
- **第 3 步与 worker 注册的竞态由闸门闭环**：`newCh` 里排队的连接对 drain
  与 CloseAll 皆不可见，worker 之后再取出注册，就是一条"清扫过后的漏网
  连接"——handler 停在无人会关的 Read 上，AwaitDone 必超时。闸门把
  `Add` 与 `CloseAll` 压进同一把锁，时序只剩两种且都安全：Add 在前 →
  CloseAll 必关它；CloseAll 在前 → Add 被拒，worker 自关且不启动
  handler。没有第三种时序，AwaitDone 的 5s 等待不再可能白付。
- 返回值语义：非 nil ⟺ 有 handler 活过了第 4 步。备选"总是返回 nil"——
  泄漏是不可沉默的故障。
- 对比题二的 `srv.Shutdown`：同样的"停收 → 排空 → 兜底强关"骨架，net/http
  内建了前两段，超时兜底同样是调用方给的 context。两题在这里互为印证：
  优雅关闭的通用形态就是这三段。

### 1.6 决策汇总表（备选与放弃理由）

| # | 决策 | 主要备选 | 为什么放弃备选 |
| --- | --- | --- | --- |
| 1 | goroutine-per-connection + worker 分发 | 单线程 eventloop 回调（gnet 式） | 回调把业务切碎、异步传染；本设计目标区间（万级连接）下 goroutine 开销可接受（Analysis §6） |
| 2 | worker 数 ≤ NumCPU，可 LockOSThread | worker 数无上限 | worker 的职责只是分发与记账，超过核数只增加争抢；LockOSThread 是可选开关（亲和性收益 vs 减少 P 灵活性，配置化） |
| 3 | 分发走 Balancer 接口，默认自适应 | 固定轮询 | 静态轮询对慢 worker 无感知；自适应按 `Load()` 自校正（Analysis §负载均衡） |
| 4 | 自适应取最小负载 + 随机破平 | 取第一个最小 | 同等空闲的 worker 会钉死在第一个上，流量不均；reservoir 采样 O(n) 一遍完成 |
| 5 | newCh 有界（100） | 无界队列 / 拒绝 | 无界=内存交给对端；拒绝丢已建连接。阻塞回压最简单 |
| 6 | AddConn 发送后重查 ctx | 只查一次 | 发送与 worker 退出竞态：发送成功但 loop 已死 → fd 泄漏或永久阻塞，重查兜住（worker.go:167-185） |
| 7 | 注册在 worker loop 上做 | 在 handler goroutine 做 | 三个关闭观察者必须看到同一账本（§1.2 取舍 1） |
| 8 | active 原子计数轮询 | sync.WaitGroup | Add 与 Wait 并发是 UB；动态启动的 handler 正是这种形态 |
| 9 | 空闲回收 = ReadDeadline | 心跳计数器 + 检查 | deadline 与阻塞 Read 天然一体；帧到达即重置，心跳免单 |
| 10 | lifecycleConn 幂等 Close | 裸 net.Conn | 四条关闭路径会重复关；幂等让 defer 无条件执行 |
| 11 | 帧头 10B 定长 + int32 长度 | varint 长度 | O(1) 边界判定、一次比较即防御；varint 省的字节抵不上解码分支（Analysis §5） |
| 12 | Length 负数检查 | 只查上界 | 高位置 1 的 uint32 读成 int32 是负数，只查上界会放行 |
| 13 | 流式分片聚合在 codec（每连接实例） | 独立重组器 | 聚合状态与连接同生共死，天然清理，无跨调用残留 |
| 14 | 聚合首片直接 append 扩容 | 拷贝到新缓冲 | 首片缓冲来自 Decode 独占分配，别名安全且省一次全量拷贝 |
| 15 | envelope 手工拼装 | 结构体 Marshal | RawMessage 的 MarshalJSON 会整段二次拷贝；1MiB 实测 21.4ms→8.6ms（Benchmark §分层定位） |
| 16 | DecodeJSONMessage 零拷贝别名 | json.RawMessage 默认拷贝 | 同上，省 1MiB 级拷贝；调用方契约"只读"写进注释 |
| 17 | 五表一锁（RWMutex） | 每表一锁 | 跨表不变量 + 锁序复杂度；读多写少下读写锁够用 |
| 18 | connTokens 反向索引兼作握手标记 | 独立 handshaked 标记 | 标记可能与事实漂移；"表里有记录"恰好就是握手完成，零成本复用 |
| 19 | 缓存条数 + 字节双预算 | 仅条数 | topic 无界、单帧 1MiB，条数封不住内存 |
| 20 | 字节预算整 topic 逐出 | 按条逐出 | 部分逐出把补发序列断在中间，破坏该 topic 的补发自洽性 |
| 21 | 订阅者无连接时也继续入缓存 | 无订阅者即清缓存 | 最后一个订阅者被剔除后的发布会静默丢失，补发名存实亡（会话还记着 topic 就可能回来要） |
| 22 | 写失败剔除订阅者但保留会话记账 | 一并删记账 | 写失败=传输死，不是客户端不想要；resume 会迁移记账并清理死连接 |
| 23 | 补发先于握手 ack | ack 后补发 | Handshake 返回即保证补发在路上；补发帧与实时推送不会交错 |
| 24 | 会话 TTL 惰性检查 + 单 janitor | 每会话一个 timer | timer 数量随会话线性；janitor 一分钟一遍 + attach 时再查一次，便宜且足够 |
| 25 | 任何入站帧刷新 lastSeen（读锁） | 仅握手/心跳刷新 | 只认心跳会把纯业务连接的会话过期掉 |
| 26 | token 用 crypto/rand，失败降级时间戳 | 直接时间戳 | 不可猜测是加固项不是必需项；熵源坏了不该拒绝服务 |
| 27 | HELLO 只允许一次 | 允许重复握手 | 重复握手会静默顶掉在用会话——"看起来成功"比明着失败危险 |
| 28 | Client pending 注册与 ID 分配同一临界区 | 分两步 | 并发 Call 的 ID 与登记可能交叉错配 |
| 29 | 迟到响应丢弃 | 保留等待重试 | 无取消协议；丢弃最简单且正确 |
| 30 | KeepAlive 失败即自杀关闭 | 无限重试 | 检测死链的意义就是让 Done() 的等待者尽快知道 |
| 31 | HeadHandler 写失败 panic | 忽略错误 | 传输死亡须走异常通道统一收尾；静默丢写出站语义不可知 |
| 32 | 出站支持五种消息形态 | 只收 []byte | 大帧可以 io.WriterTo 流式写，避免强制整缓冲 |
| 33 | executor 通道故意不关闭 | Stop 时 close | close 后并发 Submit 变 panic；ctx 取消 + nil 守卫足够（async_executor.go:63-69 注释） |
| 34 | future Do/Cancel 用 doneCh 关闭做一次性决议 | 标志位 | 关闭的 channel 是天然的广播一次性事件；Cancel 与 Do 竞态靠"已决议检查"保持第一个结果 |
| 35 | janitor 作废会话时同步清成员集与补发缓存 | 只清 sessions/connTokens，成员集等下次 Publish 写失败再剔 | 过期会话永不再回来（与 resume 的换传输同构）；不清则静默 topic 上幽灵订阅者与被扣住的缓存无限期滞留 |
| 36 | 注册闸门：`Add` 返回 bool，`CloseAll` 落锁后拒绝新注册 | ① 关闭后再重扫一遍 registry；② 把 cancel 提到 CloseAll 之前 | ① 重扫要靠 AwaitDone 超时才触发，5s 等待已白付且报错依旧；② 提前 cancel 并不消除窗口——注册与清扫仍是无同步点的两个操作，worker 完全可以在 cancel 后、CloseAll 后才注册（`-cpu=1` 实测复现）。同锁串行化才有 happens-before：拒绝→worker 自关，接受→CloseAll 必关，别无第三种时序（NOTES §17） |

---

## 2. 题目二：http-service（标准库 only 的克制）

### 2.1 分层与依赖方向

```text
main.go    装配：flag → Service(MemoryStore) → http.Server → 信号 → 排空
handler.go 传输层：1.22 模式路由、错误→状态码映射（唯一一处）、JSON 响应
service.go 业务层：key/值校验、哨兵错误、LimitReader 上限（不知道 HTTP 词汇）
store.go   存储缝：Store 接口 + RWMutex 内存实现（不知道业务规则）
```

依赖严格单向 `main → handler → Service → Store`。**单实现也定义接口**的
理由：`Store` 同时是测试缝（`failingStore` 桩注入故障）和替换缝（换
Redis 不动其他层）；错误映射集中意味着**响应契约只有一份**——加一种
错误只改 service 和 writeError 两处，路由永不关心状态码。

### 2.2 请求生命周期（数据流）

```text
PUT /kv/k  ─ ServeMux 模式匹配（方法不匹配 mux 自动 405+Allow）
  └─ handler.put → svc.Put(ctx, key, r.Body)
       ├─ validKey：1-128 可打印非空格 ASCII，逐字节判定
       ├─ io.ReadAll(LimitReader(body, max+1))    ← +1 字节区分"恰好满"与"超了"
       ├─ len>max → ErrValueTooLarge（413）；json.Valid → ErrValueInvalid（400）
       └─ store.Put：锁内置换，返回 created（201/200 之分）
响应：writeJSON({"status":"stored"}) / writeError（哨兵→状态码，500 固定文案+日志细节）
```

### 2.3 并发安全：为什么是 RWMutex + map

裸 map 并发写是运行时 `fatal error`（进程直接死，不是 race 报告）。三选一：

| 方案 | 适合场景 | 本场景判断 |
| --- | --- | --- |
| `RWMutex + map` | 读写均衡或写不罕见 | ✅ 朴素最快，单锁无嵌套 → 无死锁面 |
| `sync.Map` | 读多写少、键集离散、key 只增 | ❌ KV 服务的 put/get 频率相当，sync.Map 的双 map 维护反而慢 |
| channel 化 actor | 需要串行化复杂事务 | ❌ 单 key 点查走排队纯开销 |

两个配套决定：**锁放 Store 层**——换远程存储时锁自然消失（远程协议自带
串行化），业务层从不知道锁存在；**Put 的 Value 无防御性拷贝**——所有权
语义是"交出后不可变"，调用方 `json.RawMessage` 已是独立字节切片，锁内
拷贝纯属浪费。

### 2.4 超时矩阵

| 超时 | 作用域 | 缺失后果 |
| --- | --- | --- |
| ReadHeaderTimeout 5s | 请求头读完 | slowloris：开 socket 不发数据，goroutine 永久停车 |
| ReadTimeout 10s | 整个请求读完 | 慢速滴注 body 同样能耗死连接 |
| WriteTimeout 10s | 响应写完 | 只设读不设写：响应写一半挂起无人管 |
| IdleTimeout 60s | keep-alive 空闲 | 空闲连接永占 fd |
| shutdown-timeout 10s | 排空在途请求 | 单个卡死 handler 拖着进程不退，被编排系统强杀 |

四超时作用域递增、缺一不可的完整论证见 NOTES.md §HTTP；与题一的
`IdleTimeout` 语义对照：题一在协议层用 deadline 回收静默连接，题二在
HTTP 层用 IdleTimeout 回收空闲 keep-alive——同一问题的两个层次。

### 2.5 关键决策（备选与放弃理由）

| # | 决策 | 主要备选 | 为什么放弃备选 |
| --- | --- | --- | --- |
| 1 | `run(args) int` 函数化入口 | 逻辑写在 main | go test 不会调 main；函数化后整个生命周期可在测试里跑（含真信号） |
| 2 | `signal.NotifyContext` 且信号被消费 | 原生 signal.Notify | ctx 取消把信号变成可 select 的事件；消费掉避免进程内其他组件误反应 |
| 3 | errCh 缓冲 1 | 无缓冲 | ListenAndServe 的错误必须可投递，即使主流程已走关闭分支——无缓冲会丢或卡 |
| 4 | Shutdown 超时→退出码 1 | 永远等 | 编排系统靠退出码判断滚动重启是否干净；卡死等待=被 SIGKILL，更难看 |
| 5 | 1.22 模式路由 `"PUT /kv/{key}"` | 手写 method switch / 引框架 | mux 自带 405+Allow 与 PathValue；零依赖是题面约束，也是这里的正确选择 |
| 6 | LimitReader(max+1) | MaxBytesReader | +1 一字节区分"恰好满"与"超了"，错误分类干净（413 vs 200）；MaxBytesReader 也可，但会写 header 后才发现超限 |
| 7 | `json.Valid` 校验"恰好一个 JSON 值" | Unmarshal 进 any | Valid 不分配目标对象；"恰好一个"排除了 `1 2` 这类拼接体 |
| 8 | Value = json.RawMessage 原样存取 | Unmarshal→再 Marshal | 零重编码；题一的双重编码教训（42ms→）直接迁移过来 |
| 9 | 哨兵错误 + errors.Is 映射 | 字符串匹配 | 可组合、可 `errors.Is/As`；HTTP 词汇（状态码）不进业务层 |
| 10 | 500 固定文案，细节进日志 | 原样返回 err.Error() | 错误文本给操作者不给调用方；驱动报错/地址/堆栈外泄是信息泄露 |
| 11 | 健康检查恒 200 且极便宜 | 逐依赖深度探测 | 无外部依赖时"能回答"即健康；深度探测被打爆有雪崩先例，liveness/readiness 分离的讨论见 NOTES.md |
| 12 | slog JSON 到 stderr | fmt 日志 | 结构化可采集；stderr 与服务输出分离 |

---

## 3. 测试设计：覆盖率是怎么来的

100% 语句覆盖率不是"为指标补测试"，而是分层测试策略的副产品：

| 层 | 手段 | 例子 |
| --- | --- | --- |
| 纯逻辑单测 | 表驱动，无 I/O | 帧编解码矩阵、validKey 矩阵、negotiate |
| 组件级集成 | `net.Pipe` 同步管道 + 手搓 pipeline | `pipeServer`：不需要端口，微秒级，且 pipe 的同步语义让"写失败"可确定性构造 |
| 端到端 | 真 TCP + 真协议客户端 | reactor 全链路、真信号（`syscall.Kill` 打给自己）端到端 |
| 契约测试 | 一套用例跑所有实现 | balancer 契约：七种策略共用同一组行为断言（幂等注册/size 讲真话） |
| 故障注入 | 桩注入 | failingStore、熵源失败、写失败连接 |
| 并发靶场 | `-race` + 并发压测 | store 并发读写、客户端并发 Call |
| 泄漏检测 | goleak 包级 TestMain | 任何后台 goroutine 泄漏（janitor/keepalive/watch）当场失败 |
| 恶意输入 | fuzz 三目标 + 手工边界 + CI 挖掘门禁 | 超限长度、负长度、截断帧、非 envelope、分片交错、超限聚合；`scripts/fuzz-smoke.sh` 每次推送按固定预算变异，崩了即失败并落盘语料 |
| 长稳 | `SOAK=1` 门控 soak | churn + 稳态采样 goroutine/内存，断言单调性而非绝对值 |
| 承诺防腐 | 逐包覆盖率门禁（CI 强制） | `scripts/check-coverage.sh`：任一包低于 100% 即失败，防止 100% 声明无声腐烂 |

覆盖率本身也会腐烂：删掉一个测试或新增一个错误分支，`go test` 依然全绿，
README 里那句 100% 就退化成无人验证的口头禅。所以 100% 由
`scripts/check-coverage.sh` 逐包门禁、CI 在 `stable` leg 强制执行——
**逐包而非仓库总和**，因为总和会让大包掩护小包（1000/1000 足以把 0/1
抬成 99.90%），而 README 承诺的单位就是包。纯接口包无语句，按定义跳过；
一个无法满足的门禁只会被删掉。原理与门禁自身的测试见 NOTES.md §16。

两个值得在面试讲的细节：

- **概率分支的覆盖**：`Submit` 的"发送成功后发现已停止"分支是两臂皆就绪
  的 select（运行时随机选择），测试跑满 64 次并断言**每个** future 都以
  cancelled 解决——覆盖率以 1-2⁻⁶⁴ 概率达成，而断言本身是确定语义。
- **net.Pipe 的确定性写失败**：TCP 上"对端已关"的首写可能因半关闭语义
  成功（FIN 后写合法，RST 才失败），pipe 的同步关闭让"写必失败"变得
  确定——边界测试要选可确定性的传输模拟。

模糊测试三目标累计约 320 万次执行零发现（README 口径），这是"边界条件
防护"一节的实证数据。

---

## 4. 已知局限与演进方向

诚实列出没做的事，每条都有"为什么现在不做"：

1. **HTTP/2 式 credit 流控未实现**——当前背压是"剔除慢订阅者"+有界队列，
   对推送网关场景够用；credit 需要双向窗口记账与复杂度成倍的协议状态，
   属于下一个量级的需求。
2. **每帧分配无 sync.Pool**——Benchmark 已量化分配数（1KiB 请求 56 次分配），
   是首批优化点；池化 Frame+payload 需要解决"载荷别名零拷贝与池回收"的
   所有权冲突，做之前要想清楚。
3. **JSON 载荷的反射开销**——19 次分配来自 encoding/json；接口已隔离，
   换 protobuf 不动帧层。
4. **单 ProtocolHandler 实例承载全部会话状态**——五表一锁在单进程内正确；
   多实例部署需要会话/订阅状态外置（Redis），那是分布式章节的事。
5. **TLS 与认证未做**——Flags 预留位已留，传输加密交给部署层（LB 终结或
   future: 帧层加密标志位）。

---

*本文所有行号锚点以 2026-09 主干为准；行为语义以测试为准。*
