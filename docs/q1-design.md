# 题目一：Reactor 与自定义协议——为什么这样设计

> 本文是题目一的设计决策主线：题面与约束 → 分组的设计决策表（备选方案对比 + 为什么选这个）→ 关键代码走读 → 坦白的取舍与局限。
> 机制级细节不在这里展开：49 条决策的完整汇总、连接生命周期控制流走读、goroutine 与锁的全表在 [DESIGN](/DESIGN)；字节格式契约在 [Proto](/Proto)；宏观论证在 [Analysis](/Analysis)；性能数字在 [Benchmark](/Benchmark)；原理考点在 [NOTES](/NOTES)。

---

## 题面与约束

**题面**：设计一个高性能、可扩展的非阻塞网络通信模型，处理大量并发连接和数据传输；支持 TCP 或基于 TCP 的自定义协议；合理处理连接生命周期、异常情况与资源优化。九条具体要求与本题的落点：

| # | 题面要求 | 本仓落点 |
| --- | --- | --- |
| 1 | 事件驱动、goroutine/channel、多核 | 主从 Reactor（acceptor + worker 组），worker 数 ≤ NumCPU |
| 2 | TCP 生命周期、优雅关闭、自定义协议 | 建连/传输/关闭三段全覆盖，分级关闭五步 |
| 3 | 自定义封装解析、JSON 数据交换 | 10B 定长帧头 + JSON 载荷，成帧与序列化分层 |
| 4 | 网络/协议/应用错误、致命错误有序关闭 | 错误三分类处置表 + `ShutdownWithTimeout` 五段 |
| 5 | 性能测试、大量并发验证 | 基准 + fuzz + soak 长稳，全部进 CI（见 [Benchmark](/Benchmark)） |
| 6 | 结构清晰、配置外置 | pkg 接口 / internal 实现单向依赖；`config.example.yml` + flag 覆盖 |
| 7 | 文档与测试 | 本站与五份文档；逐包 100% 覆盖率门禁（CI 强制） |
| 8 | 自适应负载均衡 | 七种策略族，默认 Adaptive（按 `Load()` 实时负载） |
| 9 | 请求/响应、发布/订阅、流式 | 三种全做，外加握手协商、心跳、会话重连与离线补发 |

**明确假设**（题目没说、但设计必须先定的）：

- **规模区间**：万级长连接为目标。这直接决定并发模型选型——goroutine-per-connection 在这个区间完全可用，不必付出单线程 eventloop 的回调复杂度（论证见下文决策 D1）。
- **载荷格式**：JSON 是题面要求，按"可替换"设计——语义层接口隔离，换 protobuf 不动帧层。
- **部署环境**：服务器间直连 TCP，无浏览器；TLS 与认证交给部署层，帧头 Flags 预留位留着协议层加密的路。
- **性能口径**：结构正确优先，数据佐证其次——每条性能声明都要能一条命令复现，且注明噪声底（同码对照基准的差值有 ±10~15%，见 [Benchmark](/Benchmark)）。

---

## 总览：分层与依赖方向

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

四层各管一段演变轴：**换并发策略动 reactor，换字节格式动 codec，换业务语义动 protocol，换业务逻辑动 handler**——任何一层的变化都不外溢。

---

## 设计决策表

按主题分五组。每行是一个关键选择：备选方案是什么、为什么放弃、主要权衡角度。

### 并发模型

| # | 决策 | 主要备选 | 为什么放弃备选 | 角度 |
| --- | --- | --- | --- | --- |
| D1 | goroutine-per-connection + worker 分发的主从 Reactor | ① thread-per-connection；② 单线程 eventloop 回调（gnet 式） | ① OS 线程栈与切换成本随连接数线性涨，10 万连接即不可行；② 性能极致但业务被回调/状态机切碎、异步传染，万级区间下 goroutine 的 2KB 起步栈完全付得起 | 性能 + 复杂度 |
| D2 | worker 数 ≤ NumCPU，分发走 `Balancer[T]` 接口（默认自适应） | 固定轮询 / worker 数无上限 | worker 只做分发与记账，超过核数只增加争抢；静态轮询对慢 worker 无感知，自适应按 `Load()` 自校正 | 惯例 |
| D3 | `newCh` 容量 100、满则阻塞 accept | 无界队列 / 满了拒绝 | 无界 = 把内存上限交给对端；拒绝丢的是已建立的连接。阻塞回压最简单：worker 处理不过来 → accept 停 → 内核 backlog 接管 → 客户端看到"变慢"而不是服务端 OOM | 性能（防线） |
| D4 | 注册在 worker loop 上做 + 注册闸门（`Add` 与 `CloseAll` 同锁） | ① handler goroutine 里注册；② 关闭后再重扫一遍 | ① 三个关闭观察者（active 计数/drain 轮询/CloseAll）必须看到同一份连接账本；② `-cpu=1` 实测复现"排队的连接落在清扫之后从此无人可关"——同锁串行化才有 happens-before，别无第三种时序 | 复杂度（正确性） |

**D1 的四角度展开**（面试最常被追问的一条）：

- **性能**：等待 I/O 的成本从"每连接一个阻塞执行单元"降到"运行时 netpoll 统一管理 + 事件就绪才唤醒"。Go 的 goroutine 初始栈约 2KB，万级连接的栈内存与调度开销完全可接受；真正的天花板在十万级以上（见局限 L1）。
- **复杂度**：代码是同步风格——`Read`/`Write` 看着阻塞，实际由运行时挂起/唤醒。异常栈、调试、profiling 全是标准工具链；eventloop 方案里这些都要自己造。
- **Go 惯例**：net/http 服务端就是这个模型（`Server.Serve` 循环 Accept，每连接 `go conn.serve`）。自建层没有偏离惯例，只是把"连接编排"（分发、注册、关闭）从 HTTP 语义里解放出来。
- **面试表达**：先说"并发模型的本质是把等待 I/O 从执行线程中剥离"，再区分"运行时已提供的事件驱动（netpoll）"与"应用层要做的事件分发（accept → 分发 → 处理）"——这两层混为一谈是常见的减分点。

### 协议格式

| # | 决策 | 主要备选 | 为什么放弃备选 | 角度 |
| --- | --- | --- | --- | --- |
| D5 | 10B 定长帧头 `[type:1][flags:1][streamId:4][length:4]` + 长度前缀分帧 | ① 分隔符（`\n`）；② varint 长度 | ① 只适合文本、需转义、扫描开销；② 省的字节抵不上解码分支——定长让边界判定 O(1)、上限检查一次比较。字段序与 HTTP/2、Dubbo 同构（对照表见 [Analysis](/Analysis#_5-帧头与主流协议的惯例对照)） | 惯例 + 性能 |
| D6 | `Length` 用 int32 且检查负数 | 只查上界 `> MaxFrameSize` | 高位置 1 的 4 字节读成 int32 是负数，只查上界会放行——恶意长度字段绕过防线打爆内存。防御要一次比较做完：`length < 0 || length > MaxFrameSize` | 性能（防线） |
| D7 | `StreamId` 一物三用：请求/响应配对、PING/PONG 配对、PUBLISH 复用为全局单调序号 | 再加一个 seq 字段 | 多 4 字节且与 StreamId 冗余；单调序号天然就是离线补发的游标 | 复杂度 |
| D8 | envelope 手工拼装 + `DecodeJSONMessage` 的 `Data` 零拷贝别名 payload | 结构体 `Marshal`/`Unmarshal` 全程 | `RawMessage` 的二次拷贝在 1MiB 帧上是 21.4ms→8.6ms 的差距；调用方契约"只读"写进注释。信封组装后来收敛为共享核心（响应路径 −1 次分配/请求），与结构体 marshal 逐字节相等有测试钉住 | 性能 |

**成帧与序列化为什么必须分层**：TCP 是字节流没有消息边界，"帧层负责这是一条完整消息，载荷层负责消息里是什么"是两条独立的演变轴——加帧类型不改编解码，JSON 换 protobuf 时协议头不动。业务函数只见 `RequestHandler func(action string, data []byte)`，对帧一无所知。

### 错误处理与生命周期

| # | 决策 | 主要备选 | 为什么放弃备选 | 角度 |
| --- | --- | --- | --- | --- |
| D9 | 错误三分类：网络错误（关连接）、协议错误（关连接+记录）、应用错误（回错误帧继续用） | 一律断连 / 一律吞掉 | 字节流已不可信时必须断连，业务拒绝时断连则把可恢复故障放大成传输中断——处置必须跟着错误类别走，见下表 | 惯例 |
| D10 | panic 三层策略：业务 panic → recover → `FireException`；出站写失败由 HeadHandler 主动 panic 走同一通道；装配失败故意 panic | 每层 if err | 出站接口 `HandleWrite` 无返回值（与 Netty 一致），"传输死亡"从普通错误流里显式分离出来走统一收尾，比在每个写点散落错误处理更难漏 | 复杂度 |
| D11 | 分级关闭五段：停 accept → drain（10s）→ 强关残余 → 停 worker（5s，超时 dump 全栈）→ 通知 | 一刀切 `Close` | 一刀切砍掉在途请求；五段给在途连接自然结束的机会，超时兜底防单个卡死 handler 拖垮进程，dump 栈让"卡在哪"可见 | 表达 |
| D12 | 空闲回收用 `SetReadDeadline` 而不是每连接一个 timer | 心跳计数 + 定期检查 | deadline 挂在阻塞的 Read 上到点自然醒，无额外 timer 对象；"任何收到的帧都重置 deadline"让心跳免单 | 性能 |

**D9 的处置表**（越靠近传输越果断，越靠近应用越宽容）：

| 类别 | 例子 | 处置 |
| --- | --- | --- |
| 网络错误 | EOF、reset、写失败 | 关连接，业务无感 |
| 协议错误 | 超限帧、非 envelope、分片交错、重复 HELLO | 关连接 + 记录（字节流已不可信） |
| 应用错误 | 未知 action、业务返回 error | 错误 RESPONSE，**连接保持** |

### 资源与内存

| # | 决策 | 主要备选 | 为什么放弃备选 | 角度 |
| --- | --- | --- | --- | --- |
| D13 | Frame/payload 按尺寸分级池化（256B~1MiB 七档），池里存 `*[]byte` cell，回收按 cap 路由 | ① 每长度精确 freelist；② 存 `[]byte` 值 | ① 长度无界则簿记无界，分级把浪费封在 4× 内换 7 个池；② 24B 切片头进 interface 每次 Put 都装箱，cell 指针稳态零分配 | 性能 |
| D14 | 刻意不池化两条路径：客户端入站解码、`Publish` 补发缓存 | 全部路径统一池化 | 客户端 `Response.Data` 别名逃逸到应用代码，没有可追踪的归还时机；补发缓存要活过编码调用本身。**池化不是"全都要"，是"确定能归还的才要"**——公开 API"帧归调用方所有"的契约不变 | 复杂度 |
| D15 | 协议层五张表（订阅/会话/反向索引/补发缓存/字节账本）一把 RWMutex；`connTokens` 反向索引兼作握手标记 | ① 每表一把锁；② 独立 handshaked 布尔 | ① 表间有跨表不变量，分锁就要定义锁序——多锁是死锁面的代名词；② 布尔会与事实漂移，"表里有记录"恰好就是握手完成，零成本复用 | 复杂度 |

**D13/D14 的所有权清点**（池化落地前必须回答"谁还持有 payload"）：

| 路径 | 别名逃逸到 | 能回收吗 |
| --- | --- | --- |
| 服务端入站 | 逃不出 `ctx.HandleRead(frame)` 同步调用链 | ✅ dispatch 返回后 |
| 客户端入站 | `Response.Data` 经 channel 交给应用代码 | ❌ 无归还时机 |
| 出站编码缓冲 | 死在 HeadHandler 的同步 `conn.Write` | ✅ 写返回即回收 |
| `Publish` 补发缓存 | 存在 `topicCache` 活到补发之后 | ❌ 保持精确分配 |

实测（[Benchmark](/Benchmark#帧对象池化-a-b-2026-09-27)）：端到端每请求分配 42→32，池化路径稳态 0 allocs/op，公开解码路径 4→2 次分配且快 38%。

### 扩展点

| # | 决策 | 主要备选 | 为什么放弃备选 | 角度 |
| --- | --- | --- | --- | --- |
| D16 | `pkg` 放接口、`internal` 放实现，依赖单向；`Balancer`/`PipelineInitializer`/codec 全部接口化 | 包按功能切、互相引用 | reactor 需要负载均衡但不应认识具体策略——接口钉住缝之后，新增策略（一致性哈希）零改动核心循环；`internal` 在 Go 语义下对外不可导入 | 惯例 |
| D17 | 负载均衡按后端能力分层约束：`Backend` → `WeightBackend` → `CountBackend` → `LoadAware`，七种策略共用契约测试 | 一个大接口 | 策略需要的后端能力不同（IP Hash 只要 key，自适应要负载值），分层接口让每种策略只依赖它真正用的能力，一套契约测试跑所有实现 | 表达 |

---

## 关键代码走读

### 1. 帧头的防御性解码

`pkg/proto/codec.go`：

```go
length := int32(binary.BigEndian.Uint32(src[6:10])) // deliberate wrap: high bits become a negative length, rejected by the < 0 check below
if length < 0 || length > MaxFrameSize {
    return fmt.Errorf("%w: announced %d, limit %d", ErrFrameTooLarge, length, MaxFrameSize)
}
```

为什么这么写：`MaxFrameSize`（1 MiB）是"一个恶意长度字段不许打爆内存"的防线，但**只查上界是漏的**——对端把 length 高位置 1，`Uint32` 读出约 4 GiB，转成 `int32` 是负数，`> MaxFrameSize` 不成立，防线被绕过。所以负数检查不是防御性编程的装饰，是这条攻击路径的封堵点；注释明写 "deliberate wrap"，让下一个改这段代码的人不会"顺手简化"掉它。

### 2. 背压与竞态的双查

`pkg/reactor/worker.go`：

```go
func (w *Worker) AddConn(conn net.Conn) error {
	select {
	case w.newCh <- conn:
		// Re-check after the send: ctx may have been cancelled while the
		// send raced the loop's exit. ...
		select {
		case <-w.ctx.Done():
			_ = conn.Close()
			return fmt.Errorf("reactor: worker %s already stopped", w.id)
		default:
			return nil
		}
	case <-w.ctx.Done():
		_ = conn.Close()
		return fmt.Errorf("reactor: worker %s already stopped", w.id)
	}
}
```

两件事在同一小段里：**容量 100 的 `newCh` 满则阻塞**——这就是背压，压力沿 Dispatch → accept 循环回传，accept 停下，客户端看到变慢而不是 OOM；**发送成功后再查一次 ctx**——发送与 worker 退出存在竞窗，发送成功但 loop 已死则连接的 fd 无人接管，重查兜住这条泄漏路径。有界队列负责"慢"，双查负责"不漏"。

### 3. 注册闸门：把竞窗压成确定性拒绝

`pkg/reactor/registry.go`：

```go
func (r *ConnectionRegistry) Add(conn net.Conn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	r.conns[conn] = struct{}{}
	return true
}
```

`Add` 返回 bool 是这个结构的全部意义：`CloseAll` 落同一把锁并置 `closed`，此后时序只剩两种且都安全——Add 在前 → CloseAll 必关它；CloseAll 在前 → Add 被拒，worker 自关连接且不启动 handler。没有第三种时序，关闭路径的 5 秒等待不再可能白付。这个 bug 不是审出来的，是 soak 门禁按 60 秒下界在 2 核 runner 上首跑实录抓到的（自适应 balancer 的 `Next` 以 (nil, nil) 返回零值后端，一帧后 nil deref）——**门禁的价值就是让这类时序 bug 在合并前被时间 axis 抓住**。

### 4. 分级关闭五段

`pkg/reactor/reactor.go`：

```go
// stage 1: stop accepting
r.stopping.Store(true)
_ = r.listener.Close()
// stage 2: give existing connections a chance to drain
for r.workers.TotalCount() > 0 && time.Now().Before(deadline) {
	time.Sleep(10 * time.Millisecond)
}
// stage 3: force close what is left; this unblocks reads
if closed := r.workers.Registry().CloseAll(); closed > 0 { ... }
// stage 4: stop worker loops and wait for handlers to return
r.cancelFunc()
r.workers.Stop()
if !r.workers.AwaitDone(handlersExitTimeout) {
	// a handler that outlives shutdown is a leak; dump every goroutine stack
	err = fmt.Errorf("reactor: connection handlers did not exit within %s", handlersExitTimeout)
	...
}
```

顺序不可换：不先停 accept，drain 期间新连接还会进来；不先 drain 就强关，砍掉的是本可自然结束的在途请求；第 4 步超时必须 dump 全部 goroutine 栈——handler 卡死是泄漏，操作者需要看到卡在哪，而不是只看到一句超时。与题二 `srv.Shutdown` 的"停收 → 排空 → 兜底强关"是同一副骨架（见 [题二](/q2-design#_1-优雅关闭的-select-两臂)）。

### 5. 客户端并发的两处临界区决策

`pkg/proto/client.go` 的 `roundTrip`：

```go
c.mu.Lock()
c.nextID++
id := c.nextID
ch := make(chan *Response, 1)
c.pending[id] = ch
...
if msg.buf != nil {
	// the common case: writing outside the lock keeps a slow socket
	// from stalling other callers that only want to register
	c.mu.Unlock()
	_, err = c.conn.Write(msg.buf)
	...
} else {
	// fragments of one message must reach the wire back to back, so
	// concurrent callers cannot interleave their own frames between
	// them; that is worth holding the lock through the writes
	_, err = writeAll(c.conn, msg.bufs)
	c.mu.Unlock()
	...
}
```

同一函数里对锁的两种态度，各有一个不变量撑腰：**分配 ID 与注册 pending 必须同一临界区**——分两步的话，两个并发 Call 可能拿到相邻 ID 却以相反顺序注册，读循环按 ID 回投就张冠李戴；**单帧写放锁外、分片写持锁**——慢 socket 不该挡住别的调用者注册，但同一请求的分片必须背靠背上线，否则并发请求的帧会插进分片序列中间。等待通道 cap=1 是第三个决定：读循环发完就不再关心等没人在等，**解耦方向是刻意的——慢的是调用方，读循环必须永远快**。

---

## 已知取舍与局限

诚实列出没做的事，每条都有"为什么现在不做"。面试里主动说这些，比被问出来强。

- **L1 · goroutine-per-connection 的规模天花板**——每 goroutine 栈 2KB 起步，十万级连接约数百 MB 栈内存加调度压力；百万级长连接是单线程 eventloop（gnet、cloudwego/netpoll）的量级，那是用回调复杂度换内存，属于另一个量级的需求。本设计的目标区间（万级）内它是复杂度最低的正确答案。
- **L2 · HTTP/2 式 credit 流控未做**——当前背压是"有界队列 + 剔除慢订阅者"，对推送网关够用；credit 需要双向窗口记账与复杂度成倍的协议状态，是下一个量级的需求。
- **L3 · JSON 载荷的反射成本**——端到端 echo 现在每请求 31 次分配，其中反射本体约 13 次，一分没动，因为它不是拼掉一次拷贝就能消的东西。接口已隔离：换 protobuf 整块消掉，帧层不动。两条看着能省的捷径已判定不做——SWAR 扫描实测持平或更慢；手写 JSON 编解码只能再省 1~2 次装箱，却要把转义/数字/嵌套文法的正确性整片接过来，收益与风险不对称。
- **L4 · 会话状态单实例**——五表一锁在单进程内正确；多实例部署需要会话/订阅状态外置（Redis），那是分布式章节的事。
- **L5 · TLS 与认证未做**——Flags 预留位已留，传输加密交给部署层（LB 终结或未来的帧层加密标志位）。
- **L6 · 池化刻意不彻底**——客户端入站与补发缓存保持精确分配（所有权清点的结论，见 D14）；公开 API"帧归调用方所有"的契约不变。这意味着每请求仍有 2 次分配是**设计出来的**，不是遗漏。
- **L7 · 性能数字有噪声底**——采样窗口内机器满载时，同码对照基准的差值就有 ±10~15%。所有优化结论都先量噪声底再下判断（用"两侧代码完全相同"的基准量尺子），[Benchmark](/Benchmark) 里有完整方法。

---

## 与题二的互证

两道题在两处独立做出了同一个形状，这是设计方法论一致的最好证据：

- **优雅关闭**：题一的"停 accept → drain → 强关 → 停 worker"与题二 `srv.Shutdown` 的"停收 → 排空 → 超时强杀"是同一副骨架——net/http 内建了前两段，题一手写了全部五段。
- **空闲回收**：题一在协议层用 `SetReadDeadline` 回收静默连接，题二在 HTTP 层用 `IdleTimeout` 回收空闲 keep-alive——同一问题（无活动连接占着资源）的两个层次。

下一题的设计决策见[题二：http-service——为什么这样设计](/q2-design)。
