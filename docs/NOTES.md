# 技术知识梳理：原理与考点

与 [DESIGN.md](DESIGN.md) 分工：DESIGN 讲"这套系统**为什么**这么设计"，
NOTES 讲"支撑这套设计的**基础原理**"，并把每块知识挂到本仓的具体落点——
面试时可以直接从任何一节切入。每节按 **原理 → 本仓落点 → 常见追问** 组织，
追问附参考答案。

---

## 1. TCP 字节流与消息成帧："粘包/半包"的正名

**原理**：TCP 只保证字节流有序可靠，不保留应用层消息边界——"粘包"是
误称，不是包粘在一起，而是**应用层从来没有"包"这个概念**。一次 `Read`
返回的字节数取决于内核缓冲区状态：可能半条消息、可能一条半。应用层必须
自行定义边界，这叫成帧（framing）。

主流成帧法对比与取舍（为何长度前缀胜出）见
[Analysis.md §2](Analysis.md)；帧头逐字段对照 HTTP/2/Dubbo/gRPC 的
惯例考证见 [Analysis.md §5](Analysis.md)。

**本仓落点**：
- 10 字节定长帧头 `[type:1][flags:1][streamId:4][length:4]`，大端序
  （`pkg/proto/frame.go`）——大端即网络字节序，跨平台有一致的内存排布
  解释，`encoding/binary.BigEndian` 直接读写；
- `io.ReadFull` 两次：先读满 10 字节头，按 `Length` 读满载荷
  （`pkg/proto/codec.go:64-84`）——读满是"恰好一条消息"的可靠下界。

**常见追问**：

- *为什么长度字段是定长 int32 而不是 varint？* 定长换来 O(1) 边界判定与
  一次比较的防御（`length > MaxFrameSize` 即断连）；varint 省小消息的字节
  但解码有分支、上限检查不直观。HTTP/2/Dubbo 都是定长；Protobuf
  delimited 用 varint 是因为它整条消息就是 varint 世界。
- *`Length` 为什么是 `int32` 还要查负数？* 对端可以发 4 字节任意值；
  `binary.BigEndian.Uint32` 读出后转 `int32`，最高位置 1 就是负数——
  只查上界会放行负数，`make([]byte, 负数)` 直接 panic。所以解码检查是
  `length < 0 || length > MaxFrameSize`（codec.go:50）。

## 2. 帧解码的状态机与流式聚合

**原理**：流式解析的本质是**跨 Read 调用维护状态**：每读一段，状态机
前进一步（找头 → 收载荷 → 完成）。两种形态：
(a) 隐式状态机——阻塞式 `io.ReadFull` 把"缺多少字节"交给运行时挂起，
代码是同步的，状态藏在"读到了第几段"里；
(b) 显式状态机——`io.Reader` + 内部缓冲 + 手写迁移，适合回调式 eventloop。
Go 的 netpoll 让 (a) 获得回调级的效率：goroutine 在"数据未就绪"时被
gopark，就绪后原地唤醒，同步代码天然就是状态机。

**本仓落点**：`DecodeStreamed`（`pkg/proto/stream.go`）在单帧解码之上
多了一层**分片聚合状态机**：
- 收到带 `FlagMore` 的帧 → 进入聚合态，记住 `streamId` 与 `frameType`；
- 后续帧必须逐一匹配两者，任何不匹配即协议错误断连——分片一旦交错，
  字节流已不可信任，这是"宁可断不可错"的边界；
- 聚合总量超 `MaxStreamSize`（8 MiB）即 `ErrStreamTooLarge`——单帧有
  `MaxFrameSize` 封顶，多帧累加必须有第二道上限，否则"合法分片"就是
  放大攻击的载体；
- 聚合缓冲直接**别名首片**再 `append`（stream.go:53）：首片缓冲是
  `Decode` 刚分配、无其他持有者的，扩容写无人能观察到旧切片——省一次
  全量拷贝。`append` 的均摊翻倍增长让聚合的成本接近 O(n)。

**常见追问**：

- *聚合状态放哪、怎么清理？* 放在 FrameCodec（每连接一个实例）的字段
  之外——实际是 `DecodeStreamed` 的局部变量，一次调用要么聚完要么报错，
  **不跨调用持有状态**，所以没有每连接清理问题。这是"状态生命周期跟着
  调用走"的设计，代价是连接上同一时刻只能有一个进行中的逻辑帧——在
  请求/响应模型里这恰好是对的（响应端按流序返回）。
- *发送方怎么保证分片不交错？* 客户端并发 `Call` 时，多帧分片在锁内
  背靠背写出（client.go:258-263）；单帧在锁外写，不挡其他注册者。

## 3. 背压：四条路径

**原理**：背压 = 让"下游处理不动"沿调用链回传成"上游减速"，而不是在
中间某处无限堆积。有界队列 + 阻塞发送是最朴素的结构性背压。

**本仓落点**（从外到内四条）：
1. **accept → worker**：`newCh` 容量 100，满了 `Dispatch` 阻塞 accept
   循环，内核 backlog 接管（worker.go:167）。备选"满了丢弃已 accept 的
   连接"也可，但阻塞更简单且不丢已建连接。
2. **发布 → 慢订阅者**：`Publish` 对每个订阅者 `SetWriteDeadline(3s)`，
   超时/失败即剔除（protocol.go:634-646）。这是"背压的极限形态"：
   不缓存不等待，跟不上就退订——推送网关最常见的取舍。
3. **读循环 → 调用方**：pending 通道 cap=1，读循环永远不等调用方——
   慢的必须是调用方，读循环卡死会连累整条连接的所有请求。
4. **提交者 → 执行器**：executor 的任务通道无缓冲，`Execute` 直 Handoff
   给空闲 worker，没有空闲 worker 就阻塞提交者（async_executor.go）。

**常见追问**：*为什么不用 HTTP/2 式 credit 流控窗口？* credit 需要
双向窗口记账、协议状态复杂一档；本设计的场景（慢订阅者直接剔除 +
请求侧阻塞）已经覆盖主要风险，credit 列为演进方向（DESIGN §4.1）。

## 4. 内存与零拷贝

**原理**：Go 里每次堆分配都是 GC 根的候选；"零拷贝"优化的对象不是
syscall（那是 sendfile/splice 的领域），而是**用户态内多一次内存搬家**。
`json.RawMessage` 的陷阱是典型：它实现了 `MarshalJSON`，但实现是
append 式拷贝——把已经编码好的 1 MiB 载荷塞进 envelope 时会**整体重拷
一次**。

**本仓落点**（一处真实教训 + 三处主动设计）：
- **教训**（Benchmark.md 分层定位记录）：`EncodeJSON` 原来走结构体
  Marshal，1 MiB 载荷单次 21.4 ms（纯 `json.Marshal` 同载荷 4.8 ms），
  Call 往返两端各一次。修为手工拼装 envelope（`pkg/proto/json.go`），
  RawMessage 直接透传，降到 8.6 ms；等价性由字节级对照测试 + 随机
  串 quickcheck 钉死（`TestEncodeJSONMatchesStruct`）。
- **解码侧**：`zeroCopyRawMessage` 的 `UnmarshalJSON` 记录解码器输入的
  子切片而非拷贝（json.go:114-119）——`Data` 别名 `frame.Payload`，
  调用方契约"只读"写进注释。别名是有代价的：帧缓冲从此不能回池。
- **拼接侧**：手拼前先精确算 capacity（json.go:47-60），一次分配到位，
  避免 append 扩容的多次搬运。
- **题二同理**：`Value = json.RawMessage` 原样存取，校验一次、永不重编码
  （http-service/store.go:9-13）。

**常见追问**：*零拷贝的边界在哪？* 别名共享意味着所有权变窄：谁持有
别名，谁就不能释放/复用原缓冲。所以 `sync.Pool` 池化 Frame 与零拷贝
别名是**冲突**的——池化要求"用完归还"，别名要求"原缓冲活得比我久"，
二者只能选一（DESIGN §4.2 把这个冲突列为池化改造的前置问题）。

## 5. goroutine 与 GMP 调度

**原理**：G-M-P 三元组——G 是 goroutine（用户态栈，约 2 KB 起步，可
增长，栈拷贝式扩容）；M 是 OS 线程；P 是逻辑处理器（数量 = GOMAXPROCS），
G 必须挂在 P 上才能执行。调度要点：
- **阻塞让位**：goroutine 阻塞在 channel/锁/netpoll 时，gopark 让出 P，
  同一个 M 转头跑别的 G——"阻塞一个 goroutine"成本只是一个栈，不是
  一个线程；
- **系统调用阻塞不同**：M 陷入阻塞 syscall 时 P 被剥离（handoff），
  sysmon 监控超时并找空闲 P——这就是为什么"每次 Read 都陷内核"的
  模型仍然能扩展，但频繁 syscall 会产生 M 抖动；
- **work stealing**：P 本地队列空了从全局队列/其他 P 偷一半，天然负载
  均衡；网络就绪的 G 由 netpoll 注入运行队列；
- `runtime.LockOSThread` 把当前 G 钉在当前 M 上，用于需要线程亲和
  （TLS、亲和缓存）的场景。

**本仓落点**：goroutine 全表（谁、几个、何时退出）见
[DESIGN.md §1.4](DESIGN.md)。两点值得展开：
- worker 数默认 = NumCPU 且上限锁死（reactor/options.go:31-36）：worker
  的职责是分发与记账而非计算，超过核数只增加争抢；`LockThread` 是可选
  开关——注意**只有 worker loop goroutine 被钉住**，它派生的连接
  handler goroutine 并不继承钉定，所以这个选项改善的是 loop 的缓存
  亲和，不是连接处理的亲和。
- "每连接一个 goroutine"的量级账：2 KB 栈 × 10 万连接 ≈ 200 MB + 调度
  压力——可行但不省；百万连接选 eventloop 库（gnet/netpoll）。
  这是 Analysis §6 明说的适用边界。

**常见追问**：*goroutine 和线程的本质区别？* 栈大小与切换成本
（goroutine 栈 KB 级、切换是用户态函数级；线程 MB 级、切换陷内核）、
调度方（runtime vs OS）、阻塞语义（goroutine 阻塞让出 P，线程阻塞
占着 M）。

## 6. channel 深入

**原理**：channel 底层是 `hchan`：环形缓冲 + 发送/接收等待队列
（sudog 链表）+ 一把互斥锁。关键语义：
- **直接交接**：无缓冲 channel 有等待的接收方时，发送方把值直接拷进
  接收方的栈，不经过缓冲；
- **close 是广播**：close 唤醒所有等待者；向已关闭 channel 发送 panic，
  从已关闭 channel 接收立即返回零值（有缓冲则先排空）——所以
  "发送方负责 close、接收方永不 close"是铁律；
- **select 随机性**：多个 case 就绪时**伪随机**选择，防止饥饿——这不是
  细节，是可观察语义（本仓有一段真实教训，见 §15）；
- **nil channel**：收发永久阻塞，select 里把不用的 case 置 nil 是经典
  技巧；
- channel 不是万金油：单值状态用原子、共享结构用锁，channel 的价值在
  **交接所有权与事件通知**。

**本仓落点**：
- `done chan struct{}` + `close(done)` 是全仓统一的"一次性广播"惯用法
  （客户端 closed、resilient done、janitor closed）——关闭即广播，
  等待者 `select <-done` 或 `<-c.Done()`；
- "发送后重查"模式出现三次（`AddConn`、executor `Submit`/`Execute`）：
  向 channel 发送成功**不代表**对端还活着——发送与消费方退出竞态时，
  重查 `ctx.Done()` 兜底，否则连接/future 永远无人认领；
- executor 故意**不 close 任务通道**（async_executor.go:63-69）：close
  后任何并发 Submit 变 panic、worker 会收到零值函数——用 ctx 取消 +
  nil 守卫代替 close，这是"close 语义太强，退出不必 close"的范例；
- `pending` cap=1 的解耦：读循环投递后不关心调用方死活。

**常见追问**：*WaitGroup 为什么不能替代这里的 active 计数？* `Add` 与
`Wait` 并发是未定义行为，而连接 handler 是**动态**启动的——原子计数 +
轮询没有这条规则（worker.go:44-49 注释原话）。

## 7. 锁与原子：选型决策树

**原理**：
- `sync.Mutex`：常规互斥；`sync.RWMutex`：读多写少，**写者优先**
  （有写者等待时新读者阻塞，防写者饿死），**不可递归读**（同 goroutine
  嵌套 RLock 若夹着一个等锁的写者即死锁）；
- `sync.Map`：read map（无锁读）+ dirty map（带锁写）双结构，miss 计数
  触发重建。适用面窄：键集基本只增、读远多于写的旁路缓存；通用场景
  反而比 `RWMutex+map` 慢（官方文档自己这么说）；
- 原子操作：单值、无跨字段不变量时的最廉价选择，默认顺序一致性；
- 死锁面管理：**锁的数量是最小化对象**——每多一把锁就多一套锁序约定。

**本仓落点**：
- 协议层五张表一把 RWMutex（DESIGN §1.3 论证了跨表不变量为何要求
  单锁）；`lastSeen` 从锁里拆出来做 atomic——时间戳刷新发生在读锁下，
  不升级写锁，这是"锁保护结构、原子保护单值"的混合范例；
- 题二的选型论证：KV 服务读写均衡，`sync.Map` 的双 map 维护反而亏；
  锁放 Store 层还有结构性红利——换远程存储时锁自然消失；
- balancer `Iterate` 持读锁回调用户函数——回调里若再碰同一 balancer
  的写锁会死锁，这个约束靠"回调只做 Stop/Start 这类无回环操作"的
  使用纪律维持（Stop 只 cancel，不回 Interate——worker.go:115-120）。

## 8. GC 与分配经济学

**原理**：Go GC 是并发三色标记清除 + 混合写屏障，STW 亚毫秒级。成本
模型：**分配便宜、回收有税**——堆分配本身十纳秒级，但对象图越大标记
越久、写屏障越忙。因此 Go 性能优化的第一杠杆不是"少算"，而是**少分配、
分配小、分配少逃逸**。工具链：`-benchmem`（allocs/op）、
`GODEBUG=gctrace=1`（GC CPU 占比与 STW）、`go build -gcflags=-m`
（逃逸分析）。

**本仓落点**：
- Benchmark.md 的数据口径：小帧编码 1 次分配、64 KiB 大帧仍然 1 次
  （按 Length 精确分配）、1 KiB 端到端请求 56 次分配（约 19 次来自
  encoding/json 反射）——**分配数是比耗时更稳定的结构指标**，Benchmark
  三次复采都以"分配数随载荷对数增长而非随分片数线性"作结构结论；
- gctrace 的实战记录：定位流式大消息延迟时 `GODEBUG=gctrace=1` 显示
  GC CPU 占比 ~1%、STW <0.1 ms，**据此排除 GC**、把嫌疑集中到双重
  编码（Benchmark §附）——"先排除，再定位"的方法论比结论本身更值钱；
- 未做 sync.Pool 的原因见 §4 末尾：与零拷贝别名冲突。

**常见追问**：*reduce allocation 的常规手法？* 预分配 capacity、
`[]byte` 替代 string 转换、复用缓冲（池或 caller-provided）、避免
interface 装箱小值、`json.RawMessage` 透传免二次编解码。

## 9. netpoll 与 I/O 多路复用

**原理**：Linux 下 Go runtime 初始化即建 epoll 实例；net.Conn 的读写
经 `internal/poll.FD` 注册到 epoll（非阻塞 fd + 边缘触发）。goroutine
对未就绪 fd 发起 Read 时：注册 interest → gopark；epoll_wait 由
runtime 汇合（findrunnable 时 poll 或专门的 poll 线程），就绪后把等待的
G 重新入队。**应用层视角**：`Read` 看似阻塞，实为调度事件——这就是
"非阻塞 I/O 不需要回调"的机制来源。

**本仓落点**：Reactor 不自己碰 epoll，而是**借运行时的非阻塞语义做
应用层事件编排**（accept → 分发 → 处理 → 生命周期）——Analysis §事件
驱动 有与手写 epoll 回调、thread-per-connection 的三方对比；`pkg/channels`
的 Selector/SelectKeys 接口词汇预留了"将来换自管 selector"的缝，但当前
实现诚实地建立在 netpoll 之上（不重复造 epoll 轮子是设计立场，不是
偷懒）。

**常见追问**：*水平触发 vs 边缘触发？* LT：就绪状态持续报告，忘了读
会反复唤醒；ET：只在状态变化时报一次，必须一次搬空（循环读到
EAGAIN）。Go 的 netpoll 用 ET + 非阻塞 fd，靠 gopark/唤醒协议保证不丢
事件；应用层 `ReadFull` 循环里的每次"不够"都再次挂起等待新事件。

## 10. HTTP 服务的连接管理与超时

**原理**：`net/http` 默认**零超时**——每个作用域都没有兜底，slowloris
（开 socket 慢速发字节）能耗尽 goroutine 与 fd。四个超时的作用域递增：

| 超时 | 覆盖区间 | 缺失后果 |
| --- | --- | --- |
| ReadHeaderTimeout | 连接建立 → 请求头读完 | 不发头的连接永久占坑 |
| ReadTimeout | 连接建立 → 请求体读完 | 慢速滴注 body |
| WriteTimeout | 请求头读完 → 响应写完 | 响应写一半挂起无人管 |
| IdleTimeout | keep-alive 两个请求之间 | 空闲连接永占 fd |

**本仓落点**（http-service/main.go:47-51）：四个全设（5s/10s/10s/60s）。
两个配套：
- `Shutdown(ctx)` vs `Close()`：前者停收新请求 → 等在途 → 关 keep-alive
  （hijacked 连接除外，如 WebSocket 需自己管）；后者立即断一切。优雅
  关闭的完整模式两题同构（§11）；
- Go 1.22 路由：`mux.HandleFunc("PUT /kv/{key}", h)` 方法不匹配自动
  405+Allow，`r.PathValue("key")` 取参——标准库覆盖面本身就是考点
  （DESIGN §2.5 决策 5）。

**常见追问**：*为什么 IdleTimeout 可以远大于 ReadTimeout？* 空闲是
常态不是攻击；读超时防的是"开始了却不完成"，空闲超时防的是"完成后
不告别"。另一个细节：请求体大小用 `io.LimitReader(body, max+1)`，
**+1 字节**区分"恰好满"与"超了"——读满 max 字节时无法区分两者，
多读一字节让超限变成确定事件（service.go:64-78）。

## 11. 优雅关闭：一个模式，两种实现

**原理**：SIGTERM 是"通知"不是"自杀令"。通用三段式：
**停止接收 → 排空在途 → 超时兜底强关**。每段都有讲究：停止接收必须
先于排空（否则永远排不空）；排空要有界（单个卡死的 handler 不能拖着
进程被 SIGKILL）；强关要让阻塞者醒（close fd / cancel ctx）。

**本仓落点**：
- 题二：`signal.NotifyContext` → select 在 `errCh` 与 `ctx.Done()` 间 →
  `srv.Shutdown(sctx)` → 退出码 0/1 反映干净与否（main.go:64-84）；
  errCh 缓冲 1，保证主流程已走关闭分支时监听错误仍可投递不丢；
- 题一：五段分级（reactor.go:177-207）——停 accept（`stopping` 标志让
  accept 循环区分"关闭性 close"与"真失败"，避免 drain 期间错误日志
  刷屏）→ drain 10s → `Registry.CloseAll` 强关唤醒所有阻塞读 →
  ctx 取消停 worker → `AwaitDone(5s)`，超时 dump 全部 goroutine 栈并把
  非 nil 错误交给调用方；
- 关闭路径的通用惯用法：`sync.Once` 幂等（stopOnce/closeOnce）、关闭
  前先立标志（stopping）、"谁创建谁关闭"（注册在 worker loop 上的连接
  由 registry 统一强关）。

**常见追问**：*为什么退出码重要？* systemd/k8s 靠它判断滚动重启是否
干净；超时退出 1 让编排方选择重试或告警。*Shutdown 之外还要做什么？*
hijacked 连接、后台 goroutine（janitor/keepalive）、已提交未执行任务——
本仓的对应物是 `ProtocolHandler.Close()`、`Client.Close()`、
executor 的 ctx 取消。

## 12. 错误处理体系

**原理**：Go 错误处理的三层结构：
1. **哨兵错误**（`errors.New` 包级变量）：跨层传递的稳定契约，配
   `errors.Is` 匹配；
2. **错误包装**（`fmt.Errorf("%w")`）：保留链路、附加上下文，`errors.Is/As`
   沿链穿透；
3. **类型化错误**（自定义 struct）：需要携带结构化信息时用。

设计要诀：**错误在哪个层产生，就只在该层有语义**——业务层不知道
HTTP 状态码，协议层不知道日志格式。

**本仓落点**：
- 题二的哨兵矩阵：`ErrNotFound→404`、`ErrKeyInvalid/ErrValueInvalid→400`、
  `ErrValueTooLarge→413`，映射集中在 `writeError` 一处；500 用固定文案、
  细节进日志——**错误文本是给操作者的，不是给调用方的**；
- 题一的三分类（DESIGN §1.5）：网络错误关连接、协议错误关连接并记录、
  应用错误回错误响应保连接。原则一句话：**越靠近传输越果断，越靠近
  应用越宽容**——分帧被破坏意味着后续字节流不可解析，维持连接只会
  生产更多垃圾；而"未知 action"拆除传输则错杀了正常客户端；
- panic 策略三层：业务 panic → recover → FireException（单连接故障不
  波及进程）；出站写失败由 HeadHandler 主动 panic 走异常通道（出站接口
  无返回值，panic 是显式的"传输死亡"信号）；装配期编程错误故意 panic
  变成启动期可见崩溃。

**常见追问**：*为什么哨兵错误优于字符串匹配？* 可组合、可包装穿透、
IDE 可导航；字符串匹配在错误信息被包装后即失效。*什么错误该 panic？*
不变量被破坏（编程错误）而非运行条件不满足（运行错误）。

## 13. 资源释放与 goroutine 泄漏防护

**原理**：goroutine 泄漏 = 一个 goroutine 永久阻塞且无人能唤醒它。
三种经典形态与解法：
1. **阻塞在 channel**：发送/接收方先于对端退出 → 用 done channel +
   select 或带缓冲的解耦通道；
2. **阻塞在 Read**：对端静默消失 → deadline / 强关 fd；
3. **后台循环没有停止信号**：ticker/janitor → 一律配 close-able 的
   stop 通道。

检测：`go.uber.org/goleak` 在包级 TestMain 快照 goroutine，任何泄漏当场
失败。

**本仓落点**：
- **goleak 是包级守门人**（proto/executor/reactor 的 TestMain）：协议层
  尤其重要——janitor、keepalive、reconnect watch、read loop 四类后台
  goroutine 都必须随对象关闭；
- **空闲回收**：`IdleTimeout` → 每次读前 `SetReadDeadline`，静默连接的
  阻塞读到点醒来被关（worker.go:290-295）——deadline 挂在读上，不需要
  timer 对象；
- **四条关闭路径全部幂等**：`lifecycleConn.Close` 用 `atomic.Bool.Swap`
  保证重复关无害（worker.go:249-255）——对端 EOF/协议错误/发布剔除/
  服务关闭都可能关同一条连接，幂等让 defer 无条件执行；
- **本仓自己抓到的泄漏**：ResilientClient 的 `swap` 与 `Close` 竞态，
  重连成功后安装新连接会泄漏一对活连接（读循环 + 服务端 handler）——
  g leak 抓到的签名，修法是锁内"检查-安装"原子化（resilient.go:326-331
  注释原话"the exact goleak signature"）；
- 会话 janitor 的成员集回收（本任务新增修复）：过期会话的传输从订阅
  成员集移除、释放其扣住的补发缓存——**滞留资源的特征是"有账本没人
  认"**，每个后台清扫都要问一句：还有没有第二本账没清？

**常见追问**：*怎么在线上发现泄漏？* `runtime.NumGoroutine` 趋势
（soak 测试采样的就是它）、pprof goroutine profile 按创建点聚合、
`runtime.Stack(buf, true)` 全量 dump（本仓在关闭超时时就是这么做的）。

## 14. 边界条件与恶意输入防护

**原理**：对不可信输入的防线要**在字节进内存之前**生效；每个"长度"
字段都是攻击面，每个"聚合/缓存"都是放大器。防御是分层的，任何单层
失守不致命。

**本仓落点**（从线格式到会话态逐层）：

| 层 | 威胁 | 防线 | 锚点 |
| --- | --- | --- | --- |
| 帧头 | 恶意超大 Length | `> MaxFrameSize(1MiB)` 断连 | codec.go:50 |
| 帧头 | 负 Length 绕过上界 | `length < 0` 同路断连 | codec.go:50 |
| 聚合 | 分片洪泛 | `MaxStreamSize(8MiB)` 上限 | stream.go:66 |
| 聚合 | 分片交错注入 | streamId/type 不匹配即断连 | stream.go:59-64 |
| 载荷 | 非 envelope 垃圾 | 解码失败断连 | protocol.go |
| 会话 | token 猜测 | crypto/rand 16B；熵源故障降级时间戳而非拒绝服务 | protocol.go:255-263 |
| 会话 | 重复握手顶掉在用会话 | 二次 HELLO 断连 | protocol.go:335 |
| 会话 | 死 token 堆积 | TTL + janitor | protocol.go:222 |
| 缓存 | topic 无界 × 单帧 1MiB | 条数 64 + 全局 64MiB 双预算、整 topic 逐出 | protocol.go:124-133 |
| HTTP | slowloris | ReadHeaderTimeout | main.go:47 |
| HTTP | 无界 body | LimitReader(max+1) | service.go:72 |
| HTTP | 非单值 JSON | json.Valid（"恰好一个"排除拼接体） | service.go:79 |

**验证手段**：模糊测试三目标（`pkg/proto/fuzz_test.go`）累计约 320 万
次执行零发现；手工边界用例（超限/负长度/截断帧/分片交错）表驱动覆盖。
**模糊测试的正确姿势**：种子语料随 `go test` 常规跑（CI 兜底），
`-fuzz` 挖掘要有**固定预算的门禁**（`scripts/fuzz-smoke.sh`，每次推送
20s/靶）加本地深挖——三档都要有，兜底的东西各不相同：种子兜底"已知的反例
不再复发"，门禁兜底"每次推送都重新看一眼不变量"，深挖兜底"找到人类想不到
的输入"。门禁的靶名靠 `go test -list` 逐包向**工具链**要（新增 Fuzz 函数
即刻受管；"0 靶 0 崩溃"的假绿直接判失败）——注意别改成 grep 源码签名：
编译器只约束形参的**类型**（`*testing.F`）、不约束形参**名**，
`func FuzzX(t *testing.F)` 完全合法，正则里写死 `f` 就会漏掉它并照样报
PASS，这正是"用工具链当权威"的理由；崩了就把 Go 自动落盘的语料提交回来当永久回归用例。
不带 `-race` 是分工：检测器把变异吞吐打掉约一个数量级（同靶同预算实测
13.0 万 vs 1.19 万次 execs，约 11 倍），竞态归 `-race` 那步管。**约束写在
靶内的断言里（任意字节不 panic、Encode 接受的必须原样解回），fuzz 只提供
搜索力**——所以时长或"跑了多少万次"都不是安全度量。

**常见追问**：*为什么缓存预算超限整 topic 逐出而不是删最旧一条？*
删最旧会把被挤压 topic 的补发序列正好断在中间——该 topic 的补发承诺
变成半残；整 topic 拿掉则语义自洽为"有缺口，客户端可检测跳变"。
**语义完整性优先于内存均摊。**

## 15. 测试方法论：覆盖率与确定性

**原理**：好测试的两个正交维度——**覆盖**（执行到所有分支）与**确定性**
（同样输入永远同样结果）。并发代码的难点在于两者冲突：select 两臂
皆就绪、goroutine 交错点，都是运行时掷硬币。

**本仓落点**：
- **分层金字塔**：纯逻辑表驱动（无 I/O）→ `net.Pipe` 组件测试（无端口，
  微秒级）→ 真 TCP 端到端 → SOAK 长稳（门控）。DESIGN §3 有完整矩阵；
- **`net.Pipe` 的独特价值**：同步、内存内、**关闭后写必失败**——TCP 上
  "对端已关"的首写可能成功（半关闭语义），pipe 让"写失败"成为确定性
  事件，边界测试要选可确定性的传输模拟；
- **概率分支的覆盖**（真实案例）：executor `Submit` 的"发送成功后发现
  已停止"分支，两臂皆就绪、运行时随机选。初版测试"命中即 return"，
  每次运行只掷一次硬币，覆盖率 50% 赌局；修正为跑满 64 次并断言
  **每个** future 都 cancelled——覆盖概率 1-2⁻⁶⁴，且断言从"碰巧见过"
  变成"必须恒真"。教训：**测随机分支时，断言要选与硬币无关的不变量**；
- **时间不进测试**：janitor 测试直接驱动 `reapExpiredSessions`、直接改
  `lastSeen` 时间戳，不 sleep 等 ticker（janitor_test.go:56-58 注释：
  "a real sleep would bet the CI scheduler on a 30ms margin"）——把
  时间变成可注入的值，是并发代码可测性的关键一步；
- **契约测试**：七种负载均衡共用一组行为断言（幂等注册、size 讲真话、
  Next 可用），新策略自动继承全部契约。

**常见追问**：*覆盖率 100% 说明什么、不说明什么？* 说明每条语句都
执行过；不说明断言有效、不覆盖并发交错、不覆盖数据依赖。所以本仓
补的是：竞态检测（-race）管交错、goleak 管泄漏、fuzz 管恶意输入、
soak 管时间维度——**每个工具补覆盖率的一个盲区**。

---

## 考点速查（一页版）

| 提问 | 一句话答案 | 深入 |
| --- | --- | --- |
| TCP 粘包半包怎么解决？ | 没有包，只有字节流；长度前缀 O(1) 定界 | §1 |
| 非阻塞 I/O 为什么不用回调？ | netpoll 把就绪事件变成 goroutine 唤醒，同步代码即状态机 | §2 §9 |
| 背压怎么实现？ | 有界队列阻塞回传 + 慢消费者剔除，四条路径 | §3 |
| 零拷贝指什么？ | 用户态少一次内存搬家；代价是所有权变窄，与池化冲突 | §4 |
| goroutine 为什么便宜？ | 2KB 可增长栈 + 用户态调度；阻塞让出 P 不占线程 | §5 |
| channel 底层？ | hchan：缓冲环 + sudog 队列 + 锁；close 是广播，多臂 select 随机 | §6 |
| sync.Map 什么时候快？ | 键集只增、读远多于写；通用场景 RWMutex+map 更好 | §7 |
| Go GC 怎么调优？ | 少分配是第一杠杆；-benchmem/gctrace/逃逸分析三板斧 | §8 |
| HTTP 四个超时各管什么？ | 头/整请求/响应/keep-alive 空闲，作用域递增缺一不可 | §10 |
| 优雅关闭怎么做？ | 停收 → 排空 → 超时强关，退出码反映干净与否 | §11 |
| 错误怎么分类处理？ | 哨兵 + %w 包装；传输错误断连、应用错误保连接 | §12 |
| goroutine 泄漏怎么防？ | 三种形态各有解法；goleak 守门 + deadline 兜底 | §13 |
| 恶意输入怎么防？ | 长度字段全是攻击面；上限在进内存前检查，分层设防 | §14 |
| 100% 覆盖率怎么来的？ | 分层金字塔 + 确定性构造；-race/goleak/fuzz 补盲区 | §15 |
| 覆盖率怎么防回归？ | 逐包门禁 + CI 强制；纯接口包跳过，误用与失败分开退出码；`-count=1` 防缓存空跑 | §16 |
| fuzz 怎么进 CI？ | 靶名向工具链要（`go test -list` 逐包，新增即受管）+ 固定预算 + 崩语料落盘成回归用例；"0 靶 0 崩溃"与"某个包装不上"都判失败 | §14 |
| 偶现测试失败怎么治理？ | 先确定性修复，残留时序风险用 CI 定向重复测 flake 密度 | §17 |

---

## 16. 覆盖率门禁：把"承诺"变成"检查"

**原理**：覆盖率是**会腐烂的承诺**。测试被删、一个错误分支被新增，
`go test` 照样全绿，而 README 里的"100%"变成一句没人验证过的口头禅。
真正让承诺存活的东西不是写下来，是**违反时能让构建失败的那道门**。

**本仓落点**（`scripts/check-coverage.sh`，CI `stable` leg 强制）：
- **逐包比对，不看仓库总和**。总覆盖率会被大包掩护小包——`big` 1000/1000
  足以把 `tiny` 0/1 的仓库总覆盖率抬到 99.90%，看起来毫无问题，而
  README 承诺的恰恰是"每个包 100%"。门禁的单位必须与承诺的单位一致。
- **纯接口包按定义跳过**（`pkg/event` 无语句）。把它算成 0% 会让门禁
  永远失败，且没有任何测试能修——一个无法满足的门禁只会被删掉。
- **复用同一份 profile 的两种模式**：`--profile` 只做判定（门禁自己的
  测试用合成 profile 精确构造缺口，成本毫秒级），默认模式先跑测试再判定。
- **误用与失败分开**：退出码 1 = 覆盖率不达标，2 = 命令行用错。CI 里
  一次拼写错误不该被报成"覆盖率下降"。
- **浮点边界留 epsilon**：恰好等于阈值的包必须通过，不能因浮点噪声被判负。
- **`-covermode=atomic`**：非 atomic 模式下带 `-race` 跑出的计数可能偏低，
  凭空造出一个不存在的缺口。
- **门禁自身被测**（`scripts/coverage_gate_test.go`）：合成 profile 覆盖全绿、
  部分缺口、1000 缺 1、全未覆盖、健康总覆盖率掩盖坏包、阈值边界、误用
  退出码、输出顺序确定。**只测"通过"的门禁等于没测**——永远不失败的脚本
  也能打印一张令人安心的表格，所以失败方向断言得和成功方向一样严。
- **`-count=1`：门禁得能证明"这次真跑了"**。`go test` 的结果进缓存，而
  `actions/setup-go` 默认 `cache: true`（keyed on `go.sum`）会恢复 GOCACHE；
  命中时命令报 `(cached)`、**一个测试都不执行**并直接复用上一次的 profile。
  覆盖率门禁于是拿旧证据当新证据，而失败模式是安静的全绿。实测（本机可
  复现）：连跑两次 `go test -coverprofile=c.out .`，第二次输出
  `(cached) coverage: 100.0% of statements`。两种修法：

  | 做法 | 效果 | 取舍 |
  | --- | --- | --- |
  | 测试步加 `-count=1` | 只废结果缓存，保留编译缓存的加速 | 首选：改动面最小，语义精确 |
  | `setup-go` 设 `cache: false` | 连 GOCACHE 都不恢复 | CI 慢十几秒，为一个测试语义问题付全价 |

  顺带一个反直觉点：`go test -bench` **不受**此影响，基准结果不进缓存
  （实测两次都真跑），所以只有 `go test` 与覆盖率步需要 `-count=1`。
  这条也写进了门禁自己的测试：PATH 上放一个记录 argv 的假 `go`，断言
  suite 模式的调用带 `-count=1`——`--profile` 模式根本不调 `go test`，
  只能在调用点断言；撤掉 `-count=1` 该测试立刻变红（已验证）。

**可泛化的一条**：任何"跑一下就算"的门禁（覆盖率、fuzz、lint、门禁自己）
都要先问它在**缓存命中 / 工具缺席 / 参数写错**时会做什么——这三类失败
模式的共同点是安静。安全的失败模式只有一种：红。

**常见追问**：*为什么不直接用 `go test -cover` 的输出？* 那是每包一行
的即时报告，没有"任一包低于阈值就失败"的判定，也就没有门禁；而且
README 的承诺需要的是**可复现的一条命令**。*覆盖率门禁和测试是同义反复吗？*
不是——门禁防止的是**回归**（承诺被无声破坏），测试提供的是**正确性证据**；
门禁自己几乎没有分支，恰恰是最需要外部测试的地方。

---

## 17. 偶现失败（flake）的治理：确定性修复优先，残留风险用重复次数度量

**原理**：时序型 flake 的失败分布与正确性 bug 不同——正确性 bug 一修就好、一坏再现；时序 flake 的签名是**极低概率翻转一次，随后连续多轮全绿**。所以"本地跑过了"对它几乎没有证明力：单次通过的概率 evidence 是 `1-(1-p)^n`，n=1 时对任何小的 p 都接近零信息。治理分两步：

1. **能确定性消灭的，改成确定性构造**。本仓两次真实 flake 都在 reactor 包，修法都不是加重试或加大 sleep：第一次是 AddConn 外层 Done 臂与内层逻辑的竞速，让测试直接驱动内层路径、把"外层 Done 先就绪"从竞速变成构造（commit 182568f）；第二次见下面的实录——它根本不是测试问题，是一条真产品缺陷。判据：**凡是靠放宽断言或延长等待换绿的，都是没修**。
2. **消灭不掉的残留时序面（accept 循环、drain 超时、空闲回收这类与真实调度交互的结构），用 CI 定向重复把 flake 密度变成可见指标**：`go test ./pkg/reactor/ -race -count=5` 每次推送给潜在时序 bug 五次暴露机会，而不是等它在某人的本地运行里翻车。只对时序最重的包做，重复成本才有性价比；只在 stable 工具链腿上做，因为重复度量的是 flake 密度，不是工具链兼容性。

**实录：一次完整的 flake 侦破**（2026-09-26，方法论的第 1 步如何落地）：

- **现场**：全量 `-race` 跑 reactor 失败一次（14.9s，正常 11s），随后 8 轮（单包×5、全量×3）及 20× CPU 压力下 3 轮全绿。第一次的失败详情被 `tail -20` 截掉了——**教训一：追 flake 时必须留全量日志，截断输出等于销毁唯一现场**。修复推送前的最后一次 CI（2 核 runner）也独立命中了它：同一个测试、同样的 "did not exit within 5s"、同样的 5.01s 签名——2 核 runner 的窗口比 16 核本地更宽，这不是本地噪声，是随等待机的真缺陷；而修复提交的 CI（含新 ×5 步骤）全绿，即修复在 2 核环境下的直接验证。
- **激发**：`go test -race -count=3 -cpu=1,2,4` 复现，`TestReactorGracefulShutdownClosesLiveConnections` 报 "connection handlers did not exit within 5s"，耗时恰 5.01s。**`-cpu=1` 是最锋利的激发器**：P=1 时 goroutine 的推进完全依赖调度点，正常负载下微秒级的窗口被拉宽数量级——这是刻意改变系统形态来放大交错，与 CI 里"等密度说话"的重复是互补的两种手段。
- **读数**：三个数字自洽地锁死时序——总耗时 5.01s ≈ 0ms drain + 5000ms AwaitDone，说明 drain 瞬间通过（count==0）；失败实例日志**没有一行 force-closed**，说明强关时 registry 是空的；goroutine dump 里创建 handler 的 Worker.Run 已不在栈上，而 handler 本体停在 `Decode→ReadFull` 的帧头读（IO wait）。拼起来：**连接在 newCh 里排队时对 drain 和 CloseAll 均不可见，worker 之后才取出注册——一条"清扫过后的漏网连接"，handler 停在无人会关的 Read 上**。中间还有一次 select 双臂就绪掷币（conn 与 ctx.Done 同时就绪），但根治它靠的是消灭窗口本身。
- **修复**：注册闸门（`registry.go`）——`Add` 与 `CloseAll` 同锁串行化并返回 bool，`CloseAll` 落 `closed` 闸门；worker 收到拒绝就自关连接且不启动 handler。互斥锁给出 happens-before，时序只剩两种且都安全：**Add 在前 → CloseAll 必关它；CloseAll 在前 → Add 被拒、worker 自关**，没有第三种。回归测试（`TestWorkerRunClosesConnReceivedAfterCloseAll`）把"清扫先于 Run"构造出来，让拒绝分支不依赖调度器；修复后 `-cpu=1 -count=8` 全绿，`SOAK=1` 长稳（60s、11176 条连接 churn、`-race`）goroutine 回到基线、零泄漏报告。
- **教训二：goroutine dump 是案发现场本身**。错误信息只说"有 handler 没退出"，dump 直接指出它停在哪个 Read、由哪个已退出的 goroutine 创建——没有它，三种假设（close 无效？注册缺失？双臂掷币？）无从裁决。

**本仓落点**：CI 的 `Flake tracking (reactor x5)` 步骤（`.github/workflows/ci.yml`，stable 腿）。它不替代 `-race -count=1` 的全量正确性门——重复步骤的产出不是"更多正确性证据"，是**flake 密度的时间序列**：如果某次提交让 p 从 10⁻⁴ 涨到 10⁻³，count=1 的全量门大概率无感，而每周几十次推送 × 5 次重复会让它在统计上现形。

**首批密度数据与健康度结论**（步骤落地当日，2 次 push × 5 次 = 10 遍套件，全在曾命中缺陷的 2 核 runner）：10/10 零失败，×5 聚合耗时 50.9s / 50.0s（±2%，单遍 ~10s 与 Test 步骤一致）——**基线健康，修复无时序副作用**。诚实地说统计力还弱：n=10 全零时 p 的 95% 置信上界只有"三日法则"的 3/n = 30%，这个数字没资格宣布"根除了"，它的正确用法是**随时间序列收紧**——数据攒到 n=100 全零，上界才降到 3%；任何一次失败则立即把 p 的量级钉到 1/n 附近。密度跟踪的价值从来不是单点结论，是趋势。

**常见追问**：*为什么不全仓库 -count=5？* 成本线性翻五倍，而 flake 集中在时序最重的少数包——定向重复是性价比曲线上的正确点。*select 双臂就绪为什么不能靠优先级修？* 语言规范规定多臂就绪时**均匀随机**选择，没有优先级可用；所以"发送/接收后重查状态"（本仓 AddConn 重查 ctx、本节注册闸门）才是正解——把一次性检查变成收发后的二次确认，掷币结果就不再重要。*为什么不用 `-fault`/压力注入？* Go 测试原生没有 fault 注入；人为加压能放大调度交错，但改变的是系统形态而非证明密度，适合一次性排查（本节实录的 `-cpu=1` 正是此用法），不适合做成常驻门。
