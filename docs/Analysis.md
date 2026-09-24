# 需求分析

## 设计取舍：为什么

本节只回答"为什么"——每个选择对应的备选方案、取舍依据与业界惯例。机制本身的原理见后文「事件驱动」「多核利用」。

### 1. 为什么是事件驱动 + goroutine/channel

**要解决的问题**：大量并发连接下，每个连接都会频繁"等待 I/O"。如果让一个执行单元（线程）在等待期间干等，10 万连接就需要 10 万份等待资源；事件驱动的核心就是让"等待"由操作系统的 I/O 复用机制统一管理，执行单元只在数据就绪时被唤醒。

**备选方案对比**：

| 方案 | 描述 | 取舍 |
| --- | --- | --- |
| thread-per-connection | 每连接一个 OS 线程 | 栈内存与线程切换开销随连接数线性增长，难以扩展到 10 万+ 连接 |
| 手写 epoll 回调（C/C++ Reactor） | 单线程 `epoll_wait` 循环，事件触发回调 | 效率最高，但业务逻辑被回调/状态机切碎，异步传染，维护成本高 |
| 语言运行时 netpoll + goroutine | 运行时用 epoll/IOCP 管 I/O，goroutine 阻塞式写法 | 代码是同步风格，异步由运行时完成；代价是每连接一个 goroutine 的调度与栈开销（Go 中该开销很小，goroutine 初始栈仅约 2KB） |

**为什么选事件驱动**：
- 等待 I/O 的成本从"每连接一个阻塞执行单元"降到"一组 I/O 复用 + 少量事件循环"，连接数不再受线程数限制。
- 事件就绪后才唤醒处理者，空闲连接几乎零 CPU 占用，符合"资源优化"的要求。
- Go 运行时本身已内置事件驱动的 netpoll（见下节），在其之上做应用层事件编排（accept → 分发 → 处理）不必从零手写 epoll，同时保留了显式的事件模型便于控制背压与生命周期。

**为什么用 goroutine + channel 实现并发与非阻塞 I/O**：
- **不通过共享内存通信**：channel 让"新连接分发""请求入队""结果回传"等跨组件交互走消息传递，避免手写锁带来的竞态与死锁风险；有界 channel 天然形成背压（队列满则发送方阻塞，不会无限堆积内存）。
- **非阻塞 I/O 的实现路径**：goroutine 在 `Read/Write` 上"看起来阻塞"，实际由运行时 netpoll 挂起/唤醒——非阻塞来自运行时，而非业务代码里的回调。这既满足题面"非阻塞"，又保持同步可读的代码结构。
- **多核利用**：goroutine 由运行时调度到多个 P/OS 线程上，连接处理天然并行；再配合每 worker 绑定线程（`runtime.LockOSThread`）或按 CPU 数划分 Reactor，可减少跨核迁移、提高缓存命中（详见「多核利用」）。
- **惯例做法**：Go 社区的惯例是"每连接一个 goroutine + channel 做组件解耦"，事件循环细节交给运行时；`gnet`/`netpoll` 等库则提供更激进的"单线程 eventloop + 回调"以换取极致性能。本题答案选择前者为主的混合模型（Reactor 接收、channel 分发、goroutine 处理），在可读性与性能之间取平衡——这也是面试中期望讲清的 trade-off。

### 2. 为什么自定义帧协议用长度前缀

**要解决的问题**：TCP 是字节流，不保留消息边界。一次 `Read` 可能读到半条消息（半包），也可能读到多条消息粘在一起（粘包）。应用层必须自行分帧。

**备选方案对比**：

| 方案 | 描述 | 取舍 |
| --- | --- | --- |
| 特殊分隔符（如 `\n`） | 以保留字符标记消息结束 | 只适合文本；载荷含分隔符需转义；二进制数据不适用；扫描分隔符有额外开销 |
| 固定长度 | 每条消息长度恒定 | 只适合定长报文，无法承载变长 JSON/二进制 |
| 转义/转码分隔 | 对载荷中的分隔符转义（如 escape） | 增加编解码复杂度和体积，仍需逐字节扫描 |
| **长度前缀** | 头部携带 body 长度，按长度读满一帧 | 需约定字节序与头部格式；一次读分配即可定位边界，无扫描、不限内容 |

**为什么选长度前缀**：
- **边界判定是 O(1)**：读到头部中的 Length 字段后直接知道该收多少字节，不必扫描整个载荷寻找分隔符，也不需要转义——对二进制和 JSON 同样适用。
- **一次分配、整帧读取**：可预知帧长，便于精确分配缓冲区、避免反复扩容拷贝，也方便做 writev/批量 flush 优化。
- **可防御性**：长度字段可设上限（如最大帧 1MB），超限即断连，防止恶意/异常对端用超大 Length 或无限流数据打爆内存——这是分隔符方案难以统一做到的。
- **惯例做法**：长度前缀是二进制应用协议的主流选择——HTTP/2 帧头（3 字节长度 + 1 字节类型 + 1 字节 flags + 4 字节流 ID）、gRPC/HTTP2、Redis RESP 的 bulk string（`$<len>\r\n`）、Protobuf 的 `varint` 长度前缀（`delimited`）、Thrift、MQTT 都是"长度（或类型+长度）在前、载荷在后"。分隔符方案（如 HTTP/1.1 头部行、NDJSON）多见于纯文本协议。
- **本项目落点**：`pkg/proto/frame.go` 的 `FrameHeader` 即"类型 + 流 ID + 标志 + 长度"的前缀式帧头，长度字段用于界定 JSON 载荷边界——帧层负责"这是一条完整消息"，载荷层（JSON）负责"消息里是什么"，两层职责分离。

### 3. 分层与惯例小结

| 决策 | 选择 | 惯例参照 |
| --- | --- | --- |
| I/O 模型 | 事件驱动（运行时 netpoll）+ 应用层 Reactor | epoll/IOCP、Netty、gnet |
| 并发原语 | goroutine + channel | Go 惯例 CSP；Netty 的 eventloop 线程组 |
| 消息分帧 | 长度前缀帧头 | HTTP/2、gRPC、RESP bulk string、Protobuf delimited |
| 载荷序列化 | JSON | 题面要求；可替换为 Protobuf/FlatBuffers（接口已隔离） |
| 连接分配 | 可插拔负载均衡器 | Nginx/Envoy 的 balancer 策略族 |

### 4. 为什么不直接用 net/http

**先说结论**：如果是标准的请求-响应服务（REST/gRPC），直接用 `net/http` 是
唯一正确答案，自建是重复造轮子。本题要求"自定义协议 + 事件模型可控"，
`net/http` 的抽象面覆盖不了，才有自建的必要。

**相同点**：`net/http` 服务端与本项目的连接模型同源——`Server.Serve` 循环
`Accept`，每条连接 `go conn.serve`，读写交给运行时 netpoll。"每连接一个
goroutine + 同步写法"正是 Go 的官方惯例，本项目没有偏离它。

**不同点**：

| 维度 | net/http | 本项目 |
| --- | --- | --- |
| 协议语义 | 固定为 HTTP/1.1（及 h2c），Handler 收到的是"完整请求" | 应用层协议可自定义：多路复用、订阅推送、自定义帧类型 |
| 分帧方式 | HTTP/1.1 是文本协议：头部分隔符 `\r\n` + `Content-Length`/chunked | HTTP/2 式二进制长度前缀帧，一帧一个完整消息 |
| 通信模式 | 单向请求-响应（客户端发起） | 请求/响应 + 发布/订阅（服务端可主动推） |
| 连接语义抽象 | `http.Handler` 只认"请求进、响应出"；要在 HTTP 上做推送只能 Hijack/Firehose 式绕过 | 帧 + StreamId 是一等公民，语义层自由定义 |
| 优雅关闭 | `Server.Shutdown(ctx)`：停 accept → 等连接空闲 → 超时强关 | `ShutdownWithTimeout` 同样的分级流程——惯例一致 |

**判断准则**（面试表达时可直接用）：
- 需要中间件生态、TLS、浏览器兼容、标准语义 → `net/http`；
- 长连接、双向推送、单连接多路复用、协议行为需要自己掌控（帧大小上限、
  心跳节奏、订阅背压）→ 自建 Reactor + 自定义协议；
- 两者兼有的服务，常见做法是 HTTP 做管理面/接入面，自定义 TCP 协议做
  数据面（消息推送、IM、游戏网关通常如此分层）。

### 5. 帧头与主流协议的惯例对照

自定义协议不是无中生有，帧头设计逐项对照主流实现：

| 协议 | 帧头结构 | 与本项目的对应 |
| --- | --- | --- |
| 本项目 | `[type:1][flags:1][streamId:4][length:4]`（10B 定长） | — |
| HTTP/2 | `[length:3][type:1][flags:1][R+streamId:4]`（9B 定长） | 字段一一对应，只是长度字段宽度不同 |
| Dubbo | `[magic:2][标志+状态:2][id:8][bodyLen:4]`（16B 定长） | 同为"类型+流ID+长度"三件套 |
| gRPC 消息 | `[compressed-flag:1][length:4]`（5B，跑在 HTTP/2 帧上） | 长度前缀同构 |
| WebSocket | `[FIN/opcode:1][mask+长度:1~8][扩展长度][mask]` | 类型+长度在前；长度用变长编码省字节但解码复杂 |
| Redis RESP | 文本长度前缀 `$<len>\r\n` | 同思路的文本表达，仅适合文本 |

可提炼的惯例共识：
1. **类型、流 ID、长度三字段几乎是所有二进制协议的最小公倍数**——分别
   支撑语义分发、多路复用配对、分帧；
2. 长度字段用**定长整数**（HTTP/2、Dubbo、本项目）换取 O(1) 边界判定，
   或 varint（Protobuf delimited）换取小消息省字节——本项目选前者，
   简单且防御性好（上限检查只需一次比较）；
3. `Flags` 是演进预留位：HTTP/2 靠它表达 `END_STREAM` 等控制语义。
   本项目的 Flags bit 0 已用作 `FlagMore`（流式分片），其余位仍为
   压缩、加密等后续扩展保留。

### 6. 这样设计的优缺点

**优点**：

1. **连接模型简单、代码同步可读**：每连接一个 goroutine，读写就是普通的
   阻塞调用，异常栈、调试、profiling 都是标准 Go 工具链；对比手写 epoll
   回调状态机，逻辑不被异步回调切碎。
2. **故障隔离**：单连接的 handler panic 在自己的 goroutine 内 recover，
   不会拖垮其他连接或整个进程；连接的生命周期（注册/关闭/回收）互不干扰。
3. **借力运行时而非对抗运行时**：netpoll 已解决 I/O 多路复用与就绪通知，
   自建层只做连接编排（accept → 分发 → 处理 → 生命周期），代码量与出错面
   都小得多。
4. **背压是结构性的**：worker 分发走有界 channel，慢订阅者被 Publish 主动
   剔除，单个慢连接不阻塞、不拖垮整体。
5. **扩展点接口化**：负载均衡（`pkg.Balancer`）、业务处理
   （`PipelineInitializer`）、序列化（proto 包独立）都可替换，新增协议
   语义只改 `pkg/proto`，核心 reactor 不动。

**缺点与代价**：

1. **goroutine-per-connection 的规模上限**：每 goroutine 栈约 2KB 起步，
   十万级连接 ≈ 数百 MB 栈内存加调度压力；百万级长连接场景，单线程
   eventloop（gnet、cloudwego/netpoll 的模式）内存与上下文切换更省——
   那是用回调复杂度换来的，属于另一个量级的需求。
2. **跨 goroutine 传递有成本**：连接经 channel 分发存在一次加锁与指针
   传递；对比 eventloop 内零分发直接回调，多一跳开销。本设计的立场是
   这点开销换取编排清晰，值得。
3. **每帧分配**：解码一帧分配一个 Frame 与 payload 切片，无 `sync.Pool`
   缓冲复用；高吞吐下 GC 压力可观，是首批优化点（benchmark 已可复现
   每帧分配数）。
4. **JSON 载荷的体积与解析开销**：相对 protobuf 大 2~10 倍、CPU 解析贵
   一个量级；题目要求 JSON，接口已隔离，生产可平滑替换。
5. **协议成熟度**：版本协商、心跳保活、会话重连、离线补发、流式分片
   均已实现（见 Proto.md）；仍缺 HTTP/2 式流控窗口（credit）与 TLS/
   认证集成——前者对当前的慢订阅者剔除策略是可接受的替代，后者留给
   部署层（或 Flags 预留位）。

**适用边界**：万级以内长连接、需要双向推送与自定义语义的业务（推送网关、
IM、IoT 接入、游戏服务）是本设计的目标区间；标准 REST 服务用 `net/http`，
百万连接极致吞吐用 eventloop 库——三者的边界就是"谁的问题域谁出场"。

---

## 事件驱动

Go 运行时的 netpoll 系统是事件驱动的。它使用底层的 I/O 复用机制（如 epoll、kqueue、IOCP 等）来监听网络 I/O 事件，并在事件发生时通知相应的 goroutine，从而实现高效的 I/O 处理。

查看go 源码 `netpoll`

```bash
src/runtime/netpoll.go
src/runtime/netpoll_epoll.go
src/runtime/netpoll_windows.go
...
```

### 事件驱动的工作原理

#### 1. **注册文件描述符**：
- 当需要对一个 socket 进行读写操作时，Go 运行时会将该 socket 的文件描述符（fd）注册到 `netpoll` 系统中。
- 例如，在 Linux 上，这意味着将 fd 注册到 `epoll` 实例中。

#### 2. **事件循环**：
- `netpoll` 系统在后台运行一个事件循环，使用操作系统提供的 I/O 复用系统调用（如 `epoll_wait`、`kevent` 或 `GetQueuedCompletionStatus`）来等待 I/O 事件的发生。

#### 3. **处理事件**：
- 当有 I/O 事件发生时，`netpoll` 系统会唤醒等待这些事件的 goroutine，并将它们重新调度到可运行状态，以便处理 I/O 操作。

### 具体实现细节

#### 在 Linux 上

在 Linux 上，Go 使用 `epoll` 实现 `netpoll`。以下是简化的流程：

##### 1. **创建 `epoll` 实例**：
- Go 运行时在初始化时创建一个 `epoll` 实例。

##### 2. **注册文件描述符**：
- 当有新的网络连接或需要进行 I/O 操作时，将相关的文件描述符注册到 `epoll` 实例中。

##### 3. **等待事件**：
- `epoll_wait` 被调用来等待 I/O 事件。此调用会阻塞，直到一个或多个文件描述符上的事件发生。

##### 4. **处理事件**：
- 当 `epoll_wait` 返回时，表示有 I/O 事件需要处理。Go 运行时会唤醒相应的 goroutine 来处理这些事件。

#### 在 Windows 上

在 Windows 上，Go 使用 IOCP（I/O Completion Ports）实现 `netpoll`。以下是简化的流程：

##### 1. **创建 IOCP**：
- Go 运行时在初始化时创建一个 IOCP 实例。

##### 2. **关联文件描述符**：
- 当有新的网络连接或需要进行 I/O 操作时，将相关的文件描述符关联到 IOCP 实例中。

##### 3. **等待事件**：
- `GetQueuedCompletionStatus` 被调用来等待 I/O 事件。此调用会阻塞，直到一个或多个文件描述符上的事件发生。

##### 4. **处理事件**：
- 当 `GetQueuedCompletionStatus` 返回时，表示有 I/O 事件需要处理。Go 运行时会唤醒相应的 goroutine 来处理这些事件。

### 示例代码

虽然 `netpoll` 的具体实现细节对开发者是透明的，但可以通过标准库中的 `net` 包来利用这些机制。例如，下面是一个简单的 TCP 服务器，它利用 Go 的事件驱动机制来处理并发连接：

```go
package main

import (
	"fmt"
	"net"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Error reading from connection:", err)
			return
		}
		fmt.Println("Received data:", string(buffer[:n]))
	}
}

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	fmt.Println("Server listening on port 8080")
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go handleConnection(conn) // Non-blocking due to goroutine
	}
}
```

在这个示例中：

- 主 goroutine 调用 `listener.Accept` 接收新的连接，并启动一个新的 goroutine 处理每个连接。
- `handleConnection` 函数在 goroutine 中运行，执行读写操作。
- `netpoll` 系统在后台管理这些 I/O 操作，以确保它们能够高效地运行。

### 总结

Go 运行时的 `netpoll` 系统确实是事件驱动的。它利用操作系统提供的高效 I/O 复用机制，在事件发生时通知相应的 goroutine，从而实现高效的并发 I/O 处理。这种设计使得 Go 能够在处理大量并发网络连接时保持高效和可扩展性。

### 参考

- [cloudwego/netpoll](https://github.com/cloudwego/netpoll)
- [panjf2000/gnet](https://github.com/panjf2000/gnet)


## 多核利用

为了更好地利用多核 CPU，在 Go 中实现主从 Reactor 模式并启用线程绑定，可以使用 `runtime` 包来控制 goroutine 的执行。在这个示例中，我们会：

1. 启用多个从 Reactor，每个从 Reactor 运行在一个独立的 goroutine 中。
2. 每个从 Reactor 都会处理特定的网络连接，并在独立的线程上运行。
3. 使用 `runtime.LockOSThread` 来确保每个从 Reactor 绑定到特定的 OS 线程。

以下是一个示例代码：

```go
package main

import (
    "fmt"
    "net"
    "runtime"
    "sync"
)

func handleConnection(conn net.Conn) {
    defer conn.Close()
    buffer := make([]byte, 1024)
    for {
        n, err := conn.Read(buffer)
        if err != nil {
            fmt.Println("Error reading from connection:", err)
            return
        }
        fmt.Println("Received data:", string(buffer[:n]))
    }
}

func startWorker(id int, connChan <-chan net.Conn, wg *sync.WaitGroup) {
    runtime.LockOSThread() // 锁定 OS 线程
    defer runtime.UnlockOSThread()
    fmt.Printf("Worker %d started\n", id)
    
    for conn := range connChan {
        wg.Add(1)
        go handleConnection(conn)
    }
}

func main() {
    listener, err := net.Listen("tcp", ":8080")
    if err != nil {
        panic(err)
    }
    defer listener.Close()

    connChan := make(chan net.Conn)
    var wg sync.WaitGroup

    numWorkers := runtime.NumCPU() // 使用 CPU 核心数作为从 Reactor 数量
    for i := 0; i < numWorkers; i++ {
        go startWorker(i, connChan, &wg)
    }

    fmt.Println("Server listening on port 8080")
    for {
        conn, err := listener.Accept()
        if err != nil {
            fmt.Println("Error accepting connection:", err)
            continue
        }
        connChan <- conn // 将新连接发送到从 Reactor
    }

    close(connChan)
    wg.Wait()
}
```

### 代码说明

1. **主 Reactor**：
    - 主 goroutine 负责监听新连接，并将新连接发送到 `connChan` 通道。

2. **从 Reactor**：
    - 使用 `runtime.NumCPU()` 获取 CPU 核心数，并启动相应数量的从 Reactor。
    - 每个从 Reactor 运行在一个独立的 goroutine 中，并通过 `runtime.LockOSThread` 锁定 OS 线程，确保每个从 Reactor 绑定到特定的线程上。

3. **连接处理**：
    - 每个从 Reactor 从 `connChan` 通道中接收新连接，并启动一个新的 goroutine 来处理连接。

4. **同步处理**：
    - 使用 `sync.WaitGroup` 确保所有连接处理完毕后再关闭程序。

### 优点
- **高效利用多核 CPU**：每个从 Reactor 绑定到一个独立的线程，能够充分利用多核 CPU 的并行能力。
- **并发处理**：通过 goroutine 并发处理每个连接，提高了处理效率。

### 进一步优化
- **连接池和工作池**：可以进一步优化，通过连接池和工作池来减少 goroutine 的创建开销。
- **负载均衡**：策略已落地为可插拔的 `internal/balancer` 策略族（见下节），不再只是优化方向。

这个示例展示了如何在 Go 中实现主从 Reactor 模式，并通过线程绑定来提高多核 CPU 的利用率。根据具体的需求，可以进一步调整和优化代码。

实现一个具有连接池、工作池和负载均衡的主从 Reactor 模式需要进一步复杂的代码结构。下面是一个较为完整的示例，展示如何在 Go 中实现这些功能。

### 主要组件

1. **主 Reactor**：负责接收新连接并分配给从 Reactor。
2. **从 Reactor**：处理分配到的连接，并将其分发到工作池。
3. **工作池**：处理具体的业务逻辑。
4. **负载均衡**：自适应最少负载策略把新连接分给当前负载最低的 Worker；策略经 `Balancer` 接口可插拔（轮询、加权、IP 哈希等见 `internal/balancer`）。

### 示例代码

```go
package main

import (
	"fmt"
	"net"
	"runtime"
	"sync"
)

type Worker struct {
	id       int
	connChan chan net.Conn
}

func (w *Worker) start(wg *sync.WaitGroup) {
	runtime.LockOSThread() // 锁定 OS 线程
	defer runtime.UnlockOSThread()
	fmt.Printf("Worker %d started\n", w.id)
	for conn := range w.connChan {
		handleConnection(conn)
		wg.Done()
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Error reading from connection:", err)
			return
		}
		fmt.Println("Received data:", string(buffer[:n]))
	}
}

type Reactor struct {
	id       int
	connChan chan net.Conn
	workers  []*Worker
	next     int
	wg       *sync.WaitGroup
}

func (r *Reactor) start() {
	runtime.LockOSThread() // 锁定 OS 线程
	defer runtime.UnlockOSThread()
	fmt.Printf("Reactor %d started\n", r.id)
	for conn := range r.connChan {
		r.dispatch(conn)
	}
}

func (r *Reactor) dispatch(conn net.Conn) {
	worker := r.workers[r.next]
	r.next = (r.next + 1) % len(r.workers)
	r.wg.Add(1)
	worker.connChan <- conn
}

func newWorker(id int) *Worker {
	return &Worker{
		id:       id,
		connChan: make(chan net.Conn),
	}
}

func newReactor(id int, numWorkers int, wg *sync.WaitGroup) *Reactor {
	workers := make([]*Worker, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = newWorker(i)
		go workers[i].start(wg)
	}
	return &Reactor{
		id:       id,
		connChan: make(chan net.Conn),
		workers:  workers,
		next:     0,
		wg:       wg,
	}
}

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	var wg sync.WaitGroup
	numReactors := runtime.NumCPU()       // 使用 CPU 核心数作为从 Reactor 数量
	numWorkersPerReactor := 2             // 每个从 Reactor 下的 Worker 数量

	reactors := make([]*Reactor, numReactors)
	for i := 0; i < numReactors; i++ {
		reactors[i] = newReactor(i, numWorkersPerReactor, &wg)
		go reactors[i].start()
	}

	fmt.Println("Server listening on port 8080")
	nextReactor := 0
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		reactor := reactors[nextReactor]
		nextReactor = (nextReactor + 1) % numReactors
		reactor.connChan <- conn
	}

	closeAll(reactors)
	wg.Wait()
}

func closeAll(reactors []*Reactor) {
	for _, reactor := range reactors {
		close(reactor.connChan)
		for _, worker := range reactor.workers {
			close(worker.connChan)
		}
	}
}
```

### 代码解释

1. **Worker 结构**：
    - 每个 Worker 有一个唯一的 ID 和一个 `connChan` 通道，用于接收连接。
    - `start` 方法锁定 OS 线程，并在 `connChan` 上循环接收连接进行处理。

2. **Reactor 结构**：
    - 每个 Reactor 有一个唯一的 ID，一个 `connChan` 通道用于接收来自主 Reactor 的连接，一个 Worker 列表和一个调度器（`next`）。
    - `start` 方法锁定 OS 线程，并在 `connChan` 上循环接收连接，并将它们分发给下一个 Worker。
    - `dispatch` 方法实现负载均衡，将连接分配给负载均衡器选中的 Worker（实际实现为自适应最少负载）。

3. **主程序 (`main`)**：
    - 创建一个 TCP 监听器并接受连接。
    - 根据 CPU 核心数创建相应数量的 Reactor，每个 Reactor 启动多个 Worker。
    - 使用负载均衡方法将连接分发给 Reactor。
    - 使用 `sync.WaitGroup` 确保所有连接处理完毕后再关闭程序。

### 优点
- **高效利用多核 CPU**：每个 Reactor 和 Worker 绑定到独立的线程上，能够充分利用多核 CPU 的并行能力。
- **并发处理**：通过 goroutine 并发处理每个连接，提高了处理效率。
- **负载均衡**：通过负载均衡算法（默认自适应最少负载）将连接分配给不同的 Reactor 和 Worker。

### 总结
这个示例展示了如何在 Go 中实现一个具有连接池、工作池和负载均衡的主从 Reactor 模式。根据具体的需求，可以进一步调整和优化代码，以提高性能和可维护性。

---

## 负载均衡策略（internal/balancer）

题面第 8 条的落地：策略族全部实现为 `pkg.Balancer[T]` 泛型接口的插件，
按后端能力分层约束——`Backend`（最小接口）、`WeightBackend`（+ 权重）、
`CountBackend`（+ 活跃连接数）、`LoadAware`（+ 实时负载值）。新增策略
不改核心分发代码。

| 策略 | 依据 | 适用场景 | 取舍 |
| --- | --- | --- | --- |
| Round Robin | 无（顺序） | 后端同质、请求均匀 | 最平滑，但对慢后端无感知 |
| Random | 随机数 | 同上，实现最简 | 大数定律下趋近均匀，短窗口可能偏 |
| Weighted Round Robin | 静态权重 | 后端性能不齐且已知 | 平滑加权递进，避免权重大的连续命中；权重靠人工调 |
| Weighted Random | 静态权重 | 同上，接受统计意义上服从权重分布 | 无状态、实现简单，但不保证窗口内均匀 |
| Least Connections | 活跃连接数 | 长连接、请求时长不均 | 需要后端回报计数；短连接高频下计数开销显著 |
| IP Hashing | FNV 哈希 + 一致性哈希环（每后端 ≥3 虚拟节点） | 需要会话保持（同一来源总是同一后端） | 后端增减只影响相邻区段，重分布代价小；单后端热点无解 |
| **Adaptive** | `LoadAware.Load()` 实时负载，线性扫描取最小 + 随机破平 | 本文主推：负载自校正 | 无权重可调、后端掉队自动少收连接直至追平；负载指标本身要准（连接数、在途请求、EWMA 延迟皆可） |

**"自适应"的依据**：AdaptiveBalancer 不看静态配置，只比较后端自己维护
的活负载值——静态策略（轮询/权重）在请求时长不均或后端抖动时会持续把
新连接压向已过载的实例，自适应策略对此自校正：掉队的后端负载升高，
选择逻辑自动绕开它，不需要任何人工干预。平局随机化保证同等空闲的
后端分摊流量，而不是钉死在第一个上。线性扫描（O(n)）在小而基本静态
的后端集合（worker 数量级）上比维护堆更便宜。

**平滑性与会话保持的分工**：要"分布均匀的确定性"用 Weighted Round
Robin（递进扫描，权重 3:1 不会出现 3 连 1 间隔的锯齿）；要"同一会话
落同一后端"用 IP Hashing（一致性哈希环，虚拟节点抹平数据倾斜）。
两者互斥——会话保持要求路由由客户端属性决定，平滑性要求路由与
客户端无关。
