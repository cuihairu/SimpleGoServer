# 示例：可运行的完整服务端与客户端

这一对示例演示如何用本框架搭建一个真实可跑的 TCP 服务，覆盖三个核心场景：

| 场景 | 位置 | 说明 |
| --- | --- | --- |
| 并发连接 | `concurrent-client/main.go` | 多个客户端同时各建一条连接，请求互不阻塞 |
| 自定义协议收发 | 两侧 | 10 字节帧头 + JSON 载荷的请求/响应与发布/订阅 |
| 心跳与死链检测 | 两侧 | 服务端 `-idle` 回收静默连接，订阅者靠 KeepAlive 定时 PING 保活 |
| 优雅关闭 | `echo-server/main.go` | SIGINT 后停止 accept → 等存量连接排空 → 强制关闭兜底 |

## 运行

终端 1 —— 启动服务端（监听 `127.0.0.1:8080`）：

```bash
go run ./examples/echo-server
```

终端 2 —— 启动 8 个并发客户端，每个发 20 个请求，并监听 3 秒服务端推送：

```bash
go run ./examples/concurrent-client
```

预期输出（客户端）：

```text
client 0: subscribed to "ticks"
push [ticks]: {"now":"2026-09-23T19:01:39+08:00","note":"server push"}
done: 160/160 requests ok, avg latency 987µs
```

服务端收到 `ctrl-c` 后分阶段关闭并退出：

```text
shutting down…
[Info]… server is shutting down
bye
```

两个程序都支持 flag 调整（`-h` 查看全部）：

```bash
go run ./examples/echo-server -addr 127.0.0.1:9000 -broadcast 5s -idle 30s
go run ./examples/concurrent-client -addr 127.0.0.1:9000 -clients 32 -requests 100
```

### 心跳方案：服务端回收 + 客户端保活

服务端默认 `-idle 60s`：一条连接静默超过 60 秒就被强制关闭（拔网线式的
半开连接由此得到清理）。但纯接收方——比如只订阅 `ticks` 不发请求的
客户端——会被误伤，所以订阅者用 `client.KeepAlive` 每 15 秒发一次 PING：
任何收到的帧都会重置服务端的空闲计时，连接因此一直存活；反过来，若
PING 连续超时，客户端判定链路死亡并主动关闭。可以动手感受一下：

```bash
go run ./examples/echo-server -idle 5s
# 终端 2：2 秒一跳的保活让 8 秒的订阅全程存活
go run ./cmd/cli -subscribe ticks -watch 8s -keepalive 2s
# 对照：关掉保活，订阅 5 秒后被服务端回收（打印 connection closed）
go run ./cmd/cli -subscribe ticks -watch 8s -keepalive 0
```

注意保活间隔必须明显小于服务端 IdleTimeout，否则下一次 PING 还没发出
连接就先被回收了。

服务端滚动重启时，靠保活是撑不过去的——连接终究会断。此时用
`cmd/cli` 的 `-resilient`：断线后自动指数退避重连，握手携带会话 token
恢复订阅（服务端重启 token 失效时自动重发订阅），订阅体验无间断：

```bash
go run ./cmd/cli -resilient -subscribe ticks
# 另一个终端重启 echo-server，push 会自动恢复，无需重新运行 cli
```

## 代码导读

- **业务逻辑只有一个函数**：`echo-server` 里的 `handleDemo(action, data) (any, error)`。
  返回值自动成为响应载荷，返回 error 自动成为结构化错误响应，连接不断开。
- **管线组装**：`pipelineInitializer` 对每条连接执行一次，先加
  `proto.NewFrameCodec()`（字节 ↔ 帧），再加共享的 `proto.NewProtocolHandler`
  （帧 ↔ 业务语义）。
- **发布/订阅**：客户端 `Subscribe("ticks")` 后，服务端 `protocol.Publish`
  会把帧推到该 topic 的所有订阅连接；写不动的慢订阅者会被自动剔除。
- **客户端单连接多路复用**：`client.Call` 用流 ID 关联请求与响应，一条连接
  上可以并发挂任意多个在途请求；`CloseGracefully` 发 CLOSE 帧等确认，
  而不是静默断开。
- **心跳是两端配合的**：`echo-server` 设 `ServerOptions.IdleTimeout` 清理
  僵尸连接；`concurrent-client` 的订阅者用 `client.KeepAlive` 定时 PING，
  既保住自己不被回收，又在连续超时时主动发现死链并唤醒 `Done()` 等待者。

想写自己的服务，从模仿 `handleDemo` 与那段 `pipelineInitializer` 开始即可；
框架层（reactor / handler pipeline / balancer）不需要任何改动。
