# 示例：可运行的完整服务端与客户端

这一对示例演示如何用本框架搭建一个真实可跑的 TCP 服务，覆盖三个核心场景：

| 场景 | 位置 | 说明 |
| --- | --- | --- |
| 并发连接 | `concurrent-client/main.go` | 多个客户端同时各建一条连接，请求互不阻塞 |
| 自定义协议收发 | 两侧 | 10 字节帧头 + JSON 载荷的请求/响应与发布/订阅 |
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
go run ./examples/echo-server -addr 127.0.0.1:9000 -broadcast 5s
go run ./examples/concurrent-client -addr 127.0.0.1:9000 -clients 32 -requests 100
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

想写自己的服务，从模仿 `handleDemo` 与那段 `pipelineInitializer` 开始即可；
框架层（reactor / handler pipeline / balancer）不需要任何改动。
