# 自定义协议（实现说明）

本文档描述 `pkg/proto` 实现的应用层协议：帧格式、通信模式与错误处理约定，
是客户端与服务端共同遵守的线上一致性契约。

## 分层

```text
+----------------------------------------------------------+
| 业务逻辑        handleRequest(action, data) (any, error)  |
+----------------------------------------------------------+
| 语义层          ProtocolHandler：请求/响应、订阅表、心跳、  |
|                 优雅关闭（帧类型分发）                      |
+----------------------------------------------------------+
| 载荷层          JSONMessage envelope：{"action","data"}    |
+----------------------------------------------------------+
| 帧层            FrameCodec：10 字节帧头 + 定长分帧          |
+----------------------------------------------------------+
| 传输层          TCP 字节流（连接生命周期由 reactor 管理）    |
+----------------------------------------------------------+
```

帧层解决"字节流里哪一段是一条完整消息"（粘包/半包），载荷层解决"消息内容
怎么编码"，语义层解决"消息用来做什么"。业务代码只接触最上层。

## 帧格式

每帧 = 定长帧头 + 变长载荷，整数一律**大端序**：

```text
 0                   1                   2                   3
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  FrameType    |    Flags      |            StreamId           |
|     (1B)      |     (1B)      |             (4B)              |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|             Length             |        Payload ...            |
|               (4B)             |       (Length 字节)           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| 字段 | 大小 | 说明 |
| --- | --- | --- |
| FrameType | 1 B | 帧类型，见下表 |
| Flags | 1 B | 预留（压缩/分片标志位），当前恒为 0 |
| StreamId | 4 B | 流 ID：请求/响应用来配对；发布帧复用为 topic 序号 |
| Length | 4 B | 载荷字节数；解码时超过 `MaxFrameSize`（1 MiB）即协议错误并断连 |
| Payload | Length B | JSONMessage envelope 的 JSON 编码 |

帧类型（`pkg/proto/frame.go`）：

| 类型 | 方向 | 用途 |
| --- | --- | --- |
| `REQUEST` | C→S | 发起请求，期待同 StreamId 的 `RESPONSE` |
| `RESPONSE` | S→C | 请求/订阅/关闭确认的统一应答，携带原 StreamId |
| `SUBSCRIBE` | C→S | 订阅 topic（topic 名在 envelope 的 `action` 字段） |
| `UNSUBSCRIBE` | C→S | 取消订阅 |
| `PUBLISH` | S→C | 服务端向 topic 的订阅者推送 |
| `PING` | 双向 | 心跳探测 |
| `PONG` | 双向 | 心跳应答，携带原 StreamId |
| `CLOSE` | C→S | 优雅关闭通知，服务端确认后双方断开 |

## 载荷格式

所有载荷统一为 JSONMessage envelope，`action` 是动作/主题名，`data` 是
业务数据（任意 JSON 值，出错时固定为 `{"error": "..."}`）：

```json
{"action": "echo", "data": "hello"}
```

统一 envelope 的动机：响应无论成功与否结构一致，客户端可以按同一套代码
解析"成功载荷"与"错误载荷"，不需要靠帧外信息区分。

## 请求/响应模式

一条连接上并发多个在途请求（多路复用），靠 StreamId 配对：

```text
客户端                                      服务端
  | -- REQUEST  streamId=7 {"action":"echo"} --> |
  | -- REQUEST  streamId=8 {"action":"time"} --> |   （无需等待 7）
  | <-- RESPONSE streamId=8 ... ---------------- |
  | <-- RESPONSE streamId=7 ... ---------------- |
```

- 客户端（`pkg/proto/client.go`）：`Call` 在**单个临界区内**原子分配自增
  StreamId 并注册 pending channel，写帧后带超时等待；读循环按 StreamId 把
  `RESPONSE`/`PONG` 送回等待者。超时请求的迟到响应被直接丢弃。
- 服务端（`pkg/proto/protocol.go`）：按帧类型分发，业务函数返回
  `(value, error)`——value 编码成 `RESPONSE`；error 编码成
  `data={"error":"..."}` 的 `RESPONSE`（连接不断开）。

错误响应示例：

```json
{"action": "echo", "data": {"error": "unknown action \"foo\""}}
```

## 发布/订阅模式

```text
C1 -- SUBSCRIBE action="ticks" -------------------> S   订阅
C1 <-- RESPONSE action="subscribed" -------------- S   确认
S  -- PUBLISH action="ticks" data=... ------------> C1  （以及 C2、C3…）
```

- 服务端维护 `map[topic]map[net.Conn]struct{}` 订阅表（读写锁保护）。
- `Publish` 逐个写出，**单订阅者写超时 3 秒**（瞬时 `SetWriteDeadline`，
  写完即清除）：跟不上就剔除该订阅者，慢连接不拖垮发布方——这是最简
  单可行的背压策略。
- 写失败的订阅者同样被剔除，断开连接不会在订阅表里留下死条目。

## 心跳

`PING` 帧立即得到同 StreamId 的 `PONG`；`ProtocolHandler.Stats()` 暴露
收发计数供监控。客户端按需探测用 `client.Ping(timeout)`；需要持续保活时
用 `client.KeepAlive(interval, pingTimeout)`——它按固定间隔自动发 PING，
一旦探测失败（写失败或超时）即判定链路死亡，主动关闭客户端并唤醒所有
`Done()` 等待者，调用方拿到返回的 stop 函数可随时停掉循环。

服务端侧的死连接回收由 reactor 层的空闲超时承担：`ServerOptions.IdleTimeout`
大于 0 时，静默超过该时限的连接被强制关闭（读 deadline 到期），任何收到的
帧——心跳或业务流量——都会重置计时。因此完整的心跳方案是两端配合的：
客户端 `KeepAlive` 既保住自己不被服务端回收，又在 `interval +
pingTimeout` 内发现死链；服务端 `IdleTimeout` 清理不说话的僵尸连接。

## 优雅关闭

连接级关闭是**协议化的三步握手**，而非静默断开：

```text
C -- CLOSE action="bye" ---> S
C <-- RESPONSE ack -------- S
（双方各自关闭连接）
```

客户端 `CloseGracefully` 发出 `CLOSE` 并等确认，任何失败回退为直接
`Close`。服务端整体关闭的分级流程见 `reactor.ShutdownWithTimeout`
（停止 accept → drain → 强制关闭 → 停 worker）。

## 错误处理

| 层 | 错误 | 处理 |
| --- | --- | --- |
| 帧层 | 长度超 `MaxFrameSize`（`ErrFrameTooLarge`）、空帧（`ErrEmptyFrame`） | 断开连接：连"分帧"都不遵守的对端无法安全对话 |
| 帧层 | 对端正常关闭（`io.EOF`） | 静默收尾；帧中间截断（`io.ErrUnexpectedEOF`）按协议错误记录 |
| 语义层 | 未知动作、业务函数返回 error | 编码为错误 `RESPONSE`，**连接保持**——应用层错误不该拆除传输层 |
| 语义层 | 未知帧类型、载荷不是合法 envelope | 断开连接（协议错误） |
| 客户端 | 写失败、连接断开、超时 | `Call` 返回错误；断开时关闭全部 pending channel 唤醒所有等待者 |

原则：**越靠近应用的错误越宽容（响应错误即可），越靠近传输的错误越果断
（直接断连）**——分帧被破坏意味着后续字节流已不可解析，继续维持连接只
会产生更多垃圾。

## 握手与流式传输（未实现，设计预留）

以下能力帧格式已预留空间，但当前实现未包含，属扩展方向：

- **握手/版本协商**：首帧交换协议版本、压缩选项与断线重连 token（README
  加分项）。`Flags` 字段与 `FrameType` 的编号空间可承载扩展类型。
- **流式传输**：同一 StreamId 的多个 `REQUEST` 分片（借助 `Flags` 的
  MORE/END 位）聚合为一条逻辑消息；HTTP/2 的 `END_STREAM` 标志即此做法。
- **断线重连**：服务端在握手响应中下发 token，重连后凭 token 恢复订阅表。

## 参考

- [RFC 9113 — HTTP/2（帧格式：type+flags+stream id+length）](https://www.rfc-editor.org/rfc/rfc9113)
- [RSocket protocol](https://rsocket.io/about/protocol)
- [The WebSocket Protocol (RFC 6455)](https://www.rfc-editor.org/rfc/rfc6455)
