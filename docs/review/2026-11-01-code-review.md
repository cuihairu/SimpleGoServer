# 代码深审报告 — SimpleGoServer

- 日期：2026-11-01
- 范围：全仓（`cmd/` `internal/` `pkg/` `examples/` `http-service/`）
- 方法：① 安全静态扫描 → ② 隐藏 bug 猎捕 → ③ 质量门（lint / 类型检查 / 测试基线）
- 结论：**发现 5 项（P2×1，P3×4）；已修 0 项；剩余 5 项（均记入 todo.md）**。无 P0/P1，安全扫描干净，质量门全绿，无基线回归。

---

## ① 安全静态扫描 — 干净

| 检查面 | 结果 | 证据 |
| --- | --- | --- |
| `unsafe` | 0 | 全仓无 `unsafe.` 引用 |
| shell 注入 | 0 | 无 `os/exec` / `syscall.Exec` / `sh -c` / `bash -c` |
| SQL 拼接 | 0 | 无 `database/sql` / ORM |
| eval / 模板注入 | 0 | 无 `eval` / `text/template` / `html/template` |
| 反序列化 | 仅安全路径 | `json.Unmarshal` 全部用于协议信封与 `json.RawMessage` 原样透传（pkg/proto/json.go:173,189、protocol.go:362、client.go:126,360、examples/concurrent-client/main.go:164）；无 gob / pickle / yaml 任意类型 |
| 路径穿越 | 0 | 无 `filepath` / `os.Open` / `os.ReadFile` / `os.Create` 接收用户输入 |
| 硬编码密钥 | 0 | 无 password / secret / api-key / credential / private-key 字面量 |
| 随机数 | 会话令牌用 `crypto/rand` | `pkg/proto/protocol.go:4,268`（`readRandom = rand.Read`，可注入测试；第 274 行注释「entropy source is broken」）；`math/rand` 仅出现在 balancer（`#nosec G404`，仅分布公平性，无安全角色） |

无密钥、无命令执行、无 SQL、无任意反序列化、无文件路径来自用户输入——**安全面干净**。

---

## ② 隐藏 bug 猎捕 — 5 项（按严重度降序）

### P2（1 项）

#### [P2] `cmd/cli/client.go:57` — onEvent 阻塞发送可能卡死读循环

- **证据**：`onEvent := func(frame *proto.Frame) { eventCh <- frame }`（`eventCh` 缓冲 cap 16，第 56 行）。
- **机理**：`onEvent` 由客户端 readLoop 同步调用（`pkg/proto/client.go` 的非响应帧路径）。订阅主题后若服务器突发推送、且 watch 循环（`cmd/cli/client.go:149-161`）消费慢于推送速率，`eventCh` 写满后 `eventCh <- frame` **阻塞 readLoop**。
- **级联后果**：readLoop 卡住 → ① 无法处理 PONG，`KeepAlive`（client.go:108）的 ping 超时 → 客户端被误判死链强关；② 在途 `Call` 超时；③ TCP 反压 → 服务器 `Publish` 的 3s 写超时（`pkg/proto/protocol.go` `publishWriteTimeout`），可能拖慢服务器广播。
- **为何是 P2 而非 P1**：仅在「突发推送 + 慢消费者」叠加时触发；演示配置（echo-server 每 2s 一 tick）远低于消费速率，不会命中。属健壮性缺口，非正常路径错误。
- **对照**：同仓 `examples/concurrent-client/main.go:118-121` 用了正确写法——`select { case pushes <- frame: default: }`（非阻塞丢帧），注释明言「never let a slow consumer block the read loop」。CLI 未沿用。
- **建议（一行修）**：改为非阻塞发送 `select { case eventCh <- frame: default: }`（丢弃超量事件；watch 为演示用途，丢帧可接受）。

### P3（4 项）

#### [P3] `internal/balancer/weighted_random.go:35` — 全零权重时 `rand.Intn(0)` panic

- **证据**：`r := rand.Intn(w.preSum[size])`；`buildPreSum()`（45-51 行）累加 `Weight()`，若全部注册后端权重为 0，则 `preSum[size]==0`，`rand.Intn(0)` panic（invalid argument to Intn）。
- **触发面**：配置性边界（权重全 0 属误配置）；默认 balancer 是 Adaptive，非默认路径。
- **建议**：`Next` 开头对 `w.preSum[size] <= 0` 返回错误或回退 `backends[0]`。

#### [P3] `internal/executor/async_executor.go:90` — 过期注释「Stop closes both channels」

- **证据**：`worker()` 内注释称「Stop closes both channels; a closed channel makes this receive immediately ready...」。实际 `Stop()` 仅 `cancel()` ctx，从不关闭 `e.tasks` / `e.futureCh`。
- **影响**：注释描述的机制不存在；防御性 nil-guard（`if t == nil` / `if p == nil`）正确无害。纯文档误导，不改变行为。
- **建议**：改为「Stop cancels ctx; workers drain via ctx.Done」，或删除该段。

#### [P3] `cmd/server/main.go:20` — demoProtocol 的 sessionJanitor 永不 Close

- **证据**：`demoProtocol = proto.NewProtocolHandler(...)`；`NewProtocolHandler` 启动 sessionJanitor goroutine（pkg/proto/protocol.go），但 cmd/server 从不调用 `demoProtocol.Close()`。
- **影响**：进程生命周期（进程退出即回收），生产无碍；仅测试中若断言 goleak 会出现滞留 goroutine。属测试卫生而非运行缺陷。
- **建议**：cmd/server 退出路径补 `defer demoProtocol.Close()`。

#### [P3] `examples/echo-server/main.go` — broadcastLoop goroutine 未 join

- **证据**：`go broadcastLoop(protocol, broadcastEvery, stopBroadcast)`；`runServer` 在 `close(stopBroadcast)` 后 `ShutdownGracefully()` 随即返回，不等待 broadcastLoop 退出。
- **影响**：有界（stop 关闭后 select 下一轮即 return），非泄漏；但测试可观测到短暂滞留 goroutine。
- **建议**：用 `sync.WaitGroup` join，或注明「进程生命周期、有意不 join」。

---

## ③ 质量门（lint / 类型检查 / 测试基线）

| 门 | 结果 | 说明 |
| --- | --- | --- |
| gofmt | 干净 | `gofmt -l` 全仓 0 文件 |
| go vet ./... | 干净 | 0 告警 |
| staticcheck | 干净 | 0 告警（全量） |
| go build ./... | 通过 | 无编译错误 |
| go test ./... | 通过（基线） | 16 包全绿；含 `-race` 关键包（http-service、reactor、proto、executor、cmd/cli、examples） |
| 覆盖门禁 | 保持 | http-service 单包 100% 语句覆盖（CI 守卫）；其余包维持既有基线 |

> 基线对比：审计前该仓处于绿基线（task-3 收官后 git 干净）。本轮审计未改动任何产品代码（仅落报告 + todo 记录），故质量门结果与基线一致，**无回归**。

---

## 总结

- **发现 5 项**：P2×1、P3×4（安全 0、bug 5、质量门 0 回归）
- **已修 0 项**：无 P0/P1；按派工单「P2/P3 记 todo」处理，当场不修
- **剩余 5 项**：均记录于 `todo.md`（P2 带一行修复建议；P3 为边界 / 文档 / 测试卫生）
- **本次提交**：见 git log（报告 + todo 记录）
