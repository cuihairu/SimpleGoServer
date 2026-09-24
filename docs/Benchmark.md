# 性能基准

要求 5 的加分项落实：可复现的基准、明确的环境说明、多次采样、从数据读出
扩展性结论。本文记录基线数据；换环境后数据会不同，方法与结论形态可复用。

## 环境

| 项 | 值 |
| --- | --- |
| CPU | Intel Core i9-10880H（8C16T，笔记本，测试期间有后台负载） |
| 网络 | 本机回环 `127.0.0.1`（无真实网络 RTT/带宽因素） |
| Go | go1.26.5 |
| 采样 | `-benchtime 500ms -count 3`，取中位数；绝对值供量级参考，波动见原始输出 |

## 复现

```bash
# 帧编解码（纯 CPU，无网络）
go test ./pkg/proto -run '^$' -bench . -benchmem

# 真实 TCP 端到端：吞吐 / 固定并发扩展 / 连接建立拆除
go test ./pkg/reactor -run '^$' -bench . -benchmem -count 3
```

## 数据

### 帧编解码（`pkg/proto`）

| 基准 | 单次耗时 | 内存 | 分配次数 |
| --- | --- | --- | --- |
| 编码小帧（echo JSON，~64B） | ~240 ns | 64 B | 1 |
| 编解码一个来回 | ~880 ns | 128 B | 4 |
| 编码 64KiB 大帧 | ~85 µs | 72 KiB | 1 |
| JSON envelope 编组+解组 | ~13.7 µs | 793 B | 19 |

编解码本身是廉价的：小帧编码一次分配（帧缓冲），大帧 64KiB 也只有一次
分配（精确按 Length 分配）。JSON envelope 的 19 次分配全部来自
`encoding/json` 反射编组——这是 JSON 作为载荷格式的固定成本，也是
「优缺点」一节说"接口已隔离、可替换 Protobuf"的量化依据。

### 端到端（`pkg/reactor`，真实 TCP + 完整协议栈）

| 基准 | 单请求延迟 | 折算吞吐 |
| --- | --- | --- |
| EchoThroughput（16 并发客户端，RunParallel） | 69~152 µs | ~10 万 req/s 量级 |
| EchoClients clients=1 | ~120 µs | ~8 千 req/s（单连接串行 RTT） |
| EchoClients clients=8 | ~100 µs | ~8 万 req/s |
| EchoClients clients=64 | ~160 µs | ~40 万 req/s |
| ConnChurn（建连+1 请求+断开/次） | ~580 µs | ~1700 连接/秒 |

### 流式大消息（`pkg/reactor`，StreamedCall）

超过分片阈值（`MaxFrameSize - 64KiB`）的载荷在请求与响应两个方向都拆成
`FlagMore` 分片帧，1KiB 档是不分片的基线：

| 基准 | 载荷 | 分片数（单向） |
| --- | --- | --- |
| StreamedCall size=1KiB | 1 KiB | 1（基线） |
| StreamedCall size=1MiB | 1 MiB | 2 |
| StreamedCall size=4MiB | 4 MiB | 5 |

复现：

```bash
go test ./pkg/reactor -run '^$' -bench StreamedCall -benchmem
```

采集注意事项：这组数据对**机器负载极其敏感**。首采期间系统 load 超过
100（16 逻辑核），绝对值比低负载时失真约一个数量级（1KiB 基线本应与
EchoClients clients=1 的 ~120µs 同量级，实测却到了毫秒级），因此不在此
记录绝对数字——请以干净环境复现为准。基准的价值在于结构与回归对比：
分片数只随载荷对数增长、每次往返的分配数不随载荷大小暴涨（聚合缓冲
一次分配为主），若复现时分配数随分片数线性爆炸，即说明聚合路径出现
了多余的中间拷贝。

## 解读

1. **吞吐随客户端数近线性扩展**：1→8 客户端吞吐 ×10，8→64 再 ×5——
   单连接是串行往返（一次一个在途请求），吞吐上限由 RTT 决定；多连接
   分散到多个 worker goroutine 后并行度接近 CPU 核数。这验证了
   "每连接一个 goroutine + 多 worker"模型的多核利用是真实的。
2. **64 客户端时单请求延迟上升（100→160µs）**：并发争抢调度与锁的
   代价，属正常形态——延迟换吞吐，总吞吐仍在大涨。
3. **Churn 是昂贵操作（~0.6ms/连接周期，102 次分配）**：建连需要
   accept + goroutine 创建 + pipeline 组装（2 个 handler 对象）+
   CLOSE 握手 + 注册表增删。结论与业界一致：短连接业务的成本大头在
   连接管理，客户端应复用连接（本协议的单连接多路复用即为此设计）。
4. **已知的优化方向**（对应 Analysis.md 优缺点）：每请求 52 次分配
   中约 19 次来自 JSON 反射；帧解码每帧分配 Frame+payload，可用
   `sync.Pool` 复用。基准已可复现，优化前后可用同一命令对比。

## 局限

- 回环网络无真实 RTT，跨机延迟将主导单请求数字；
- echo 业务近乎零计算，真实业务吞吐上限更低；
- 未包含长稳测试与内存泄漏曲线（可用 `load_test.go` 的压测用例拉长
  时间并配合 `runtime.ReadMemStats` 观察）。
