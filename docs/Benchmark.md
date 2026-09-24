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

2026-09-24 补采（load 20~30，8C16T，`-benchtime 500ms -count 3` 取中位）：

| 基准 | 单请求延迟 | 折算吞吐 | 内存/次 | 分配/次 |
| --- | --- | --- | --- | --- |
| size=1KiB | ~258 µs | — | 13.7 KB | 55 |
| size=1MiB | ~109 ms | ~9.6 MB/s | 19.3 MB | 98 |
| size=4MiB | ~411 ms | ~10 MB/s | 81 MB | 138 |

仍然不视作干净环境：1KiB 基线 ~258µs 已进入与 EchoClients clients=1
（~120µs）同量级的合理区间（首采时是毫秒级失真），可以确认基线归位；
1MiB/4MiB 档的绝对值仍偏高且三档吞吐都钉在 ~10MB/s 的恒定形态，与
分片数无明显相关——既可能是残余后台负载（本次 load 仍 20~30，远非
空载）的调度噪声，也不能排除流式路径存在逐分片的固定等待，留待真正
空载的机器复采后再下结论。两次采集（taskset 2 核与全机 16 线程）数字
几乎相同，至少排除了"调度并发度不足"这一解释。

结构结论稳固且两次采集一致：分配次数随载荷对数增长（55→98→138），
不随分片数线性爆炸——聚合缓冲一次分配为主的实现没有被大载荷打穿；
若未来复现时分配数随分片数线性上涨，即说明聚合路径出现了多余的中间
拷贝。

#### 附：同日分层定位记录（供空载复采时对照）

补采后追做了一轮二分定位（全部在 load 12~30 下，结论以事实链记录，
定性留给空载复采）：

1. **回环本底**：裸 TCP 单块写 1MiB 连续 20 次，实测 ~1750 MB/s——
   机器虽非空载，大块 socket I/O 本身不慢，175 倍差距说明全栈慢不是
   「机器慢」三个字能盖住的。
2. **proto 层单独测**（裸 TCP echo goroutine + `proto.Dial` 客户端，
   绕开 reactor）：1MiB ≈ 43ms、4MiB ≈ 155ms，两档折算吞吐都钉在
   ~50 MB/s——瓶颈主要在 proto 层，reactor 在其上再放大 ~2.6 倍。
3. **逐事件时间线**（server 端打点）：server 读聚合 <0.5ms、回写
   960KiB 片 0.2~0.5ms 全部飞快；整个延迟集中在**客户端 conn 的写
   （~25ms）与读（~24ms）**两段。
4. `GODEBUG=gctrace=1`：GC CPU 占比 ~1%，单次 STW <0.1ms——排除。
5. 客户端 conn 与裸 conn 无配置差异（`net.Dial` 直出、Go 默认
   TCP_NODELAY），写路径 `writeAll` 就是裸 `conn.Write`——「客户端
   侧独慢」的机制未知。当前首推假设：本机常驻高负载放大了 TCP 接收
   窗口 refill 的调度间隙（960KiB 分片 ≈ 数次窗口往返 × 毫秒级调度
   延迟），但这**必须**在空载机器上用同一实验复测才能定论——若空载
   后客户端段延迟消失，则纯属环境；若仍在，按第 3 条的打点方法继续
   收窄。

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
