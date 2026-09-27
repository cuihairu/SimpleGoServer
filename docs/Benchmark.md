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

2026-09-25 空载终采（load <8，`-benchtime 500ms -count 3` 取中位）：

| 基准 | 单请求延迟 | 折算吞吐 | 内存/次 | 分配/次 |
| --- | --- | --- | --- | --- |
| size=1KiB | ~156 µs | — | 13.6 KB | 56 |
| size=1MiB | ~52.0 ms | ~20.1 MB/s | 19.1 MB | 92 |
| size=4MiB | ~204 ms | ~20.5 MB/s | 79 MB | 133 |

前两轮悬而未决的问题在空载下全部落定：

- **~10 MB/s 恒定形态确认为负载噪声**：空载吞吐翻倍至 ~20 MB/s，
  且两档高度一致（19.9~21.1 MB/s，波动 <6%）——不是调度并发不足，
  也不是逐分片固定等待，就是此前机器负载支配了绝对值。
- **~20 MB/s 恒定形态本身是 proto 层的固有属性**：同窗口分层探针
  （`proto.Dial` 直连裸协议 echo server，绕开 reactor）给出 1MiB
  52.0 ms、4MiB 198 ms——与全栈几乎重合。历史负载下「reactor 再放大
  ~2.6 倍」（proto 43 ms vs 全栈 109 ms）在空载下消失（放大 ≈ 1.0），
  属调度噪声而非真实成本。C' 残差观察点（~10 ms 级服务端读成本）
  在空载下并入本结论，不再单列。
- **结构结论第三次复核一致**：分配次数 56→92→133 仍随载荷对数增长，
  与分片数无关。
- 同窗口其余基准同步归位：EchoClients clients=1/8/64 → ~99/21/11 µs
  （负载下 120/100/160 µs，64 并发反超是并发掩盖延迟的正常形态）、
  ConnChurn ~305 µs；帧编解码纯 CPU 基准快 3.4~4.5 倍——印证上文
  各数据表全部带负载失真，空载值以本节为准。
- 残余的 ~20 MB/s 流速上限（对比裸 socket 单块写 ~1750 MB/s，~90 倍
  差距）集中在 proto 层读路径：ReadFull 分片循环与 JSON envelope
  编解码是后续优化真正值得瞄准的位置。

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
5. **根因已找到并修复（2b1ff8f）**：`EncodeJSON` 把大载荷编码了两次
   ——先 `json.Marshal(data)`，再经 `JSONMessage` 结构体里
   `json.RawMessage` 的 append 式 MarshalJSON 逐字节重拷贝一次；
   1MiB 载荷单次 EncodeJSON 实测 21.4ms（纯 json.Marshal 同载荷仅
   4.8ms），Call 往返两端各编码一次。同进程 A/B/C 对照把 42ms 的
   Call 拆解为：裸 socket 分片写 464µs、手拼协议帧打同一 echo server
   6.7ms、proto.Client 42.4ms——差额与 EncodeJSON 的开销吻合。
   修复为手工拼装 envelope（RawMessage 透传、字节级等价有测试钉住），
   EncodeJSON 降到 8.6ms（残余为 string 必经的首次 marshal）。
6. **终审（修复后同负载带复采，load 19~29）**：
   - StreamedCall 全档位：1KiB 258→186µs（-28%）、1MiB 109→73ms
     （9.6→14.5MB/s，-33%）、4MiB 411→260ms（10→16.2MB/s，-37%），
     分配数 98→93（1KiB 档 55→56，新增的 action 转义小分配）。
   - EncodeJSON 交替对照（worktree 切换修复前后、同机同时刻）：
     修复前中位 ~26.5ms（17~60ms 大幅波动）、修复后 ~10.5ms
     （4~14ms）；以同轮 json.Marshal(string) 为基线归一，开销从
     ~6.6 倍降到 ~2.1 倍——即「单次 marshal + 手拼」的理论形态。
   - 单轮 C/C' 复测（各只采 1~3 轮）噪声高达 2 倍，不足为凭；
     C' 残差 ~6~13ms 提示服务端 ReadFull 读模式在负载下另有
     ~10ms 级独立成本，留作后续优化观察点，不构成回归。
   - 结论：双重编码修复在单元级与端到端（同带中位）两个层面
     确证有效；剩余吞吐仍受机器负载支配，绝对值以空载环境为准。
   - （2026-09-25 空载终采已完成，见上文「流式大消息」一节：
     ~20 MB/s 为 proto 层固有形态，reactor 无真实放大，C' 残差
     观察点并入该结论。）

## 帧对象池化 A/B（2026-09-27）

对应 DESIGN §1.3 与决策 #37~#46：把 `Frame` 与 payload 缓冲按尺寸分级接入
`sync.Pool`，释放点写死在 dispatch 返回后 / 分片 append 之后 / 编码缓冲写完
之后。**采样窗口内本机不是空载**（14 核全部 95~100% 占用，负载来自与本仓
无关的 rustc / cargo test / 另一条会话的基准，见下文「噪声底」），所以这一节
按本文档一贯口径办：**分配数与 B/op 是结论，ns/op 只作方向性参考**。

**采样协议**（两轮，互为交叉验证）：

- **A 轮（结构指标）**：两侧交替、迭代数钉死（`-benchtime 2000x` / 大载荷
  `30x`，`-count 3`）。迭代数固定意味着 setup 成本被摊薄，allocs/op 与
  B/op 在任何负载下都稳定可比。
- **B 轮（延迟参考）**：6 轮交替，**奇数轮 After 先跑、偶数轮 Before 先跑**，
  抵消"谁先跑谁占便宜"的系统性偏差；每侧 12 个样本取 **min**（最少被抢占的
  一次）。命令：
  `go test ./pkg/proto -run '^$' -bench . -benchmem -count 2 -benchtime 300ms`
  与 `go test ./pkg/reactor -run '^$' -bench . -benchmem -count 2 -benchtime 500ms`。

Before 侧取 HEAD `e12fb9e` 的独立 worktree，与 After 侧同一台机器、同一段
时间窗内交替执行。

### 噪声底：用"两侧代码完全相同"的基准量出这把尺子

`EncodeSmallFrame`、`EncodeLargeFrame`、`JSONEnvelopeString1MiB` 三条基准走
的代码在池化前后**逐字节相同**（公开 `Encode` 未改），它们的差值就是纯粹的
测量噪声：

| 基准（两侧同码） | Before min | After min | 差值 |
| --- | --- | --- | --- |
| BenchmarkEncodeSmallFrame | 54.5 ns | 51.2 ns | -6.1% |
| BenchmarkEncodeLargeFrame | 22.3 µs | 24.3 µs | +9.1% |
| BenchmarkJSONEnvelopeString1MiB | 4.19 ms | 3.58 ms | -14.5% |

即**本窗口的噪声底约 ±10~15%**。下文任何落在这个带子里的差值都不作结论。

### 单元级（`pkg/proto`）

| 基准 | 池化前 | 池化后 | ns（min，B 轮） |
| --- | --- | --- | --- |
| EncodeSmallFrame（公开 `Encode`） | 1 alloc / 64 B | 1 alloc / 64 B | 54.5 → 51.2 ns（同码对照） |
| **EncodeSmallFramePooled** | — | **0 alloc / 0 B** | — → 49.9 ns |
| EncodeDecodeRoundTrip（公开 `Decode`） | 4 alloc / 128 B | **2 alloc / 112 B** | 202.5 → **124.6 ns（-38%）** |
| **PooledDecodeRoundTrip** | — | **0 alloc / 0 B** | — → 117.7 ns |
| EncodeLargeFrame | 1 alloc / 73728 B | 1 alloc / 73728 B | 22.3 → 24.3 µs（同码对照） |
| JSONEnvelope | 15 alloc / 401 B | 15 alloc / **417 B** | 4.27 → 4.15 µs |
| JSONEnvelopeString1MiB | 6~7 alloc / 1.00 MB | 6~7 alloc / 1.00 MB | 4.19 → 3.58 ms（同码对照） |

三条结论：

1. **公开解码路径 -38%，超出噪声底一个量级，机制清楚**：帧头改为解析进帧
   自己的内联 `scratch [HeaderSize]byte`（`parseHeaderInto`），省掉旧路径的
   `make([]byte, HeaderSize)` 与中间 `*FrameHeader` 两次分配——4 → 2 次
   分配、128 → 112 B，对应 202.5 → 124.6 ns（两次独立采样分别给出 -24.9%
   与 -38.5%，方向一致）。
2. **池化路径稳态 0 分配**，但**延迟与公开路径基本持平**（编码 49.9 vs
   51.2 ns，解码 117.7 vs 124.6 ns，都在噪声底内）。这是 `sync.Pool` 的
   本质：Get/Put 的固定开销大致抵掉一次 malloc，**换来的不是延迟而是分配
   数与 GC 压力**（NOTES §8 的"小对象不要池化"经验在 64 B 档得到印证：
   收益为零也不为负，1 MiB 档才见真金白银）。
3. **JSON envelope 每次多 16 B**，唯一一处真实代价：`Frame` 结构体从 40 B
   涨到 64 B（新增 `payloadCell` 与 `scratch`），size class 从 48 跳到 64。
   字段顺序是为此专门排过的——同样的字段接在 `Payload` 后面实测 72 B，
   会落到 80 类的 size class，白扔 16 B。端到端每次请求解码一帧，这 16 B
   已被 -266 B/op 的降幅完全覆盖。

两条新基准是池化路径的专用形态（`decodeFrame` / `encodeFrameBuffered` +
`release`），公开 API 保持精确分配，理由见 DESIGN §1.3 的所有权表。

### 端到端（`pkg/reactor`，真实 TCP + 完整协议栈）

A 轮（迭代数钉死）的结构指标与 B 轮的延迟参考并列；两轮的分配数完全一致。

| 基准 | 分配/次（前 → 后） | 内存/次（前 → 后） | ns（min，B 轮） |
| --- | --- | --- | --- |
| EchoThroughput | 42 → **32** (-24%) | 1898 → **1632 B** (-14%) | 11.8 → 10.9 µs（噪声内） |
| EchoClients clients=1 | 42 → **32** | 1896 → **1626 B** | 101.4 → 104.1 µs（噪声内） |
| EchoClients clients=8 | 42 → **32** | 1936 → **1634 B** | 16.5 → 18.9 µs（噪声内） |
| EchoClients clients=64 | 42 → **32** | 1907 → **1647 B** | 12.5 → 10.6 µs（噪声内） |
| ConnChurn | 85 → **74** (-13%) | 4456 → **4296 B** | 314 → 323 µs（噪声内） |
| StreamedCall size=1KiB | 43 → **33** (-23%) | 9688 → **6127 B** (-37%) | 142.9 → 122.6 µs（噪声内） |
| StreamedCall size=1MiB | 85 → **77** | 12.00 → **10.57 MB** (-12%) | 15.8 → 13.8 ms（噪声内） |
| StreamedCall size=4MiB | 125 → **100** (-20%) | 58.20 → **53.41 MB** (-8%) | 65.7 → 57.7 ms（噪声内） |

**读法**：

1. **分配数全线下降，无一条上升**——这是本次改造的无回归判据，也是本节
   唯一能承重的结论。以 `EchoThroughput` 的 42 → 32 为例，`-memprofilerate=1`
   的逐点差分给出精确分解（一次请求往返，四条路径各一份）：

   | 路径 | 前 | 后 | 差 |
   | --- | --- | --- | --- |
   | 服务端入站解码 | 4（`Frame` + payload + 帧头 scratch + `*FrameHeader`） | **0** | -4 |
   | 服务端出站编码 | 2（`[][]byte{buf}` 簿记 + 编码缓冲） | **0** | -2 |
   | 客户端出站编码 | 2（同上） | **0** | -2 |
   | 客户端入站解码 | 4 | **2**（`Frame` + payload） | -2 |
   | 合计 | 42 | **32** | **-10** |

   两条附带结论：客户端入站那 -2 **不是池化**（`Response.Data` 别名逃逸，
   按设计不进池），而是帧头改为解析进帧自己的内联 `scratch`，省掉
   `make([]byte, HeaderSize)` 与中间 `*FrameHeader`；出站那 -2 里有一半
   也不是池化，而是 `encodedMessage` 把"一次编码返回两个平行切片"的簿记
   彻底消掉（单缓冲形态零簿记分配）。**即 10 次里有 4 次来自簿记与帧头
   解析的消除，只有 6 次来自池化本身。**
2. **大消息的收益在字节数而非延迟**：1 MiB 档每次少分配 ~1.4 MB、4 MiB 档
   少 ~4.8 MB——分片缓冲在 append 进聚合后立刻回池，下一个请求直接复用，
   省掉的不只是分配，还有大块内存的**首次触碰 page fault 与内核清零**。
   1 KiB 档另有 -3.5 KB/次（两条 1 KiB 载荷帧的 payload 与编码缓冲都命中
   1 KiB 类）。但吞吐数字（1 MiB 15.8 → 13.8 ms）落在噪声底内，所以只能说
   "分配形态变好了"，不能说"跑得更快了"。
3. **ConnChurn 是降幅最小的一条**（-13%）：建连成本的大头是 accept、
   goroutine 创建、pipeline 组装与 CLOSE 握手，池化管不到。
4. **回滚安全性**：客户端入站、Publish 补发缓存仍是精确分配（DESIGN §1.3
   表格），所以即便某个 handler 在池化路径上误留了 payload 别名，受影响的
   也只有该 handler 自己的后续请求——pool_test.go 的别名复用 fuzz 靶
   （`FuzzPooledDecodeReuse`）专门盯这条线。
5. **延迟结论的边界**：本节所有端到端差值都在 ±10~15% 的噪声底内
   （同码对照见上），因此**不能据此宣称吞吐或延迟改善**；能宣称的只有
   "分配数与内存/次下降，且延迟无可测量的回归"。要给出延迟结论，需在空载
   机器上按同一协议复采——这正是本文档 2026-09-25 那轮"空载终采"建立的
   规矩。

## 编解码热点实测优化（2026-09-27）

对象：`pkg/proto/json.go` 的 JSON 信封组装。`EncodeJSON` 早已手拼信封
（大载荷省掉一次全量拷贝），但服务端响应路径（`handleRequestFrame`、
`mustJSON`）仍在走"先 marshal 业务体、再 marshal `JSONMessage` 结构体"
两次编码——第二次纯粹是搬运已经编好的字节，还要付 marshaler 查找、结构体
装箱与一次结果克隆（实测消失的正是后两处，见下文清点）。改动是把手拼信封
收敛成 `envelope.bytes` 一个共享核心，三处调用方都走它；`TestEnvelopeBytesMatchesStruct` 把共享
核心与结构体 marshal 钉死为逐字节相等，所以线格式不变。

**采样协议**：Before 侧取 HEAD `0145254` 的独立 worktree，按本文档 A 轮口径
`BenchmarkReactorEchoThroughput -benchmem -benchtime 2000x` 两侧交替执行，
同机、同一时间窗（窗口内负载约 10/14）。分配数是结论，ns/op 只看有无回归。

| 基准 | Before | After | 结论 |
| --- | --- | --- | --- |
| EchoThroughput allocs/op（3 组交替） | 32, 32, 32 | 31, 31, 31 | **−1/次**，六轮零重叠 |
| EchoThroughput B/op | 1699, 1703, 1708 | 1654, 1660, 1659 | **−46 B/次** |
| EchoThroughput ns/op（每侧取 min） | 8.99 µs | 8.49 µs | −5.5%，落在 ±10~15% 噪声底内，不作宣称 |
| JSONEnvelope（小信封，单元） | 15 allocs / 421 B | 15 allocs / 421 B | 工作量相同的对照：splice 换了归属，分配一分不变 |
| JSONEnvelopeString1MiB | 7 allocs / ~1.00 MB | 7 allocs / ~1.00 MB | 同上；时间两侧相差 ~10%，噪声内 |

**分配点清点**（`GODEBUG=memprofilerate=1` + `-test.memprofile`，两侧同窗口
各跑 2000 次 echo，对象数按 b.N 归一）——少掉的分配有名字：

| 站点 | Before | After |
| --- | --- | --- |
| `handleRequestFrame` 自身（逃逸的 `&JSONMessage` 装箱） | 1.0 | **0** |
| `bytes.Clone`（stdlib marshal 的结果克隆） | 2.0 | **1.0** |
| `EncodeJSON` 自身（请求路径的 splice） | 2.0 | 1.0 |
| `envelope.bytes`（共享 splice） | — | 2.0 |

读法：响应路径原先为"搬运已经编好的字节"付两次分配——结构体装箱一次、
`json.Marshal` 把 encoder 缓冲克隆成结果一次；现在只剩一次精确容量的
`make`，净 **−1**。请求路径没有变快也没有变慢，只是那一次 splice 从
`EncodeJSON` 搬进了共享核心，这正是两条"工作量相同"的单元基准（15→15、
7→7）读平的原因——它们是这一节的尺子：改动没让它们多做工作，读数就没动。
B/op −46 与消失的结构体装箱同量级（`JSONMessage` 是 16+24=40 B 的值，落
48 B 尺寸类）。数字小，但它有分配点上的名字，不是采样抖出来的。延迟侧不作
任何宣称。

**口径修正（初稿读数）**：本节初稿用 `-benchtime 1000x -count 5` 采到
Before 33/33/33/33/32、After 32/31/31/31/31，读作"−2"。改按本文档 A 轮
口径的 2000x 复查后，两侧稳定读作 32/31，且分配点清点确认结构上只少 1 次
——固定迭代数的 benchtime 摊不薄每 worker 的 dial/handshake setup 与
`sync.Pool` 重填，会把 allocs/op 整体抬高（「帧对象池化 A/B」标注过同一类
采样偏差；两侧抬幅为何不同，这里没有实测解释，不作归因）。两种口径下 After
都读 31，而结构上的差由分配点清点定性：**−1**，两个站点各有名字。结论采
2000x 读数，初稿数字据实留档。

**被否掉的优化也记一笔**：`jsonPlainASCII` 曾被改写为 SWAR 逐词扫描
（1 MiB 纯 ASCII 字符串是信封组装里整遍过字节的一趟，byte loop 看似可疑）。
微基准（1 MiB 全 `"a"`，`-benchtime 100x -count 5`，取 min 抗负载毛刺）：
byte loop 2.64 ms，SWAR 3.66 ms——**持平或更慢，已回退**。机制上也说得通：
byte loop 在全 plain 输入下分支完全可预测，约 3 cycles/byte，已接近 memcpy
速度；而 SWAR 每词仍要付一次 8 字节拷贝（`[]byte(s[i:i+8])` 虽被编译器优化
到零分配，拷贝本身还在）加十余条位运算，省不下东西。结论：这处扫描保持
byte loop，`json.go` 注释里留了这一条，免得后人重踩。

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
   —— 第二项已于 2026-09-27 落地，实测见上文「帧对象池化 A/B」：端到端
   每请求 42 → 32 次分配，公开解码路径 4 → 2 次且快 38%，池化路径
   稳态 0 分配。**第一项（JSON 反射）仍是最大的单块成本**，但它现在有了
   实测的成分表：小信封单元基准 15 次分配里约 11.5 次属于 `encoding/json`
   （marshal 侧 `reflect.unsafe_New` 8.5 + 结果克隆 1，解码侧 2），另 2 次是
   我们自己的信封 splice 与 Frame 装箱——基线那句"19 次全部来自反射"是
   初始环境的读数，按站点重采后并非全部。端到端 echo 的反射本体约 13 次
   （见上文「编解码热点实测优化」的清点），信封搬运那部分已经收敛，剩下的
   不是搬运优化能碰的。接口已隔离，换 Protobuf 可整块消掉反射这一份。

## 长稳测试与内存泄漏曲线

`pkg/reactor/soak_test.go` 的 `TestSoakMemoryStability` 驱动两种负载形态，
周期采样 `runtime.NumGoroutine` 与 `runtime.ReadMemStats`。断言只针对
单调增长与最终回收，不针对绝对值（GC 时机与机器负载会让绝对值 flaky）。
`SOAK=1` 门控，CI 默认跳过：

```bash
SOAK=1 SOAK_SECONDS=180 go test ./pkg/reactor -run TestSoak -race -timeout 15m
```

实测（2026-09-24，16 核本机，`-race`，90 秒档）：

- **连接 churn**（45 秒跑完 18376 个完整连接周期，Dial + 8 次请求 +
  优雅关闭）：goroutines 恒 20、heapInuse 恒 3MiB——accept / serveConn /
  close 回收路径零泄漏；
- **长连接稳态**（8 条连接持续请求 45 秒）：goroutines 44→36 恒定
  （差值是采样点落在调用循环结束前后），heap 3-4MiB，无爬升趋势；
- **回收**：全部客户端显式关闭后，goroutine 立即回到静默水位（20），
  与 server 自身循环展开后的占位完全一致。

结论：短连接 churn 与长连接稳态两种形态下资源占用均平稳，未发现
慢性泄漏；与全仓逐测试的 goleak 检测互为印证。

## 局限

- 回环网络无真实 RTT，跨机延迟将主导单请求数字；
- echo 业务近乎零计算，真实业务吞吐上限更低。
