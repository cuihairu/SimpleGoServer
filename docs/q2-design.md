# 题目二：http-service——为什么这样设计

> 本文是题目二的设计决策主线：题面与约束 → 分层 → 分组的设计决策表（备选方案对比 + 为什么选这个）→ 关键代码走读 → 坦白的取舍与局限。
> 题目二考的不是功能量，而是服务"**活得体面、死得干净**"的工程形态——所以本文大部分决策关于生命周期、防线与可测试性，而不是功能实现。题一的同主线文档见[题一：Reactor 与自定义协议](/q1-design)。

---

## 题面与约束

**题面**：用 Go 标准库实现单二进制 HTTP 服务——内存键值 API（`PUT/GET/DELETE /kv/{key}`，值为单个 JSON 值）加 `GET /healthz`。七条验收点与本题的落点：

| # | 验收点 | 本仓落点 |
| --- | --- | --- |
| 1 | 优雅启动与关闭、退出码反映是否干净 | `signal.NotifyContext` + `srv.Shutdown` + 超时兜底，干净 0 / 异常 1 |
| 2 | `/healthz`，说清"健康"是什么 | 无外部依赖 ⟹ "能回答请求"即健康，恒 200 且极便宜 |
| 3 | 三层单向依赖、存储可替换 | `handler → Service → Store` 接口缝，桩注入故障测试 |
| 4 | 并发安全、`-race` 干净、锁方案理由 | Store 层 `RWMutex + map`，选型对比见 D8 |
| 5 | 超时齐全、错误统一映射、请求体上限 | 四超时 + `LimitReader(max+1)` + `writeError` 一处集中 |
| 6 | 入口函数化、各层可独立测、真信号端到端 | `run(args) int`；httptest 两模式；`syscall.Kill` 打自己 + goleak |
| 7 | 只用标准库、不做多余抽象 | Go 1.22 模式路由；单目录 4 源文件，每层一文件 |

**明确假设**：

- **单实例、单二进制、内存存储**——重启即失是题面给定前提，但 ctx 全链路透传让换远程存储时其他层零改动（见局限 L1）。
- **标准库 only 是题面约束，也是这里的正确选择**——路由到几百条、需要中间件生态时框架收益才盖过成本，说得出这个临界点比会用框架更稀缺。
- **无认证无 TLS**——部署在可信网络/反代之后，这是面试服务的合理边界，生产形态见局限 L4。

---

## 分层与依赖方向

```text
main.go    装配：flag → Service(MemoryStore) → http.Server → 信号 → 排空
handler.go 传输层：1.22 模式路由、错误→状态码映射（唯一一处）、JSON 响应
service.go 业务层：key/值校验、哨兵错误、LimitReader 上限（不知道 HTTP 词汇）
store.go   存储缝：Store 接口 + RWMutex 内存实现（不知道业务规则）
```

依赖严格单向 `main → handler → Service → Store`。每一层只认识下一层的抽象：handler 只见哨兵错误不见状态码以外的东西；Service 只见 `Store` 接口不见锁；Store 只见字节不见业务规则。**错误映射集中意味着响应契约只有一份**——加一种错误只改 service 和 `writeError` 两处，路由永不关心状态码。

---

## 设计决策表

### 生命周期

| # | 决策 | 主要备选 | 为什么放弃备选 | 角度 |
| --- | --- | --- | --- | --- |
| D1 | `run(args []string) int` 函数化入口，main 只做 `os.Exit` 翻译 | 逻辑写在 main 里 | go test 不会替你调 main——函数化后整条"监听 → 服务 → 收信号 → 排空"生命周期都在测试里跑（含真信号），退出码直接可断言 | 表达 |
| D2 | `signal.NotifyContext` 把 SIGINT/SIGTERM 变成 ctx 取消，且信号被消费 | 原生 `signal.Notify` channel | ctx 取消让信号变成可 select 的事件、可随调用链传播；消费掉避免进程内其他组件（如被引入的库再装一次 handler）误反应 | 惯例 |
| D3 | `errCh` 缓冲 1；Shutdown 超时 → 退出码 1 | 无缓冲 channel / 永远等 | 无缓冲会丢或卡——ListenAndServe 的错误必须可投递，即使主流程已走关闭分支；退出码是编排系统判断滚动重启是否干净的依据，卡死等待 = 被 SIGKILL，更难看 | 复杂度 |

### HTTP 层

| # | 决策 | 主要备选 | 为什么放弃备选 | 角度 |
| --- | --- | --- | --- | --- |
| D4 | Go 1.22 模式路由 `"GET /kv/{key}"` | 手写 method switch / 引路由框架 | mux 自带 405+Allow（方法不匹配）与 `PathValue`；零依赖是题面约束，也恰好是正确选择——7 条路由撑不起框架的成本 | 惯例 |
| D5 | 四超时全设，作用域递增：ReadHeader 5s → Read 10s → Write 10s → Idle 60s | 只设读超时 / 全不设 | net/http 默认零超时是隐患不是特性："开 socket 不发数据"的客户端能永久占住一个 goroutine（slowloris 原型）；只设读不设写，响应写一半挂起就没人管 | 性能（防线） |
| D6 | `io.LimitReader(body, max+1)` 读 maxValue+1 字节 | `http.MaxBytesReader` / 无界读 | +1 字节区分"恰好满"与"超了"，超限立刻 413，无界输入永不进内存；MaxBytesReader 也可，但会先写 header 才发现超限，错误分类不如 +1 干净 | 性能（防线） |

**超时矩阵**（每个超时管一段、缺一个的后果）：

| 超时 | 作用域 | 缺失后果 |
| --- | --- | --- |
| ReadHeaderTimeout 5s | 请求头读完 | slowloris：开 socket 不发数据，goroutine 永久停车 |
| ReadTimeout 10s | 整个请求读完 | 慢速滴注 body 同样能耗死连接 |
| WriteTimeout 10s | 响应写完 | 只设读不设写：响应写一半挂起无人管 |
| IdleTimeout 60s | keep-alive 空闲 | 空闲连接永占 fd |
| shutdown-timeout 10s | 排空在途请求 | 单个卡死 handler 拖着进程不退，被编排系统强杀 |

### 业务与存储

| # | 决策 | 主要备选 | 为什么放弃备选 | 角度 |
| --- | --- | --- | --- | --- |
| D7 | 哨兵错误 + `errors.Is` 映射，`writeError` 一处集中；500 固定文案细节进日志 | 字符串匹配 / 原样返回 `err.Error()` | 哨兵错误可组合可 `errors.Is/As`，HTTP 词汇（状态码）不进业务层；错误文本是给操作者的不是给调用方的——驱动报错/地址/堆栈外泄是信息泄露 | 复杂度 |
| D8 | Store 用 `RWMutex + map`，锁放 Store 层 | ① `sync.Map`；② channel 化 actor；③ 锁上移业务层 | ① KV 服务 put/get 频率相当，sync.Map 为读多写少优化，双 map 维护反而慢；② 单 key 点查走排队纯开销；③ 换远程存储时锁自然消失（远程协议自带串行化），业务层从不知道锁存在——锁跟着实现走 | 性能 |
| D9 | `Value = json.RawMessage` 原样存取，校验一次不再重编码 | Unmarshal → 再 Marshal | 零重编码；这是题一踩过的坑直接迁移——TCP 服务 1MiB Call 曾因双重编码花 42ms，`RawMessage` 透传后才回来（见 [Benchmark](/Benchmark)） | 性能 |
| D10 | `Put` 的 Value 无防御性拷贝；`Put` 返回 created 区分 201/200 | 锁内拷贝 / 统一 200 | 所有权语义是"交出后不可变"，调用方 `json.RawMessage` 已是独立字节切片，锁内拷贝纯属浪费；created 是一行成本换 REST 语义正确 | 复杂度 |

**D8 的三方案对比**（裸 map 并发写是运行时 `fatal error`——进程直接死，不是 race 报告）：

| 方案 | 适合场景 | 本场景判断 |
| --- | --- | --- |
| `RWMutex + map` | 读写均衡或写不罕见 | ✅ 朴素最快，单锁无嵌套 → 无死锁面 |
| `sync.Map` | 读多写少、键集离散、key 只增 | ❌ put/get 频率相当，双 map 维护反而慢 |
| channel 化 actor | 需要串行化复杂事务 | ❌ 单 key 点查走排队纯开销 |

### 可测试性

| # | 决策 | 主要备选 | 为什么放弃备选 | 角度 |
| --- | --- | --- | --- | --- |
| D11 | 三层各测各的：handler 用 `httptest.NewRecorder` 直测、业务表驱动、存储 `-race` 并发靶场 | 只写端到端 | Recorder 直测无端口微秒级；分层测试的失败信息直接指向出错的层。端到端另有，但不是唯一手段 | 惯例 |
| D12 | 真信号端到端：`syscall.Kill` 打给自己 + 断言退出码 + goleak | mock 信号 / 跳过不测 | 测的是部署时真正发生的事——`run` 已在测试进程内装好信号处理器，`Kill` 打进去走的就是生产路径；mock 信号测的是 mock | 表达 |

---

## 关键代码走读

### 1. 优雅关闭的 select 两臂

`http-service/main.go`：

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()

// Buffered: ListenAndServe's error must be deliverable even if the
// main flow already moved on to the shutdown branch.
errCh := make(chan error, 1)
go func() { errCh <- srv.ListenAndServe() }()

select {
case err := <-errCh:
	// ErrServerClosed only ever comes after Shutdown, and the only
	// Shutdown call lives in the ctx branch below — so reaching this
	// case at all means the bind failed (or the listener died under us)
	log.Error("server failed", "err", err)
	return 1
case <-ctx.Done():
	sctx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		log.Error("graceful shutdown timed out", "err", err)
		return 1
	}
	log.Info("shutdown complete")
	return 0
}
```

四角度拆解这段"就一个 select"的代码：

- **正确性**：`ListenAndServe` 总是返回非 nil，但 `ErrServerClosed` 只可能来自 Shutdown——而唯一的 Shutdown 在 ctx 臂里，所以走到 errCh 臂就一定是 bind 失败。注释把这层推理写明，读者不需要自己推。
- **防线**：`Shutdown` 的超时把"单个卡死的 handler"变成 deadline 而不是不可杀的进程；退出码 1 让编排系统知道这次滚动重启不干净。
- **Go 惯例**：`NotifyContext` + `defer stop()` 是标准库给这个场景的正解——信号变成 ctx 取消，select 就能把"信号到来"与"监听出错"放进同一个等待点。
- **面试表达**：一句话版本——"SIGTERM 是承诺，不是自杀令：先停止接收，等在途请求走完（有超时兜底），退出码告诉编排系统干不干净。"

### 2. 错误映射集中在一处

`http-service/handler.go`：

```go
func (h *handler) writeError(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, ErrKeyInvalid), errors.Is(err, ErrValueInvalid):
		code = http.StatusBadRequest
	case errors.Is(err, ErrValueTooLarge):
		code = http.StatusRequestEntityTooLarge
	}
	if code == http.StatusInternalServerError {
		h.log.Error("internal error", "err", err)
		err = errors.New("internal error")
	}
	writeJSON(w, code, map[string]string{"error": err.Error()})
}
```

两个决定：**响应契约只有一份**——业务层用哨兵错误表达失败，状态码只在这里出现一次，路由处理函数永远不碰状态码；**500 固定文案**——内部错误细节（驱动报错、地址、堆栈）进日志不进响应，错误文本是给操作者的，不是给调用方的。这是"信息分级"意识的最小完整样例。

### 3. LimitReader 的 +1 字节

`http-service/service.go`：

```go
raw, err := io.ReadAll(io.LimitReader(body, s.maxValue+1))
if err != nil {
	return false, fmt.Errorf("read body: %w", err)
}
if int64(len(raw)) > s.maxValue {
	return false, ErrValueTooLarge
}
if !json.Valid(raw) {
	return false, ErrValueInvalid
}
```

多读那一字节是为了区分"恰好满"与"超了"：只读 max 字节时，一个正好 max 的合法值和一个 10 GiB 的值读出来一样长，没法分别回 200 还是 413。读 max+1 字节，`len(raw) > max` 就是干净的越界信号。错误检查的顺序也有讲究：先大小后 JSON 有效性——超限的 body 没必要花解析成本。`json.Valid` 而不是 `Unmarshal` 进 `any`：校验"恰好一个 JSON 值"不需要分配目标对象（`1 2` 这类拼接体也会被拒）。

### 4. 锁与所有权的最小形态

`http-service/store.go`：

```go
// Put stores v and reports whether the key is new. The caller hands
// over ownership: v is never mutated afterwards, so no defensive copy
// is needed under the lock.
func (m *MemoryStore) Put(_ context.Context, key string, v Value) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.items[key]
	m.items[key] = v
	return !exists, nil
}
```

整个存储层 30 行，但三个决定都在：**RWMutex 而不是 sync.Map**（读写均衡场景朴素最快，见 D8 对比表）；**无防御性拷贝**——所有权契约"交出后不可变"写在注释里，锁内拷贝是浪费；**`ctx` 参数今天用不上（`_`）也保留**——内存存储没有取消语义，但签名先留好，换远程存储那天 handler 和 Service 一行不改。接口是缝不是仪式：`Store` 同时是测试缝（`failingStore` 桩注入故障）和替换缝（换 Redis 不动其他层）。

---

## 已知取舍与局限

- **L1 · 内存存储重启即失**——题面给定，但边界画得很自觉：`Store` 接口 + ctx 全链路透传，换 Redis/Postgres 时其他层零改动；唯一要补的是给远程实现接超时与重试，签名的缝已经留好。
- **L2 · 单实例**——没有水平扩展、没有多副本一致性；KV 语义下加实例首先要外置存储（同 L1），然后才谈得上无状态扩容。
- **L3 · `/healthz` 恒 200，只有 liveness 语义**——无外部依赖时"能回答"即健康，这是定义的自觉而不是偷懒。一旦接上远程存储，就该拆 readiness（依赖可用才导流量）与 liveness（死了才重启）两个端点——把它们混成一个探针是常见事故源。检查必须便宜：深度探测被打爆拖垮服务的雪崩先例不少。
- **L4 · 无认证、无 TLS**——按可信网络部署假设；生产补齐的位置在 handler 之前加一层中间件（或交给反代），三层结构不需要为此改动。
- **L5 · `json.Valid` 不是模式校验**——它只保证"恰好一个合法 JSON 值"，值内部结构（字段、类型）业务不关心也不该关心：这是 KV 存字节的服务，不是文档数据库。
- **L6 · 无 TTL、无事务、无列举**——题面只要求三个动词；加每一个都要先过"这道题考的是不是它"这一关。克制本身是考点。
- **L7 · 退出码契约依赖编排系统真的看它**——systemd/k8s 靠退出码判断滚动重启是否顺利；本地裸跑时它只是退出码。这不是本服务的缺陷，是它的运行假设。

---

## 与题一的互证

- **优雅关闭同一副骨架**：这里的"停收 → 排空 → 超时强杀"与题一的"停 accept → drain → 强关 → 停 worker"是同一个模式，net/http 内建了前两段，题一手写了全部五段。
- **无界输入的防线同一个思路**：这里 `LimitReader(max+1)` 封请求体，题一 `MaxFrameSize + 负长度检查` 封帧长——"所有无界的东西都必须有界"在两道题里各落了一次地。
- **空闲回收两个层次**：HTTP 层的 `IdleTimeout` 与协议层的 `SetReadDeadline`，回收的是同一类东西——不再活动的连接。

题一的设计决策见[题一：Reactor 与自定义协议——为什么这样设计](/q1-design)。
