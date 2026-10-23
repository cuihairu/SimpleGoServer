# http-service：设计取舍

参考实现在 [`http-service/`](https://github.com/cuihairu/SimpleGoServer/tree/main/http-service)，四个源文件。题面考的不是功能量，是服务的生命周期形态：怎么启动、怎么死干净、出错了说什么、能不能被测。下面每个关键选择都列出当时的备选和放弃它的理由。

## 服务边界

单二进制 HTTP 服务，内存键值 API 加一个健康检查端点。值的定义是「恰好一个 JSON 值」：不校验字段，不校验类型，字节原样存原样取。

| 请求 | 成功 | 失败 |
| --- | --- | --- |
| `PUT /kv/{key}` | 201 新建 / 200 覆盖，body `{"status":"stored"}` | 400 key 或值非法；413 超过 `-max-bytes` |
| `GET /kv/{key}` | 200，body 是当初存进去的字节 | 400；404 |
| `DELETE /kv/{key}` | 204，无 body | 400；404 |
| `GET /healthz` | 200，`{"status":"ok"}` | 无 |

边界是题面给定的，但四条假设要写明，否则后面的取舍没有判断依据：

- **单实例、单进程、内存存储。** 重启即失是题面的设定。`Store` 是接口，`ctx` 透传到每一层，换远程存储时其他文件不动（见 [没做的事](#没做的事) 里补什么）。
- **只用标准库。** 七条验收点里明写着这条。四条路由、一个存储，实现层没有引入框架的余地。
- **部署在可信网络或反向代理之后。** 无认证无 TLS 是这个边界的直接后果。
- **调用方是可信但可能出错。** 输入要校验（key 字符集、值是合法 JSON、体积有上限），但不为恶意攻击者设计：没有速率限制，没有配额，没有审计。

## 分层与依赖方向

```text
main.go     装配：flag → Service(MemoryStore) → http.Server → 信号 → 排空
handler.go  传输层：1.22 模式路由、错误→状态码映射（唯一一处）、写 JSON 响应
service.go  业务层：key 校验、值的体积与合法性、哨兵错误（不认识 HTTP 词汇）
store.go    存储缝：Store 接口 + RWMutex 内存实现（不认识业务规则）
```

依赖单向 `main → handler → Service → Store`，没有反向引用。每层只认识下一层的抽象：handler 拿到的是哨兵错误而不是状态码枚举，Service 拿到的是 `Store` 接口而不是 `*MemoryStore`，store 只管字节。

这么切的实际收益有两个。一是响应契约只有一份：加一种错误要改的地方是 `service.go` 加一个哨兵、`handler.go` 的 `writeError` 加一个 case，路由函数永远不出现状态码。二是存储可以整体换掉而不牵动其他层——这不是为将来预留的抽象，测试里的 `failingStore` 桩现在就靠它注入故障。

## 决策表

### 生命周期

| 决策 | 备选 | 为什么这样选 |
| --- | --- | --- |
| `run(args []string) int` 函数化入口，`main` 只做 `os.Exit` 翻译 | 逻辑写在 `main` 里 | `go test` 不会替你调 `main`。函数化之后监听、收信号、排空、退出码这条链整条都能在测试里跑，退出码直接可断言 |
| `signal.NotifyContext` 把 SIGINT/SIGTERM 变成 ctx 取消，信号被消费掉 | 原生 `signal.Notify` 收 channel | ctx 取消是可以 `select`、可以往下游传的事件。消费掉信号是为了让进程里其他组件不会各自装一份 handler 然后重复响应 |
| `errCh` 缓冲 1 | 无缓冲 | `ListenAndServe` 的错误必须可投递，即使主流程已经走关闭分支。无缓冲时这个错误会丢或者把 goroutine 卡在发送上 |
| 关闭超时后返回退出码 1 | 永远等 | 编排系统靠退出码判断这次滚动重启干不干净。无限等待的实际结局是被 SIGKILL，那比重启失败更难排查 |
| 退出码三态：flag 错 2、启动失败或关闭超时 1、干净关闭 0 | 全部 0 / 全部 1 | flag 错是用法问题，启动失败和关闭超时是运行问题，干净关闭没有故事。合成一个数字就丢掉了区分能力 |

### HTTP 层

| 决策 | 备选 | 为什么这样选 |
| --- | --- | --- |
| Go 1.22 模式路由 `"PUT /kv/{key}"` | 手写 method switch / 引路由框架 | `ServeMux` 自带方法不匹配时的 405 和 `Allow` 头，自带 `r.PathValue` 取参。四条路由撑不起框架的依赖成本，而零依赖正是题面的约束 |
| 四个超时全设，作用域递增（5s / 10s / 10s / 60s） | 只设读超时 / 都不设 | `net/http` 的默认值是零超时，这是隐患不是特性。开一个 socket 不发数据的客户端能把一个 goroutine 永久停在那儿。逐项理由见下面的超时矩阵 |
| `io.LimitReader(body, max+1)` | `http.MaxBytesReader` / 不限 | 多读一个字节让「恰好等于上限」和「超了上限」变成可区分的两件事，错误分类干净。不限则攻击者可以用一个请求耗尽内存。`MaxBytesReader` 也能用，但它先写响应头再报错，超限这次请求已经占用了连接 |
| 值的合法性用 `json.Valid` | `Unmarshal` 进 `any` / 完全不校验 | `json.Valid` 不分配目标对象，而且它检查的是「恰好一个值」，`1 2` 这种拼接体会被拒。完全不校验则把非法字节存进去，之后每次读都要重新处理这个坏值 |
| 先判大小再判 JSON | 先 `json.Valid` | 超限的 body 不值得付解析成本。顺序反过来会让 1 GiB 的垃圾先被完整扫描一遍 |
| key 按字节判定：长度 1–128，每字节落在 `'!'`–`'~'` | `strings.ContainsFunc` + Unicode 类别 / 交给调用方 | 逐字节判定没有分配也没有 Unicode 慢路径。这个字符集就是「可打印、不含空格」的准确表达，注释里写明了这个区间在 ASCII 里的含义 |

**超时矩阵。** 每个超时管一段区间，缺哪一段就漏掉哪一类卡死：

| 超时 | 作用域 | 缺失后果 |
| --- | --- | --- |
| `ReadHeaderTimeout` 5s | 连接到请求头读完 | 开 socket 不发数据，goroutine 永久停车（slowloris 的原型） |
| `ReadTimeout` 10s | 连接到请求体读完 | 慢速滴注 body，同样能耗死连接 |
| `WriteTimeout` 10s | 响应写完 | 只设读不设写，响应写一半挂起就没人管 |
| `IdleTimeout` 60s | keep-alive 请求之间 | 空闲连接一直占着 fd |
| `-shutdown-timeout` 10s | 排空在途请求 | 单个卡死的 handler 拖着进程不退，最终被编排系统强杀 |

`IdleTimeout` 明显大于 `ReadTimeout` 是有意的。空闲是正常状态，读超时防的是「开始了却不完成」。

### 业务与存储

| 决策 | 备选 | 为什么这样选 |
| --- | --- | --- |
| 哨兵错误 + `errors.Is` 映射，状态码只在 `writeError` 一处出现 | 字符串匹配 / 各路由自己写状态码 | 哨兵可组合、可 `errors.Is`/`As`，且状态码这种 HTTP 词汇不进入业务层。字符串匹配在错误文案被改一个字时就静默失效 |
| 500 响应固定为 `{"error":"internal error"}`，真实原因进日志 | 原样返回 `err.Error()` | 驱动报错、内部地址、堆栈片段都是给操作者的信息，不是给调用方的。分级很便宜，漏掉的代价是信息泄露 |
| `MemoryStore` 用 `RWMutex + map` | `sync.Map` / channel 化 actor | 见下面的三方案对比 |
| 锁放在 Store 内部 | 锁上移到 Service 或 handler | 锁跟着实现走。换成远程存储时锁自然消失（远程协议自带串行化），业务层从来不知道锁存在 |
| `Store` 三个方法都带 `ctx`，内存实现用 `_` 占位 | 只在需要的层带 ctx / 干脆不带 | 今天的内存实现没有取消语义，但签名先留好。换远程存储那天 handler 和 Service 一行不改，只需要给新实现补上超时和重试 |
| `Value = json.RawMessage`，校验一次之后不再重编码 | `Unmarshal` 进结构体再 `Marshal` 出去 | 双重编码是本仓另一个服务踩过的坑：1 MiB 的调用往返曾花掉 42 ms，换成原样透传才降回来。这题没有理由再踩一次 |
| `Put` 锁内不做防御性拷贝 | 拷一份进 map | 所有权约定是「交出后不可变」，调用方传进来的已经是独立切片。锁内拷贝只是把一次 O(n) 内存复制放在持锁的临界区里 |
| `Put` 返回 `created` 用来区分 201 / 200 | 一律 200 | 一行成本换正确的 REST 语义，也让调用方能区分「新建」和「覆盖」 |
| `DELETE` 键不存在返回 404 而不是 204 | 幂等地一律 204 | 这里选择让调用方能发现「删了个不存在的键」。如果这个 API 要挂在重试逻辑后面，幂等 204 更合适——判断依据是调用方会不会重试，而不是 REST 教条 |
| `/healthz` 恒 200，探测本身做到最便宜 | 遍历依赖做深度探测 | 没有外部依赖时，「能回答请求」就是健康的完整定义。深度探测在依赖抖动时会把探针本身变成雪崩的一环 |
| 日志用 `slog` 的 JSON handler 输出到 stderr | `fmt` / 文本日志 | 结构化日志能直接被采集，stderr 与服务输出分离，排障时不会和业务输出混在一起 |

**三种并发安全方案。** 裸 map 的并发写是运行时 `fatal error`，进程直接死，连 race detector 的报告都没有，所以这里必须显式选一种：

| 方案 | 适合的场景 | 本题的判断 |
| --- | --- | --- |
| `RWMutex + map` | 读写均衡，或者写不罕见 | 采纳。朴素实现最快，单锁不嵌套因此没有死锁面 |
| `sync.Map` | 读多写少、键集离散、键只增不删 | 不采纳。KV 服务的写和读频率相当，`sync.Map` 的双 map 维护在这时是纯开销 |
| channel 化 actor | 需要把复杂事务串行化 | 不采纳。单键点查走排队是纯开销，事务需求不存在 |

### 可测试性

| 决策 | 备选 | 为什么这样选 |
| --- | --- | --- |
| 三层各测各的：handler 用 `httptest.NewRecorder` 直测，Service 表驱动，store 跑并发靶场 | 只写端到端 | `Recorder` 不占端口，微秒级。分层测试的失败信息直接指向出错的层，不会变成「请求返回了 500，去找」 |
| 生命周期用真信号测：`syscall.Kill` 打给自己，断言退出码 | mock 信号通道 | 测的是部署时真正发生的事。`run` 已经在测试进程里装好信号处理器，`Kill` 打进去走的就是生产路径。mock 信号测的是 mock |
| 每次生命周期测试末尾 `goleak.VerifyNone` | 只断言退出码 | 退出码 0 不代表没有后台 goroutine 残留。`Shutdown` 超时后仍挂着的 handler，只有泄漏检测能看见 |
| `osExit` 做成包级变量，测试里替换 | 测 `run` 就够了 | `main` 只做一件事（把退出码翻译成 `os.Exit`），那就让这一行也有测试。代价是一个可替换的全局变量，收益是「翻译这一步」不会成为无人覆盖的代码 |

## 代码走查

### 关闭路径的两臂 select

`http-service/main.go`：

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()

errCh := make(chan error, 1)
go func() { errCh <- srv.ListenAndServe() }()

select {
case err := <-errCh:
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

`ListenAndServe` 总是返回非 nil 错误，但 `http.ErrServerClosed` 只可能来自 `Shutdown`，而代码里唯一的 `Shutdown` 调用就在 ctx 臂里。所以能走到 errCh 臂就一定是绑定端口失败，退出码 1 是准确的，不需要在错误值上再判断一次。这条推理写在了源码注释里，读者不用自己重推。

`Shutdown` 做三件事：停止接受新连接、等在途 handler 返回、关掉 keep-alive 连接。外层的 `WithTimeout` 把「某个 handler 卡住」从「进程不可杀」变成「deadline 到期」。这个超时是排空的有界保证，不是可选项。

`defer stop()` 不能省：它让 `NotifyContext` 注册的信号处理器在 `run` 返回时摘掉，否则测试进程里连着跑几个用例之后，进程会持续吞掉自己的 SIGTERM。

### 状态码只在一处出现

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

每个哨兵错误恰好对应一个状态码，`ErrKeyInvalid` 和 `ErrValueInvalid` 合并到 400 是有意的：调用方能做的补救都是「改请求」。落到 500 的分支把错误详情换成固定文案再写进响应体，同时把真实错误交给 logger——响应给调用方，日志给操作者，两者内容不同是设计而不是疏漏。

写错误响应的顺序是「先定状态码，再写 header，最后写 body」。一旦 header 写出去了就不能改，所以 `writeJSON` 里的顺序是设置 Content-Type、`WriteHeader`、`Encode`，没有能反悔的余地。

### 多读一个字节

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
return s.store.Put(ctx, key, Value(raw))
```

只读 `max` 字节的话，一个正好 1 MiB 的合法值和一个 10 GiB 的值读出来长度一样，没法分别回答 200 和 413。读 `max+1` 字节之后，`len(raw) > max` 就是干净的越界信号，多出来的那一字节是判断成本，不是浪费。

`json.Valid` 之后紧接着 `Value(raw)`，中间没有任何 `Unmarshal` 再 `Marshal`。读进来的字节被 `LimitReader` 复制到 `raw` 这个独立切片里，交给 store 之后所有权转移，store 不再碰它。整条路径上一次编码都没有。

### 锁内的五行

`http-service/store.go`：

```go
func (m *MemoryStore) Put(_ context.Context, key string, v Value) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.items[key]
	m.items[key] = v
	return !exists, nil
}
```

写锁、判存在、赋值、返回新建标记，一次临界区完成，没有嵌套锁也没有持锁期间的 I/O。`created` 用赋值前的一次查找得到，比「先 `Get` 再 `Put`」少一次加锁，也避免了两次操作之间状态被别人改掉。

`v` 不拷贝是因为所有权写在了接口注释里：调用方交出之后不再修改。`ctx` 写成 `_ context.Context` 是同一个思路的反面——现在用不上，但接口签名要预留，否则换存储时改的是所有调用方。

## 测试怎么落地

题面要求「各层可独立测试」和「有真信号驱动的优雅关闭端到端测试」，落到具体手段：

| 层 | 手段 | 覆盖的场景 |
| --- | --- | --- |
| Service | 表驱动，无 I/O | key 字符集与长度的边界矩阵；值不是 JSON、`1 2` 这类拼接体、超限；`failingStore` 桩注入的读失败、删除不存在、写入错误 |
| handler | `httptest.NewRecorder` 直测 | 三条路由的成功响应与状态码；每个哨兵到状态码的映射；内部错误文本不出现在响应里 |
| store | `-race` 下的并发读写 | 多 goroutine 同时读写同一个 map，覆盖的是「忘了加锁」这类问题——`go test -race` 会当场报出来 |
| 生命周期 | `run()` + 真信号 + `goleak` | 端口被占用返回 1；flag 错误返回 2；`main` 把退出码翻译成 `os.Exit`；关停超时返回 1；完整生命周期——监听、应答真请求、真 SIGTERM、排空、退出 0、之后拒绝新连接 |

关停超时那个用例值得单说：它用裸 TCP 连上之后只写半个 body，靠 `Expect: 100-continue` 让 handler 确定停在 `ReadAll` 上，再打 SIGTERM。这样构造出来的卡死是确定的，不是靠 sleep 赌出来的。测试断言的是退出码 1，也就是「排空没排干净，进程如实报告」。

`http-service` 这个包的语句覆盖率是 100%，但这个数字本身不是目标。删掉一个测试或者新增一个错误分支，`go test` 依然全绿，README 里那句 100% 就变成无人验证的口头禅。真正维持它的是 [`scripts/check-coverage.sh`](https://github.com/cuihairu/SimpleGoServer/blob/main/scripts/check-coverage.sh)：CI 在每个推送上按包检查覆盖率，低于 100% 即失败。门禁的单位是包而不是仓库总和，因为总和会让大包掩护小包。

## 没做的事

每条都写了「为什么现在不做」和「生产环境补什么」。

- **数据只在内存里，重启即失。** 题面给定。补法是写一个 `Store` 实现（Redis、Postgres、或者落盘的 BoltDB），并给远程实现补上超时与重试——签名的缝已经留好了，但要清楚接口带 `ctx` 不等于实现了取消。
- **键集无界。** 单个值有 1 MiB 上限（`-max-bytes` 可调），但 map 的条目数没有上限，写入速率也没有配额。持续写入可以让内存一直涨到进程被 OOM killer 干掉。生产环境需要条目数上限、TTL 淘汰或 LRU，题面只要求了三个动词，克制本身是这道题的考点。
- **单实例，没有水平扩展。** 没有多副本，没有副本间一致性。KV 语义下要加实例，第一步一定是把存储外置，第二步才谈得上无状态扩容。
- **`/healthz` 只有 liveness 语义。** 恒 200 只说明进程能应答。接上外部依赖之后要拆成两个探针：liveness 判断「要不要重启我」，readiness 判断「要不要给我导流量」。把两者合成一个探针是常见的事故源。
- **没有认证，没有 TLS。** 补的位置在 handler 之前加一层中间件，或者交给反向代理。三层结构不需要为此改动。
- **`json.Valid` 不是模式校验。** 值内部是什么结构，服务不关心也不该关心：这是存字节的 KV，不是文档数据库。真要校验字段，就该在 Service 层加一层 schema，那是另一个量级的需求。
- **没有 TTL、事务、列举接口。** 题面只要求 `PUT`/`GET`/`DELETE`。列举接口是第一个危险的需求——它天然需要分页、游标和排序，键集无界的问题会在这里第一次真正疼起来。
- **单把 `RWMutex` 是扩展上限。** 读写都走同一把锁的写侧，QPS 上来之后瓶颈清晰可见。生产形态是分片 map（按 key 哈希分 N 把锁）或换存储结构，取决于读写的实际比例。
- **退出码契约依赖编排系统真的看它。** systemd、k8s 会据此判断滚动重启是否顺利，本地裸跑时它只是一个退出码。这是运行假设，不是缺陷，但它需要被部署配置兑现。

## 复现

```bash
# 构建、静态检查、格式检查
go build ./... && go vet ./... && gofmt -l .

# 本服务的测试：单元 + 端到端 + 竞态 + 泄漏检测
go test ./http-service/ -race -count=2

# 跑起来
go run ./http-service -addr 127.0.0.1:8080
curl -i -XPUT 127.0.0.1:8080/kv/k -d '{"ok":true}'   # 201
curl -i 127.0.0.1:8080/kv/k                          # 200，回显原字节
curl -i 127.0.0.1:8080/healthz                       # 200
```

优雅关闭可以手动看：另开一个终端发请求的同时 `kill -TERM`，在途请求会走完，进程以 0 退出。

## 题一去哪了

同一批面试题里的另一道（主从 Reactor + 自定义 TCP 帧协议）已经独立成库：[JsonStream](https://github.com/cuihairu/jsonstream)，它的[架构与取舍文档](https://github.com/cuihairu/jsonstream/blob/main/docs/DESIGN.md)里有那题的设计决策。本仓剩下的实现代码仍可运行，但文档站只讲这一道题。
