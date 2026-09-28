# SimpleGoServer

一道后端面试题的参考实现：**写一个简单 Go 服务**。内存键值 API（`PUT/GET/DELETE /kv/{key}`，值为单个 JSON 值）加一个 `GET /healthz` 健康检查端点，单二进制，只用标准库。功能刻意简单——这道题考的不是功能量，而是服务"**活得体面、死得干净**"的工程形态。

同一批面试题的另一道（高性能非阻塞网络通信模型 + 自定义 TCP 帧协议）已独立成 [JsonStream](https://github.com/cuihairu/jsonstream)，题面、参考实现与协议文档都在那边。

**在线文档站**：<https://cuihairu.github.io/SimpleGoServer/> —— 设计取舍、代码走读、没做的事与复现命令。

---

## 一、面试题要求

用 Go 标准库实现一个单二进制 HTTP 服务：内存键值 API（`PUT/GET/DELETE /kv/{key}`，值为单个 JSON 值）加一个 `GET /healthz` 健康检查端点。功能刻意简单——这道题考的不是功能量，而是服务"**活得体面、死得干净**"的工程形态。

验收点七条：

1. **优雅启动与关闭**：进程收到 SIGINT/SIGTERM 后停止接收新请求，等在途请求完成（有超时兜底）再退出；退出码反映关闭是否干净。
2. **健康检查**：`/healthz` 返回 200 与服务状态；说清楚你认为的"健康"是什么。
3. **结构分层**：传输（HTTP）→ 业务规则 → 存储三层，依赖单向；存储可替换而其他层零改动。
4. **并发安全**：内存存储被并发读写，`-race` 下无数据竞争；说明锁方案的选择理由。
5. **超时与错误处理**：Server 各类超时齐全；错误统一映射到状态码，内部错误细节不外泄；请求体大小有上限。
6. **可测试性**：入口函数化（flag 进、退出码出）；各层可独立测试（表驱动 + httptest）；有真信号驱动的优雅关闭端到端测试。
7. **约束**：只用标准库；单目录多文件可以，不做多余抽象。

---

## 二、设计与对应知识点

**1. 优雅启动与关闭——SIGTERM 是承诺，不是自杀令。**
部署系统（systemd、k8s）滚动重启时给进程发 SIGTERM，等它退出；超时后才强杀。所以信号的正确读法是"通知"：`signal.NotifyContext` 把 SIGINT/SIGTERM 变成 ctx 取消，主流程 select 在"信号到来"与"ListenAndServe 出错"之间，收到信号后调 `srv.Shutdown`（停止接收 → 排空在途请求 → 关 keep-alive），超时兜底防单个卡死的 handler 拖垮整个进程。退出码也要认真：干净退出 0，超时/异常 1——编排系统靠它判断滚动重启是否顺利。

**考察什么**：是否知道 net/http 默认对信号的行为是直接终止（在途请求被砍）；`Shutdown` 与 `Close` 的区别（排空 vs 立断）；退出码语义。

- 合格：信号 → Shutdown → 超时兜底的链路完整，退出码正确。
- 加分：区分"停止接收"与"排空在途"；说明 Shutdown 超时后强杀的取舍；提到 k8s 的 `terminationGracePeriodSeconds` 与 preStop hook 的配合（hook 只是流程编排，进程内优雅关闭仍是必须的）。

**2. 健康检查——先回答"健康是什么"再写端点。**
本服务无外部依赖，"能回答请求"即"健康"，`/healthz` 恒回 200。一旦接上远程存储，健康的定义就变了：进程活着 ≠ 服务可用。k8s 把这两个概念拆成 liveness（死了就重启）与 readiness（没就绪别导流量）两个端点——把它们混成一个探针是常见事故源。检查本身必须便宜：健康检查被打爆而拖垮服务的先例不少。

**考察什么**：健康语义的思考深度（liveness/readiness 分离）；检查成本意识。

- 合格：有端点且语义明确。
- 加分：liveness/readiness 分离的讨论；说得出"检查要便宜"以及深度检查（逐依赖 ping）的雪崩风险。

**3. 结构分层——接口是缝，不是仪式。**
三层单向依赖：`handler`（HTTP 语义：状态码、错误 JSON）→ `Service`（业务规则：key 合法性、大小上限、JSON 校验，用哨兵错误表达失败）→ `Store` 接口（存储缝）。有人会问"单实现也要接口？"——要，因为这个接口同时是测试接缝（`failingStore` 桩注入故障）和替换缝（换 Redis/Postgres 不动其他层）。`ctx` 全链路透传：内存存储今天用不上，远程存储明天就需要超时与取消的通道，签名先留好。错误→状态码映射集中在 `writeError` 一处，响应契约不散落。

**考察什么**：分层的真实收益（换实现不动他人、故障注入可测）；哨兵错误 vs 字符串匹配；ctx 透传的意义。

- 合格：三层边界清晰、错误映射集中、依赖单向。
- 加分：论证接口的存在理由（接缝而非仪式）；ctx 透传；用桩存储示范故障注入测试。

**4. 并发安全——裸 map 并发写会直接崩进程。**
Go 的 map 不是并发安全的，并发读写不保护轻则 race、重则 `fatal error: concurrent map read and map write` 直接杀死进程（这不是 race detector 的报告，是运行时的不可恢复错误）。选型对比：`RWMutex + map`——读写均衡时最朴素也最快；`sync.Map`——为读多写少、键集离散的场景优化，本场景读写均衡用它反而慢；channel 化 actor——单 key 点查走排队纯开销。锁放在 Store 层还有个结构性理由：换远程存储时锁自然消失（远程协议自带串行化），业务层从不知道锁的存在。另一个细节是所有权：`Put` 收到的 `Value` 交出后不可变，锁内无需防御性拷贝。

**考察什么**：裸 map 并发写的后果；RWMutex/sync.Map/channel 三方案的适用面；`-race` 是否是常规动作。

- 合格：互斥保护 + `-race` 干净。
- 加分：方案对比有依据；所有权语义讲清；说明单锁无嵌套所以无死锁面。

**5. 超时与错误处理——net/http 默认零超时是隐患不是特性。**
默认配置下一个"打开 socket 然后不发数据"的客户端能永久占住一个 goroutine（slowloris 攻击的原型）。四个超时作用域递增：`ReadHeaderTimeout`（请求头读完）、`ReadTimeout`（整个请求读完）、`WriteTimeout`（响应写完）、`IdleTimeout`（keep-alive 空闲）——只设读不设写，响应写一半挂起就没人管。请求体用 `io.LimitReader` 读 `maxValue+1` 字节：多读那一字节是为了区分"恰好满"与"超了"，超限立刻 413，无界输入永远不进内存。错误侧：哨兵错误经 `errors.Is` 映射到状态码；500 用固定文案，内部错误细节（驱动报错、地址、堆栈）进日志不进响应——错误文本是给操作者的，不是给调用方的。

**考察什么**：四个超时各管哪一段、缺一个会怎样；LimitReader 的 +1 技巧；错误信息分级意识。

- 合格：四超时 + 大小上限 + 统一错误映射。
- 加分：逐个讲清超时作用域；错误分级（客户端看什么/日志记什么）；提到 `http.MaxBytesReader` 这类替代方案。

**6. 可测试性——可测试性是设计出来的，不是测出来的。**
`main()` 里写全流程就没法测（go test 不会替你调 main），所以入口拆成 `run(args []string) int`——flag 进、退出码出，main 只做 `os.Exit` 翻译，整条"监听 → 服务 → 收信号 → 排空"生命周期都在 `go test` 里跑。分层各测各的：handler 用 `httptest.NewRecorder` 直测（无端口、微秒级），业务层表驱动测校验矩阵，存储层给 `-race` 喂并发流量。最后一条端到端测试走真端口、发真信号（`syscall.Kill` 打给自己，此时信号处理器必然已装好）、断言退出码与"关闭后拒绝新连接"，再用 goleak 验证无 goroutine 残留——测的是部署时真正发生的事。

**考察什么**：函数化入口与接缝设计；httptest 两种模式（Recorder 直测 vs `NewServer` 走网络）的取舍；泄漏检测意识。

- 合格：各层可独立测试 + table-driven。
- 加分：真信号驱动的端到端；`-race` 并发靶场；goleak 兜底。

**7. 约束（标准库 only）——克制本身是考点。**
路由不引框架，用 Go 1.22 的模式语法：`mux.HandleFunc("GET /kv/{key}", ...)`，方法不匹配 mux 自己回 405（带 Allow 头），`r.PathValue("key")` 取路径参数——这要求对标准库覆盖面的了解足够新。克制不是教条：路由到几百条、需要中间件生态时，chi/echo 这类框架的收益才盖过成本，说得出这个临界点比会用框架更稀缺。单目录 7 个文件，每层一文件，结构即文档。

**考察什么**：标准库（尤其 1.22 新路由）的熟悉度；抽象的时机判断。

- 合格：零第三方依赖完成全部验收点。
- 加分：说得出引框架的收益临界点；对 1.22 mux 方法匹配与 405/Allow 行为的了解。

---

## 三、实现与测试结果

实现位置——单目录 4 个源文件加 4 个测试文件，每层一文件，零第三方依赖：

```text
http-service/
  main.go          入口：flag 解析、装配、信号、优雅关闭（run(args) int 可测）
  handler.go       HTTP 层：1.22 模式路由、错误→状态码映射、JSON 响应
  service.go       业务层：key/值校验、哨兵错误、LimitReader 大小上限
  store.go         存储层：Store 接口 + RWMutex 内存实现
  *_test.go        各层表驱动测试 + -race 并发靶场 + 真信号端到端 + goleak
```

测试结果（2026-09-24）：`go test ./http-service/ -race -count=2` 全绿（2 核约束下用 `taskset -c 0,1` 复验同样全绿）；demo 冒烟逐状态码核验：`PUT` 首次 201 / 覆盖 200、`GET` 200、非法 JSON 400、坏 key 400、超限 413、方法不匹配 405（带 Allow）、未知键 404、`DELETE` 204；SIGTERM 后打印 shutdown complete、退出码 0、端口拒绝新连接。

验证命令（逐条可复制）：

```bash
# 全部测试：各层表驱动 + 竞态检测 + goroutine 泄漏检测，-count=2 防状态残留
go test ./http-service/ -race -count=2

# demo：终端 1 启动服务（-addr 缺省 127.0.0.1:8080）
go run ./http-service

# 终端 2：健康检查 → 写入 → 读取 → 删除 → 优雅关闭验证
curl -s localhost:8080/healthz
curl -si -X PUT --data '{"n":42}' localhost:8080/kv/answer   # 201 Created
curl -s localhost:8080/kv/answer                             # {"n":42}
curl -si -X POST --data '1' localhost:8080/kv/answer         # 405（Allow: DELETE, GET, HEAD, PUT，GET 模式隐含 HEAD）
curl -si -X PUT --data 'not json' localhost:8080/kv/k        # 400
curl -si -X DELETE localhost:8080/kv/answer                  # 204 No Content
curl -si localhost:8080/kv/answer                            # 404

# 终端 1 按 Ctrl+C（或 kill -TERM）：日志打出 "shutdown complete"，退出码 0
```

深入阅读：[在线文档站](https://cuihairu.github.io/SimpleGoServer/)——四组决策表、代码走读与"没做的事"清单；题面的完整拆解也在站内 [设计取舍](https://cuihairu.github.io/SimpleGoServer/design) 一页。
