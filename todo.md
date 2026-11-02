# TODO

> 需求清单：一条一句。完成项保留一行证据（commit + 测试数）。

## KV HTTP 服务（http-service/）
- [x] 优雅启动与关闭：SIGINT/SIGTERM 后停收新请求、排空在途、超时兜底、退出码反映结果 — 29718e5，30 用例 -race -count=2 全绿
- [x] 健康检查：/healthz 返回 200 并写明健康语义 — 29718e5，30 用例全绿
- [x] 三层结构：传输→业务→存储单向依赖，存储可替换且其他层零改动 — 29718e5，30 用例全绿
- [x] 并发安全：-race 下无竞争，锁方案有论证 — 29718e5，30 用例全绿
- [x] 超时与错误处理：四类超时齐全、错误统一映射状态码、请求体大小上限 — 29718e5，30 用例全绿
- [x] 可测试性：run(args) int 入口、各层表驱动、真信号端到端 — 29718e5，30 用例全绿
- [x] 约束：只用标准库、单目录、不做多余抽象 — 29718e5，30 用例全绿
- [x] 语句覆盖 100% 且 CI 逐包守卫 — cb6f64f，scripts/check-coverage.sh
- [x] gosec 告警归零并加 CI 门禁 — 6685ae5，0 告警

## 帧协议服务（cmd/ internal/ pkg/ examples/）
- [x] Reactor 与自定义 TCP 帧协议实现可运行 — cmd/server、examples/echo-server（活跃提交至 ebbd0cd）
- [x] 示例与用法文档 — examples/README.md、config.example.yml

## 文档
- [x] 中英 README 徽章与链接修复（img 徽章、单斜杠、zh 链接指 /zh/）— 本轮 commit，check:links 全绿
- [x] 帧协议归属口径对齐源码（实现仍在仓内，站点仅讲 KV 题）— 本轮 commit
- [x] 文档与源码一致性审计（6 处差异全修复）— 本轮 commit，docs/审计-文档一致性.md

## 代码深审 2026-11-01（P2/P3 记录，见 docs/review/2026-11-01-code-review.md）
- [x] P2：cmd/cli/client.go:57 onEvent 阻塞发送可能卡死读循环（突发推送+慢消费者 → PONG 不处理 → keepalive 误断）；建议改非阻塞 select 丢帧，参照 examples/concurrent-client/main.go:118-121 — 0a49906，全包 go build ./... && go test ./... 全绿（含新增回归测试 TestEventSinkDropsWhenFull）
- [x] P3：internal/balancer/weighted_random.go:35 全零权重时 rand.Intn(preSum[size]==0) panic；建议 Next 对 preSum[size]<=0 返回错误或回退 backends[0] — 本轮 commit，go build ./... && go test -race ./... 全绿（含 TestWeightedRandomBalancer_AllZeroWeights）
- [x] P3：internal/executor/async_executor.go:90 过期注释「Stop closes both channels」（实际仅 cancel ctx，不关闭通道）；建议改述或删除 — c01239a，internal/executor/async_executor.go:90，go test -race ./internal/executor/ + go build/vet 全绿
- [x] P3：cmd/server/main.go:20 demoProtocol sessionJanitor 永不 Close（进程生命周期无碍，测试 goleak 可滞留）；建议退出路径 defer demoProtocol.Close() — 7eea718，cmd/server/main.go:21,49 以 sync.Once 保证测试可重复关闭，go test -race ./cmd/server/ + go build/vet 全绿
- [x] P3：examples/echo-server/main.go broadcastLoop goroutine 未 join（stop 关闭后有界退出，非泄漏；测试可短暂滞留）；建议 WaitGroup join 或注明有意不 join — edf36cf，examples/echo-server/main.go:107,122 使用 sync.WaitGroup join，go test -race ./examples/echo-server/ + go build/vet 全绿

## 审核发现
- [x] 审核发现：eventSink 非阻塞丢帧后，ResilientClient 的重放游标已在 recordCursor 中推进，被丢弃的 PUBLISH 无法在重连时重放，存在断线重连丢消息风险 — a4d352d，新增 NewAcknowledgedResilientClient（pkg/proto/resilient.go:136）+ CLI 接入（cmd/cli/client.go:79,184），全仓 go build/vet/go test -race/100% 覆盖/gosec 全绿（含 TestAcknowledgedResilientClientKeepsDroppedPublishReplayable）
- [x] 审核发现：WeightedRandomBalancer.Register 不校验 Weight≤0（对比 weighted_round_robin.go:51 显式拒绝 ≤0），混合正负权重会使 preSum 非单调，破坏 sort.Search 的「前缀 false/后缀 true」前提，加权分布失真（越界已排除：守卫保证 preSum[size]>r 恒真、preSum[0]=0>r 恒假，故 index∈[1,size]）；且新增守卫 `preSum[size]<=0` 仅检查总和，注释「every backend carries a non-positive weight」与实现不符 — 906a393，internal/balancer/weighted_random.go:61,66 显式拒绝 ≤0，go build/vet/test -race + 100% 覆盖/gosec 全绿
- [x] 审核发现：全零权重回退固定打满 backends[0]，与 WeightedRoundRobin 将 Weight≤0 视为非法注册的语义不一致；若 0 权重语义为「排空/不接流量」，回退目标可能违背语义，建议确认或改返回错误 — 906a393，internal/balancer/weighted_random.go:35,41 改为返回错误，go build/vet/test -race + 100% 覆盖/gosec 全绿
- [x] 审核发现：新增测试仅覆盖全零权重场景，未覆盖「零+正混合」（0 权重后端不应被选中，如 [0,10]）与「正负混合」（preSum 非单调）场景，建议补表驱动用例 — 906a393，internal/balancer/weighted_random_test.go:73 表驱动 + :134 分布用例，go test -race ./internal/balancer/ 全绿
- [x] 审核发现：本轮复审 160ad4aa3691..HEAD（c16d1c9，逐文件审 weighted_random.go / weighted_random_test.go / todo.md）复核既有 3 条发现成立（Register 不校验 Weight≤0 致 preSum 非单调；preSum[size]<=0 仅查总和、注释「every backend carries a non-positive weight」不符实；全零/非正和回退 backends[0] 与 WeightedRoundRobin「≤0 非法」语义不一致；测试仅覆盖全零未覆盖零+正与正负混合），未见新增问题；go build ./... && go test -race ./internal/balancer/ 全绿，未改动代码 — 906a393，上述复核项已按根因修复并测试
- [x] 审核发现：实证确认（本轮 160ad4aa3691..HEAD 独立验证）——①混合正负权重 [+1,+10,-10,+5]（总和 +6>0 越过新守卫）preSum=[0,1,11,1,6] 非单调，20000 次 Next 仅选中 a/b，权重 +5 的 d 从未命中，证实 sort.Search 前提破坏、分布失真；②[+5,-10]（总和 -5≤0）触发守卫返回 backends[0]（其权重为 +5 非正？否，为正），注释「every backend carries a non-positive weight」确不符实；③[0,+10] 零+正混合下 10000 次仅选 b、0 权重 a 零命中，说明该路径行为正确但未被测试覆盖；go build ./... && go test -race ./internal/balancer/ 全绿，未改代码 — 906a393，非正权重注册拒绝、分布与退化路径均有测试
