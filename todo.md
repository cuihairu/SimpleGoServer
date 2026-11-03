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
- [x] 审核发现：NewAcknowledgedResilientClient 的游标只对「被接受」的 PUBLISH 无条件推进到其序号，若序列为「接受(1)→接受(2)→拒绝(3)→接受(4)」，游标跳到 4，而服务器仅重放 seq>cursor，被拒的 3 永远不回放——「drop 重放而非丢失」的保证仅在拒绝帧为末尾帧时成立；单 topic 单调游标无法表达稀疏接受集，需推进到连续前缀而非最新接受 — 3647ee5，pkg/proto/resilient.go:91-125 以 acknowledgePublish 维护「连续接受前缀 + 已接受跳帧集合」，全仓 go build/vet/test -race 全绿（含 TestAcknowledgedCursorKeepsContiguousPrefix）
- [x] 审核发现：TestAcknowledgedResilientClientKeepsDroppedPublishReplayable 不稳定——`alive := rc.client != nil` 在 killCurrent 后立即为真（旧 client 尚未被 swap 移除），重连等待形同空转；断言结果取决于「drain 循环先清空 2 槽缓冲」还是「后台重连先到」：若重放的 seq3 先到则被重新拒绝进满缓冲，永不达 received，3s 超时失败；实测 40 次单跑失败 14 次（约 35%），-race -count=10 亦失败 — 3647ee5，pkg/proto/resilient_test.go:176-195 断开前先清空两槽缓冲 + 以 `rc.client != dead` 等待新连接安装，-race -count=5 全绿
- [x] 审核发现：新增测试未覆盖「接受→拒绝→接受」的中段空洞场景（被拒帧夹在中间时永不被重放），仅覆盖末尾拒绝；建议补中段空洞与游标回退（重放 seq 可能使游标倒退→重复投递）用例 — 3647ee5，pkg/proto/resilient_test.go:214-249 TestAcknowledgedCursorKeepsContiguousPrefix 覆盖 1,2,4→3 中段空洞与陈旧重放不回退
- [x] 审核发现：weighted_random.go:35-42 守卫仅检查 preSum[size]<=0（总和），未校验单调性；若注册后某后端权重被改成负数且总和仍为正（如 [+5,-1] → preSum=[0,5,4]），守卫放行但 preSum 非单调，破坏 sort.Search「前缀 false/后缀 true」前提，选中后端分布失真（非 panic，越界已被 rand.Intn 上界排除）；注释声称「mutated to zero or negative」已覆盖，与实际不符，应校验每个 preSum[i]>preSum[i-1] 或逐项权重 — 3647ee5，internal/balancer/weighted_random.go:43-50 逐项校验严格递增，go build/vet/test -race 全绿
- [x] 审核发现：表驱动/分布测试仅覆盖「全零总和」与「零+正」，未覆盖「负权重单项 + 总和仍正（非单调）」的变异后场景 — 3647ee5，internal/balancer/weighted_random_test.go:164-189 TestWeightedRandomBalancer_NextRefusesMutatedNegativeWeightWithPositiveSum
- [x] 审核发现：recordCursor 已在生产代码中不再被调用（NewResilientClient 改用 decodePublish+setPublishCursor），仅被 api_surface_test.go 使用，属死代码/重复逻辑 — 3647ee5，删除 pkg/proto/resilient.go:75-88 recordCursor，api_surface_test.go 改测 setPublishCursor（含陈旧重放不回退）
- [x] 审核发现：本轮 c16d1c9b6607..HEAD 覆盖 11 文件（cmd/cli、cmd/server、examples/echo-server、internal/balancer、internal/executor、pkg/proto、todo.md），除上述 6 项外其余改动（cli 接入 acknowledged 客户端、cmd/server sync.Once Close、echo-server WaitGroup join、executor 注释更正、Register 拒绝 ≤0）未见问题 — 3647ee5，go build ./...、go vet ./...、go test ./internal/balancer ./internal/executor ./cmd/cli ./cmd/server ./examples/echo-server 全绿
- [x] 审核发现：NewAcknowledgedResilientClient 的「连续接受前缀 + acceptedGaps」游标仅推进游标本身、不用于过滤重连时服务器重放的帧——「接受1,2→拒绝3→接受4」后游标停在 2、acceptedGaps={4}，服务器按 seq>cursor 重放 3 与 4，重放的 4 在 onAck 被再次调用形成重复投递 — e457659，pkg/proto/resilient.go:87 在调用 onAck 前过滤已接受帧，pkg/proto/resilient_test.go:406 端到端回归；go build/vet/test -race、100% 覆盖、gosec 全绿
- [x] 审核发现：测试缺口——缺少「接受→拒绝→接受」重连后已接受跳帧不被重复投递的端到端用例 — e457659，pkg/proto/resilient_test.go:406 TestAcknowledgedClientDoesNotRedeliverAcceptedGapAfterReplay；go build/vet/test -race、100% 覆盖、gosec 全绿
- [x] 审核发现：weighted_random.go 新增严格递增守卫与既有总和守卫在 size==1 时被提前 return 短路，单个后端权重被变异为非正后 Next 仍返回该后端且 err==nil — f52c21a，internal/balancer/weighted_random.go:32 删除单后端短路，internal/balancer/weighted_random_test.go:131 回归；go build/vet/test -race、100% 覆盖、gosec 全绿
- [x] 审核发现：新增守卫使 Next 每次调用 O(n) 线性扫描 preSum，正常权重全正时守卫恒过、无正确性收益，后端多时属纯开销 — e457659，internal/balancer/weighted_random.go:44-49 保留递增校验以拒绝 post-Register 变异数据并换取确定性问题防御，本项按性能取舍不属实：既有测试要求拒绝变异非正/负权重，移除守卫会破坏正确性路径
- [x] 审核发现：acknowledgePublish 的「连续接受前缀 + acceptedGaps」模型假设单 topic 序列从 1 连续，但服务器 seq 是跨 topic 全局递增，多 topic 下游标卡住、接受集无限增长并重放已接受/订阅前帧 — e457659，pkg/proto/protocol.go:88,519,651 改为按 topic 独立序列并随订阅释放清理，pkg/proto/resilient_test.go:496 覆盖多 topic 交错；go build/vet/test -race、100% 覆盖、gosec 全绿
- [x] 审核发现：服务器 per-topic 发布序列在全部订阅者/会话释放后重置为 1（UNSUBSCRIBE 时 dropTopicCacheLocked 删除 topicSeqs），但 ResilientClient.Unsubscribe 不清除该 topic 的 cursors/acceptedPublishes，重新订阅后新发布 seq<=旧游标被 shouldDeliverPublish 过滤，静默丢弃前若干条 — pkg/proto/resilient.go:281 在 Unsubscribe 中删除 cursors/acceptedPublishes，pkg/proto/api_surface_test.go:62 新增 TestAcknowledgedClientUnsubscribeClearsReplayState 端到端回归；go build/vet/test -race、100% 覆盖、gosec 全绿
- [x] 审核发现：TestAcknowledgedClientDoesNotRedeliverAcceptedGapAfterReplay 重连等待沿用旧模式 `alive := rc.client != nil`（未与 killCurrent 返回的 dead client 比较），killCurrent 后旧 client 仍安装使等待立即返回，重连断言依赖后续 rejectedCalls 而非新连接安装，属此前 3647ee5 已修复的同类 flaky 模式回归 — pkg/proto/resilient_test.go:448,455 改为 `alive := rc.client != nil && rc.client != dead`；go build/vet/test -race、100% 覆盖、gosec 全绿
