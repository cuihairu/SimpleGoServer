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
