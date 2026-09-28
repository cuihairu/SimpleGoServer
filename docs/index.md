---
layout: home

hero:
  name: SimpleGoServer
  text: 两道后端面试题的设计决策档案
  tagline: 一「重」一「轻」两道题：主从 Reactor + 自定义 TCP 帧协议，与标准库 only 的 HTTP 服务。本站主线是「为什么这样设计」——每个关键选择都列出备选方案、放弃理由，以及坦白的局限清单。参考实现全部可运行、可复现，CI 门禁在每次推送上验证其中的承诺。
  image:
    src: /logo.svg
    alt: SimpleGoServer
  actions:
    - theme: brand
      text: 题一 · 为什么这样设计
      link: /q1-design
    - theme: alt
      text: 题二 · 为什么这样设计
      link: /q2-design
    - theme: alt
      text: 架构与取舍（深读）
      link: /DESIGN

features:
  - icon: 🧭
    title: 题一：Reactor 与自定义协议
    details: 并发模型、协议格式、错误处理、资源防线、扩展点——五组设计决策，每组一张「备选与放弃理由」表，配关键代码走读与坦白局限清单。
    link: /q1-design
    linkText: 看决策
  - icon: 🎯
    title: 题二：http-service
    details: 「活得体面、死得干净」的工程形态：优雅关闭、四超时、三层单向依赖、锁选型、可测试性——每个选择都有为什么。
    link: /q2-design
    linkText: 看决策
  - icon: 🏗
    title: 架构与取舍（DESIGN）
    details: 49 条决策汇总表、连接生命周期控制流走读、goroutine 与锁的完整清单——设计文档的深读层。
    link: /DESIGN
    linkText: 深入架构
  - icon: 📡
    title: 自定义协议说明（Proto）
    details: 帧格式逐字段、五种交互模式（请求/响应、发布/订阅、流式、心跳、会话重连补发）、错误处理契约——线上字节格式的单一事实源。
    link: /Proto
    linkText: 读协议
  - icon: 📊
    title: 性能基准（Benchmark）
    details: 帧编解码、端到端吞吐/并发扩展/连接churn、池化 A/B、热点优化实录，以及噪声底的量法——所有性能声明都有可复现的数据。
    link: /Benchmark
    linkText: 看数据
  - icon: 🧠
    title: 知识点梳理（NOTES）
    details: 按面试考点组织：TCP 成帧、背压、零拷贝、GMP、channel、锁选型、GC 分配经济学、覆盖率门禁、flake 治理——每条先原理后实现。
    link: /NOTES
    linkText: 复习考点
---

## 两题一套方法论

两道题规模悬殊，设计方法论是同一套。面试时可以先立这张框架再分述：

| 维度 | 题一：Reactor + 自定义协议 | 题二：http-service |
| --- | --- | --- |
| 分层 | 传输(reactor) / 成帧(codec) / 语义(protocol) / 业务(handler) | 传输(handler) / 业务(service) / 存储(store) |
| 并发单位 | 每连接一个 goroutine，worker 分发 | net/http 内建每连接一 goroutine |
| 生命周期 | accept → dispatch → serve → 分级关闭 | listen → serve → 信号 → drain |
| 错误策略 | 三分类（网络/协议/应用）+ 分级关闭 | 哨兵错误 → 统一状态码映射 |
| 资源防线 | MaxFrameSize/MaxStreamSize/缓存预算/空闲回收 | 四超时/请求体上限/优雅关闭超时 |

核心原则一句话：**越靠近传输的错误越果断（断连），越靠近应用的错误越宽容（回错误响应）；所有无界的东西（队列、缓存、输入、等待）都必须有界**。

## 复现验证

```bash
# 构建、静态检查、格式检查
go build ./... && go vet ./... && gofmt -l .

# 全量测试：单元 + 端到端集成 + 竞态检测 + goroutine 泄漏检测
go test ./... -race

# 性能基准：帧编解码 + 真实 TCP 端到端
go test ./pkg/proto ./pkg/reactor -bench . -benchmem

# 题二：单二进制 HTTP 服务
go test ./http-service/ -race -count=2
go run ./http-service
```

仓库全貌（题面原文、逐条验证命令、实现结构）见 [README](https://github.com/cuihairu/SimpleGoServer#readme)；五份深入文档（DESIGN / Proto / Analysis / Benchmark / NOTES）就住在 [docs/](https://github.com/cuihairu/SimpleGoServer/tree/main/docs) 里，本站即其在线版加两篇设计决策主线。
