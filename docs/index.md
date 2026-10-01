---
layout: home

hero:
  name: SimpleGoServer
  text: 一个 Go 服务的设计取舍
  tagline: 标准库写的内存键值服务，功能只有三个动词。真正花心思的是生命周期：错误怎么映射、超时怎么设、并发怎么锁、进程怎么死干净，以及哪些事明确没做、为什么。每个选择都留下备选和放弃的理由。
  image:
    src: /logo.svg
    alt: SimpleGoServer
  actions:
    - theme: brand
      text: 读设计取舍
      link: /design
    - theme: alt
      text: 跳到没做的事
      link: /design#没做的事

features:
  - title: 四组决策表
    details: 生命周期、HTTP 层、业务与存储、可测试性。每行给出当时的备选方案，以及为什么没选它——超时矩阵、并发三方案、错误映射的边界都在这里。
    link: /design#决策表
    linkText: 看决策
  - title: 四段代码走查
    details: 关闭路径的两臂 select、状态码只在一处出现、多读一个字节的边界处理、锁内不拷贝的五行。顺带写清每处为什么容易被改坏。
    link: /design#代码走查
    linkText: 走代码
  - title: 没做的事
    details: 键集无界、单实例、健康检查只有 liveness、无认证无 TLS。每条写清为什么现在不做，以及生产环境该怎么补。
    link: /design#没做的事
    linkText: 看局限
  - title: 可复现
    details: 四个源文件，单包语句覆盖率 100%，由 CI 逐包门禁维持。测试手段包括真信号打给自己、裸 TCP 构造确定卡死、goleak 抓残留 goroutine。
    link: /design#测试怎么落地
    linkText: 看测试
---

## 服务长什么样

单二进制 HTTP 服务，内存键值 API 加一个健康检查端点。值定义为「恰好一个 JSON 值」：不校验字段，不校验类型，字节原样存原样取。

| 请求 | 成功 | 失败 |
| --- | --- | --- |
| `PUT /kv/{key}` | 201 新建 / 200 覆盖 | 400 非法 · 413 超限 |
| `GET /kv/{key}` | 200，回显当初的字节 | 400 非法 · 404 |
| `DELETE /kv/{key}` | 204 | 400 非法 · 404 |
| `GET /healthz` | 200 | — |

```text
main.go     装配：flag → Service(MemoryStore) → http.Server → 信号 → 排空
handler.go  传输层：1.22 模式路由、错误→状态码映射（唯一一处）、写 JSON 响应
service.go  业务层：key 校验、值的体积与合法性、哨兵错误（不认识 HTTP 词汇）
store.go    存储缝：Store 接口 + RWMutex 内存实现（不认识业务规则）
```

依赖单向 `main → handler → Service → Store`。`Store` 是接口这件事已经在兑现：测试里的 `failingStore` 桩靠它注入故障，换存储时其他文件不用改。

## 自己跑一遍

```bash
go test ./http-service/ -race -count=2
go run ./http-service -addr 127.0.0.1:8080

curl -i -XPUT 127.0.0.1:8080/kv/k -d '{"ok":true}'   # 201
curl -i 127.0.0.1:8080/kv/k                          # 200
kill -TERM <pid>                                      # 在途请求走完，退出码 0
```

题面原文、验收点与逐条验证命令见 [README](https://github.com/cuihairu/SimpleGoServer#readme)，实现代码在 [`http-service/`](https://github.com/cuihairu/SimpleGoServer/tree/main/http-service)。同一批面试题的 Reactor 与自定义帧协议那题已独立成 [JsonStream](https://github.com/cuihairu/jsonstream)。
