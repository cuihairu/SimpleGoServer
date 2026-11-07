---
layout: home

hero:
  name: SimpleGoServer
  text: Design trade-offs in a Go service
  tagline: "An in-memory key-value service built only on the standard library. The real work is the lifecycle: how errors map, how timeouts are chosen, how concurrency is locked, how the process exits cleanly, and what is deliberately left out."
  image:
    src: /logo.svg
    alt: SimpleGoServer
  actions:
    - theme: brand
      text: Read the trade-offs
      link: /design
    - theme: alt
      text: Layers and glossary
      link: /architecture
    - theme: alt
      text: Jump to what is missing
      link: /design#what-is-not-here

features:
  - title: Four decision tables
    details: Lifecycle, HTTP layer, business and storage, and testability. Each row records the alternative considered and why it was rejected.
    link: /design#decision-tables
    linkText: See decisions
  - title: Four code walkthroughs
    details: The two-arm select on shutdown, status codes defined in one place, the extra byte read at the size boundary, and the five lines inside the lock.
    link: /design#code-walkthroughs
    linkText: Walk the code
  - title: What is not here
    details: Unbounded keys, a single instance, liveness-only health checks, no auth and no TLS. Each limitation explains why it is acceptable now and what production would add.
    link: /design#what-is-not-here
    linkText: Read limits
  - title: Reproducible
    details: Four source files, 100% statement coverage for the package, enforced per package in CI. Tests use real signals, raw TCP to create a deterministic stall, and goleak for leaked goroutines.
    link: /design#testing-in-practice
    linkText: See tests
---

## Architecture

The layers follow the code, one file each, dependencies pointing one way:

![SimpleGoServer layered architecture](/images/architecture-en.svg)

For the full runnable source set, see the [architecture and layers page](/architecture); its concepts are anchored in the [glossary](/glossary).

## What the service looks like

A single-binary HTTP service with an in-memory key-value API and a health endpoint. A value is defined as "exactly one JSON value": fields and types are not inspected, and bytes are stored and returned as-is.

| Request | Success | Failure |
| --- | --- | --- |
| `PUT /kv/{key}` | 201 created / 200 replaced | 400 invalid · 413 too large |
| `GET /kv/{key}` | 200, returns the original bytes | 400 invalid · 404 |
| `DELETE /kv/{key}` | 204 | 400 invalid · 404 |
| `GET /healthz` | 200 | — |

```text
main.go     wiring: flags → Service(MemoryStore) → http.Server → signal → drain
handler.go  transport: Go 1.22 pattern routing, error→status mapping (one place), JSON responses
service.go  business: key validation, value size and validity, sentinel errors (no HTTP vocabulary)
store.go    storage seam: Store interface + RWMutex in-memory implementation (no business rules)
```

Dependencies point one way: `main → handler → Service → Store`. The `Store` interface already earns its keep: the `failingStore` test double uses it to inject failures, and changing storage does not require touching the other files.

## Run it yourself

```bash
go test ./http-service/ -race -count=2
go run ./http-service -addr 127.0.0.1:8080

curl -i -XPUT 127.0.0.1:8080/kv/k -d '{"ok":true}'   # 201
curl -i 127.0.0.1:8080/kv/k                          # 200
kill -TERM <pid>                                      # in-flight requests finish, exit code 0
```

The original prompt, acceptance points, and per-item verification commands are in the [README](https://github.com/cuihairu/SimpleGoServer#readme). The implementation lives in [`http-service/`](https://github.com/cuihairu/SimpleGoServer/tree/main/http-service). The Reactor and custom framing exercise from the same interview set is its own repository, [JsonStream](https://github.com/cuihairu/jsonstream); this repository also keeps a runnable implementation under [`cmd/`](https://github.com/cuihairu/SimpleGoServer/tree/main/cmd), [`internal/`](https://github.com/cuihairu/SimpleGoServer/tree/main/internal), [`pkg/`](https://github.com/cuihairu/SimpleGoServer/tree/main/pkg) and [`examples/`](https://github.com/cuihairu/SimpleGoServer/tree/main/examples). The [English glossary](/glossary) pairs with the [Chinese glossary](/zh/glossary).
