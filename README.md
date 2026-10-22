[English](README.md) | [中文](README.zh.md)

# SimpleGoServer

<p align="center">[![Status](https://img.shields.io/badge/status-in%20development-orange)](https://github.com/cuihairu/SimpleGoServer/releases)</p>

A reference implementation for a backend interview question: **build a simple Go service**. It exposes an in-memory key-value API (`PUT/GET/DELETE /kv/{key}`, the value being a single JSON value) plus a `GET /healthz` health-check endpoint, ships as a single binary, and uses only the standard library. The feature set is deliberately minimal: the question does not measure how much the service does, but the engineering shape of a service that **lives gracefully and dies cleanly**.

Another question from the same interview set (a high-performance non-blocking network communication model plus a custom TCP framing protocol) has since become an independent project, [JsonStream](https://github.com/cuihairu/jsonstream); the problem statement, reference implementation, and protocol documentation all live there.

**Online documentation site**: <https://cuihairu.github.io/SimpleGoServer/> — design trade-offs, code walkthroughs, the things deliberately left out, and commands to reproduce the results.

---

## 1. Interview Question Requirements

The problem statement in one sentence: using only the Go standard library, build a single-binary HTTP service — an in-memory key-value API plus `GET /healthz`. The requirements all come down to seven acceptance criteria:

1. **Graceful startup and shutdown**: after receiving SIGINT/SIGTERM the process stops accepting new requests, waits for in-flight requests to finish (with a timeout as the backstop), and then exits; the exit code reflects whether the shutdown was clean.
2. **Health check**: `/healthz` returns 200 together with the service's status; state clearly what you consider "healthy" to mean.
3. **Layered structure**: three layers — transport (HTTP) → business rules → storage — with one-way dependencies; the storage can be replaced with zero changes to the other layers.
4. **Concurrency safety**: the in-memory store is read and written concurrently, with no data races under `-race`; explain the reasoning behind your choice of locking scheme.
5. **Timeouts and error handling**: the server sets every relevant timeout; errors map uniformly to status codes and internal error details never leak out; the request body has a size cap.
6. **Testability**: the entry point is a function (flags in, exit code out); every layer is independently testable (table-driven + httptest); there is an end-to-end graceful-shutdown test driven by a real signal.
7. **Constraints**: standard library only; multiple files in a single directory are acceptable; no gratuitous abstraction.

---

## 2. Design and the Corresponding Knowledge Points

**1. Graceful startup and shutdown — SIGTERM is a promise, not a death sentence.**
Deployment systems (systemd, Kubernetes) send SIGTERM during a rolling restart and wait for the process to exit; only after a timeout do they force-kill it. So the right way to read the signal is as a notification: `signal.NotifyContext` turns SIGINT/SIGTERM into ctx cancellation, and the main flow selects between "the signal arrived" and "ListenAndServe returned an error". On signal it calls `srv.Shutdown` (stop accepting → drain in-flight requests → close keep-alive), with a timeout backstop so that a single stuck handler cannot take the whole process down. Exit codes deserve equal care: 0 for a clean exit, 1 for a timeout or other failure — the orchestrator uses that code to judge whether the rolling restart went smoothly.

**What it tests**: whether you know that net/http's default behavior on a signal is immediate termination (in-flight requests are cut off); the difference between `Shutdown` and `Close` (drain vs. cut immediately); the semantics of exit codes.

- Pass: the chain signal → Shutdown → timeout backstop is complete and the exit code is correct.
- Bonus: distinguishing "stop accepting" from "drain in-flight"; explaining the trade-off of force-killing after the Shutdown timeout; mentioning how Kubernetes' `terminationGracePeriodSeconds` interacts with preStop hooks (the hook only orchestrates the sequence; graceful shutdown inside the process remains mandatory).

**2. Health check — answer "what is healthy?" before writing the endpoint.**
This service has no external dependencies, so "able to answer requests" is what "healthy" means, and `/healthz` always returns 200. Once a remote store is attached, the definition changes: the process being alive is not the same as the service being usable. Kubernetes splits these two notions into liveness (restart when dead) and readiness (route no traffic until ready) — merging them into a single probe is a common source of incidents. The check itself must be cheap: there is no shortage of cases where a flood of health checks dragged the whole service down.

**What it tests**: depth of thinking about health semantics (the liveness/readiness split); awareness of check cost.

- Pass: an endpoint exists and its semantics are clear.
- Bonus: discussing the liveness/readiness split; articulating that "the check must be cheap" and the avalanche risk of deep checks (pinging every dependency).

**3. Layered structure — the interface is a seam, not a ceremony.**
Three layers with one-way dependencies: `handler` (HTTP semantics: status codes, error JSON) → `Service` (business rules: key validity, size cap, JSON validation, expressed as sentinel errors) → the `Store` interface (the storage seam). Some will ask, "an interface for a single implementation?" Yes — because this interface doubles as a test seam (the `failingStore` stub injects faults) and a replacement seam (swap in Redis/Postgres without touching the other layers). `ctx` is threaded through the entire chain: the in-memory store has no use for it today, but a remote store will need the channel for timeouts and cancellation tomorrow, so the signatures reserve it now. The error → status-code mapping is concentrated in a single place, `writeError`, so the response contract never gets scattered.

**What it tests**: the real payoff of layering (swap an implementation without touching the rest; fault injection is testable); sentinel errors vs. string matching; the point of threading ctx.

- Pass: clean layer boundaries, centralized error mapping, one-way dependencies.
- Bonus: arguing why the interface exists (a seam, not a ceremony); threading ctx; demonstrating fault-injection tests with a stub store.

**4. Concurrency safety — a concurrent write to a bare map crashes the process outright.**
Go maps are not concurrency-safe. Unprotected concurrent reads and writes buy you anything from a race report to `fatal error: concurrent map read and map write`, which kills the process outright — not a race-detector report but an unrecoverable runtime error. Comparing the options: `RWMutex + map` is the plainest and fastest choice when reads and writes are balanced; `sync.Map` is optimized for read-heavy workloads with a disjoint key set, and in this balanced workload it is actually slower; a channel-based actor turns single-key lookups into queue round-trips, pure overhead. There is also a structural reason to keep the lock in the Store layer: when the store becomes remote, the lock disappears naturally (the remote protocol serializes access anyway), and the business layer never knows a lock existed. One more detail is ownership: the `Value` handed to `Put` is immutable once surrendered, so no defensive copy is needed inside the lock.

**What it tests**: the consequences of concurrent writes to a bare map; which workloads fit RWMutex, sync.Map, and the channel-based approach; whether `-race` is part of the routine.

- Pass: mutex protection plus a clean `-race` run.
- Bonus: a grounded comparison of the options; a clear account of ownership semantics; pointing out that a single, non-nested lock has no deadlock surface.

**5. Timeouts and error handling — net/http's default zero timeouts are a hazard, not a feature.**
With default settings, a client that opens a socket and then sends nothing can pin a goroutine forever (the prototype of a slowloris attack). The four timeouts cover progressively larger scopes: `ReadHeaderTimeout` (time to read the request headers), `ReadTimeout` (time to read the entire request), `WriteTimeout` (time to finish writing the response), `IdleTimeout` (keep-alive idle time). Set only the read timeouts and a response that stalls halfway through writing has no one watching it. The request body is read through an `io.LimitReader` capped at `maxValue+1` bytes: the extra byte distinguishes "exactly full" from "over the limit"; anything over the limit gets an immediate 413, and unbounded input never enters memory. On the error side, sentinel errors map to status codes through `errors.Is`; the 500 body is a fixed message, and internal error details (driver errors, addresses, stacks) go to the logs, never into the response — error text is for the operator, not the caller.

**What it tests**: which span each of the four timeouts covers and what breaks when one is missing; the LimitReader +1 trick; awareness of tiering error information.

- Pass: the four timeouts plus a size cap plus a uniform error mapping.
- Bonus: explaining each timeout's scope one by one; tiering error information (what the client sees vs. what the logs record); mentioning alternatives such as `http.MaxBytesReader`.

**6. Testability — testability is designed in, not tested in.**
Write the whole flow inside `main()` and it cannot be tested (go test will not call main for you), so the entry point is factored into `run(args []string) int` — flags in, exit code out, with `main` doing nothing but translate the result into `os.Exit`. The entire "listen → serve → catch signal → drain" lifecycle then runs inside `go test`. Each layer is tested on its own: the handler is exercised directly with `httptest.NewRecorder` (no port, microsecond-scale), the business layer runs its validation matrix as table-driven tests, and the storage layer is fed concurrent traffic under `-race`. The final end-to-end test uses a real port and a real signal (`syscall.Kill` sent to the process itself, by which point the signal handler is guaranteed to be installed), asserts the exit code and that "new connections are refused after shutdown", then uses goleak to verify that no goroutines are left behind — it tests what actually happens at deployment time.

**What it tests**: a function-based entry point and seam design; the trade-offs between httptest's two modes (direct Recorder testing vs. `NewServer` over the network); awareness of leak detection.

- Pass: every layer independently testable, table-driven.
- Bonus: a real-signal end-to-end test; a `-race` concurrency testbed; goleak as the final safety net.

**7. Constraints (standard library only) — restraint itself is part of the test.**
No routing framework; Go 1.22's pattern syntax is used instead: `mux.HandleFunc("GET /kv/{key}", ...)`. On a method mismatch the mux itself answers 405 (with an Allow header), and `r.PathValue("key")` reads the path parameter — this demands knowledge of the standard library's coverage that is current enough. Restraint is not dogma: only when routes reach into the hundreds or a middleware ecosystem is needed does the payoff of a framework like chi or echo outweigh its cost, and being able to name that threshold is rarer than knowing how to use a framework. Four source files in a single directory, paired with four test files, one file per layer — the structure is the documentation.

**What it tests**: familiarity with the standard library (especially the new 1.22 router); judgment on when to abstract.

- Pass: all acceptance criteria met with zero third-party dependencies.
- Bonus: naming the threshold at which a framework pays off; knowing the 1.22 mux method matching and its 405/Allow behavior.

---

## 3. Implementation and Test Results

Where the implementation lives — 4 source files plus 4 test files in a single directory, one file per layer, zero third-party dependencies:

```text
http-service/
  main.go          entry point: flag parsing, wiring, signals, graceful shutdown (run(args) int is testable)
  handler.go       HTTP layer: 1.22 pattern routing, error → status-code mapping, JSON responses
  service.go       business layer: key/value validation, sentinel errors, LimitReader size cap
  store.go         storage layer: Store interface + RWMutex in-memory implementation
  *_test.go        per-layer table-driven tests + -race concurrency testbed + real-signal end-to-end + goleak
```

Test results (re-run on 2026-09-29): `go test ./http-service/ -race -count=2` is fully green (re-verified green under a 2-core constraint with `taskset -c 0,1`); a demo smoke test verified each status code in turn: `PUT` 201 on first write / 200 on overwrite, `GET` 200, invalid JSON 400, bad key 400, over-limit 413, method mismatch 405 (with Allow), unknown key 404, `DELETE` 204; after SIGTERM the log prints shutdown complete, the exit code is 0, and the port refuses new connections.

Verification commands (each line is copy-pasteable):

```bash
# All tests: per-layer table-driven + race detection + goroutine leak detection; -count=2 guards against leftover state
go test ./http-service/ -race -count=2

# Demo: start the service in terminal 1 (-addr defaults to 127.0.0.1:8080)
go run ./http-service

# Terminal 2: health check → write → read → delete → graceful-shutdown verification
curl -s localhost:8080/healthz
curl -si -X PUT --data '{"n":42}' localhost:8080/kv/answer   # 201 Created
curl -s localhost:8080/kv/answer                             # {"n":42}
curl -si -X POST --data '1' localhost:8080/kv/answer         # 405 (Allow: DELETE, GET, HEAD, PUT; the GET pattern implies HEAD)
curl -si -X PUT --data 'not json' localhost:8080/kv/k        # 400
curl -si -X DELETE localhost:8080/kv/answer                  # 204 No Content
curl -si localhost:8080/kv/answer                            # 404

# Back in terminal 1, press Ctrl+C (or kill -TERM): the log prints "shutdown complete", exit code 0
```

Further reading: the [online documentation site](https://cuihairu.github.io/SimpleGoServer/) — four sets of decision tables, code walkthroughs, and the "what we did not do" list; the complete breakdown of the problem statement is also on the site's [Design Trade-offs](https://cuihairu.github.io/SimpleGoServer/design) page.
