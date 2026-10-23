# http-service: design trade-offs

The reference implementation is in [`http-service/`](https://github.com/cuihairu/SimpleGoServer/tree/main/http-service) and consists of four source files. The exercise is not about feature count; it is about the shape of a service lifecycle: how it starts, how it dies cleanly, what it says when something fails, and whether it can be tested. Every key choice below records the alternative considered and why it was rejected.

## Service boundary

A single-binary HTTP service with an in-memory key-value API and a health endpoint. A value is "exactly one JSON value": fields and types are not inspected, and bytes are stored and returned as-is.

| Request | Success | Failure |
| --- | --- | --- |
| `PUT /kv/{key}` | 201 created / 200 replaced, body `{"status":"stored"}` | 400 invalid key or value; 413 above `-max-bytes` |
| `GET /kv/{key}` | 200, body is the original bytes | 400; 404 |
| `DELETE /kv/{key}` | 204, no body | 400; 404 |
| `GET /healthz` | 200, `{"status":"ok"}` | none |

The boundary comes from the prompt, but four assumptions must be explicit or the trade-offs have no basis:

- **One instance, one process, in-memory storage.** Losing data on restart is part of the prompt. `Store` is an interface and `ctx` flows through every layer, so a remote implementation changes no other file (see what production would add in [What is not here](#what-is-not-here)).
- **Standard library only.** One of the seven acceptance points says so. Four routes and one store leave no room for a framework.
- **Deployed on a trusted network or behind a reverse proxy.** No auth and no TLS follow directly from that boundary.
- **Callers are trusted but fallible.** Input is validated (key character set, valid JSON value, size limit), but the service is not designed for malicious attackers: no rate limiting, quotas, or audit log.

## Layers and dependency direction

```text
main.go     wiring: flags → Service(MemoryStore) → http.Server → signal → drain
handler.go  transport: Go 1.22 pattern routing, error→status mapping (one place), JSON responses
service.go  business: key validation, value size and validity, sentinel errors (no HTTP vocabulary)
store.go    storage seam: Store interface + RWMutex in-memory implementation (no business rules)
```

Dependencies run one way: `main → handler → Service → Store`, with no reverse references. Each layer knows only the next layer's abstraction: the handler receives sentinel errors rather than a status-code enum, the Service receives a `Store` interface rather than `*MemoryStore`, and the store only deals in bytes.

That division pays off twice. First, the response contract exists in one place: adding an error means adding a sentinel in `service.go` and a case in `handler.go`'s `writeError`; route functions never mention status codes. Second, storage can be replaced without touching other layers. This is not speculative abstraction for the future: the `failingStore` test double uses it today to inject failures.

## Decision tables

### Lifecycle

| Decision | Alternative | Why this choice |
| --- | --- | --- |
| A function entry point `run(args []string) int`, with `main` only translating to `os.Exit` | Put the logic in `main` | `go test` will not call `main` for you. Once it is a function, listening, signal handling, draining, and exit codes can all run in tests, and the exit code is directly assertable. |
| `signal.NotifyContext` turns SIGINT/SIGTERM into context cancellation and consumes the signal | Native `signal.Notify` with a channel | Context cancellation can be selected on and propagated downstream. Consuming the signal prevents other components from installing their own handlers and reacting twice. |
| Buffered `errCh` of size 1 | Unbuffered | The `ListenAndServe` error must be deliverable even if the main flow has entered shutdown. Unbuffered, the error is either lost or the goroutine blocks on send. |
| Return exit code 1 after a shutdown timeout | Wait forever | The orchestrator uses the exit code to tell whether a rolling restart was clean. Waiting forever ends in SIGKILL, which is harder to diagnose than a failed restart. |
| Three exit-code states: flag error 2, startup failure or shutdown timeout 1, clean shutdown 0 | Everything 0 / everything 1 | A flag error is a usage problem; startup failure and shutdown timeout are runtime problems; a clean shutdown has no story. Collapsing them loses the distinction. |

### HTTP layer

| Decision | Alternative | Why this choice |
| --- | --- | --- |
| Go 1.22 pattern routing such as `"PUT /kv/{key}"` | Hand-written method switch / a router framework | `ServeMux` provides 405 and `Allow` on method mismatch and `r.PathValue` for parameters. Four routes cannot justify a dependency, and zero dependencies is the exercise constraint. |
| Set all four timeouts with increasing scopes (5s / 10s / 10s / 60s) | Read timeout only / none | `net/http` defaults to zero timeouts, which is a hazard rather than a feature. A client that opens a socket and sends nothing can park a goroutine forever. See the timeout matrix below. |
| `io.LimitReader(body, max+1)` | `http.MaxBytesReader` / no limit | Reading one extra byte makes "exactly at the limit" and "over the limit" distinguishable, which keeps error classification clean. No limit lets one request exhaust memory. `MaxBytesReader` works too, but it writes response headers before reporting the error, so the oversized request already owns the connection. |
| Validate values with `json.Valid` | `Unmarshal` into `any` / no validation | `json.Valid` allocates no destination and checks for exactly one value, so concatenated values such as `1 2` are rejected. No validation stores bad bytes that must be reinterpreted on every read. |
| Check size before JSON validity | Check `json.Valid` first | An oversized body does not deserve parsing cost. Reversing the order makes the service scan a 1 GiB junk body first. |
| Validate keys byte by byte: length 1–128, each byte in `'!'`–`'~'` | `strings.ContainsFunc` with Unicode categories / delegate to callers | Byte-wise validation allocates nothing and avoids the Unicode slow path. The range is the precise expression of "printable, no spaces"; a comment documents what the range means in ASCII. |

**Timeout matrix.** Each timeout owns an interval; missing one leaves a class of stalls uncovered:

| Timeout | Scope | Consequence if missing |
| --- | --- | --- |
| `ReadHeaderTimeout` 5s | Connection through request headers | A client opens a socket and sends nothing; the goroutine parks forever (the slowloris prototype). |
| `ReadTimeout` 10s | Connection through request body | A slow-drip body can likewise starve a connection. |
| `WriteTimeout` 10s | Through response completion | With reads but no writes, a half-written response can hang with nobody watching. |
| `IdleTimeout` 60s | Between keep-alive requests | Idle connections hold file descriptors indefinitely. |
| `-shutdown-timeout` 10s | Draining in-flight requests | One stuck handler holds the process until the orchestrator force-kills it. |

`IdleTimeout` being clearly larger than `ReadTimeout` is intentional. Idle is a normal state; the read timeout guards against "started but never finished."

### Business and storage

| Decision | Alternative | Why this choice |
| --- | --- | --- |
| Sentinel errors mapped with `errors.Is`, with status codes appearing only in `writeError` | String matching / each route writing its own status | Sentinels compose and support `errors.Is`/`As`, and HTTP vocabulary stays out of the business layer. String matching silently breaks when an error message changes by one character. |
| 500 responses are always `{"error":"internal error"}`; the real cause goes to logs | Return `err.Error()` verbatim | Driver errors, internal addresses, and stack fragments are for operators, not callers. Redaction is cheap; leaking is not. |
| `MemoryStore` uses `RWMutex + map` | `sync.Map` / a channel-based actor | See the three-way comparison below. |
| Keep the lock inside the store | Move it into the Service or handler | The lock follows the implementation. With remote storage the lock disappears naturally because the protocol serializes access; the business layer never knows a lock exists. |
| All three `Store` methods take `ctx`; the in-memory implementation uses `_` | Pass `ctx` only where needed / omit it | The in-memory implementation has no cancellation semantics today, but the signatures are ready. On the day remote storage arrives, the handler and Service do not change; the new implementation adds timeouts and retries. |
| `Value = json.RawMessage`; after validation the value is never re-encoded | `Unmarshal` into a struct and `Marshal` back out | Double encoding has bitten another service in this repository: a 1 MiB call round trip took 42 ms until it passed bytes through unchanged. There is no reason to repeat that mistake here. |
| `Put` makes no defensive copy inside the lock | Copy into the map | Ownership is "immutable after handoff"; the caller already passes an independent slice. Copying under the lock only moves an O(n) copy into the critical section. |
| `Put` returns `created` to choose 201 / 200 | Always 200 | One line buys correct REST semantics and lets callers distinguish creation from replacement. |
| `DELETE` on a missing key returns 404 rather than 204 | Always 204 (idempotent) | This API lets the caller discover that a key did not exist. If it were placed behind retry logic, idempotent 204 would be better. The deciding factor is whether callers retry, not REST dogma. |
| `/healthz` always returns 200 and is as cheap as possible | Deep-check every dependency | With no external dependencies, "can answer a request" is the complete definition of healthy. Deep checks turn the probe into part of the avalanche when dependencies wobble. |
| Log through `slog`'s JSON handler to stderr | `fmt` / text logs | Structured logs are directly ingestible, and stderr separates service output from diagnostics. |

**Three concurrency-safe designs.** Concurrent writes to a bare map are a runtime `fatal error`: the process dies without even a race-detector report, so one design must be chosen explicitly:

| Design | Fits | Verdict for this exercise |
| --- | --- | --- |
| `RWMutex + map` | Balanced reads and writes, or writes are not rare | Adopted. The plain implementation is fastest, and a single non-nested lock has no deadlock surface. |
| `sync.Map` | Read-heavy, sparse key set, keys only added | Rejected. A KV service reads and writes at comparable rates; maintaining two maps is pure overhead here. |
| Channel-based actor | Complex transactions need serialization | Rejected. Point lookups would pay queueing overhead for transactional semantics that do not exist. |

### Testability

| Decision | Alternative | Why this choice |
| --- | --- | --- |
| Test each layer separately: handlers with `httptest.NewRecorder`, Service with table-driven tests, store under concurrency | End-to-end tests only | `Recorder` uses no port and runs in microseconds. A layered failure points directly at the broken layer instead of "the request returned 500, go find out why." |
| Test the lifecycle with real signals: `syscall.Kill` to self and assert the exit code | Mock the signal channel | This tests what actually happens in deployment. `run` installs the handler in the test process, so `Kill` follows the production path. Mocking signals tests the mock. |
| Call `goleak.VerifyNone` at the end of every lifecycle test | Assert only the exit code | Exit code 0 does not prove no background goroutines remain. A handler still hanging after a `Shutdown` timeout is visible only to leak detection. |
| Make `osExit` a package variable and replace it in tests | Testing `run` is enough | `main` does one thing (translate the exit code into `os.Exit`), so that line gets a test too. The cost is one replaceable global; the benefit is that the translation step is not uncovered code. |

## Code walkthroughs

### The two-arm select on shutdown

`http-service/main.go`:

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

`ListenAndServe` always returns a non-nil error, but `http.ErrServerClosed` can only come from `Shutdown`, and the only `Shutdown` call is in the context arm. Therefore reaching the `errCh` arm always means binding the port failed, so exit code 1 is accurate and the error value needs no second check. That reasoning is recorded in a source comment so readers do not have to reconstruct it.

`Shutdown` does three things: stop accepting new connections, wait for in-flight handlers, and close keep-alive connections. The outer `WithTimeout` turns "one handler is stuck" into "the deadline expired" instead of "the process cannot be killed." That timeout is the bounded guarantee for draining, not an option.

`defer stop()` is not optional: it removes the signal handler registered by `NotifyContext` when `run` returns. Without it, a test process that runs several cases keeps swallowing its own SIGTERM.

### Status codes in exactly one place

`http-service/handler.go`:

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

Each sentinel maps to exactly one status code. `ErrKeyInvalid` and `ErrValueInvalid` share 400 intentionally: the caller's remedy is the same in both cases, change the request. The 500 branch replaces details with a fixed message in the response and sends the real error to the logger. Different content for callers and operators is design, not omission.

The write order is "choose the status, write headers, write the body." Once headers are out they cannot change, so `writeJSON` sets Content-Type, calls `WriteHeader`, then encodes, with no room to reconsider.

### One byte over the boundary

`http-service/service.go`:

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

Reading only `max` bytes makes a valid value exactly 1 MiB and a 10 GiB body look the same length, so the service cannot answer 200 versus 413. Reading `max+1` bytes makes `len(raw) > max` a clean overflow signal; the extra byte is the cost of distinguishing them, not waste.

`json.Valid` is followed immediately by `Value(raw)`, with no `Unmarshal`/`Marshal` in between. `LimitReader` copies the incoming bytes into the independent `raw` slice, ownership transfers to the store, and the store never mutates it. The whole path performs zero encodes.

### Five lines inside the lock

`http-service/store.go`:

```go
func (m *MemoryStore) Put(_ context.Context, key string, v Value) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.items[key]
	m.items[key] = v
	return !exists, nil
}
```

The critical section contains only the map lookup and assignment. A defensive copy would turn an O(n) allocation into part of the locked region for no safety gain, because the ownership contract is that the caller hands over an independent immutable slice. The boolean return is computed before unlocking and carries the 201/200 distinction out without a second lookup.

## Testing in practice

The tests do not just assert "it works"; they cover three specific claims: each layer's error mapping, a real-signal graceful shutdown, and leak-free completion. The concrete techniques are:

| Layer | Technique | Scenarios covered |
| --- | --- | --- |
| Service | Table-driven, no I/O | Key character-set and length boundaries; non-JSON values, concatenated values such as `1 2`, oversized values; read failures, deletes of missing keys, and write errors injected by `failingStore`. |
| handler | `httptest.NewRecorder` directly | Success responses and status codes for all three routes; mapping from every sentinel to a status; internal error text never appears in the response. |
| store | Concurrent reads and writes under `-race` | Multiple goroutines touching the same map, which catches "forgot to lock" immediately under `go test -race`. |
| lifecycle | `run()` + real signals + `goleak` | Occupied port returns 1; bad flags return 2; `main` translates the exit code to `os.Exit`; shutdown timeout returns 1; full lifecycle of listen, serve a real request, real SIGTERM, drain, exit 0, then refuse new connections. |

The shutdown-timeout case deserves a note. It opens a raw TCP connection, writes half a body, uses `Expect: 100-continue` so the handler deterministically blocks in `ReadAll`, then sends SIGTERM. That stall is constructed, not gambled on with sleeps. The assertion is exit code 1: draining did not finish and the process reports that honestly.

The `http-service` package has 100% statement coverage, but the number is not the goal. Delete a test or add an error branch and `go test` still passes; the 100% line in the README would become an unverified slogan. What maintains it is [`scripts/check-coverage.sh`](https://github.com/cuihairu/SimpleGoServer/blob/main/scripts/check-coverage.sh): CI checks coverage per package on every push and fails below 100%. The unit is the package, not the repository total, because a total lets large packages hide small ones.

## What is not here

Each entry states why it is out of scope now and what production would add.

- **Data lives only in memory and is lost on restart.** Given by the prompt. The fix is a `Store` implementation (Redis, Postgres, or on-disk BoltDB) plus timeouts and retries for the remote implementation. The interface seam is ready, but a `ctx` parameter in the signature does not mean cancellation is implemented.
- **The key set is unbounded.** A single value is capped at 1 MiB (`-max-bytes` adjustable), but the number of map entries and the write rate have no quota. Sustained writes can grow memory until the OOM killer ends the process. Production needs an entry cap, TTL eviction, or LRU; the prompt asked for three verbs, and restraint is part of the exercise.
- **One instance, no horizontal scaling.** No replicas, no cross-replica consistency. To add instances under KV semantics, externalize storage first and only then discuss stateless scaling.
- **`/healthz` has only liveness semantics.** Always returning 200 only proves the process can answer. Once external dependencies exist, split it into two probes: liveness asks "should I restart this?" and readiness asks "should traffic reach this?" Combining them is a common source of incidents.
- **No auth, no TLS.** Add middleware before the handler or delegate to a reverse proxy. The three-layer structure does not need to change.
- **`json.Valid` is not schema validation.** The service neither knows nor should know the structure inside a value: this is a byte KV store, not a document database. Real field validation belongs in a Service-layer schema and is a different order of requirement.
- **No TTL, transactions, or listing endpoint.** The prompt requires only `PUT`/`GET`/`DELETE`. Listing is the first dangerous request: it naturally needs pagination, cursors, and ordering, and the unbounded key set starts to hurt there.
- **A single `RWMutex` is the scaling ceiling.** Both reads and writes take the same lock. At higher QPS the bottleneck is obvious. Production would shard the map by key hash or change the storage structure, depending on the real read/write ratio.
- **The exit-code contract assumes the orchestrator observes it.** systemd and Kubernetes use it to judge a rolling restart; a bare local run sees only a number. That is an operational assumption, not a defect, but deployment configuration must honor it.

## Reproduce

```bash
# Build, static checks, formatting
go build ./... && go vet ./... && gofmt -l .

# This service: unit + end-to-end + race + leak detection
go test ./http-service/ -race -count=2

# Run it
go run ./http-service -addr 127.0.0.1:8080
curl -i -XPUT 127.0.0.1:8080/kv/k -d '{"ok":true}'   # 201
curl -i 127.0.0.1:8080/kv/k                          # 200, original bytes
curl -i 127.0.0.1:8080/healthz                       # 200
```

To watch graceful shutdown, send requests from another terminal and `kill -TERM` the process: in-flight requests finish and the process exits 0.

## Where did the first exercise go?

The other exercise from the same interview set (a master/worker Reactor plus a custom TCP framing protocol) is its own repository: [JsonStream](https://github.com/cuihairu/jsonstream). Its [architecture and trade-off document](https://github.com/cuihairu/jsonstream/blob/main/docs/DESIGN.md) records those decisions. The remaining implementation in this repository still runs, but this documentation site covers only the KV exercise.
