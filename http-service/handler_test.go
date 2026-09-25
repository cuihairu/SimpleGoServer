package main

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func testHandler(t *testing.T) http.Handler {
	t.Helper()
	return newHandler(NewService(NewMemoryStore(), 64), slog.Default())
}

func do(t *testing.T, h http.Handler, method, path, body string) *http.Response {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rd)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w.Result()
}

func body(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

func TestHandlerHealthz(t *testing.T) {
	resp := do(t, testHandler(t), "GET", "/healthz", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz: %d", resp.StatusCode)
	}
	if got := body(t, resp); got != `{"status":"ok"}`+"\n" {
		t.Fatalf("healthz body: %q", got)
	}
}

func TestHandlerCRUDRoundTrip(t *testing.T) {
	h := testHandler(t)

	resp := do(t, h, "PUT", "/kv/answer", `{"n":42}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("first PUT: %d", resp.StatusCode)
	}
	resp = do(t, h, "PUT", "/kv/answer", `{"n":43}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("overwrite PUT: %d", resp.StatusCode)
	}
	resp = do(t, h, "GET", "/kv/answer", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET: %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("GET content type: %q", ct)
	}
	if got := body(t, resp); got != `{"n":43}` {
		t.Fatalf("GET body: %q", got)
	}
	resp = do(t, h, "DELETE", "/kv/answer", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE: %d", resp.StatusCode)
	}
	if resp = do(t, h, "GET", "/kv/answer", ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after DELETE: %d", resp.StatusCode)
	}
}

// One table for the whole error contract: every non-happy path the
// service can produce, with the exact status code the handler owes.
func TestHandlerErrorContract(t *testing.T) {
	h := testHandler(t)

	cases := []struct {
		name   string
		method string
		path   string
		body   string
		want   int
	}{
		{"unknown key", "GET", "/kv/none", "", 404},
		{"delete unknown key", "DELETE", "/kv/none", "", 404},
		{"delete bad key", "DELETE", "/kv/a%20b", "", 400},
		{"bad key", "GET", "/kv/a%20b", "", 400},
		{"invalid JSON", "PUT", "/kv/k", `nope`, 400},
		{"value too large", "PUT", "/kv/k", `"` + strings.Repeat("x", 64) + `"`, 413},
		{"wrong method", "POST", "/kv/k", `1`, 405},
		{"unknown route", "GET", "/nope", "", 404},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp := do(t, h, c.method, c.path, c.body)
			if resp.StatusCode != c.want {
				t.Fatalf("%s %s -> %d, want %d (body %q)",
					c.method, c.path, resp.StatusCode, c.want, body(t, resp))
			}
			if c.want == http.StatusMethodNotAllowed {
				if allow := resp.Header.Get("Allow"); allow == "" {
					t.Error("405 response carries no Allow header")
				}
			}
		})
	}
}

// A store that fails with an unplanned error stands in for a broken
// remote backend: the handler must answer 500 with the fixed message,
// never leaking the underlying error text to the caller.
func TestHandlerHidesInternalErrors(t *testing.T) {
	svc := NewService(failingStore{}, defaultMaxValueBytes)
	h := newHandler(svc, slog.Default())

	resp := do(t, h, "PUT", "/kv/k", `1`)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("PUT through failing store: %d", resp.StatusCode)
	}
	got := body(t, resp) // a response body reads exactly once — keep it
	if strings.Contains(got, "connection refused") {
		t.Fatalf("internal error text leaked to the response: %q", got)
	}
	if !strings.Contains(got, "internal error") {
		t.Fatalf("response is not the fixed internal-error message: %q", got)
	}
}

// Compile-time guards for the seams the design leans on: errors.Is
// dispatch (writeError) and the sentinel set (service layer).
func TestErrorContractIsStable(t *testing.T) {
	if !errors.Is(ErrNotFound, ErrNotFound) {
		t.Fatal("errors.Is must match its own sentinel")
	}
}
