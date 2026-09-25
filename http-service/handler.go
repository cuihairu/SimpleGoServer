package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
)

// newHandler wires the Service to HTTP. Routing uses the Go 1.22
// pattern syntax ("METHOD /path/{var}"): the mux itself answers wrong
// methods with 405 and extracts {key} via r.PathValue, so no
// third-party router and no hand-written method switch are needed.
func newHandler(svc *Service, log *slog.Logger) http.Handler {
	h := &handler{svc: svc, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.health)
	mux.HandleFunc("GET /kv/{key}", h.get)
	mux.HandleFunc("PUT /kv/{key}", h.put)
	mux.HandleFunc("DELETE /kv/{key}", h.delete)
	return mux
}

type handler struct {
	svc *Service
	log *slog.Logger
}

// writeError maps one service error to one status code, in one place:
// the response contract lives here instead of being scattered across
// routes. Internal errors log the detail but answer with a fixed
// message — error text is for the operator, never for the caller.
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

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (h *handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	v, err := h.svc.Get(r.Context(), r.PathValue("key"))
	if err != nil {
		h.writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(v) // #nosec G705 -- v is json.Valid-checked and served as application/json; the KV echo is the contract, never rendered as HTML
}

func (h *handler) put(w http.ResponseWriter, r *http.Request) {
	created, err := h.svc.Put(r.Context(), r.PathValue("key"), r.Body)
	if err != nil {
		h.writeError(w, err)
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	writeJSON(w, code, map[string]string{"status": "stored"})
}

func (h *handler) delete(w http.ResponseWriter, r *http.Request) {
	deleted, err := h.svc.Delete(r.Context(), r.PathValue("key"))
	if err != nil {
		h.writeError(w, err)
		return
	}
	if !deleted {
		h.writeError(w, ErrNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
