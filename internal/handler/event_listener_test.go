package handler

import (
	"bytes"
	"log"
	"net"
	"testing"
)

// newTestErrorHandler builds a non-singleton ErrorHandler whose output
// lands in buffers we can assert on. The singleton from NewErrorHandler
// writes to the process streams and cannot be inspected.
func newTestErrorHandler() (ErrorHandler, *bytes.Buffer, *bytes.Buffer) {
	info := &bytes.Buffer{}
	errs := &bytes.Buffer{}
	return ErrorHandler{
		logger:    log.New(info, "", 0),
		errLogger: log.New(errs, "", 0),
	}, info, errs
}

func TestErrorHandlerCallbacks(t *testing.T) {
	e, info, errs := newTestErrorHandler()

	e.OnStartup()
	e.OnReload()
	e.OnShutdown()
	if info.Len() == 0 {
		t.Fatal("lifecycle callbacks wrote nothing")
	}

	conn, _ := net.Pipe()
	defer conn.Close()
	e.OnConnect(conn)
	if got := info.String(); got == "" {
		t.Fatal("OnConnect wrote nothing")
	}

	e.OnError("boom")
	e.HandleException(nil, "kaboom")
	if got := errs.String(); !bytes.Contains([]byte(got), []byte("boom")) {
		t.Fatalf("error output = %q, want it to contain boom", got)
	}
	if !bytes.Contains([]byte(errs.Bytes()), []byte("kaboom")) {
		t.Fatal("HandleException wrote nothing to the error log")
	}
}
