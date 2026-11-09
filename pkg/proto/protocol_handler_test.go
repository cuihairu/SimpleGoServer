package proto

import (
	"encoding/json"
	"testing"
	"time"
)

func TestProtocolHandlerRequestResponse(t *testing.T) {
	_, conn := pipeServer(t, echoHandler)

	frame, err := EncodeJSON(REQUEST, 42, "echo", "hello")
	if err != nil {
		t.Fatalf("EncodeJSON(): %v", err)
	}
	writeFrame(t, conn, frame)

	resp := readFrame(t, conn)
	if resp.Header.FrameType != RESPONSE {
		t.Fatalf("frame type = %s, want RESPONSE", resp.Header.FrameType)
	}
	if resp.Header.StreamId != 42 {
		t.Fatalf("stream id = %d, want 42 (request correlation)", resp.Header.StreamId)
	}
	// responses carry the JSONMessage envelope
	env, err := DecodeJSONMessage(resp)
	if err != nil {
		t.Fatalf("DecodeJSONMessage(): %v", err)
	}
	if env.Action != "echo" {
		t.Fatalf("envelope action = %q, want echo", env.Action)
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(env.Data, &body); err != nil {
		t.Fatalf("payload: %v", err)
	}
	var echoed string
	if err := json.Unmarshal(body["data"], &echoed); err != nil {
		t.Fatalf("echoed data: %v", err)
	}
	if echoed != "hello" {
		t.Fatalf("echo = %q, want hello", echoed)
	}
}

func TestProtocolHandlerErrorResponse(t *testing.T) {
	_, conn := pipeServer(t, echoHandler)

	frame, _ := EncodeJSON(REQUEST, 7, "fail", nil)
	writeFrame(t, conn, frame)

	resp := readFrame(t, conn)
	msg, err := DecodeJSONMessage(resp)
	if err != nil {
		t.Fatalf("DecodeJSONMessage(): %v", err)
	}
	var errMsg ErrorMessage
	if err := json.Unmarshal(msg.Data, &errMsg); err != nil {
		t.Fatalf("error payload: %v", err)
	}
	if errMsg.Error != "intentional failure" {
		t.Fatalf("error = %q", errMsg.Error)
	}
	// the connection must survive a failed request
	frame2, _ := EncodeJSON(REQUEST, 8, "echo", "still-alive")
	writeFrame(t, conn, frame2)
	resp2 := readFrame(t, conn)
	if resp2.Header.StreamId != 8 {
		t.Fatalf("stream id after error = %d, connection should stay open", resp2.Header.StreamId)
	}
}

func TestProtocolHandlerPingPong(t *testing.T) {
	ph, conn := pipeServer(t, echoHandler)

	frame, _ := EncodeJSON(PING, 9, "ping", nil)
	writeFrame(t, conn, frame)

	resp := readFrame(t, conn)
	if resp.Header.FrameType != PONG || resp.Header.StreamId != 9 {
		t.Fatalf("got %s stream %d, want PONG stream 9", resp.Header.FrameType, resp.Header.StreamId)
	}
	pings, _ := ph.Stats()
	if pings != 1 {
		t.Fatalf("stats pings=%d, want 1", pings)
	}
}

func TestProtocolHandlerCloseFrame(t *testing.T) {
	_, conn := pipeServer(t, echoHandler)

	frame, _ := EncodeJSON(CLOSE, 12, "bye", nil)
	writeFrame(t, conn, frame)

	ack := readFrame(t, conn)
	if ack.Header.FrameType != RESPONSE {
		t.Fatalf("close ack type = %s, want RESPONSE", ack.Header.FrameType)
	}
	// server closes its side; a read must eventually fail
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(make([]byte, 16)); err == nil {
		t.Fatal("expected read error after CLOSE handshake")
	}
}

func TestProtocolHandlerRejectsNonFrameMessage(t *testing.T) {
	ph, conn := pipeServer(t, echoHandler)
	// hand the protocol handler a message that is not a *Frame; it must
	// close the connection instead of panicking
	ph.HandleRead(&inboundContextStub{conn: conn}, "not a frame")
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(make([]byte, 8)); err == nil {
		t.Fatal("expected connection to close after protocol violation")
	}
}
