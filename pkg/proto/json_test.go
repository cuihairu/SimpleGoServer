package proto

import (
	"encoding/json"
	"testing"
)

// TestEncodeJSONMatchesStruct pins the hand-assembled envelope to
// byte-for-byte equality with what marshaling the JSONMessage struct emits
// — the fast path exists because the struct marshal double-copied large
// payloads, and it is only sound while the two encodings agree.
func TestEncodeJSONMatchesStruct(t *testing.T) {
	reference := func(action string, raw json.RawMessage) []byte {
		payload, _ := json.Marshal(&JSONMessage{Action: action, Data: raw})
		return payload
	}
	cases := []struct {
		name   string
		action string
		data   any
	}{
		{"nil data omits the field", "ping", nil},
		{"raw message passes through", "echo", json.RawMessage(`{"a":1,"b":[2,3]}`)},
		{"string payload", "echo", "hello"},
		{"empty raw message is omitted", "echo", json.RawMessage("")},
		{"action escaping", "ac\"tion", "x"},
		{"unicode payload", "echo", "héllo→世界"},
		{"struct payload", "echo", struct {
			N int    `json:"n"`
			S string `json:"s"`
		}{7, "seven"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			frame, err := EncodeJSON(REQUEST, 1, tc.action, tc.data)
			if err != nil {
				t.Fatalf("EncodeJSON: %v", err)
			}
			want := reference(tc.action, mustRaw(tc.data))
			if string(frame.Payload) != string(want) {
				t.Fatalf("payload mismatch:\n got: %s\nwant: %s", frame.Payload, want)
			}
		})
	}
}

// mustRaw mirrors what EncodeJSON does with data before the envelope step.
func mustRaw(data any) json.RawMessage {
	if data == nil {
		return nil
	}
	if rm, ok := data.(json.RawMessage); ok {
		return rm
	}
	encoded, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	return encoded
}
