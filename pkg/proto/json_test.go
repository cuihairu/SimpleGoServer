package proto

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"testing/quick"
	"unsafe"
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
		{"plain-ascii long string takes the splice path", "echo", strings.Repeat("plain ascii 0123456789", 64)},
		{"space and tilde are splice-safe bounds", "echo", " ~!#$%&'()*+,-./:;<=>?@[]^_`{|}"},
		{"del byte falls back to json.Marshal", "echo", string([]byte{0x7F})},
		{"quote falls back to json.Marshal", "echo", `say "hi"`},
		{"backslash falls back to json.Marshal", "echo", `C:\go\path`},
		{"control byte falls back to json.Marshal", "echo", "line\nbreak"},
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

// TestEncodeJSONRandomStringsMatchesStruct drives random strings across the
// splice fast path and the json.Marshal fallback, asserting both emit what
// the struct marshal emits. quick's default generator draws from the whole
// unicode range, so plain-ASCII strings — the fast path's domain — almost
// never come up; the custom generator mixes both populations explicitly,
// biasing hard toward splice-eligible bytes while keeping escape-boundary
// characters in rotation for the fallback.
func TestEncodeJSONRandomStringsMatchesStruct(t *testing.T) {
	// splice-eligible domain: printable ASCII minus quote, backslash and
	// the HTML-escaping set — exactly jsonPlainASCII's alphabet
	const plain = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 !#$%()*+,-./:;=?@[]^_`{|}~'"
	// boundary characters that must fall back and stay escaped correctly
	const boundary = "\"\\<>&\n\t\x7fé世"
	mixed := func(rand *rand.Rand) string {
		var b strings.Builder
		n := rand.Intn(64)
		for i := 0; i < n; i++ {
			if rand.Intn(4) == 0 {
				b.WriteByte(boundary[rand.Intn(len(boundary))])
			} else {
				b.WriteByte(plain[rand.Intn(len(plain))])
			}
		}
		return b.String()
	}
	reference := func(action string, s string) []byte {
		payload, _ := json.Marshal(&JSONMessage{Action: action, Data: mustRaw(s)})
		return payload
	}
	if err := quick.Check(func(action, s string) bool {
		frame, err := EncodeJSON(REQUEST, 1, action, s)
		if err != nil {
			return false
		}
		return string(frame.Payload) == string(reference(action, s))
	}, &quick.Config{
		MaxCount: 2000,
		Values: func(values []reflect.Value, rand *rand.Rand) {
			values[0] = reflect.ValueOf(mixed(rand))
			values[1] = reflect.ValueOf(mixed(rand))
		},
	}); err != nil {
		t.Fatalf("random strings diverge from the struct marshal: %v", err)
	}
}

// TestDecodeJSONDataAliasesPayload pins the zero-copy contract of
// DecodeJSONMessage: Data shares the frame payload's backing array instead
// of copying it. Callers rely on this for large responses — an accidental
// copy back would reintroduce a full-payload memcpy per message.
func TestDecodeJSONDataAliasesPayload(t *testing.T) {
	payload := `{"action":"echo","data":"` + strings.Repeat("x", 128) + `"}`
	frame := &Frame{Header: FrameHeader{FrameType: RESPONSE, StreamId: 1}, Payload: []byte(payload)}
	msg, err := DecodeJSONMessage(frame)
	if err != nil {
		t.Fatalf("DecodeJSONMessage: %v", err)
	}
	if msg.Action != "echo" {
		t.Fatalf("action = %q, want echo", msg.Action)
	}
	dataStart := uintptr(unsafe.Pointer(unsafe.SliceData([]byte(msg.Data))))
	payStart := uintptr(unsafe.Pointer(unsafe.SliceData(frame.Payload)))
	if dataStart == 0 || dataStart < payStart || dataStart >= payStart+uintptr(len(frame.Payload)) {
		t.Fatal("Data does not alias the frame payload: the zero-copy contract is broken")
	}
}
