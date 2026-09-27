package proto

import (
	"encoding/json"
	"errors"
)

// JSONMessage is the application-level envelope carried in a frame payload.
// Action names the operation ("echo", "chat.send", ...), Data holds the
// operation-specific payload as raw JSON so it can be decoded lazily.
type JSONMessage struct {
	Action string          `json:"action"`
	Data   json.RawMessage `json:"data,omitempty"`
}

// EncodeJSON builds a frame whose payload is a JSONMessage wrapping data.
// data may be nil; it is then omitted from the payload.
//
// The envelope is assembled by hand rather than by marshaling a
// JSONMessage struct: a large pre-encoded payload would otherwise be
// copied through json.RawMessage's append-based MarshalJSON a second
// time, and on a 1MiB call that double encoding dominated the entire
// round trip (21ms of 42ms measured). The hand assembly emits byte-for-
// byte what the struct marshal emits — see TestEncodeJSONMatchesStruct.
func EncodeJSON(t FrameType, streamId uint32, action string, data any) (*Frame, error) {
	var e envelope
	if data != nil {
		if rm, ok := data.(json.RawMessage); ok {
			e.raw = rm // already-encoded JSON: pass through without re-marshaling
		} else if s, ok := data.(string); ok && jsonPlainASCII(s) {
			// plain-ASCII string: spliced straight into the payload below,
			// skipping json.Marshal's intermediate buffer entirely
			e.stringData, e.directString = s, true
		} else {
			encoded, err := json.Marshal(data)
			if err != nil {
				return nil, err
			}
			e.raw = encoded
		}
	}
	e.action = action
	payload := e.bytes()
	return &Frame{
		Header: FrameHeader{
			FrameType: t,
			StreamId:  streamId,
			// an overflowed int32 goes negative and is rejected by Encode's
			// sign check, so a corrupt length can never reach the wire
			Length: int32(len(payload)), // #nosec G115
		},
		Payload: payload,
	}, nil
}

// envelope is the JSONMessage wire form under assembly: one splice body
// shared by EncodeJSON and the server response paths (handleRequestFrame,
// mustJSON). Before the shared core existed those paths marshaled the
// JSONMessage struct through encoding/json — paying the marshaler lookup,
// the boxed struct and an encoder buffer per response just to move bytes
// that were already encoded; end-to-end echo medians moved 33→31
// allocs/op (Benchmark.md「编解码热点实测优化」). byte-for-byte equality
// with the struct marshal is pinned by TestEncodeJSONMatchesStruct.
type envelope struct {
	action string

	// raw is the already-encoded JSON body (nil means "no data field").
	raw json.RawMessage

	// stringData is a plain-ASCII string body that can be spliced between
	// quotes without json.Marshal; directString selects it over raw.
	stringData   string
	directString bool
}

// bytes assembles the payload. json.Marshal of a plain string cannot
// fail — there is deliberately no error path, like mustJSON and reply in
// protocol.go.
func (e envelope) bytes() []byte {
	actionPlain := jsonPlainASCII(e.action)
	var capacity int
	if actionPlain {
		if e.directString {
			capacity = len(`{"action":"`) + len(e.action) + len(`","data":"`) + len(e.stringData) + len(`"}"`)
		} else {
			capacity = len(`{"action":"`) + len(e.action) + len(`","data":`) + len(e.raw) + 1
		}
	} else {
		if e.directString {
			capacity = len(`{"action":`) + len(`""`) + len(`,"data":"`) + len(e.stringData) + len(`"}"`)
		} else {
			capacity = len(`{"action":`) + len(`""`) + len(`,"data":`) + len(e.raw) + 1
		}
	}
	payload := make([]byte, 0, capacity)
	payload = append(payload, `{"action":`...)
	if actionPlain {
		payload = append(payload, '"')
		payload = append(payload, e.action...)
		payload = append(payload, '"')
	} else {
		actionJSON, _ := json.Marshal(e.action) // tiny; handles escaping
		payload = append(payload, actionJSON...)
	}
	if e.directString {
		payload = append(payload, `,"data":"`...)
		payload = append(payload, e.stringData...)
		payload = append(payload, '"')
	} else if len(e.raw) > 0 { // omitempty: an empty RawMessage is omitted, like the struct's tag
		payload = append(payload, `,"data":`...)
		payload = append(payload, e.raw...)
	}
	payload = append(payload, '}')
	return payload
}

// jsonPlainASCII reports whether s can be wrapped in bare quotes and be
// byte-for-byte what the stdlib encoder emits: every byte is printable
// ASCII, and none of the characters stdlib escapes — quote, backslash,
// and the HTML-escaping set < > & (the default encoder escapes those even
// in pure ASCII). This is strictly narrower than what the stdlib encoder
// handles — anything else takes the json.Marshal path — so the splice is
// exactly equivalent to marshaling the same string (asserted by
// TestEncodeJSONMatchesStruct and the plain-ASCII quick generator).
//
// Deliberately a plain byte loop, not SWAR: a word-at-a-time form was
// prototyped and measured at parity-or-slower on 1MiB plain strings
// (the loop already runs at ~3 cycles/byte with predictable branches,
// while SWAR still pays a copy per word), so the extra bit-tricks bought
// nothing — see Benchmark.md「编解码热点实测优化」.
func jsonPlainASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c > 0x7E || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
			return false
		}
	}
	return true
}

// zeroCopyRawMessage mirrors json.RawMessage except its UnmarshalJSON
// records the decoder's own sub-slice of the input instead of copying it.
// DecodeJSONMessage is its only user: the returned Data aliases
// frame.Payload, which the caller already owns and treats as read-only —
// on a 1MiB response that aliasing spares a full megabyte copy plus
// json's pre-validation pass is unaffected either way.
type zeroCopyRawMessage []byte

func (m *zeroCopyRawMessage) UnmarshalJSON(data []byte) error {
	*m = data
	return nil
}

// DecodeJSONMessage unmarshals a frame payload into a JSONMessage.
//
// The Data field aliases the frame payload rather than copying it (the
// struct's json.RawMessage would append-copy the whole payload); callers
// must treat Data as read-only for as long as they hold the frame. On the
// server pipeline that means: read it, pass it on, or return it inside the
// response value — the response is marshaled before the codec recycles the
// frame — but do not stash it anywhere that outlives the handler's return,
// because the pooled payload buffer goes back to the pool the moment
// dispatch finishes (see pool.go).
func DecodeJSONMessage(frame *Frame) (*JSONMessage, error) {
	if frame == nil {
		return nil, errors.New("proto: frame is nil")
	}
	var shadow struct {
		Action string             `json:"action"`
		Data   zeroCopyRawMessage `json:"data,omitempty"`
	}
	if err := json.Unmarshal(frame.Payload, &shadow); err != nil {
		return nil, err
	}
	return &JSONMessage{Action: shadow.Action, Data: json.RawMessage(shadow.Data)}, nil
}

// DecodeJSONData unmarshals the Data field of a frame payload into out.
// A frame without data is not an error; out is left untouched.
func DecodeJSONData(frame *Frame, out any) error {
	msg, err := DecodeJSONMessage(frame)
	if err != nil {
		return err
	}
	if len(msg.Data) == 0 {
		return nil
	}
	return json.Unmarshal(msg.Data, out)
}
