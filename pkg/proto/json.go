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
	var raw json.RawMessage
	stringData := ""
	directString := false
	if data != nil {
		if rm, ok := data.(json.RawMessage); ok {
			raw = rm // already-encoded JSON: pass through without re-marshaling
		} else if s, ok := data.(string); ok && jsonPlainASCII(s) {
			// plain-ASCII string: spliced straight into the payload below,
			// skipping json.Marshal's intermediate buffer entirely
			stringData, directString = s, true
		} else {
			encoded, err := json.Marshal(data)
			if err != nil {
				return nil, err
			}
			raw = encoded
		}
	}
	// json.Marshal of a plain string cannot fail — there is deliberately
	// no error path, like mustJSON and reply in protocol.go
	actionPlain := jsonPlainASCII(action)
	var capacity int
	if actionPlain {
		if directString {
			capacity = len(`{"action":"`) + len(action) + len(`","data":"`) + len(stringData) + len(`"}"`)
		} else {
			capacity = len(`{"action":"`) + len(action) + len(`","data":`) + len(raw) + 1
		}
	} else {
		if directString {
			capacity = len(`{"action":`) + len(`""`) + len(`,"data":"`) + len(stringData) + len(`"}"`)
		} else {
			capacity = len(`{"action":`) + len(`""`) + len(`,"data":`) + len(raw) + 1
		}
	}
	payload := make([]byte, 0, capacity)
	payload = append(payload, `{"action":`...)
	if actionPlain {
		payload = append(payload, '"')
		payload = append(payload, action...)
		payload = append(payload, '"')
	} else {
		actionJSON, _ := json.Marshal(action) // tiny; handles escaping
		payload = append(payload, actionJSON...)
	}
	if directString {
		payload = append(payload, `,"data":"`...)
		payload = append(payload, stringData...)
		payload = append(payload, '"')
	} else if len(raw) > 0 { // omitempty: an empty RawMessage is omitted, like the struct's tag
		payload = append(payload, `,"data":`...)
		payload = append(payload, raw...)
	}
	payload = append(payload, '}')
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

// jsonPlainASCII reports whether s can be wrapped in bare quotes and be
// byte-for-byte what the stdlib encoder emits: every byte is printable
// ASCII, and none of the characters stdlib escapes — quote, backslash,
// and the HTML-escaping set < > & (the default encoder escapes those even
// in pure ASCII). This is strictly narrower than what the stdlib encoder
// handles — anything else takes the json.Marshal path — so the splice is
// exactly equivalent to marshaling the same string (asserted by
// TestEncodeJSONMatchesStruct and the plain-ASCII quick generator).
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
// must treat Data as read-only for as long as they hold the frame.
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
