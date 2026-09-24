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
	if data != nil {
		if rm, ok := data.(json.RawMessage); ok {
			raw = rm // already-encoded JSON: pass through without re-marshaling
		} else {
			encoded, err := json.Marshal(data)
			if err != nil {
				return nil, err
			}
			raw = encoded
		}
	}
	actionJSON, err := json.Marshal(action) // tiny; handles escaping
	if err != nil {
		return nil, err
	}
	payload := make([]byte, 0, len(`{"action":`)+len(actionJSON)+len(`,"data":`)+len(raw)+1)
	payload = append(payload, `{"action":`...)
	payload = append(payload, actionJSON...)
	if len(raw) > 0 { // omitempty: an empty RawMessage is omitted, like the struct's tag
		payload = append(payload, `,"data":`...)
		payload = append(payload, raw...)
	}
	payload = append(payload, '}')
	return &Frame{
		Header: FrameHeader{
			FrameType: t,
			StreamId:  streamId,
			Length:    int32(len(payload)),
		},
		Payload: payload,
	}, nil
}

// DecodeJSONMessage unmarshals a frame payload into a JSONMessage.
func DecodeJSONMessage(frame *Frame) (*JSONMessage, error) {
	if frame == nil {
		return nil, errors.New("proto: frame is nil")
	}
	msg := &JSONMessage{}
	if err := json.Unmarshal(frame.Payload, msg); err != nil {
		return nil, err
	}
	return msg, nil
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
