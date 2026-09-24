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
func EncodeJSON(t FrameType, streamId uint32, action string, data any) (*Frame, error) {
	var raw json.RawMessage
	if data != nil {
		encoded, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		raw = encoded
	}
	// the envelope holds our own string-plus-raw-JSON types, so this
	// marshal cannot fail and there is deliberately no error path
	payload, _ := json.Marshal(&JSONMessage{Action: action, Data: raw})
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
