package proto

import "fmt"

type FrameType uint8

const (
	REQUEST     FrameType = iota // 请求，期待同 StreamId 的 RESPONSE
	RESPONSE                     // 响应，与 REQUEST/HELLO/SUBSCRIBE 通过 StreamId 关联
	PUBLISH                      // 发布，向 topic 推送（StreamId 复用为 topic hash，载荷携带 topic）
	SUBSCRIBE                    // 订阅 topic
	UNSUBSCRIBE                  // 取消订阅 topic
	PING                         // 心跳探测
	PONG                         // 心跳应答
	CLOSE                        // 优雅关闭通知，对端收到后可主动断开
	HELLO                        // 握手首帧，协商协议版本（载荷为 versions 列表）
)

func (t FrameType) String() string {
	switch t {
	case REQUEST:
		return "REQUEST"
	case RESPONSE:
		return "RESPONSE"
	case PUBLISH:
		return "PUBLISH"
	case SUBSCRIBE:
		return "SUBSCRIBE"
	case UNSUBSCRIBE:
		return "UNSUBSCRIBE"
	case PING:
		return "PING"
	case PONG:
		return "PONG"
	case CLOSE:
		return "CLOSE"
	case HELLO:
		return "HELLO"
	default:
		return fmt.Sprintf("FrameType(%d)", uint8(t))
	}
}

// HeaderSize is the fixed size of the frame header in bytes:
// [type:1][flags:1][streamId:4][length:4], all integers big-endian.
const HeaderSize = 10

// MaxFrameSize bounds the payload length accepted when decoding. A peer
// announcing a larger frame is treated as protocol error and disconnected,
// which keeps a malicious or broken peer from exhausting memory.
const MaxFrameSize = 1 << 20 // 1MB

type FrameHeader struct {
	FrameType FrameType
	Flags     uint8
	StreamId  uint32
	Length    int32
}

// Id packs frame type and stream id into a single 32-bit value,
// mainly for logging and tracing.
func (f *FrameHeader) Id() uint32 {
	return (uint32(f.FrameType) << 24) | (f.StreamId & 0x00FFFFFF)
}

func (f FrameHeader) String() string {
	return fmt.Sprintf("{type:%s flags:%d streamId:%d length:%d}", f.FrameType, f.Flags, f.StreamId, f.Length)
}

// Frame is a fully decoded protocol frame: header plus payload.
type Frame struct {
	Header  FrameHeader
	Payload []byte
}

func (f *Frame) String() string {
	return fmt.Sprintf("Frame(header:%s payload:%d bytes)", f.Header, len(f.Payload))
}
