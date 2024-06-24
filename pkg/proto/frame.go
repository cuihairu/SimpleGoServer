package proto

type FrameType uint8

const (
	STEP FrameType = iota
	KEEPALIVE
	RESUME
	RESPONSE
	SUBSCRIBE
	UNSUBSCRIBE
)

type FrameHeader struct {
	FrameType FrameType
	StreamId  uint32
	Flags     int32
	Length    int32
}

func (f *FrameHeader) Id() uint32 {
	return (uint32(f.FrameType) << 24) & f.StreamId
}
