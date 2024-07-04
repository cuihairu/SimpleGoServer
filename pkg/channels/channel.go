package channels

import (
	"github.com/cuihairu/simplegoserver/pkg/handler"
	"net"
)

type Channel interface {
	net.Conn
	ID() int64
	IsActive() bool
	Writev(data [][]byte) error
	Flush()
	WriteAndFlush([]byte) error
	Pipeline() handler.Pipeline
	Attachment() any
	SetAttachment(attachment any)
}
