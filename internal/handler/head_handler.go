package handler

import (
	"bytes"
	"fmt"
	"github.com/cuihairu/simplegoserver/pkg/handler"
	"github.com/cuihairu/simplegoserver/pkg/utils"
	"io"
	"sync"
)

var (
	onceHeadHandler     sync.Once
	instanceHeadHandler *HeadHandler
)

type HeadHandler struct {
}

func NewHeadHandler() *HeadHandler {
	onceHeadHandler.Do(func() {
		instanceHeadHandler = &HeadHandler{}
	})
	return instanceHeadHandler
}

type Buffer interface {
	Bytes() []byte
}

func (h HeadHandler) HandleWrite(ctx handler.OutboundContext, message handler.Message) {
	switch m := message.(type) {
	case []byte:
		utils.AssertWriteLength(ctx.Conn().Write(m))
	case [][]byte:
		var buffer bytes.Buffer
		for _, b := range m {
			utils.AssertWriteLength(buffer.Write(b))
		}
		utils.AssertWriteLength(ctx.Conn().Write(buffer.Bytes()))
	case Buffer:
		utils.AssertWriteLength(ctx.Conn().Write(m.Bytes()))
	case io.WriterTo:
		utils.AssertWriteLength(m.WriteTo(ctx.Conn()))
	case io.Reader:
		utils.AssertWriteLength(io.Copy(ctx.Conn(), m))
	default:
		panic(fmt.Errorf("unsupported type: %T", m))
	}
}

var _ handler.OutboundHandler = (*HeadHandler)(nil)
