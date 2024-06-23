package handler

import (
	"bytes"
	"fmt"
	"github.com/cuihairu/simplegoserver/pkg/utils"
	"io"
)

type HeadHandler struct {
}

func (h HeadHandler) HandleWrite(ctx OutboundContext, message Message) {
	switch m := message.(type) {
	case []byte:
		utils.AssertWriteLength(ctx.Conn().Write(m))
	case [][]byte:
		var buffer bytes.Buffer
		for _, b := range m {
			utils.AssertWriteLength(buffer.Write(b))
		}
		utils.AssertWriteLength(ctx.Conn().Write(buffer.Bytes()))
	case *bytes.Buffer:
		utils.AssertWriteLength(ctx.Conn().Write(m.Bytes()))
	case io.WriterTo:
		utils.AssertWriteLength(m.WriteTo(ctx.Conn()))
	case io.Reader:
		utils.AssertWriteLength(io.Copy(ctx.Conn(), m))
	default:
		panic(fmt.Errorf("unsupported type: %T", m))
	}
}

var _ OutboundHandler = (*HeadHandler)(nil)
