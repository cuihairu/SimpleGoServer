package handler

import (
	"fmt"
	"github.com/cuihairu/simplegoserver/pkg/handler"
	"os"
	"sync"
)

var (
	onceTailHeader     sync.Once
	instanceTailHeader *TailHeader
)

type TailHeader struct {
}

func NewTailHeader() *TailHeader {
	onceTailHeader.Do(func() {
		instanceTailHeader = &TailHeader{}
	})
	return instanceTailHeader
}

func (t TailHeader) HandleException(ctx handler.ExceptionContext, ex handler.Exception) {
	fmt.Fprintln(os.Stderr,
		"An HandleException() event was fired, and it reached at the tail of the pipeline.",
		"It usually means the last handler in the pipeline did not handle the exception.",
		"We will close the channels, If you don't want to close the channels please add HandleException() to the pipeline.\n",
		"Exception throw on ", ctx.Conn().RemoteAddr(), "\n",
		ex,
	)
	ctx.Conn().Close()
}

var _ handler.ExceptionHandler = (*TailHeader)(nil)
