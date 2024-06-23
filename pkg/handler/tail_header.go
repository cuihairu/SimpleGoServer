package handler

import (
	"fmt"
	"os"
)

type TailHeader struct {
}

func (t TailHeader) HandleException(ctx ExceptionContext, ex Exception) {
	fmt.Fprintln(os.Stderr,
		"An HandleException() event was fired, and it reached at the tail of the pipeline.",
		"It usually means the last handler in the pipeline did not handle the exception.",
		"We will close the channel, If you don't want to close the channel please add HandleException() to the pipeline.\n",
		"Exception throw on ", ctx.Conn().RemoteAddr(), "\n",
		ex,
	)
	ctx.Conn().Close()
}

var _ ExceptionHandler = (*TailHeader)(nil)
