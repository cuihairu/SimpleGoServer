package executor

import (
	"fmt"
	"github.com/cuihairu/simplegoserver/pkg"
	"testing"
	"time"
)

func TestNewAsyncExecutor(t *testing.T) {
	var executor pkg.Executor = pkg.NewAsyncExecutor(2)
	executor.Start()
	ret := make(chan int, 1)
	executor.Exec(func() {
		fmt.Printf("start: %+v\n", time.Now())
		select {
		case <-time.After(2 * time.Second):
			fmt.Printf("end: %v\n", time.Now())
			ret <- 1
		}
	})
	<-ret
	executor.Stop()
}
