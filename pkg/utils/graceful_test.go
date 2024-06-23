package utils

import (
	"os"
	"testing"
	"time"
)

func TestNewGraceful_Stop(t *testing.T) {
	graceful := NewGraceful(func(signal os.Signal) {
		t.Logf("single :%s", signal.String())
	}, func() {
		t.Logf("reload ....")
	})
	go func() {
		time.Sleep(3 * time.Second)
		graceful.Stop()
	}()
	graceful.Wait()
}

func TestGraceful_Reload(t *testing.T) {
	graceful := NewGraceful(func(signal os.Signal) {
		t.Logf("single :%s", signal.String())
	}, func() {
		t.Logf("reload ....")
	})
	go func() {
		time.Sleep(3 * time.Second)
		graceful.Reload()
		graceful.Stop()
	}()
	graceful.Wait()
}
