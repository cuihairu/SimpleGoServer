package channels

import (
	"strings"
	"time"
)

type SelectKeys uint32

const (
	OP_READ    SelectKeys = 1
	OP_WRITE   SelectKeys = 2
	OP_CONNECT SelectKeys = 4
	OP_ACCEPT  SelectKeys = 8
)

func (s SelectKeys) String() string {
	var ss []string
	if s&OP_READ != 0 {
		ss = append(ss, "OP_READ")
	}
	if s&OP_WRITE != 0 {
		ss = append(ss, "OP_WRITE")
	}
	if s&OP_CONNECT != 0 {
		ss = append(ss, "OP_CONNECT")
	}
	if s&OP_ACCEPT != 0 {
		ss = append(ss, "OP_ACCEPT")
	}
	return strings.Join(ss, "|")
}

func (s SelectKeys) Has(key SelectKeys) bool {
	return s&key != 0
}

func (s SelectKeys) Add(key SelectKeys) SelectKeys {
	return s | key
}

func (s SelectKeys) Remove(key SelectKeys) SelectKeys {
	return s &^ key
}

type Selector interface {
	Select() ([]Channel, error)
	SelectWithTimeout(timeout time.Duration) ([]Channel, error)
	SelectNow() ([]Channel, error)
	Wakeup()
	SelectKeys() SelectKeys
	Close() error
}
