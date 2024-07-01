package tcp

import "time"

type Options struct {
	Timeout         time.Duration
	network         string
	address         string
	linger          int
	keepAlive       bool
	keepAlivePeriod int
	noDelay         bool
}
