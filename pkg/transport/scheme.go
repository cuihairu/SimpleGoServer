package transport

import (
	"fmt"
	"net/url"
	"strings"
)

type Schemes []string

func (ss Schemes) String() string {
	return strings.Join(ss, ",")
}

func (ss Schemes) WithDefaultScheme(u *url.URL) error {
	if len(ss) == 0 {
		return fmt.Errorf("no schemes specified")
	}
	if u == nil {
		return fmt.Errorf("nil URL")
	}
	if u.Scheme == "" {
		u.Scheme = ss[0]
	}
	return nil
}

// is valid scheme
func (ss Schemes) Valid(scheme string) bool {
	return ss.indexOf(scheme) >= 0
}

func (ss Schemes) indexOf(scheme string) int {
	for i, s := range ss {
		if s == scheme {
			return i
		}
	}
	return -1
}

func (ss Schemes) Contains(scheme string) bool {
	return ss.indexOf(scheme) >= 0
}

func (ss Schemes) Add(scheme string) Schemes {
	if ss.Contains(scheme) {
		return ss
	}
	return append(ss, scheme)
}
