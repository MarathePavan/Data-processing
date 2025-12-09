package util

import (
	"fmt"
	"time"
)

type Timer struct {
	start time.Time
	name  string
}

func NewTimer(name string) *Timer {
	return &Timer{name: name, start: time.Now()}
}

func (t *Timer) Elapsed() time.Duration {
	return time.Since(t.start)
}

func (t *Timer) Print() {
	fmt.Printf("%s elapsed=%s\n", t.name, t.Elapsed().String())
}
