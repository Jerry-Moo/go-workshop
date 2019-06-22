package coarsetime

import (
	"sync/atomic"
	"time"
)

var (
	coarsetime atomic.Value
	frequency  = time.Millisecond * 100
)

// FloorTimeNow returns the current time from the range (now - 100ms, now],
// This is a faster alternative to time.Now().
func FloorTimeNow() time.Time {
	tp := coarsetime.Load().(*time.Time)
	return (*tp)
}

// CeilingTimeNow returns the current time from the range [now, now+100ms).
// This is a faster alternative to time.Now()
func CeilingTimeNow() time.Time {
	tp := coarsetime.Load().(*time.Time)
	return (*tp).Add(frequency)
}

func init() {
	t := time.Now().Truncate(frequency)
	coarsetime.Store(&t)
	go func() {
		for {
			time.Sleep(frequency)
			t := time.Now().Truncate(frequency)
			coarsetime.Store(&t)
		}
	}()
}
