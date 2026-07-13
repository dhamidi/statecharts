package statecharts

import (
	"sort"
	"sync"
	"time"
)

// Clock abstracts time so delayed-<send> timers can be replaced with
// compute-only bookkeeping during log replay (see Rehydrate), instead of
// arming real OS timers for events that already fired historically.
type Clock interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) (stop func() bool)
}

type realClock struct{}

// NewRealClock returns the default Clock, backed by the real wall clock and
// time.AfterFunc.
func NewRealClock() Clock { return realClock{} }

func (realClock) Now() time.Time { return time.Now() }

func (realClock) AfterFunc(d time.Duration, f func()) func() bool {
	t := time.AfterFunc(d, f)
	return t.Stop
}

// ManualClock is a Clock for tests: time only advances when Advance is
// called, and pending callbacks fire synchronously, in fire-time order, as
// part of that call instead of on a real timer goroutine.
type ManualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*manualTimer
}

type manualTimer struct {
	fireAt  time.Time
	fn      func()
	fired   bool
	stopped bool
}

// NewManualClock returns a ManualClock starting at start.
func NewManualClock(start time.Time) *ManualClock {
	return &ManualClock{now: start}
}

func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *ManualClock) AfterFunc(d time.Duration, f func()) func() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &manualTimer{fireAt: c.now.Add(d), fn: f}
	c.timers = append(c.timers, t)
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if t.fired || t.stopped {
			return false
		}
		t.stopped = true
		return true
	}
}

// Advance moves the clock forward by d, then synchronously fires (in
// fire-time order) any timers now due.
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	var due []*manualTimer
	for _, t := range c.timers {
		if !t.fired && !t.stopped && !t.fireAt.After(now) {
			due = append(due, t)
		}
	}
	sort.Slice(due, func(i, j int) bool { return due[i].fireAt.Before(due[j].fireAt) })
	for _, t := range due {
		t.fired = true
	}
	c.mu.Unlock()

	for _, t := range due {
		t.fn()
	}
}
