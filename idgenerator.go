package statecharts

import (
	"fmt"
	"sync"
)

// IDGenerator mints a fresh id, such as an Instance's SCXML 5.10
// _sessionid. Swapping it for a deterministic stand-in, the way Clock
// abstracts time, lets a test assert against a specific id instead of only
// checking that some unpredictable text was assigned.
type IDGenerator interface {
	NewID() string
}

// IDGeneratorFunc adapts a plain func into an IDGenerator, mirroring
// http.HandlerFunc.
type IDGeneratorFunc func() string

// NewID calls f.
func (f IDGeneratorFunc) NewID() string { return f() }

// ManualIDGenerator is an IDGenerator for tests. Each call to NewID returns
// the next value from a sequential counter ("id-1", "id-2", ...) instead of
// random text, so a test can assert against a specific, reproducible id
// rather than merely checking that one was assigned.
type ManualIDGenerator struct {
	mu   sync.Mutex
	next int
}

// NewID returns the next sequential id, starting at "id-1".
func (g *ManualIDGenerator) NewID() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.next++
	return fmt.Sprintf("id-%d", g.next)
}
