package statecharts

import (
	"fmt"
	"io"
	"sync"
)

// Logger is where ExecContext.Log's output goes: a chart's diagnostic
// output, distinct from any event traffic or persisted Log entry. label
// and data mirror <log>'s own label-plus-expr shape.
type Logger interface {
	// Log records one diagnostic entry. It is called synchronously, inline,
	// from the interpreter's own goroutine, and is not expected to hand off
	// to a goroutine of its own the way IOProcessor.Send is -- a Logger call
	// is a local, in-process write, not real I/O.
	Log(label string, data Value)
}

type noopLogger struct{}

func (noopLogger) Log(string, Value) {}

// NoopLogger discards every call. It is the default Logger for an
// Instance built with no WithLogger option, so a chart that never calls
// ExecContext.Log behaves identically whether or not WithLogger was ever
// configured.
var NoopLogger Logger = noopLogger{}

// WriterLogger is a Logger that writes one line per call to an underlying
// io.Writer. It is safe for concurrent use by multiple goroutines: calls to
// Log are serialized, so multiple Instances or actors sharing one
// WriterLogger never interleave partial writes.
type WriterLogger struct {
	mu sync.Mutex
	w  io.Writer
}

// NewWriterLogger returns a WriterLogger that writes to w.
func NewWriterLogger(w io.Writer) *WriterLogger {
	return &WriterLogger{w: w}
}

// Log writes label and data as a single line to the underlying io.Writer.
func (l *WriterLogger) Log(label string, data Value) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.w, "%s: %v\n", label, data)
}
