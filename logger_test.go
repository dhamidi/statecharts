package statecharts

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingLogger is a test double that records every Log call it
// receives, in order.
type recordingLogger struct {
	calls []recordedLogCall
}

type recordedLogCall struct {
	label string
	data  any
}

func (l *recordingLogger) Log(label string, data any) {
	l.calls = append(l.calls, recordedLogCall{label: label, data: data})
}

func TestExecContextLogCallsConfiguredLogger(t *testing.T) {
	logOnEntry := Action(func(d *Door, ec ExecContext) error {
		ec.Log("entered", "open")
		return nil
	})

	chart, err := Build(
		Compound("door", "closed",
			Children(
				Atomic("closed", On("open.request", Target("open"))),
				Atomic("open", OnEntry(logOnEntry)),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	rec := &recordingLogger{}
	d := &Door{}
	in := New(chart, d, WithLogger(rec))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := in.Send(ctx, Event{Name: "open.request", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if len(rec.calls) != 1 {
		t.Fatalf("logger recorded %d calls, want 1: %v", len(rec.calls), rec.calls)
	}
	if rec.calls[0].label != "entered" || rec.calls[0].data != "open" {
		t.Fatalf("logger recorded %+v, want {entered open}", rec.calls[0])
	}
}

// A chart that never calls ExecContext.Log, and one that does but was built
// with no WithLogger option, must behave identically -- Log is a silent
// no-op with nothing configured, whether that's a bare ExecContext{} or a
// default Instance.
func TestExecContextLogIsNoopWithoutLogger(t *testing.T) {
	var bare ExecContext
	bare.Log("label", "data") // must not panic

	logOnEntry := Action(func(d *Door, ec ExecContext) error {
		ec.Log("entered", "open")
		return nil
	})
	chart, err := Build(
		Compound("door", "closed",
			Children(
				Atomic("closed", On("open.request", Target("open"))),
				Atomic("open", OnEntry(logOnEntry)),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	d := &Door{}
	in := New(chart, d) // no WithLogger -- defaults to NoopLogger
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := in.Send(ctx, Event{Name: "open.request", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if !hasState(in.Configuration(), "open") {
		t.Fatalf("configuration = %v, want 'open'", in.Configuration())
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestWriterLoggerWritesLine(t *testing.T) {
	var buf bytes.Buffer
	l := NewWriterLogger(&buf)
	l.Log("received", 42)

	want := "received: 42\n"
	if got := buf.String(); got != want {
		t.Fatalf("WriterLogger wrote %q, want %q", got, want)
	}
}

// TestWriterLoggerConcurrentLogIsRaceFree reproduces a data race in
// WriterLogger.Log: actors.System shares one configured Logger across every
// actor it activates, and each activated Instance runs on its own
// goroutine, so an unsynchronized Fprintf races against itself under
// go test -race. Concurrent Log calls must be serialized and every call
// must still land in the output.
func TestWriterLoggerConcurrentLogIsRaceFree(t *testing.T) {
	var buf bytes.Buffer
	l := NewWriterLogger(&buf)

	const goroutines = 20
	const callsEach = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < callsEach; i++ {
				l.Log("concurrent", g*callsEach+i)
			}
		}(g)
	}
	wg.Wait()

	want := goroutines * callsEach
	if got := strings.Count(buf.String(), "\n"); got != want {
		t.Fatalf("output has %d lines, want %d (some Log calls were lost or interleaved)", got, want)
	}
}

type spyLogger struct {
	logCount int
}

func (s *spyLogger) Log(string, any) { s.logCount++ }

func TestRehydrateSuppressesLoggerDuringReplayThenGoesLive(t *testing.T) {
	ctx := context.Background()
	log := newMemLog()
	store := newMemSnapshotStore()
	sessionID := SessionID("sess-logger")

	logAction := func(ec ExecContext) error {
		ec.Log("transition", nil)
		return nil
	}

	chart, err := Build(
		Compound("m", "a",
			Children(
				Atomic("a", On("go", Target("b"), Then(logAction))),
				Atomic("b", On("back", Target("a"), Then(logAction))),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	spy := &spyLogger{}
	in := New(chart, nil, WithLogger(spy))
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ev := Event{Name: "go", Type: EventExternal}
	if _, err := log.Append(ctx, LogEntry{SessionID: sessionID, Kind: KindExternalEvent, Timestamp: time.Now().UTC(), Event: ev}); err != nil {
		t.Fatalf("log.Append: %v", err)
	}
	if err := in.Send(ctx, ev); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if spy.logCount != 1 {
		t.Fatalf("live logCount = %d, want 1", spy.logCount)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	spy2 := &spyLogger{}
	in2, err := Rehydrate(ctx, chart, nil, log, store, sessionID, NoopIOProcessor, WithLogger(spy2))
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}
	if spy2.logCount != 0 {
		t.Fatalf("spy2.logCount after replay = %d, want 0 (Logger calls must be suppressed during replay)", spy2.logCount)
	}
	if !hasState(in2.Configuration(), "b") {
		t.Fatalf("configuration after replay = %v, want 'b'", in2.Configuration())
	}

	// Now live: a fresh transition through the same logging action should
	// reach the real Logger, proving the gate flips to pass-through once
	// replay has caught up.
	if err := in2.Send(ctx, Event{Name: "back", Type: EventExternal}); err != nil {
		t.Fatalf("Send after Rehydrate: %v", err)
	}
	if spy2.logCount != 1 {
		t.Fatalf("spy2.logCount after going live = %d, want 1", spy2.logCount)
	}
	if err := in2.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
