package statecharts

import (
	"bytes"
	"context"
	"fmt"
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
	data  Value
}

func (l *recordingLogger) Log(label string, data Value) {
	l.calls = append(l.calls, recordedLogCall{label: label, data: data})
}

type panicLogger struct{}

func (panicLogger) Log(string, Value) { panic("logger failed") }

func TestLoggerPanicDoesNotAffectInterpretation(t *testing.T) {
	var actions []string
	model := NewGoModel(func() *struct{} { return &struct{}{} })
	logThenRecord, err := model.Action("logger.log-then-record", "v1", func(_ *struct{}, ec ExecContext, _ []Value) error {
		ec.Log("diagnostic", Int64Value(42))
		actions = append(actions, "after-log")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	recordNext, err := model.Action("logger.record-next-action", "v1", func(_ *struct{}, _ ExecContext, _ []Value) error {
		actions = append(actions, "next-action")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	chart, err := Build(
		Compound("root", "active",
			Children(
				Atomic("active",
					On("go", Target("target"), Then(logThenRecord.Do(), recordNext.Do())),
				),
				Atomic("target", On(string(ErrEventExecution), Target("wrong"))),
				Atomic("wrong"),
			),
		), model, WithRevisionSalt("logger-panic-v1"),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	ip, err := chart.NewInstance(WithLogger(panicLogger{}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := ip.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer ip.Stop(ctx)
	if err := ip.Send(ctx, Event{Name: "go", Type: EventExternal}); err != nil {
		t.Fatal(err)
	}

	if got, want := strings.Join(actions, ","), "after-log,next-action"; got != want {
		t.Fatalf("actions after logger panic = %q, want %q", got, want)
	}
	if !hasState(ip.Configuration(), "target") {
		t.Fatalf("configuration = %v, want target; logger panic must not produce error.execution", ip.Configuration())
	}
}

func TestExecContextLogCallsConfiguredLogger(t *testing.T) {
	model := NewGoModel(func() *Door { return &Door{} })

	chart, err := Build(
		Compound("door", "closed",
			Children(
				Atomic("closed", On("open.request", Target("open"))),
				Atomic("open", OnEntry(LogValue("entered", GoLiteral(testStringValue("open"))))),
			),
		), model, WithRevisionSalt("logger-configured-v1"),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	rec := &recordingLogger{}
	in, err := chart.NewInstance(WithLogger(rec))
	if err != nil {
		t.Fatal(err)
	}
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
	if rec.calls[0].label != "entered" || !rec.calls[0].data.Equal(testStringValue("open")) {
		t.Fatalf("logger recorded %+v, want {entered open}", rec.calls[0])
	}
}

func TestExecContextLogClonesValueAtEvaluation(t *testing.T) {
	source := map[string]Value{"count": Int64Value(1)}
	payload := Value{kind: ValueKindMap, object: source}
	rec := &recordingLogger{}
	model := NewGoModel(func() *struct{} { return &struct{}{} })
	mutate, err := model.Action("logger.mutate-source", "v1", func(_ *struct{}, _ ExecContext, _ []Value) error { source["count"] = Int64Value(9); return nil })
	if err != nil {
		t.Fatal(err)
	}
	chart, err := Build(Atomic("ready", OnEntry(LogValue("payload", GoLiteral(payload)), mutate.Do())), model, WithRevisionSalt("logger-clone-v1"))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	in, err := chart.NewInstance(WithLogger(rec))
	if err != nil {
		t.Fatal(err)
	}
	if err := in.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer in.Stop(context.Background())
	if len(rec.calls) != 1 {
		t.Fatalf("logger recorded %d calls, want 1", len(rec.calls))
	}
	fields, ok := rec.calls[0].data.AsMap()
	if !ok || !fields["count"].Equal(Int64Value(1)) {
		t.Fatalf("logged payload = %#v, want isolated count=1", rec.calls[0].data)
	}
}

// A chart that never calls ExecContext.Log, and one that does but was built
// with no WithLogger option, must behave identically -- Log is a silent
// no-op with nothing configured, whether that's a bare ExecContext{} or a
// default Instance.
func TestExecContextLogIsNoopWithoutLogger(t *testing.T) {
	var bare ExecContext
	bare.Log("label", testStringValue("data")) // must not panic

	model := NewGoModel(func() *Door { return &Door{} })
	chart, err := Build(
		Compound("door", "closed",
			Children(
				Atomic("closed", On("open.request", Target("open"))),
				Atomic("open", OnEntry(LogValue("entered", GoLiteral(testStringValue("open"))))),
			),
		), model, WithRevisionSalt("logger-noop-v1"),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in, err := chart.NewInstance() // no WithLogger -- defaults to NoopLogger
	if err != nil {
		t.Fatal(err)
	}
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
	l.Log("received", Int64Value(42))

	want := fmt.Sprintf("received: %v\n", Int64Value(42))
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
				l.Log("concurrent", Int64Value(int64(g*callsEach+i)))
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

func (s *spyLogger) Log(string, Value) { s.logCount++ }

func TestRehydrateSuppressesLoggerDuringReplayThenGoesLive(t *testing.T) {
	ctx := context.Background()
	log := newMemLog()
	store := newMemSnapshotStore()
	sessionID := SessionID("sess-logger")

	model := NewGoModel(func() *struct{} { return &struct{}{} })
	chart, err := Build(
		Compound("m", "a",
			Children(
				Atomic("a", On("go", Target("b"), Then(LogValue("transition", GoLiteral(Value{}))))),
				Atomic("b", On("back", Target("a"), Then(LogValue("transition", GoLiteral(Value{}))))),
			),
		), model, WithRevisionSalt("logger-rehydrate-v1"),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	spy := &spyLogger{}
	in, err := chart.NewInstance(WithLogger(spy))
	if err != nil {
		t.Fatal(err)
	}
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
	in2, err := chart.Rehydrate(ctx, log, store, sessionID, NoopIOProcessor, WithLogger(spy2))
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
