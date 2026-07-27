package statecharts

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

type recordingMacrostepObserver struct {
	enabled atomic.Bool
	traces  []MacrostepTrace
}

func (o *recordingMacrostepObserver) Enabled() bool { return o.enabled.Load() }
func (o *recordingMacrostepObserver) ObserveMacrostep(trace MacrostepTrace) {
	o.traces = append(o.traces, trace)
}

func traceTestChart(t *testing.T, root StateDefinition) *Chart {
	t.Helper()
	chart, err := Build(root, NewGoModel(func() *struct{} { return &struct{}{} }))
	if err != nil {
		t.Fatal(err)
	}
	return chart
}

func TestMacrostepTraceInitialEntryAndExternalInternalProvenance(t *testing.T) {
	chart := traceTestChart(t, Compound("root", "a", Children(
		Atomic("a", On("go", Target("b"), Then(Raise("raised")))),
		Atomic("b", On("", Target("c"))),
		Atomic("c", On("raised", Target("d"))),
		Atomic("d"),
	)))
	observer := &recordingMacrostepObserver{}
	observer.enabled.Store(true)
	clock := NewManualClock(time.Date(2026, 7, 27, 12, 0, 0, 0, time.FixedZone("test", 3600)))
	in, err := chart.NewInstance(WithClock(clock), WithMacrostepObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	if err := in.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(observer.traces) != 1 {
		t.Fatalf("initial traces = %d, want 1", len(observer.traces))
	}
	initial := observer.traces[0]
	if initial.Trigger != nil || len(initial.Before) != 0 || !reflect.DeepEqual(initial.After, []Identifier{"a"}) {
		t.Fatalf("initial trace = %#v", initial)
	}
	if len(initial.Microsteps) != 1 || !reflect.DeepEqual(initial.Microsteps[0].Entered, []Identifier{"a"}) {
		t.Fatalf("initial microsteps = %#v", initial.Microsteps)
	}

	payload, _ := MapValue(map[string]Value{"value": Int64Value(7)})
	if err := in.Send(context.Background(), Event{Name: "go", Data: payload}); err != nil {
		t.Fatal(err)
	}
	if len(observer.traces) != 2 {
		t.Fatalf("traces = %d, want 2", len(observer.traces))
	}
	trace := observer.traces[1]
	if trace.Sequence != 2 || trace.Timestamp.Location() != time.UTC || !trace.Timestamp.Equal(clock.Now()) {
		t.Fatalf("sequence/timestamp = %d/%v", trace.Sequence, trace.Timestamp)
	}
	if !reflect.DeepEqual(trace.Before, []Identifier{"a"}) || !reflect.DeepEqual(trace.After, []Identifier{"d"}) {
		t.Fatalf("before/after = %v/%v", trace.Before, trace.After)
	}
	if len(trace.Microsteps) != 3 {
		t.Fatalf("microsteps = %#v, want 3", trace.Microsteps)
	}
	if trace.Microsteps[0].Trigger.Name != "go" || trace.Microsteps[1].Trigger != nil || trace.Microsteps[2].Trigger.Name != "raised" {
		t.Fatalf("trigger provenance = %#v", trace.Microsteps)
	}
	wantRefs := []TransitionRef{{Source: "a", Index: 0}, {Source: "b", Index: 0}, {Source: "c", Index: 0}}
	for i, want := range wantRefs {
		if !reflect.DeepEqual(trace.Microsteps[i].Transitions, []TransitionRef{want}) {
			t.Fatalf("microstep %d refs = %v, want %v", i, trace.Microsteps[i].Transitions, want)
		}
	}
}

func TestMacrostepObserverCanEnableLiveAndPanicsAreDiagnostic(t *testing.T) {
	chart := traceTestChart(t, Atomic("active", On("ping")))
	observer := &recordingMacrostepObserver{}
	in, err := chart.NewInstance(WithMacrostepObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	if err := in.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(observer.traces) != 0 {
		t.Fatal("disabled observer received bootstrap")
	}
	observer.enabled.Store(true)
	if err := in.Send(context.Background(), Event{Name: "ping"}); err != nil {
		t.Fatal(err)
	}
	if len(observer.traces) != 1 {
		t.Fatalf("enabled observer traces = %d, want 1", len(observer.traces))
	}

	panicking := MacrostepObserverFunc(func(MacrostepTrace) { panic("diagnostic") })
	in2, err := chart.NewInstance(WithMacrostepObserver(panicking))
	if err != nil {
		t.Fatal(err)
	}
	if err := in2.Start(context.Background()); err != nil {
		t.Fatalf("observer panic changed Start: %v", err)
	}
	if err := in2.Send(context.Background(), Event{Name: "ping"}); err != nil {
		t.Fatalf("observer panic changed Send: %v", err)
	}
}

func TestMacrostepTraceTerminalCompletionError(t *testing.T) {
	chart := traceTestChart(t, Compound("root", "active", Children(
		Atomic("active", On("finish", Target("done"))), Final("done"),
	)))
	observer := &recordingMacrostepObserver{}
	observer.enabled.Store(true)
	want := errors.New("completion failed")
	in, err := chart.NewInstance(WithMacrostepObserver(observer), WithCompletionHook(func() error { return want }))
	if err != nil {
		t.Fatal(err)
	}
	if err := in.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := in.Send(context.Background(), Event{Name: "finish"}); !errors.Is(err, want) {
		t.Fatalf("Send error = %v, want %v", err, want)
	}
	last := observer.traces[len(observer.traces)-1]
	if !last.Terminal || last.TerminalError == "" || !errors.Is(in.Err(), want) {
		t.Fatalf("terminal trace = %#v", last)
	}
}

func TestRestoredQueuedMacrostepsCaptureTerminalIndividually(t *testing.T) {
	chart := traceTestChart(t, Compound("root", "a", Children(
		Atomic("a", On("first", Target("b"))),
		Atomic("b", On("finish", Target("done"))), Final("done"),
	)))
	base, err := chart.NewInstance()
	if err != nil {
		t.Fatal(err)
	}
	if err := base.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	snap, err := base.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := base.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	snap.ExternalQueue = []Event{{Name: "first"}, {Name: "finish"}}
	observer := &recordingMacrostepObserver{}
	observer.enabled.Store(true)
	in, err := chart.Restore(snap, WithMacrostepObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	if err := in.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(observer.traces) != 2 {
		t.Fatalf("traces = %#v", observer.traces)
	}
	if observer.traces[0].Terminal || !observer.traces[1].Terminal {
		t.Fatalf("terminal flags = %v, %v", observer.traces[0].Terminal, observer.traces[1].Terminal)
	}
}

func TestDelayedInternalSendStartsOneMacrostep(t *testing.T) {
	chart := traceTestChart(t, Compound("root", "waiting", Children(
		Atomic("waiting", OnEntry(Send("tick", SendTarget("#_internal"), SendDelay(time.Second))), On("tick", Target("done"))),
		Atomic("done"),
	)))
	observer := &recordingMacrostepObserver{}
	observer.enabled.Store(true)
	clock := NewManualClock(time.Unix(0, 0))
	in, err := chart.NewInstance(WithClock(clock), WithMacrostepObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	if err := in.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if _, err := in.Inspect(t.Context()); err != nil {
		t.Fatal(err)
	} // actor barrier
	if len(observer.traces) != 2 {
		t.Fatalf("traces = %#v, want bootstrap plus timer", observer.traces)
	}
	trace := observer.traces[1]
	if trace.Trigger == nil || trace.Trigger.Name != "tick" || len(trace.Microsteps) != 1 ||
		trace.Microsteps[0].Trigger == nil || trace.Microsteps[0].Trigger.Name != "tick" {
		t.Fatalf("timer trace provenance = %#v", trace)
	}
}

func TestMacrostepTraceObserverOwnsDeepValues(t *testing.T) {
	chart := traceTestChart(t, Compound("root", "a", Children(
		Atomic("a", On("go", Target("b"))), Atomic("b", On("back", Target("a"))),
	)))
	definitionBefore := chart.Definition()
	var first MacrostepTrace
	var subsequent MacrostepTrace
	observer := MacrostepObserverFunc(func(trace MacrostepTrace) {
		if trace.Trigger != nil && trace.Trigger.Name == "back" {
			subsequent = cloneMacrostepTrace(trace)
		}
		if trace.Trigger == nil || trace.Trigger.Name != "go" {
			return
		}
		first = cloneMacrostepTrace(trace)
		trace.Trigger.Name = "mutated"
		trace.Trigger.Data = Int64Value(99)
		trace.Before[0] = "bad-before"
		trace.After[0] = "bad-after"
		trace.Microsteps[0].Trigger.Name = "bad-step"
		trace.Microsteps[0].Trigger.Data = Int64Value(98)
		trace.Microsteps[0].Transitions[0] = TransitionRef{Source: "bad", Index: 99}
		trace.Microsteps[0].Exited[0] = "bad-exit"
		trace.Microsteps[0].Entered[0] = "bad-enter"
	})
	in, err := chart.NewInstance(WithMacrostepObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	if err := in.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := in.Send(t.Context(), Event{Name: "go", Data: Int64Value(7)}); err != nil {
		t.Fatal(err)
	}
	if got := in.Configuration(); !reflect.DeepEqual(got, []Identifier{"b"}) {
		t.Fatalf("configuration = %v", got)
	}
	if got := chart.Definition(); !reflect.DeepEqual(got, definitionBefore) {
		t.Fatal("observer mutated pinned definition")
	}
	if err := in.Send(t.Context(), Event{Name: "back", Data: Int64Value(8)}); err != nil {
		t.Fatal(err)
	}
	if first.Trigger.Name != "go" || first.Microsteps[0].Trigger.Name != "go" ||
		!reflect.DeepEqual(first.Before, []Identifier{"a"}) || !reflect.DeepEqual(first.After, []Identifier{"b"}) {
		t.Fatalf("retained trace changed = %#v", first)
	}
	if subsequent.Trigger == nil || subsequent.Trigger.Name != "back" ||
		!reflect.DeepEqual(subsequent.Microsteps[0].Transitions, []TransitionRef{{Source: "b", Index: 0}}) {
		t.Fatalf("subsequent trace = %#v", subsequent)
	}
}

func TestNonEventRequestsDoNotCreateMacrostepTraces(t *testing.T) {
	chart := traceTestChart(t, Atomic("active"))
	newInstance := func(t *testing.T) (*Instance, *recordingMacrostepObserver) {
		o := &recordingMacrostepObserver{}
		o.enabled.Store(true)
		in, err := chart.NewInstance(WithMacrostepObserver(o))
		if err != nil {
			t.Fatal(err)
		}
		if err := in.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		return in, o
	}
	in, o := newInstance(t)
	if _, err := in.Inspect(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := in.Snapshot(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(o.traces) != 1 {
		t.Fatalf("Inspect/Snapshot traces = %d", len(o.traces))
	}
	if err := in.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(o.traces) != 1 {
		t.Fatalf("Stop traces = %d", len(o.traces))
	}
	in, o = newInstance(t)
	if err := in.Checkpoint(t.Context(), func(Snapshot) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if len(o.traces) != 1 {
		t.Fatalf("Checkpoint traces = %d", len(o.traces))
	}
}

func TestUnmatchedExternalEventIsOneEmptyTraceAndOneSequence(t *testing.T) {
	chart := traceTestChart(t, Atomic("active", On("match")))
	o := &recordingMacrostepObserver{}
	o.enabled.Store(true)
	in, err := chart.NewInstance(WithMacrostepObserver(o))
	if err != nil {
		t.Fatal(err)
	}
	if err := in.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := in.Send(t.Context(), Event{Name: "unmatched"}); err != nil {
		t.Fatal(err)
	}
	if err := in.Send(t.Context(), Event{Name: "match"}); err != nil {
		t.Fatal(err)
	}
	if len(o.traces) != 3 || o.traces[1].Sequence != 2 || o.traces[2].Sequence != 3 || len(o.traces[1].Microsteps) != 0 {
		t.Fatalf("traces = %#v", o.traces)
	}
}

func TestRehydrateSuppressesMacrostepTracesUntilLive(t *testing.T) {
	chart := traceTestChart(t, Compound("root", "a", Children(
		Atomic("a", On("go", Target("b"))),
		Atomic("b", On("back", Target("a"))),
	)))
	ctx := context.Background()
	log := newMemLog()
	const sessionID SessionID = "macrostep-replay"
	if _, err := log.Append(ctx, LogEntry{
		SessionID: sessionID, Kind: KindExternalEvent, Timestamp: time.Unix(1, 0), Event: Event{Name: "go"},
	}); err != nil {
		t.Fatal(err)
	}
	observer := &recordingMacrostepObserver{}
	observer.enabled.Store(true)
	in, err := chart.Rehydrate(ctx, log, newMemSnapshotStore(), sessionID, NoopIOProcessor,
		WithMacrostepObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	if len(observer.traces) != 0 {
		t.Fatalf("replay traces = %d, want 0", len(observer.traces))
	}
	if err := in.Send(ctx, Event{Name: "back"}); err != nil {
		t.Fatal(err)
	}
	if len(observer.traces) != 1 || observer.traces[0].Trigger.Name != "back" {
		t.Fatalf("live traces = %#v, want one back trace", observer.traces)
	}
}
