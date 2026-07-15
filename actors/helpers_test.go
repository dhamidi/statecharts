package actors

import (
	"context"
	"testing"
	"time"

	"github.com/dhamidi/statecharts"
)

// hasStateID reports whether want is present in ids -- the actors package's
// own copy of the small helper the root package's tests use (unexported,
// so not reusable directly).
func hasStateID(ids []statecharts.Identifier, want statecharts.Identifier) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func mustStringValue(t *testing.T, value string) statecharts.Value {
	t.Helper()
	got, err := statecharts.StringValue(value)
	if err != nil {
		t.Fatalf("StringValue(%q): %v", value, err)
	}
	return got
}

func mustMapValue(t *testing.T, values map[string]statecharts.Value) statecharts.Value {
	t.Helper()
	got, err := statecharts.MapValue(values)
	if err != nil {
		t.Fatalf("MapValue: %v", err)
	}
	return got
}

// waitFor polls cond until it returns true or timeout elapses, failing t
// otherwise. Cross-actor delivery hops through a goroutine (see router.go),
// so tests that observe its effect need to wait for it rather than assert
// immediately after the triggering call returns.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s", timeout)
		}
		time.Sleep(time.Millisecond)
	}
}

// testResident and testInstanceFor are white-box test helpers (this file is
// part of package actors, not actors_test) that peek at a System's resident
// table directly -- the only way to observe residency from outside, since
// paging is an internal implementation detail with no public accessor.

func testResident(s *System, name statecharts.Identifier) bool {
	return testInstanceFor(s, name) != nil
}

func testInstanceFor(s *System, name statecharts.Identifier) *statecharts.Instance {
	e, ok := s.resolve(name)
	if !ok {
		return nil
	}
	return e.instance.Load()
}

// --- shared test chart builders -----------------------------------------

// counterModel is a datamodel that records how many times its Then(inc)
// action actually ran, letting tests distinguish "state resumed via
// checkpoint, action not re-run" from "action re-applied" without any
// public way to inspect a running actor's datamodel.
type counterModel struct {
	Applied int
}

// buildLadderChart returns a chart with four states chained by "inc"
// (s0->s1->s2->s3, s3 self-looping on further "inc"), each transition
// incrementing a counterModel. Every datamodel value the chart's factory
// produces is appended to sink, so a test can inspect whichever one a
// System most recently activated.
func buildLadderChart(sink *[]*counterModel) *statecharts.Chart {
	inc := statecharts.Action(func(d *counterModel, ec statecharts.ExecContext) error {
		d.Applied++
		return nil
	})
	chart, err := statecharts.Build(
		statecharts.Compound("ladder", "s0",
			statecharts.Children(
				statecharts.Atomic("s0", statecharts.On("inc", statecharts.Target("s1"), statecharts.Then(inc))),
				statecharts.Atomic("s1", statecharts.On("inc", statecharts.Target("s2"), statecharts.Then(inc))),
				statecharts.Atomic("s2", statecharts.On("inc", statecharts.Target("s3"), statecharts.Then(inc))),
				statecharts.Atomic("s3", statecharts.On("inc", statecharts.Then(inc))),
			),
		),
		statecharts.WithNewDatamodel(func() any {
			d := &counterModel{}
			*sink = append(*sink, d)
			return d
		}), statecharts.WithVersion("test-v1"))
	if err != nil {
		panic(err)
	}
	return chart
}

// locationModel records the address (via ExecContext.IOProcessorLocation) an
// action observed while running inside a System, for tests confirming an
// actor discovers its own routingProcessor-advertised name.
type locationModel struct {
	Location string
	OK       bool
}

// buildLocationChart returns a chart that, on "check", records
// ec.IOProcessorLocation(statecharts.SCXMLEventProcessor) into its datamodel and moves to
// "checked". Every datamodel value produced is appended to sink.
func buildLocationChart(sink *[]*locationModel) *statecharts.Chart {
	record := statecharts.Action(func(d *locationModel, ec statecharts.ExecContext) error {
		loc, ok := ec.IOProcessorLocation(statecharts.SCXMLEventProcessor)
		d.Location, d.OK = loc.String(), ok
		return nil
	})
	chart, err := statecharts.Build(
		statecharts.Compound("locator", "idle",
			statecharts.Children(
				statecharts.Atomic("idle", statecharts.On("check", statecharts.Target("checked"), statecharts.Then(record))),
				statecharts.Atomic("checked"),
			),
		),
		statecharts.WithNewDatamodel(func() any {
			d := &locationModel{}
			*sink = append(*sink, d)
			return d
		}), statecharts.WithVersion("test-v1"))
	if err != nil {
		panic(err)
	}
	return chart
}

// callerModel is the datamodel for buildCallerChart, recording who replied.
type callerModel struct {
	ReceivedFrom statecharts.Identifier
}

// buildResponderChart returns a single-state chart that replies "pong",
// targeted at ev.Origin, to every "ping" it receives -- the receiving half
// of the peer-messaging test pair.
func buildResponderChart() *statecharts.Chart {
	reply := func(ec statecharts.ExecContext) error {
		ev, _ := ec.Event()
		ec.Send("pong", statecharts.SendOptions{Target: ev.Origin})
		return nil
	}
	chart, err := statecharts.Build(
		statecharts.Atomic("responder", statecharts.On("ping", statecharts.Then(reply))),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		panic(err)
	}
	return chart
}

// buildCallerChart returns a chart that, on "go", sends "ping" to the named
// target and waits for "pong", recording the replying actor's name (via
// Origin) into its datamodel. Every datamodel value produced is appended to
// sink.
func buildCallerChart(sink *[]*callerModel, target statecharts.Identifier) *statecharts.Chart {
	sendPing := statecharts.Action(func(d *callerModel, ec statecharts.ExecContext) error {
		ec.Send("ping", statecharts.SendOptions{Target: target})
		return nil
	})
	recordOrigin := statecharts.Action(func(d *callerModel, ec statecharts.ExecContext) error {
		ev, _ := ec.Event()
		d.ReceivedFrom = ev.Origin
		return nil
	})
	chart, err := statecharts.Build(
		statecharts.Compound("caller", "idle",
			statecharts.Children(
				statecharts.Atomic("idle", statecharts.On("go", statecharts.Target("waiting"), statecharts.Then(sendPing))),
				statecharts.Atomic("waiting", statecharts.On("pong", statecharts.Target("done"), statecharts.Then(recordOrigin))),
				statecharts.Atomic("done"),
			),
		),
		statecharts.WithNewDatamodel(func() any {
			d := &callerModel{}
			*sink = append(*sink, d)
			return d
		}), statecharts.WithVersion("test-v1"))
	if err != nil {
		panic(err)
	}
	return chart
}

// buildInvokingChart returns a chart whose only state holds an <invoke>
// that blocks until cancelled -- a stand-in for a long-running external
// service, so a test can exercise the actor system's "don't evict while an
// invoke is active" rule (System.pickEvictionVictim, System.runSweep)
// without racing the invoke's own goroutine actually starting: recording it
// in activeInvokes/invokesByID (what statecharts.Instance.HasActiveInvokes
// observes) happens synchronously within the entering macrostep, before
// Spawn/Start even returns.
func buildInvokingChart() *statecharts.Chart {
	chart, err := statecharts.Build(
		statecharts.Atomic("invoking",
			statecharts.Invoke(func(ctx context.Context, params statecharts.Value, io statecharts.InvokeIO) (statecharts.Value, error) {
				<-ctx.Done()
				return statecharts.NullValue(), nil
			}),
		),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		panic(err)
	}
	return chart
}

// buildFinishingChart returns a chart that reaches its own top-level final
// state ("done") on "finish", for exercising the actor system's eviction of
// an actor that has stopped on its own (System.reapFinished).
func buildFinishingChart() *statecharts.Chart {
	chart, err := statecharts.Build(
		statecharts.Compound("finisher", "running",
			statecharts.Children(
				statecharts.Atomic("running", statecharts.On("finish", statecharts.Target("done"))),
				statecharts.Final("done"),
			),
		),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		panic(err)
	}
	return chart
}

// buildDelayedFinishingChart is buildFinishingChart's counterpart for
// reaching the final state entirely from an internal delayed <send> --
// with delay -- rather than from an externally Told event, so a test can
// confirm System's periodic sweep (not just its inline post-Deliver check)
// is what eventually reaps it.
func buildDelayedFinishingChart(delay time.Duration) *statecharts.Chart {
	chart, err := statecharts.Build(
		statecharts.Compound("delayedFinisher", "running",
			statecharts.Children(
				statecharts.Atomic("running",
					statecharts.OnEntry(statecharts.SendEvent("finish", statecharts.SendOptions{Delay: delay})),
					statecharts.On("finish", statecharts.Target("done")),
				),
				statecharts.Final("done"),
			),
		),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		panic(err)
	}
	return chart
}

// buildInitAbortChart returns a durable-timer test chart whose "init" event
// enters running and schedules a two-second self-send to enter aborted.
func buildInitAbortChart() *statecharts.Chart {
	chart, err := statecharts.Build(
		statecharts.Compound("operation", "idle",
			statecharts.Children(
				statecharts.Atomic("idle", statecharts.On("init", statecharts.Target("running"),
					statecharts.Then(statecharts.SendEvent("abort", statecharts.SendOptions{
						SendID: "abort-operation",
						Delay:  2 * time.Second,
					})))),
				statecharts.Atomic("running", statecharts.On("abort", statecharts.Target("aborted"))),
				statecharts.Atomic("aborted"),
			),
		),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		panic(err)
	}
	return chart
}

// buildCommTestChart returns a chart that sends to an unaddressable target
// on "go" and reacts to the resulting error.communication with an
// observable state transition, for tests that need to see a dispatch
// failure without a Go panic or a silent drop.
func buildCommTestChart(unknownTarget statecharts.Identifier) *statecharts.Chart {
	chart, err := statecharts.Build(
		statecharts.Compound("commtest", "idle",
			statecharts.Children(
				statecharts.Atomic("idle", statecharts.On("go", statecharts.Target("waiting"),
					statecharts.Then(statecharts.SendEvent("ping", statecharts.SendOptions{Target: unknownTarget})))),
				statecharts.Atomic("waiting", statecharts.On("error.communication", statecharts.Target("failed"))),
				statecharts.Atomic("failed"),
			),
		),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		panic(err)
	}
	return chart
}

type asyncFailureModel struct {
	Event statecharts.Event
	Seen  bool
}

func buildAsyncFailureSender(sink *[]*asyncFailureModel, target statecharts.Identifier) *statecharts.Chart {
	send := statecharts.Action(func(_ *asyncFailureModel, ec statecharts.ExecContext) error {
		ec.Send("ping", statecharts.SendOptions{SendID: "request-7", Target: target})
		return nil
	})
	record := statecharts.Action(func(d *asyncFailureModel, ec statecharts.ExecContext) error {
		d.Event, d.Seen = ec.Event()
		return nil
	})
	chart, err := statecharts.Build(
		statecharts.Compound("async-failure-sender", "idle",
			statecharts.Children(
				statecharts.Atomic("idle", statecharts.On("go", statecharts.Target("waiting"), statecharts.Then(send))),
				statecharts.Atomic("waiting", statecharts.On(string(statecharts.ErrEventCommunication), statecharts.Target("failed"), statecharts.Then(record))),
				statecharts.Atomic("failed"),
			),
		),
		statecharts.WithNewDatamodel(func() any {
			d := &asyncFailureModel{}
			*sink = append(*sink, d)
			return d
		}), statecharts.WithVersion("test-v1"))
	if err != nil {
		panic(err)
	}
	return chart
}
