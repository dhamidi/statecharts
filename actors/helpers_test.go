package actors

import (
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
		}),
	)
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
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }),
	)
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
		}),
	)
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
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }),
	)
	if err != nil {
		panic(err)
	}
	return chart
}
