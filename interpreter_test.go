package statecharts

import (
	"errors"
	"testing"
)

func hasState(ids []Identifier, want Identifier) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func TestInterpreterBasicTransitionAndGuard(t *testing.T) {
	notLocked := Cond(func(d *Door, ec ExecContext) bool { return !d.Locked })
	recordOpen := Action(func(d *Door, ec ExecContext) error { d.OpenCount++; return nil })

	chart, err := Build(
		Compound("door", "closed",
			Children(
				Atomic("closed",
					On("open.request", Target("open"), If(notLocked), Then(recordOpen)),
				),
				Atomic("open",
					On("close.request", Target("closed")),
				),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	d := &Door{Locked: true}
	ip := newInterpretation(chart, d)
	ip.start()

	if got := ip.activeStates(); !hasState(got, "closed") {
		t.Fatalf("initial configuration = %v, want to contain 'closed'", got)
	}

	// locked: guard blocks the transition
	ip.enqueue(Event{Name: "open.request", Type: EventExternal})
	ip.processNextExternal()
	if got := ip.activeStates(); !hasState(got, "closed") {
		t.Fatalf("after blocked transition, configuration = %v, want still 'closed'", got)
	}
	if d.OpenCount != 0 {
		t.Fatalf("OpenCount = %d, want 0 (guard should have blocked action)", d.OpenCount)
	}

	// unlock, try again
	d.Locked = false
	ip.enqueue(Event{Name: "open.request", Type: EventExternal})
	ip.processNextExternal()
	if got := ip.activeStates(); !hasState(got, "open") || hasState(got, "closed") {
		t.Fatalf("after transition, configuration = %v, want only 'open'", got)
	}
	if d.OpenCount != 1 {
		t.Fatalf("OpenCount = %d, want 1", d.OpenCount)
	}
}

func TestInterpreterParallelRegionsIndependent(t *testing.T) {
	chart, err := Build(
		Parallel("machine",
			Children(
				Compound("motor", "off",
					Children(
						Atomic("off", On("motor.start", Target("on"))),
						Atomic("on", On("motor.stop", Target("off"))),
					),
				),
				Compound("light", "dark",
					Children(
						Atomic("dark", On("light.on", Target("lit"))),
						Atomic("lit", On("light.off", Target("dark"))),
					),
				),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	ip := newInterpretation(chart, nil)
	ip.start()

	got := ip.activeStates()
	if !hasState(got, "off") || !hasState(got, "dark") {
		t.Fatalf("initial configuration = %v, want both 'off' and 'dark'", got)
	}

	// firing motor.start should only affect the motor region
	ip.enqueue(Event{Name: "motor.start", Type: EventExternal})
	ip.processNextExternal()
	got = ip.activeStates()
	if !hasState(got, "on") || hasState(got, "off") {
		t.Fatalf("motor region = %v, want only 'on'", got)
	}
	if !hasState(got, "dark") {
		t.Fatalf("light region changed unexpectedly: %v", got)
	}

	// and light.on should only affect the light region
	ip.enqueue(Event{Name: "light.on", Type: EventExternal})
	ip.processNextExternal()
	got = ip.activeStates()
	if !hasState(got, "on") || !hasState(got, "lit") || hasState(got, "dark") {
		t.Fatalf("configuration after both events = %v, want 'on' and 'lit'", got)
	}
}

func TestInterpreterShallowHistory(t *testing.T) {
	chart, err := Build(
		Compound("app", "running",
			Children(
				Compound("running", "step1",
					Children(
						Atomic("step1", On("next", Target("step2"))),
						Atomic("step2", On("next", Target("step3"))),
						Atomic("step3"),
						History("running.hist", Shallow, "step1"),
					),
				),
				Atomic("paused", On("resume", Target("running.hist"))),
			),
			On("pause", Target("paused")),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	ip := newInterpretation(chart, nil)
	ip.start()

	ip.enqueue(Event{Name: "next", Type: EventExternal})
	ip.processNextExternal() // now at step2

	ip.enqueue(Event{Name: "pause", Type: EventExternal})
	ip.processNextExternal() // now at paused; history should record step2

	got := ip.activeStates()
	if !hasState(got, "paused") || hasState(got, "step2") {
		t.Fatalf("after pause, configuration = %v, want only 'paused'", got)
	}

	ip.enqueue(Event{Name: "resume", Type: EventExternal})
	ip.processNextExternal()

	got = ip.activeStates()
	if !hasState(got, "step2") {
		t.Fatalf("after resume, configuration = %v, want to contain 'step2' (shallow history)", got)
	}
}

func TestInterpreterDeepHistory(t *testing.T) {
	chart, err := Build(
		Compound("app", "running",
			Children(
				Compound("running", "phase1",
					Children(
						Compound("phase1", "sub1",
							Children(
								Atomic("sub1", On("next", Target("sub2"))),
								Atomic("sub2"),
							),
						),
						History("running.hist", Deep, "phase1"),
					),
				),
				Atomic("paused", On("resume", Target("running.hist"))),
			),
			On("pause", Target("paused")),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	ip := newInterpretation(chart, nil)
	ip.start()

	ip.enqueue(Event{Name: "next", Type: EventExternal})
	ip.processNextExternal() // now at phase1/sub2

	if got := ip.activeStates(); !hasState(got, "sub2") {
		t.Fatalf("expected to be at sub2 before pausing, got %v", got)
	}

	ip.enqueue(Event{Name: "pause", Type: EventExternal})
	ip.processNextExternal()

	ip.enqueue(Event{Name: "resume", Type: EventExternal})
	ip.processNextExternal()

	got := ip.activeStates()
	if !hasState(got, "sub2") {
		t.Fatalf("after resume, configuration = %v, want to contain 'sub2' (deep history)", got)
	}
	if !hasState(got, "phase1") {
		t.Fatalf("after resume, configuration = %v, want to contain ancestor 'phase1'", got)
	}
}

func TestInterpreterEventlessTransition(t *testing.T) {
	type counter struct{ n int }
	c := &counter{}
	incr := Action(func(d *counter, ec ExecContext) error { d.n++; return nil })

	chart, err := Build(
		Compound("m", "a",
			Children(
				Atomic("a", OnEntry(incr), Eventless(Target("b"))),
				Atomic("b"),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	ip := newInterpretation(chart, c)
	ip.start()

	got := ip.activeStates()
	if !hasState(got, "b") || hasState(got, "a") {
		t.Fatalf("configuration after start = %v, want automatic advance straight to 'b'", got)
	}
	if c.n != 1 {
		t.Fatalf("onEntry(a) ran %d times, want exactly 1", c.n)
	}
}

func TestInterpreterInternalVsExternalTransition(t *testing.T) {
	type counter struct{ entries int }
	c := &counter{}
	incr := Action(func(d *counter, ec ExecContext) error { d.entries++; return nil })

	// The internal/external distinction only matters when the transition's
	// own source is the compound state whose re-entry is in question (here,
	// "parent" itself transitioning to its own child "child2") -- a
	// transition declared on a mere sibling child would always resolve its
	// domain to the shared parent regardless of the internal flag.
	buildChart := func(internal bool) *Chart {
		opts := []TransitionOption{Target("child2")}
		if internal {
			opts = append(opts, AsInternal())
		}
		chart, err := Build(
			Compound("root", "parent",
				Children(
					Compound("parent", "child1",
						Children(
							Atomic("child1"),
							Atomic("child2"),
						),
						OnEntry(incr),
						On("go", opts...),
					),
				),
			),
		)
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return chart
	}

	// external (default): parent is re-entered (exited+entered), so its
	// onEntry runs a second time.
	c.entries = 0
	chart := buildChart(false)
	ip := newInterpretation(chart, c)
	ip.start()
	if c.entries != 1 {
		t.Fatalf("initial parent onEntry count = %d, want 1", c.entries)
	}
	ip.enqueue(Event{Name: "go", Type: EventExternal})
	ip.processNextExternal()
	if c.entries != 2 {
		t.Fatalf("external transition: parent onEntry count = %d, want 2 (re-entered)", c.entries)
	}

	// internal: parent is NOT exited/re-entered.
	c.entries = 0
	chart = buildChart(true)
	ip = newInterpretation(chart, c)
	ip.start()
	if c.entries != 1 {
		t.Fatalf("initial parent onEntry count = %d, want 1", c.entries)
	}
	ip.enqueue(Event{Name: "go", Type: EventExternal})
	ip.processNextExternal()
	if c.entries != 1 {
		t.Fatalf("internal transition: parent onEntry count = %d, want 1 (not re-entered)", c.entries)
	}
	got := ip.activeStates()
	if !hasState(got, "child2") {
		t.Fatalf("configuration after internal transition = %v, want to contain 'child2'", got)
	}
}

func TestInterpreterDoneStateEvent(t *testing.T) {
	chart, err := Build(
		Compound("app", "working",
			Children(
				Compound("working", "step1",
					Children(
						Atomic("step1", On("finish", Target("done"))),
						Final("done"),
					),
				),
				Atomic("finished"),
			),
			On("done.state.working", Target("finished")),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	ip := newInterpretation(chart, nil)
	ip.start()
	ip.enqueue(Event{Name: "finish", Type: EventExternal})
	ip.processNextExternal()

	got := ip.activeStates()
	if !hasState(got, "finished") {
		t.Fatalf("configuration = %v, want to contain 'finished' after done.state.working propagated", got)
	}
}

func TestInterpreterTopLevelFinalStops(t *testing.T) {
	chart, err := Build(
		Compound("app", "running",
			Children(
				Atomic("running", On("finish", Target("done"))),
				Final("done"),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	ip := newInterpretation(chart, nil)
	ip.start()
	if !ip.running {
		t.Fatalf("expected running=true after start")
	}
	ip.enqueue(Event{Name: "finish", Type: EventExternal})
	ip.processNextExternal()
	if ip.running {
		t.Fatalf("expected running=false after entering top-level final state")
	}
}

// SCXML 5.10.1: every event carries the sendid of whichever <send>
// produced it, so a handler can correlate a reply (or a self-raised
// follow-up) back to the specific send that generated it. Self-delivered
// sends -- the default target and "#_internal" -- went through a
// different path than genuinely external ones (which only ever attach
// SendID to the IOProcessor's SendRequest) and silently dropped it.
func TestInterpreterSendIDPropagatesToSelfDeliveredEvents(t *testing.T) {
	var gotSendID Identifier
	chart, err := Build(
		Atomic("only",
			OnEntry(Action(func(d *struct{}, ec ExecContext) error {
				ec.Send("ping", SendOptions{SendID: "my-id", Target: "#_internal"})
				return nil
			})),
			On("ping", Then(Action(func(d *struct{}, ec ExecContext) error {
				ev, _ := ec.Event()
				gotSendID = ev.SendID
				return nil
			}))),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	ip := newInterpretation(chart, &struct{}{})
	ip.start()

	if gotSendID != "my-id" {
		t.Fatalf("_event.sendid = %q, want %q", gotSendID, "my-id")
	}
}

// SCXML Appendix D's exitInterpreter() procedure requires every state still
// in the configuration -- not just the one whose entry flipped running to
// false -- to run its onexit handlers, in exit order, once the machine
// stops. A parallel region untouched by the transition into the top-level
// final state is exactly the case a naive "just stop the loop"
// implementation misses.
func TestInterpreterExitInterpreterRunsRemainingOnExit(t *testing.T) {
	var exitOrder []Identifier
	record := func(id Identifier) ActionFunc {
		return func(ec ExecContext) error {
			exitOrder = append(exitOrder, id)
			return nil
		}
	}

	chart, err := Build(
		Compound("root", "running",
			Children(
				Parallel("running",
					Children(
						Compound("a", "a1",
							Children(
								Atomic("a1", On("go", Target("aDone"))),
								Final("aDone"),
							),
						),
						Compound("b", "b1",
							Children(
								Atomic("b1", OnExit(record("b1"))),
							),
							OnExit(record("b")),
						),
					),
					OnExit(record("running")),
				),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	ip := newInterpretation(chart, nil)
	ip.start()
	ip.enqueue(Event{Name: "go", Type: EventExternal})
	ip.processNextExternal()

	// Region "a" reached its final state, but "running" (parallel) isn't
	// done since region "b" is still in "b1" -- nothing should have exited
	// yet.
	if len(exitOrder) != 0 {
		t.Fatalf("exitOrder = %v, want empty -- chart is still running", exitOrder)
	}
	if !ip.running {
		t.Fatalf("expected running=true; only one of two parallel regions reached final")
	}

	ip.running = false
	ip.exitInterpreter()

	want := []Identifier{"b1", "b", "running"}
	if len(exitOrder) != len(want) {
		t.Fatalf("exitOrder = %v, want %v", exitOrder, want)
	}
	for i, id := range want {
		if exitOrder[i] != id {
			t.Fatalf("exitOrder = %v, want %v", exitOrder, want)
		}
	}
}

// A non-nil error returned by an ActionFunc is reported as an
// error.execution platform event (SCXML 5.10.2/C.1), not returned to any
// caller as a Go error -- a sibling transition armed against it must be
// able to match and fire on it, mirroring how error.communication already
// works for a failing <invoke> (see TestInvokeErrorBecomesCommunicationError).
func TestInterpreterActionErrorBecomesExecutionErrorEvent(t *testing.T) {
	boom := errors.New("boom")
	failing := Action(func(d *Door, ec ExecContext) error { return boom })

	var gotErr error
	recordErr := Action(func(d *Door, ec ExecContext) error {
		ev, _ := ec.Event()
		gotErr, _ = ev.Data.(error)
		return nil
	})

	chart, err := Build(
		Compound("m", "a",
			Children(
				Atomic("a",
					OnEntry(failing),
					On(string(ErrEventExecution), Target("b"), Then(recordErr)),
				),
				Atomic("b"),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	d := &Door{}
	ip := newInterpretation(chart, d)
	ip.start()

	if got := ip.activeStates(); !hasState(got, "b") {
		t.Fatalf("configuration = %v, want to contain 'b' after error.execution transition", got)
	}
	if gotErr != boom {
		t.Fatalf("error.execution Data = %v, want %v", gotErr, boom)
	}
}
