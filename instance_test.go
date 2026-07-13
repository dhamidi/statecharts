package statecharts

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestInstanceBasicLifecycle(t *testing.T) {
	notLocked := Cond(func(d *Door, ec ExecContext) bool { return !d.Locked })
	recordOpen := Action(func(d *Door, ec ExecContext) error { d.OpenCount++; return nil })

	chart, err := Build(
		Compound("door", "closed",
			Children(
				Atomic("closed", On("open.request", Target("open"), If(notLocked), Then(recordOpen))),
				Atomic("open", On("close.request", Target("closed"))),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	d := &Door{}
	in := New(chart, d)
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !hasState(in.Configuration(), "closed") {
		t.Fatalf("initial configuration = %v, want 'closed'", in.Configuration())
	}

	if err := in.Send(ctx, Event{Name: "open.request", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if !hasState(in.Configuration(), "open") {
		t.Fatalf("configuration after Send = %v, want 'open'", in.Configuration())
	}
	if d.OpenCount != 1 {
		t.Fatalf("OpenCount = %d, want 1", d.OpenCount)
	}

	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := in.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if err := in.Err(); err != nil {
		t.Fatalf("Err() after clean stop = %v, want nil", err)
	}

	// Send/Stop ordering: after Stop, further Sends must not hang, and
	// must never be silently dropped while reporting success.
	if err := in.Send(ctx, Event{Name: "open.request", Type: EventExternal}); !errors.Is(err, ErrInstanceStopped) {
		t.Fatalf("Send after Stop: got %v, want ErrInstanceStopped", err)
	}
}

func TestInstanceSendStopOrdering(t *testing.T) {
	var enteredB bool
	chart, err := Build(
		Compound("m", "a",
			Children(
				Atomic("a", On("go", Target("b"))),
				Atomic("b", OnEntry(Action(func(d *struct{}, ec ExecContext) error {
					enteredB = true
					return nil
				}))),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, &struct{}{})
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Send then immediately Stop: FIFO ordering through the same ingress
	// path guarantees the Send's effect lands before Stop takes hold. Once
	// Stop takes hold, exitInterpreter empties the configuration (SCXML
	// Appendix D), so "b" was reached is checked via a side effect of
	// entering it, not via a post-Stop Configuration() snapshot.
	if err := in.Send(ctx, Event{Name: "go", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := in.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	if !enteredB {
		t.Fatalf("expected 'b' to have been entered before Stop took hold")
	}
}

func TestInstanceStopRunsOnExitForRemainingStates(t *testing.T) {
	var exited bool
	chart, err := Build(
		Compound("m", "a",
			Children(
				Atomic("a", OnExit(Action(func(d *struct{}, ec ExecContext) error {
					exited = true
					return nil
				}))),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, &struct{}{})
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !exited {
		t.Fatalf("Stop() must run onexit for every state still active (SCXML Appendix D exitInterpreter), but it did not run")
	}
}

func TestInstanceNaturalTerminationRunsOnExitForRemainingStates(t *testing.T) {
	// The chart's root StateSpec plays the role of SCXML's own <scxml>
	// wrapper element -- never itself a member of the configuration (see
	// interpretation.start(), which stops addAncestorStatesToEnter before
	// reaching it) -- so "done" must be a direct child of root for
	// reaching it to flip running to false at all; "app" is the
	// intermediate, real ancestor whose onexit must still fire once it
	// does.
	var exitedApp, exitedFinal bool
	chart, err := Build(
		Compound("root", "app",
			Children(
				Compound("app", "a",
					Children(
						Atomic("a", On("go", Target("done"))),
					),
					OnExit(Action(func(d *struct{}, ec ExecContext) error {
						exitedApp = true
						return nil
					})),
				),
				Final("done", OnExit(Action(func(d *struct{}, ec ExecContext) error {
					exitedFinal = true
					return nil
				}))),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, &struct{}{})
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := in.Send(ctx, Event{Name: "go", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := in.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !exitedApp || !exitedFinal {
		t.Fatalf("reaching a top-level final state must run onexit for every remaining active state (app=%v, done=%v)", exitedApp, exitedFinal)
	}
}

func TestInstancePanicBecomesTerminalError(t *testing.T) {
	boom := Action(func(d *struct{}, ec ExecContext) error {
		panic("boom")
	})

	chart, err := Build(
		Compound("m", "a",
			Children(
				Atomic("a", On("go", Target("b"), Then(boom))),
				Atomic("b"),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, &struct{}{})
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// The Send call itself may or may not observe an error (the panic
	// happens while processing, possibly after the reply is already sent);
	// what matters is that the instance terminates with a non-nil Err().
	_ = in.Send(ctx, Event{Name: "go", Type: EventExternal})

	if err := in.Wait(ctx); err == nil {
		t.Fatalf("Wait() after panic: expected non-nil error")
	}
	if in.Err() == nil {
		t.Fatalf("Err() after panic: expected non-nil error")
	}
}

func TestInstanceDelayedSendFiresAndCanBeCancelled(t *testing.T) {
	scheduleTimeout := Action(func(d *struct{}, ec ExecContext) error {
		ec.Send("timeout", SendOptions{SendID: "t1", Delay: 5 * time.Second})
		return nil
	})

	chart, err := Build(
		Compound("m", "waiting",
			Children(
				Atomic("waiting", OnEntry(scheduleTimeout), On("timeout", Target("done"))),
				Atomic("done"),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	clock := NewManualClock(time.Unix(0, 0))
	in := New(chart, &struct{}{}, WithClock(clock))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	clock.Advance(5 * time.Second)
	// synchronize with the actor goroutine: since both this send and the
	// timer hand-off go through the same FIFO inbox, waiting for this
	// reply guarantees the timer-fired event was already processed.
	if err := in.Send(ctx, Event{Name: "noop", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if !hasState(in.Configuration(), "done") {
		t.Fatalf("configuration = %v, want 'done' after delayed send fired", in.Configuration())
	}
}

func TestInstanceCancelledSendNeverFires(t *testing.T) {
	scheduleAndCancel := Action(func(d *struct{}, ec ExecContext) error {
		ec.Send("timeout", SendOptions{SendID: "t1", Delay: 5 * time.Second})
		ec.Cancel("t1")
		return nil
	})

	chart, err := Build(
		Compound("m", "waiting",
			Children(
				Atomic("waiting", OnEntry(scheduleAndCancel), On("timeout", Target("done"))),
				Atomic("done"),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	clock := NewManualClock(time.Unix(0, 0))
	in := New(chart, &struct{}{}, WithClock(clock))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	clock.Advance(5 * time.Second)
	if err := in.Send(ctx, Event{Name: "noop", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if !hasState(in.Configuration(), "waiting") {
		t.Fatalf("configuration = %v, want still 'waiting' (send was cancelled)", in.Configuration())
	}
}
