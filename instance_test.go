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
	chart, err := Build(
		Compound("m", "a",
			Children(
				Atomic("a", On("go", Target("b"))),
				Atomic("b"),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, nil)
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Send then immediately Stop: FIFO ordering through the same ingress
	// path guarantees the Send's effect lands before Stop takes hold.
	if err := in.Send(ctx, Event{Name: "go", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := in.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	if !hasState(in.Configuration(), "b") {
		t.Fatalf("configuration = %v, want 'b' (Send should be processed before Stop)", in.Configuration())
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
