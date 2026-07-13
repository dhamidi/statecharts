package actors

import (
	"context"
	"testing"
	"time"

	"github.com/dhamidi/statecharts"
)

// TestFallbackRoutesToOtherSystemAndReplyRoutesBack is the round-trip case
// WithFallback and Bridge exist for: an actor in sysA addresses a
// namespaced name that belongs to sysB, sysA's own routing doesn't
// recognize it, the fallback forwards it into sysB, and the reply -- sent
// by the responding actor to ev.Origin, exactly like any other reply --
// finds its way back to the original sender via sysB's own fallback.
func TestFallbackRoutesToOtherSystemAndReplyRoutesBack(t *testing.T) {
	ctx := context.Background()

	responder := buildResponderChart()
	var dms []*callerModel
	caller := buildCallerChart(&dms, "warehouse-b.responder-1")

	// sysA's Bridge needs sysB as its target, but sysB's own Bridge needs
	// sysA -- built first here -- as its target, so sysA's Bridge starts
	// with a nil target and is wired up with SetTarget once sysB exists.
	bridgeToB := NewBridge("warehouse-b", nil, "warehouse-a")
	sysA := NewSystem(WithFallback(bridgeToB))
	if err := sysA.Register(caller); err != nil {
		t.Fatalf("Register(caller): %v", err)
	}
	if err := sysA.Spawn(ctx, "caller-1", caller.ID()); err != nil {
		t.Fatalf("Spawn(caller-1): %v", err)
	}

	sysB := NewSystem(WithFallback(NewBridge("warehouse-a", sysA, "warehouse-b")))
	if err := sysB.Register(responder); err != nil {
		t.Fatalf("Register(responder): %v", err)
	}
	if err := sysB.Spawn(ctx, "responder-1", responder.ID()); err != nil {
		t.Fatalf("Spawn(responder-1): %v", err)
	}
	bridgeToB.SetTarget(sysB)

	if err := sysA.Tell(ctx, "caller-1", statecharts.Event{Name: "go", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell: %v", err)
	}

	inst := testInstanceFor(sysA, "caller-1")
	if inst == nil {
		t.Fatalf("caller-1 not resident after Spawn")
	}
	waitFor(t, 2*time.Second, func() bool { return hasStateID(inst.Configuration(), "done") })

	if len(dms) == 0 {
		t.Fatalf("caller datamodel count = 0, want at least 1")
	}
	live := dms[len(dms)-1]
	want := statecharts.Identifier("warehouse-b.responder-1")
	if live.ReceivedFrom != want {
		t.Fatalf("ReceivedFrom = %q, want %q", live.ReceivedFrom, want)
	}

	if err := sysA.Stop(ctx); err != nil {
		t.Fatalf("sysA.Stop: %v", err)
	}
	if err := sysB.Stop(ctx); err != nil {
		t.Fatalf("sysB.Stop: %v", err)
	}
}

// TestFallbackRejectsNameOutsideItsNamespace is finding-shaped coverage for
// "no regression to single-system behavior": a target that carries no
// namespace a configured fallback recognizes must still surface as an
// ordinary "unknown actor" communication error, exactly as it would with no
// fallback configured at all.
func TestFallbackRejectsNameOutsideItsNamespace(t *testing.T) {
	ctx := context.Background()
	chart := buildCommTestChart("nobody-home")

	sysB := NewSystem()
	sysA := NewSystem(WithFallback(NewBridge("warehouse-b", sysB, "warehouse-a")))
	if err := sysA.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sysA.Spawn(ctx, "m-1", chart.ID()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	if err := sysA.Tell(ctx, "m-1", statecharts.Event{Name: "go", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell: %v", err)
	}

	inst := testInstanceFor(sysA, "m-1")
	if inst == nil {
		t.Fatalf("m-1 not resident after Spawn")
	}
	if !hasStateID(inst.Configuration(), "failed") {
		t.Fatalf("configuration = %v, want to contain 'failed' for a name outside the fallback's namespace", inst.Configuration())
	}

	if err := sysA.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestFallbackRejectsNameNotSpawnedInTargetSystem covers the other half of
// Bridge.Send's synchronous check: the namespace matches, but the name that
// remains was never spawned in the target System.
func TestFallbackRejectsNameNotSpawnedInTargetSystem(t *testing.T) {
	ctx := context.Background()
	chart := buildCommTestChart("warehouse-b.billing")

	sysB := NewSystem() // "billing" never spawned here
	sysA := NewSystem(WithFallback(NewBridge("warehouse-b", sysB, "warehouse-a")))
	if err := sysA.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sysA.Spawn(ctx, "m-1", chart.ID()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	if err := sysA.Tell(ctx, "m-1", statecharts.Event{Name: "go", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell: %v", err)
	}

	inst := testInstanceFor(sysA, "m-1")
	if inst == nil {
		t.Fatalf("m-1 not resident after Spawn")
	}
	if !hasStateID(inst.Configuration(), "failed") {
		t.Fatalf("configuration = %v, want to contain 'failed' for a name the target system never spawned", inst.Configuration())
	}

	if err := sysA.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestFallbackSendDoesNotBlockSender proves the non-blocking contract every
// IOProcessor.Send must honor (see the package doc and WithFallback) holds
// for the fallback path too: the target actor's own action is wedged
// indefinitely, so if Bridge.Send (or routingProcessor.Send's delegation to
// it) blocked on delivery, sysA.Tell below would hang until the test's
// deadline. Instead it must return almost immediately, well before release
// is ever closed.
func TestFallbackSendDoesNotBlockSender(t *testing.T) {
	ctx := context.Background()

	release := make(chan struct{})
	defer close(release)

	wedge := statecharts.Action(func(_ *struct{}, ec statecharts.ExecContext) error {
		<-release // never returns until the test releases it
		return nil
	})
	wedgedChart, err := statecharts.Build(
		statecharts.Atomic("wedged", statecharts.On("ping", statecharts.Then(wedge))),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }),
	)
	if err != nil {
		t.Fatalf("Build(wedgedChart): %v", err)
	}

	sysB := NewSystem()
	if err := sysB.Register(wedgedChart); err != nil {
		t.Fatalf("Register(wedgedChart): %v", err)
	}
	if err := sysB.Spawn(ctx, "wedged-1", wedgedChart.ID()); err != nil {
		t.Fatalf("Spawn(wedged-1): %v", err)
	}

	sendPing := statecharts.Action(func(_ *struct{}, ec statecharts.ExecContext) error {
		ec.Send("ping", statecharts.SendOptions{Target: "warehouse-b.wedged-1"})
		return nil
	})
	callerChart, err := statecharts.Build(
		statecharts.Atomic("idle", statecharts.On("go", statecharts.Then(sendPing))),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }),
	)
	if err != nil {
		t.Fatalf("Build(callerChart): %v", err)
	}

	sysA := NewSystem(WithFallback(NewBridge("warehouse-b", sysB, "warehouse-a")))
	if err := sysA.Register(callerChart); err != nil {
		t.Fatalf("Register(callerChart): %v", err)
	}
	if err := sysA.Spawn(ctx, "caller-1", callerChart.ID()); err != nil {
		t.Fatalf("Spawn(caller-1): %v", err)
	}

	start := time.Now()
	if err := sysA.Tell(ctx, "caller-1", statecharts.Event{Name: "go", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Tell took %s against a wedged remote actor -- the fallback path must hand off delivery, not block the sender", elapsed)
	}

	// sysB's own "wedged-1" instance is deliberately left running -- its
	// action only returns once the deferred close(release) above runs at
	// this test's end -- so only sysA, whose actor is not wedged, is
	// stopped here.
	if err := sysA.Stop(ctx); err != nil {
		t.Fatalf("sysA.Stop: %v", err)
	}
}
