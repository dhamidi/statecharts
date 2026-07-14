package actors

import (
	"context"
	"testing"
	"time"

	"github.com/dhamidi/statecharts"
)

// TestFallbackRoutesToOtherSystemAndReplyRoutesBack is the round-trip case
// WithFallback and Bridge exist for: an actor in sysA addresses a
// node-qualified actor ID that belongs to sysB, sysA's own routing doesn't
// recognize it, the fallback forwards it into sysB, and the reply -- sent
// by the responding actor to ev.Origin, exactly like any other reply --
// finds its way back to the original sender via sysB's own fallback.
func TestFallbackRoutesToOtherSystemAndReplyRoutesBack(t *testing.T) {
	ctx := context.Background()

	responder := buildResponderChart()
	var dms []*callerModel
	caller := buildCallerChart(&dms, "responder-1@warehouse-b")

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
	want := statecharts.Identifier("responder-1@warehouse-b")
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
// node a configured fallback recognizes must still surface as an
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
		t.Fatalf("configuration = %v, want to contain 'failed' for a target outside the fallback's node", inst.Configuration())
	}

	if err := sysA.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestFallbackRejectsNameNotSpawnedInTargetSystem covers the other half of
// Bridge.Send's synchronous check: the node matches, but the actor ID was
// never spawned in the target System.
func TestFallbackRejectsNameNotSpawnedInTargetSystem(t *testing.T) {
	ctx := context.Background()
	chart := buildCommTestChart("billing@warehouse-b")

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
		ec.Send("ping", statecharts.SendOptions{Target: "wedged-1@warehouse-b"})
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
	close(release)

	// Stop waits for accepted Bridge work, so release the deliberately wedged
	// target after proving Send itself returned promptly.
	if err := sysA.Stop(ctx); err != nil {
		t.Fatalf("sysA.Stop: %v", err)
	}
	if err := sysB.Stop(ctx); err != nil {
		t.Fatalf("sysB.Stop: %v", err)
	}
}

func TestBridgeUsesSystemNodeNamesForQualifiedRoundTrip(t *testing.T) {
	ctx := context.Background()
	responder := buildResponderChart()
	var dms []*callerModel
	caller := buildCallerChart(&dms, "responder-1@warehouse-b")

	toB := NewBridge("warehouse-b", nil, "ignored-source-name")
	sysA := NewSystem(WithNodeName("warehouse-a"), WithFallback(toB))
	sysB := NewSystem(
		WithNodeName("warehouse-b"),
		WithFallback(NewBridge("warehouse-a", sysA, "ignored-target-name")),
	)
	toB.SetTarget(sysB)
	if err := sysA.Register(caller); err != nil {
		t.Fatalf("Register caller: %v", err)
	}
	if err := sysB.Register(responder); err != nil {
		t.Fatalf("Register responder: %v", err)
	}
	if err := sysA.Spawn(ctx, "caller-1", caller.ID()); err != nil {
		t.Fatalf("Spawn caller: %v", err)
	}
	if err := sysB.Spawn(ctx, "responder-1", responder.ID()); err != nil {
		t.Fatalf("Spawn responder: %v", err)
	}
	if err := sysA.Tell(ctx, "caller-1", statecharts.Event{Name: "go", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell: %v", err)
	}
	callerInstance := testInstanceFor(sysA, "caller-1")
	waitFor(t, 2*time.Second, func() bool { return hasStateID(callerInstance.Configuration(), "done") })
	if got := dms[len(dms)-1].ReceivedFrom; got != "responder-1@warehouse-b" {
		t.Fatalf("reply Origin = %q, want target System's node-qualified actor address", got)
	}
	if err := sysA.Stop(ctx); err != nil {
		t.Fatalf("Stop A: %v", err)
	}
	if err := sysB.Stop(ctx); err != nil {
		t.Fatalf("Stop B: %v", err)
	}
}

func TestSourceSystemStopWaitsForBridgeDelivery(t *testing.T) {
	ctx := context.Background()
	entered := make(chan struct{})
	release := make(chan struct{})
	targetAction := statecharts.Action(func(_ *struct{}, _ statecharts.ExecContext) error {
		close(entered)
		<-release
		return nil
	})
	targetChart, err := statecharts.Build(
		statecharts.Atomic("bridge-target", statecharts.On("ping", statecharts.Then(targetAction))),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }),
	)
	if err != nil {
		t.Fatalf("Build target: %v", err)
	}
	sysB := NewSystem(WithNodeName("warehouse-b"))
	if err := sysB.Register(targetChart); err != nil {
		t.Fatalf("Register target: %v", err)
	}
	if err := sysB.Spawn(ctx, "target", targetChart.ID()); err != nil {
		t.Fatalf("Spawn target: %v", err)
	}
	send := statecharts.Action(func(_ *struct{}, ec statecharts.ExecContext) error {
		ec.Send("ping", statecharts.SendOptions{Target: "target@warehouse-b"})
		return nil
	})
	sourceChart, err := statecharts.Build(
		statecharts.Atomic("bridge-source", statecharts.On("go", statecharts.Then(send))),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }),
	)
	if err != nil {
		t.Fatalf("Build source: %v", err)
	}
	sysA := NewSystem(
		WithNodeName("warehouse-a"),
		WithFallback(NewBridge("warehouse-b", sysB, "warehouse-a")),
	)
	if err := sysA.Register(sourceChart); err != nil {
		t.Fatalf("Register source: %v", err)
	}
	if err := sysA.Spawn(ctx, "source", sourceChart.ID()); err != nil {
		t.Fatalf("Spawn source: %v", err)
	}
	if err := sysA.Tell(ctx, "source", statecharts.Event{Name: "go", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatalf("bridge delivery did not reach target")
	}

	stopped := make(chan error, 1)
	go func() { stopped <- sysA.Stop(ctx) }()
	select {
	case err := <-stopped:
		close(release)
		t.Fatalf("source Stop returned %v while its bridge delivery was still running", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatalf("source Stop after release: %v", err)
	}
	if err := sysB.Stop(ctx); err != nil {
		t.Fatalf("target Stop: %v", err)
	}
}

func TestBridgeAsyncFailureReturnsToSendingActor(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)
	store := &toggleLoadSnapshotStore{SnapshotStore: log}
	target, err := statecharts.Build(
		statecharts.Atomic("bridge-failure-target"),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }),
	)
	if err != nil {
		t.Fatalf("Build target: %v", err)
	}
	sysB := NewSystem(
		WithNodeName("warehouse-b"), WithLog(log), WithSnapshotStore(store), WithIdleTimeout(0),
	)
	if err := sysB.Register(target); err != nil {
		t.Fatalf("Register target: %v", err)
	}
	if err := sysB.Spawn(ctx, "target", target.ID(), Durable()); err != nil {
		t.Fatalf("Spawn target: %v", err)
	}
	entry, _ := sysB.resolve("target")
	entry.mu.Lock()
	if err := sysB.evictLocked(ctx, entry); err != nil {
		entry.mu.Unlock()
		t.Fatalf("evict target: %v", err)
	}
	entry.mu.Unlock()
	store.setFail(true)

	var dms []*asyncFailureModel
	sender := buildAsyncFailureSender(&dms, "target@warehouse-b")
	sysA := NewSystem(
		WithNodeName("warehouse-a"),
		WithFallback(NewBridge("warehouse-b", sysB, "warehouse-a")),
	)
	if err := sysA.Register(sender); err != nil {
		t.Fatalf("Register sender: %v", err)
	}
	if err := sysA.Spawn(ctx, "sender", sender.ID()); err != nil {
		t.Fatalf("Spawn sender: %v", err)
	}
	if err := sysA.Tell(ctx, "sender", statecharts.Event{Name: "go", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell sender: %v", err)
	}
	senderInstance := testInstanceFor(sysA, "sender")
	waitFor(t, 2*time.Second, func() bool { return hasStateID(senderInstance.Configuration(), "failed") })
	live := dms[len(dms)-1]
	if !live.Seen || live.Event.Type != statecharts.EventPlatform || live.Event.SendID != "request-7" {
		t.Fatalf("bridge failure event = %+v, seen=%v; want platform error with SendID request-7", live.Event, live.Seen)
	}
	if err := sysA.Stop(ctx); err != nil {
		t.Fatalf("Stop A: %v", err)
	}
	store.setFail(false)
	if err := sysB.Stop(ctx); err != nil {
		t.Fatalf("Stop B: %v", err)
	}
}
