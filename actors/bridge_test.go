package actors

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dhamidi/statecharts"
)

// TestFallbackRoutesToOtherSystemAndReplyRoutesBack is the round-trip case
// WithSCXMLPeer and Bridge exist for: an actor in sysA addresses a
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
	sysA := NewSystem(WithSCXMLPeer(bridgeToB))
	if err := sysA.Register(caller); err != nil {
		t.Fatalf("Register(caller): %v", err)
	}
	if err := sysA.Spawn(ctx, "caller-1", caller.ID()); err != nil {
		t.Fatalf("Spawn(caller-1): %v", err)
	}

	sysB := NewSystem(WithSCXMLPeer(NewBridge("warehouse-a", sysA, "warehouse-b")))
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
	sysA := NewSystem(WithSCXMLPeer(NewBridge("warehouse-b", sysB, "warehouse-a")))
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
	sysA := NewSystem(WithSCXMLPeer(NewBridge("warehouse-b", sysB, "warehouse-a")))
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
// IOProcessor.Send must honor (see the package doc and WithSCXMLPeer) holds
// for the fallback path too: the target actor's own action is wedged
// indefinitely, so if Bridge.Send (or routingProcessor.Send's delegation to
// it) blocked on delivery, sysA.Tell below would hang until the test's
// deadline. Instead it must return almost immediately, well before release
// is ever closed.
func TestFallbackSendDoesNotBlockSender(t *testing.T) {
	ctx := context.Background()

	release := make(chan struct{})

	wedgedModel := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	wedge, err := wedgedModel.Action("bridge.wedge-target", "v1", func(_ *struct{}, _ statecharts.ExecContext, _ []statecharts.Value) error {
		<-release // never returns until the test releases it
		return nil
	})
	if err != nil {
		t.Fatalf("register wedge action: %v", err)
	}
	wedgedChart, err := statecharts.Build(
		statecharts.Atomic("wedged", statecharts.On("ping", statecharts.Then(wedge.Do()))),
		wedgedModel, statecharts.WithRevisionSalt("test-v1"))
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

	callerModel := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	callerChart, err := statecharts.Build(
		statecharts.Atomic("idle", statecharts.On("go", statecharts.Then(
			statecharts.Send("ping", statecharts.SendTarget("wedged-1@warehouse-b")),
		))),
		callerModel, statecharts.WithRevisionSalt("test-v1"))
	if err != nil {
		t.Fatalf("Build(callerChart): %v", err)
	}

	sysA := NewSystem(WithSCXMLPeer(NewBridge("warehouse-b", sysB, "warehouse-a")))
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
	sysA := NewSystem(WithNodeName("warehouse-a"), WithSCXMLPeer(toB))
	sysB := NewSystem(
		WithNodeName("warehouse-b"),
		WithSCXMLPeer(NewBridge("warehouse-a", sysA, "ignored-target-name")),
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

func TestBridgePreservesNestedValueAndDeliveryIdentity(t *testing.T) {
	ctx := context.Background()
	received := make(chan statecharts.Event, 1)
	targetModel := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	record, err := targetModel.Action("bridge.record-nested-delivery", "v1", func(_ *struct{}, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ev, _ := ec.Event()
		received <- ev
		return nil
	})
	if err != nil {
		t.Fatalf("register record action: %v", err)
	}
	targetChart, err := statecharts.Build(
		statecharts.Atomic("bridge-value-target", statecharts.On("nested", statecharts.Then(record.Do()))),
		targetModel, statecharts.WithRevisionSalt("test-v1"))
	if err != nil {
		t.Fatalf("Build target: %v", err)
	}
	target := NewSystem(WithNodeName("warehouse-b"))
	if err := target.Register(targetChart); err != nil {
		t.Fatalf("Register target: %v", err)
	}
	if err := target.Spawn(ctx, "target", targetChart.ID()); err != nil {
		t.Fatalf("Spawn target: %v", err)
	}

	label, err := statecharts.StringValue("fragile")
	if err != nil {
		t.Fatalf("StringValue: %v", err)
	}
	nested, err := statecharts.MapValue(map[string]statecharts.Value{
		"items": statecharts.ListValue([]statecharts.Value{
			statecharts.Int64Value(7),
			statecharts.ListValue([]statecharts.Value{label}),
		}),
	})
	if err != nil {
		t.Fatalf("MapValue: %v", err)
	}
	payload, err := statecharts.TaggedValue("bridge.package/v1", nested)
	if err != nil {
		t.Fatalf("TaggedValue: %v", err)
	}

	bridge := NewBridge("warehouse-b", target, "warehouse-a")
	const deliveryID statecharts.DeliveryID = "warehouse-a:delivery-17"
	if err := bridge.Send(ctx, statecharts.SendRequest{
		DeliveryID: deliveryID,
		Target:     "target@warehouse-b",
		Type:       statecharts.SCXMLEventProcessor,
		Event:      "nested",
		Data:       payload,
	}); err != nil {
		t.Fatalf("Bridge.Send: %v", err)
	}

	select {
	case event := <-received:
		if event.DeliveryID != deliveryID {
			t.Fatalf("DeliveryID = %q, want %q", event.DeliveryID, deliveryID)
		}
		if !event.Data.Equal(payload) {
			t.Fatalf("nested bridge payload = %#v, want %#v", event.Data, payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bridge did not deliver nested payload")
	}
	if err := target.Stop(ctx); err != nil {
		t.Fatalf("Stop target: %v", err)
	}
}

func TestSourceSystemStopWaitsForBridgeDelivery(t *testing.T) {
	synctest.Test(t, testSourceSystemStopWaitsForBridgeDelivery)
}

func testSourceSystemStopWaitsForBridgeDelivery(t *testing.T) {
	ctx := context.Background()
	entered := make(chan struct{})
	release := make(chan struct{})
	targetModel := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	targetAction, err := targetModel.Action("bridge.block-target-delivery", "v1", func(_ *struct{}, _ statecharts.ExecContext, _ []statecharts.Value) error {
		close(entered)
		<-release
		return nil
	})
	if err != nil {
		t.Fatalf("register target action: %v", err)
	}
	targetChart, err := statecharts.Build(
		statecharts.Atomic("bridge-target", statecharts.On("ping", statecharts.Then(targetAction.Do()))),
		targetModel, statecharts.WithRevisionSalt("test-v1"))
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
	sourceModel := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	sourceChart, err := statecharts.Build(
		statecharts.Atomic("bridge-source", statecharts.On("go", statecharts.Then(
			statecharts.Send("ping", statecharts.SendTarget("target@warehouse-b")),
		))),
		sourceModel, statecharts.WithRevisionSalt("test-v1"))
	if err != nil {
		t.Fatalf("Build source: %v", err)
	}
	sysA := NewSystem(
		WithNodeName("warehouse-a"),
		WithSCXMLPeer(NewBridge("warehouse-b", sysB, "warehouse-a")),
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
	synctest.Wait()
	select {
	case err := <-stopped:
		close(release)
		t.Fatalf("source Stop returned %v while its bridge delivery was still running", err)
	default:
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
	store := &toggleLoadSnapshotStore{Storage: log}
	target, err := statecharts.Build(
		statecharts.Atomic("bridge-failure-target"),
		statecharts.NewGoModel(func() *struct{} { return &struct{}{} }), statecharts.WithRevisionSalt("test-v1"))
	if err != nil {
		t.Fatalf("Build target: %v", err)
	}
	sysB := NewSystem(
		WithNodeName("warehouse-b"), WithStorage(store), WithIdleTimeout(0),
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
		WithSCXMLPeer(NewBridge("warehouse-b", sysB, "warehouse-a")),
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
