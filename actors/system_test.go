package actors

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dhamidi/statecharts"
)

func TestRegisterRejectsChartWithoutDatamodelFactory(t *testing.T) {
	chart, err := statecharts.Build(statecharts.Atomic("solo"))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	sys := NewSystem()
	if err := sys.Register(chart); err == nil {
		t.Fatalf("Register: expected error for a chart with no datamodel factory")
	}
}

func TestRegisterRejectsDuplicateChartID(t *testing.T) {
	build := func() *statecharts.Chart {
		c, err := statecharts.Build(statecharts.Atomic("dup"), statecharts.WithNewDatamodel(func() any { return &struct{}{} }))
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return c
	}
	sys := NewSystem()
	if err := sys.Register(build()); err != nil {
		t.Fatalf("Register (first): %v", err)
	}
	if err := sys.Register(build()); err == nil {
		t.Fatalf("Register (second): expected error for colliding chart ID")
	}
}

func TestSpawnRejectsUnregisteredKind(t *testing.T) {
	sys := NewSystem()
	if err := sys.Spawn(context.Background(), "x", "never-registered"); err == nil {
		t.Fatalf("Spawn: expected error for a kind that was never Registered")
	}
}

func TestSpawnDurableRejectsMissingLogOrSnapshotStore(t *testing.T) {
	chart, err := statecharts.Build(statecharts.Atomic("solo"), statecharts.WithNewDatamodel(func() any { return &struct{}{} }))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	sys := NewSystem()
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(context.Background(), "d-1", chart.ID(), Durable()); err == nil {
		t.Fatalf("Spawn(Durable()): expected error without WithLog/WithSnapshotStore")
	}
}

func TestPeerMessagingSetsOriginAndAllowsReply(t *testing.T) {
	ctx := context.Background()
	var dms []*callerModel

	responder := buildResponderChart()
	caller := buildCallerChart(&dms, "responder-1")

	sys := NewSystem()
	if err := sys.Register(responder); err != nil {
		t.Fatalf("Register(responder): %v", err)
	}
	if err := sys.Register(caller); err != nil {
		t.Fatalf("Register(caller): %v", err)
	}
	if err := sys.Spawn(ctx, "responder-1", responder.ID()); err != nil {
		t.Fatalf("Spawn(responder-1): %v", err)
	}
	if err := sys.Spawn(ctx, "caller-1", caller.ID()); err != nil {
		t.Fatalf("Spawn(caller-1): %v", err)
	}

	if err := sys.Tell(ctx, "caller-1", statecharts.Event{Name: "go", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell: %v", err)
	}

	inst := testInstanceFor(sys, "caller-1")
	if inst == nil {
		t.Fatalf("caller-1 not resident after Spawn")
	}
	waitFor(t, 2*time.Second, func() bool { return hasStateID(inst.Configuration(), "done") })

	if len(dms) == 0 {
		t.Fatalf("caller datamodel count = 0, want at least 1")
	}
	// Register's own check that the chart has a datamodel factory
	// (chart.NewDatamodel()'s ok) calls that factory once itself, so the
	// live datamodel actually wired into the running actor is the last one
	// produced, not necessarily dms[0].
	live := dms[len(dms)-1]
	if live.ReceivedFrom != "responder-1" {
		t.Fatalf("ReceivedFrom = %q, want %q", live.ReceivedFrom, "responder-1")
	}

	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestSpawnNonDurableInstanceIDMatchesName(t *testing.T) {
	ctx := context.Background()
	chart, err := statecharts.Build(statecharts.Atomic("solo"), statecharts.WithNewDatamodel(func() any { return &struct{}{} }))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	sys := NewSystem()
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "solo-name", chart.ID()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	inst := testInstanceFor(sys, "solo-name")
	if inst == nil {
		t.Fatalf("solo-name not resident after Spawn")
	}
	if inst.ID() != "solo-name" {
		t.Fatalf("Instance.ID() = %q, want %q (the spawned name)", inst.ID(), "solo-name")
	}

	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestSpawnDurableInstanceIDMatchesName(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)

	var dms []*counterModel
	chart := buildLadderChart(&dms)

	sys := NewSystem(WithLog(log), WithSnapshotStore(log))
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "durable-name", chart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	inst := testInstanceFor(sys, "durable-name")
	if inst == nil {
		t.Fatalf("durable-name not resident after Spawn")
	}
	if inst.ID() != "durable-name" {
		t.Fatalf("Instance.ID() = %q, want %q (the spawned name)", inst.ID(), "durable-name")
	}

	// Paging out and back in (via idle eviction, simulated directly through
	// Stop+re-Spawn against the same Log) must still land on the same ID,
	// since Rehydrate's sessionID parameter is entry.name either way.
	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	sys2 := NewSystem(WithLog(log), WithSnapshotStore(log))
	if err := sys2.Register(buildLadderChart(&dms)); err != nil {
		t.Fatalf("Register (sys2): %v", err)
	}
	if err := sys2.Spawn(ctx, "durable-name", chart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn (sys2): %v", err)
	}
	inst2 := testInstanceFor(sys2, "durable-name")
	if inst2 == nil {
		t.Fatalf("durable-name not resident in sys2 after Spawn")
	}
	if inst2.ID() != "durable-name" {
		t.Fatalf("resumed Instance.ID() = %q, want %q", inst2.ID(), "durable-name")
	}
	if err := sys2.Stop(ctx); err != nil {
		t.Fatalf("Stop (sys2): %v", err)
	}
}

// TestSpawnNonDurableActionSeesOwnIOProcessorLocation confirms
// routingProcessor.IOProcessors surfaces the actor's own spawned name as its
// _ioprocessors "actors" Location -- the same name any other actor in sys
// already reaches it by (see routingProcessor.Send).
func TestSpawnNonDurableActionSeesOwnIOProcessorLocation(t *testing.T) {
	ctx := context.Background()
	var dms []*locationModel
	chart := buildLocationChart(&dms)

	sys := NewSystem()
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "locator-1", chart.ID()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	inst := testInstanceFor(sys, "locator-1")
	if inst == nil {
		t.Fatalf("locator-1 not resident after Spawn")
	}

	if err := sys.Tell(ctx, "locator-1", statecharts.Event{Name: "check", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return hasStateID(inst.Configuration(), "checked") })

	live := dms[len(dms)-1]
	if !live.OK || live.Location != "locator-1" {
		t.Fatalf("ec.IOProcessorLocation(%q) = (%q, %v), want (%q, true)", originTypeActors, live.Location, live.OK, "locator-1")
	}

	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestSpawnDurableActionSeesOwnIOProcessorLocation is the Durable() twin of
// TestSpawnNonDurableActionSeesOwnIOProcessorLocation -- a durable actor's
// routingProcessor is wrapped by replayGate (via Rehydrate), so this also
// exercises that wrapper's own IOProcessors forwarding.
func TestSpawnDurableActionSeesOwnIOProcessorLocation(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)

	var dms []*locationModel
	chart := buildLocationChart(&dms)

	sys := NewSystem(WithLog(log), WithSnapshotStore(log))
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "durable-locator", chart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	inst := testInstanceFor(sys, "durable-locator")
	if inst == nil {
		t.Fatalf("durable-locator not resident after Spawn")
	}

	if err := sys.Tell(ctx, "durable-locator", statecharts.Event{Name: "check", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return hasStateID(inst.Configuration(), "checked") })

	live := dms[len(dms)-1]
	if !live.OK || live.Location != "durable-locator" {
		t.Fatalf("ec.IOProcessorLocation(%q) = (%q, %v), want (%q, true)", originTypeActors, live.Location, live.OK, "durable-locator")
	}

	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestSendToUnknownActorSurfacesAsCommunicationError(t *testing.T) {
	ctx := context.Background()
	chart := buildCommTestChart("nobody-home")

	sys := NewSystem()
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "m-1", chart.ID()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	if err := sys.Tell(ctx, "m-1", statecharts.Event{Name: "go", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell: %v", err)
	}

	inst := testInstanceFor(sys, "m-1")
	if inst == nil {
		t.Fatalf("m-1 not resident after Spawn")
	}
	if !hasStateID(inst.Configuration(), "failed") {
		t.Fatalf("configuration = %v, want to contain 'failed' after sending to an unknown actor", inst.Configuration())
	}

	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestNonDurableActorSurvivesResidencyAndIdlePressure(t *testing.T) {
	ctx := context.Background()
	clock := statecharts.NewManualClock(time.Unix(0, 0))
	var dms []*counterModel
	chart := buildLadderChart(&dms)

	sys := NewSystem(WithIdleTimeout(time.Minute), WithClock(clock))
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "solo-1", chart.ID()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	clock.Advance(10 * time.Minute)

	if !testResident(sys, "solo-1") {
		t.Fatalf("non-durable actor must never be evicted by idle timeout")
	}

	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestSpawnStopRaceLeavesNoOrphanedInstance is finding #4: Spawn observing
// stopped==false has no synchronization with Stop's own "mark stopped,
// then snapshot the table" sequence, so a Spawn racing a Stop call could
// previously insert (and activate) a brand-new actor that Stop's snapshot
// never saw, leaving its goroutine running forever after Stop returned as
// having "stopped everything". This fires many concurrent Spawns against
// distinct, never-before-seen names at the same time as a single Stop
// call and requires: every Spawn that returned nil left behind an
// Instance whose goroutine is confirmed exited (via a bounded Wait) by
// the time this test checks it, not just "eventually, maybe". Run with
// -race and -count=N.
func TestSpawnStopRaceLeavesNoOrphanedInstance(t *testing.T) {
	ctx := context.Background()
	// A plain, no-shared-state datamodel factory -- unlike
	// buildLadderChart's, which appends every produced datamodel to a
	// shared, unsynchronized sink slice for other tests' benefit. That
	// sink is not safe for the concurrent activations (distinct names,
	// hence distinct entry.mu, hence genuinely concurrent
	// chart.NewDatamodel() calls) this test deliberately creates; racing
	// on it would be a bug in the test's own fixture, not in the code
	// under test.
	chart, err := statecharts.Build(
		statecharts.Atomic("solo"),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	sys := NewSystem()
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}

	const n = 200
	instances := make([]*statecharts.Instance, n)
	spawnErrs := make([]error, n)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = sys.Stop(ctx)
	}()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := statecharts.Identifier(fmt.Sprintf("orphan-%d", i))
			err := sys.Spawn(ctx, name, chart.ID())
			spawnErrs[i] = err
			if err == nil {
				instances[i] = testInstanceFor(sys, name)
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if spawnErrs[i] != nil {
			// Refused outright (system already stopping/stopped) --
			// exactly what the fix is supposed to make possible, and
			// there is nothing resident to check.
			continue
		}
		inst := instances[i]
		if inst == nil {
			t.Fatalf("Spawn %d returned nil error but no instance is resident", i)
		}
		waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := inst.Wait(waitCtx)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("instance %d is still running 2s after Stop returned -- orphaned by the Spawn/Stop race", i)
		}
	}
}

// TestStopHonorsContextDeadline is finding #5: Stop's final wait for
// in-flight asynchronous deliveries must be bounded by ctx, not
// unconditional -- a wedged delivery (here, a target whose action never
// returns) must not be able to hang Stop forever regardless of ctx's
// deadline.
func TestStopHonorsContextDeadline(t *testing.T) {
	ctx := context.Background()

	release := make(chan struct{})
	wedge := statecharts.Action(func(_ *struct{}, ec statecharts.ExecContext) error {
		<-release // never returns until the test releases it
		return nil
	})
	wedged, err := statecharts.Build(
		statecharts.Atomic("wedged", statecharts.On("go", statecharts.Then(wedge))),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer close(release)

	sys := NewSystem()
	if err := sys.Register(wedged); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "wedged-1", wedged.ID()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	// Send "go" through the routing IOProcessor's own asynchronous path
	// (not Tell) so it is asyncWG-tracked exactly like a peer Send would
	// be, and hangs inside the wedged action until release is closed --
	// simulating an in-flight delivery Stop must not wait on forever.
	sys.asyncWG.Add(1)
	go func() {
		defer sys.asyncWG.Done()
		sys.deliverAsync(context.Background(), "wedged-1", statecharts.Event{Name: "go", Type: statecharts.EventExternal}, nil)
	}()

	// Give the goroutine above a moment to actually enter the wedged
	// action before racing Stop against it.
	time.Sleep(20 * time.Millisecond)

	stopCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = sys.Stop(stopCtx)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > time.Second {
		t.Fatalf("Stop took %s to honor a 100ms ctx deadline -- it waited on the wedged delivery instead", elapsed)
	}
}

// TestWithOnSweepErrorObservesEvictionFailures is finding #6: a
// persistently failing SnapshotStore.Save during an idle-timeout sweep
// must be observable by an operator, not silently swallowed forever, and
// must not stop the sweep from continuing to try other actors.
func TestWithOnSweepErrorObservesEvictionFailures(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)
	clock := statecharts.NewManualClock(time.Unix(0, 0))

	var dms []*counterModel
	chart := buildLadderChart(&dms)

	failingStore := &alwaysFailingSnapshotStore{SnapshotStore: log}

	var mu sync.Mutex
	var failures []statecharts.Identifier
	sys := NewSystem(
		WithLog(log), WithSnapshotStore(failingStore),
		WithIdleTimeout(time.Minute),
		WithClock(clock),
		WithOnSweepError(func(name statecharts.Identifier, err error) {
			mu.Lock()
			defer mu.Unlock()
			failures = append(failures, name)
		}),
	)
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "flaky-1", chart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if err := sys.Tell(ctx, "flaky-1", statecharts.Event{Name: "inc", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell: %v", err)
	}

	clock.Advance(2 * time.Minute)

	mu.Lock()
	got := append([]statecharts.Identifier(nil), failures...)
	mu.Unlock()
	if len(got) != 1 || got[0] != "flaky-1" {
		t.Fatalf("sweep failures = %v, want exactly one for %q", got, "flaky-1")
	}
	// The failed eviction must not have removed the actor from residency --
	// its Instance is exactly as still-alive as evictLocked's own doc
	// comment promises for a failed checkpoint.
	if !testResident(sys, "flaky-1") {
		t.Fatalf("flaky-1 must remain resident after a failed checkpoint attempt")
	}
}

// alwaysFailingSnapshotStore wraps a working SnapshotStore but makes every
// Save fail, for exercising WithOnSweepError without needing a real
// storage failure.
type alwaysFailingSnapshotStore struct {
	statecharts.SnapshotStore
}

func (a *alwaysFailingSnapshotStore) Save(ctx context.Context, sessionID string, cp statecharts.Checkpoint) error {
	return fmt.Errorf("alwaysFailingSnapshotStore: Save always fails")
}
