package actors

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dhamidi/statecharts"
)

type actorTestDatamodelProgram struct {
	created chan struct{}
}

func (p actorTestDatamodelProgram) Fingerprint() []byte { return []byte("actor-test/v1") }

func (actorTestDatamodelProgram) ResolveExpression(statecharts.Expression) (statecharts.CompiledExpression, error) {
	return nil, errors.New("actor test program has no expressions")
}

func (actorTestDatamodelProgram) ResolveFunction(statecharts.FunctionRef) (statecharts.CompiledExpression, error) {
	return nil, errors.New("actor test program has no function references")
}

func (actorTestDatamodelProgram) ResolveDataLocation(statecharts.Identifier) (statecharts.CompiledExpression, error) {
	return nil, errors.New("actor test program has no data locations")
}

func (p actorTestDatamodelProgram) NewSession(statecharts.SessionOptions) (statecharts.DatamodelSession, error) {
	p.created <- struct{}{}
	return actorTestDatamodelSession{}, nil
}

type actorTestDatamodelSession struct{}

func (actorTestDatamodelSession) EvaluateBoolean(statecharts.ExecContext, statecharts.CompiledExpression) (bool, error) {
	return true, nil
}

func (actorTestDatamodelSession) EvaluateValue(statecharts.ExecContext, statecharts.CompiledExpression) (statecharts.Value, error) {
	return statecharts.Value{}, nil
}

func (actorTestDatamodelSession) Assign(statecharts.ExecContext, statecharts.CompiledExpression, statecharts.Value) error {
	return nil
}

func (actorTestDatamodelSession) Execute(statecharts.ExecContext, statecharts.CompiledExpression) error {
	return nil
}

func (actorTestDatamodelSession) ForEach(statecharts.ExecContext, statecharts.CompiledExpression, statecharts.IterationBindings, func() error) error {
	return nil
}

func (actorTestDatamodelSession) EncodeSnapshot() ([]byte, error) { return nil, nil }
func (actorTestDatamodelSession) DecodeSnapshot([]byte) error     { return nil }
func (actorTestDatamodelSession) Close() error                    { return nil }

func TestSpawnUsesRegisteredDatamodelProgram(t *testing.T) {
	created := make(chan struct{}, 1)
	chart, err := statecharts.Build(
		statecharts.Atomic("program"),
		statecharts.WithDatamodelProgram(actorTestDatamodelProgram{created: created}),
		statecharts.WithVersion("test-v1"),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	sys := NewSystem()
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(context.Background(), "program-1", chart.ID()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	select {
	case <-created:
	default:
		t.Fatal("Spawn did not create a session from the registered datamodel program")
	}
	if err := sys.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestRegisterRejectsChartWithoutDatamodelFactory(t *testing.T) {
	chart, err := statecharts.Build(statecharts.Atomic("solo"), statecharts.WithVersion("test-v1"))
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
		c, err := statecharts.Build(statecharts.Atomic("dup"), statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
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
	err := sys.Spawn(context.Background(), "x", "never-registered")
	if !errors.Is(err, ErrKindNotRegistered) {
		t.Fatalf("Spawn: err = %v, want ErrKindNotRegistered", err)
	}
}

func TestSpawnDurableRequiresStorageWhileEphemeralDoesNotTouchIt(t *testing.T) {
	chart, err := statecharts.Build(statecharts.Atomic("solo"), statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	sys := NewSystem()
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(context.Background(), "ephemeral", chart.ID()); err != nil {
		t.Fatalf("ephemeral Spawn without storage: %v", err)
	}
	spawnErr := sys.Spawn(context.Background(), "d-1", chart.ID(), Durable())
	if !errors.Is(spawnErr, ErrDurabilityUnsupported) {
		t.Fatalf("Spawn(Durable()): err = %v, want ErrDurabilityUnsupported", spawnErr)
	}
	durableSys := NewSystem(WithStorage(openTestLog(t)))
	if err := durableSys.Register(chart); err != nil {
		t.Fatalf("Register durable system: %v", err)
	}
	if err := durableSys.Spawn(context.Background(), "d-1", chart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn(Durable()) with storage: %v", err)
	}
	if err := durableSys.Stop(context.Background()); err != nil {
		t.Fatalf("Stop durable system: %v", err)
	}
}

// TestSpawnFailsWithResidencyExhaustedWhenNothingEvictable covers admit's
// failure path: a non-durable resident actor can never be evicted to make
// room (pickEvictionVictim only ever returns durable actors, since a
// non-durable actor has no Log to rebuild itself from), so a residency
// limit that is already met by non-durable occupants alone leaves Spawn
// with nothing it can free up.
func TestSpawnFailsWithResidencyExhaustedWhenNothingEvictable(t *testing.T) {
	ctx := context.Background()
	var dms []*counterModel
	chart := buildLadderChart(&dms)

	sys := NewSystem(WithMaxResident(1))
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "keep-1", chart.ID()); err != nil {
		t.Fatalf("Spawn(keep-1): %v", err)
	}

	spawnErr := sys.Spawn(ctx, "second-1", chart.ID())
	if !errors.Is(spawnErr, ErrResidencyExhausted) {
		t.Fatalf("Spawn(second-1): err = %v, want ErrResidencyExhausted", spawnErr)
	}

	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
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
	// The most recently produced datamodel is the one wired into the actor.
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
	chart, err := statecharts.Build(statecharts.Atomic("solo"), statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
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

	sys := NewSystem(WithStorage(log))
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

	sys2 := NewSystem(WithStorage(log))
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
// _ioprocessors SCXML Location -- the same name any other actor in sys
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
		t.Fatalf("SCXML processor location = (%q, %v), want (%q, true)", live.Location, live.OK, "locator-1")
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

	sys := NewSystem(WithStorage(log))
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
		t.Fatalf("SCXML processor location = (%q, %v), want (%q, true)", live.Location, live.OK, "durable-locator")
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
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
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
			// there is nothing resident to check. Confirm it was actually
			// refused for that reason, not some other failure the race
			// happened to produce.
			if !errors.Is(spawnErrs[i], ErrSystemStopped) {
				t.Fatalf("Spawn %d failed with %v, want ErrSystemStopped", i, spawnErrs[i])
			}
			continue
		}
		inst := instances[i]
		if inst == nil {
			// Spawn succeeded, but by the time this goroutine called
			// testInstanceFor, entry.instance was already cleared. This is
			// a third legitimate outcome of the race, not a bug: Stop's own
			// per-entry teardown goroutine for this same name blocks on
			// entry.mu for as long as activateLocked holds it, and the
			// instant activateLocked releases it (as part of Spawn
			// returning to this goroutine), Stop's already-waiting
			// teardown is free to acquire it and stop-and-clear the
			// instance, with no ordering guarantee relative to this
			// goroutine's own very next statement. It is not an orphan --
			// evictLocked and Stop's teardown both always call
			// Instance.Stop before clearing entry.instance, in every code
			// path -- it just means nothing here ever held a reference to
			// wait on. Confirm the *entry* itself still exists (entries
			// are never deleted from the table, so Spawn genuinely did
			// register the name; a missing entry would be a real bug) and
			// move on: there is nothing further to check for this index.
			name := statecharts.Identifier(fmt.Sprintf("orphan-%d", i))
			if _, ok := sys.resolve(name); !ok {
				t.Fatalf("Spawn %d returned nil error but name %q isn't even registered in the table", i, name)
			}
			continue
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
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
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

	// Send "go" through the routing dispatcher (not Tell), where it hangs
	// inside the wedged action until release is closed -- simulating accepted
	// peer work that Stop must wait for, but not beyond its context deadline.
	if err := sys.enqueueDispatch(func() {
		sys.deliverAsync(context.Background(), "wedged-1", statecharts.Event{Name: "go", Type: statecharts.EventExternal}, nil, "")
	}); err != nil {
		t.Fatalf("enqueue dispatch: %v", err)
	}

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

func TestStopCanRetryCleanupAfterDeadline(t *testing.T) {
	ctx := context.Background()
	entered := make(chan struct{})
	release := make(chan struct{})
	wedge := statecharts.Action(func(_ *struct{}, ec statecharts.ExecContext) error {
		close(entered)
		<-release
		return nil
	})
	chart, err := statecharts.Build(
		statecharts.Atomic("wedged-retry", statecharts.On("go", statecharts.Then(wedge))),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	sys := NewSystem()
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "wedged-retry-1", chart.ID()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	tellDone := make(chan error, 1)
	go func() {
		tellDone <- sys.Tell(ctx, "wedged-retry-1", statecharts.Event{Name: "go", Type: statecharts.EventExternal})
	}()
	<-entered

	stopCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	err = sys.Stop(stopCtx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		close(release)
		t.Fatalf("first Stop error = %v, want context.DeadlineExceeded", err)
	}

	retryDone := make(chan error, 1)
	go func() { retryDone <- sys.Stop(ctx) }()
	select {
	case err := <-retryDone:
		close(release)
		t.Fatalf("retry Stop returned %v before the wedged actor was released; it did not retry cleanup", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-tellDone; err != nil {
		t.Fatalf("Tell: %v", err)
	}
	if err := <-retryDone; err != nil {
		t.Fatalf("retry Stop: %v", err)
	}
	if testResident(sys, "wedged-retry-1") {
		t.Fatalf("actor remained resident after successful retry Stop")
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

	failingStore := &alwaysFailingSnapshotStore{Storage: log}

	var mu sync.Mutex
	var failures []statecharts.Identifier
	sys := NewSystem(
		WithStorage(failingStore),
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
	Storage
}

func (a *alwaysFailingSnapshotStore) Save(ctx context.Context, sessionID statecharts.SessionID, cp statecharts.Checkpoint) error {
	return fmt.Errorf("alwaysFailingSnapshotStore: Save always fails")
}

type toggleLoadSnapshotStore struct {
	Storage

	mu   sync.Mutex
	fail bool
}

func (s *toggleLoadSnapshotStore) setFail(fail bool) {
	s.mu.Lock()
	s.fail = fail
	s.mu.Unlock()
}

func (s *toggleLoadSnapshotStore) Load(ctx context.Context, sessionID statecharts.SessionID) (statecharts.Checkpoint, bool, error) {
	s.mu.Lock()
	fail := s.fail
	s.mu.Unlock()
	if fail {
		return statecharts.Checkpoint{}, false, fmt.Errorf("forced load failure for %q", sessionID)
	}
	return s.Storage.Load(ctx, sessionID)
}

// TestStopAggregatesAllTeardownErrors is the regression test for Stop's
// error handling: with two resident durable actors that both fail their
// checkpoint during teardown, Stop must report both failures instead of
// keeping only whichever one happened to win the race to record itself
// first. Each actor's own goroutine hits the same failing SnapshotStore.Save
// independently, so this exercises the exact concurrent race the old
// first-error-wins logic lost data to.
func TestStopAggregatesAllTeardownErrors(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)
	failingStore := &alwaysFailingSnapshotStore{Storage: log}

	var dms []*counterModel
	chart := buildLadderChart(&dms)

	sys := NewSystem(WithStorage(failingStore))
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}

	names := []statecharts.Identifier{"failing-1", "failing-2"}
	for _, name := range names {
		if err := sys.Spawn(ctx, name, chart.ID(), Durable()); err != nil {
			t.Fatalf("Spawn(%q): %v", name, err)
		}
	}

	err := sys.Stop(ctx)
	if err == nil {
		t.Fatalf("Stop: expected a non-nil aggregated error, got nil")
	}

	// errors.Join's result implements Unwrap() []error (see the errors
	// package doc) -- unwrapping it, rather than pattern-matching on a
	// single sentinel, is how a caller distinguishes "aggregated every
	// failure" from "kept only one and discarded the rest".
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		t.Fatalf("Stop error %v does not implement Unwrap() []error -- not an errors.Join result", err)
	}
	sub := joined.Unwrap()
	if len(sub) != len(names) {
		t.Fatalf("Stop error aggregates %d error(s), want %d (one per failed actor): %v", len(sub), len(names), err)
	}
	for _, name := range names {
		if !strings.Contains(err.Error(), string(name)) {
			t.Fatalf("Stop error %v does not mention failed actor %q -- lost under first-error-wins", err, name)
		}
	}
}

func TestNodeNameQualifiesActorAddressAndLocalRouting(t *testing.T) {
	ctx := context.Background()
	var dms []*callerModel
	responder := buildResponderChart()
	caller := buildCallerChart(&dms, "services.responder-1@warehouse-a")

	sys := NewSystem(WithNodeName("warehouse-a"))
	if err := sys.Register(responder); err != nil {
		t.Fatalf("Register responder: %v", err)
	}
	if err := sys.Register(caller); err != nil {
		t.Fatalf("Register caller: %v", err)
	}
	if err := sys.Spawn(ctx, "services.responder-1", responder.ID()); err != nil {
		t.Fatalf("Spawn responder: %v", err)
	}
	if err := sys.Spawn(ctx, "clients.caller-1", caller.ID()); err != nil {
		t.Fatalf("Spawn caller: %v", err)
	}

	callerInstance := testInstanceFor(sys, "clients.caller-1")
	if callerInstance.ID() != "clients.caller-1" {
		t.Fatalf("caller Instance.ID() = %q, want stable hierarchical actor ID %q", callerInstance.ID(), "clients.caller-1")
	}
	if err := sys.Tell(ctx, "clients.caller-1@warehouse-a", statecharts.Event{Name: "go", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell by qualified address: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return hasStateID(callerInstance.Configuration(), "done") })
	if got := dms[len(dms)-1].ReceivedFrom; got != "services.responder-1@warehouse-a" {
		t.Fatalf("reply Origin = %q, want routable address %q", got, "services.responder-1@warehouse-a")
	}

	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestSpawnRejectsRoutingKeyAsActorID(t *testing.T) {
	ctx := context.Background()
	chart, err := statecharts.Build(
		statecharts.Atomic("worker"),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	sys := NewSystem(WithNodeName("host-a"))
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "workers.invoice-42@host-a", chart.ID()); !errors.Is(err, ErrInvalidActorID) {
		t.Fatalf("Spawn error = %v, want ErrInvalidActorID for a routing key", err)
	}
	if _, ok := sys.resolve("workers.invoice-42@host-a"); ok {
		t.Fatalf("rejected routing key was inserted into the actor table")
	}
	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestIsResidentAcceptsLocalIDAndRoutingKeyWithoutPagingIn(t *testing.T) {
	ctx := context.Background()
	chart, err := statecharts.Build(statecharts.Atomic("worker"), statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatal(err)
	}
	log := openTestLog(t)
	sys := NewSystem(WithNodeName("host-a"), WithStorage(log), WithMaxResident(1))
	if err := sys.Register(chart); err != nil {
		t.Fatal(err)
	}
	if err := sys.Spawn(ctx, "one", chart.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	if !sys.IsResident("one") || !sys.IsResident("one@host-a") {
		t.Fatal("resident actor was not reported resident")
	}
	if sys.IsResident("missing") || sys.IsResident("one@host-b") {
		t.Fatal("unknown or remote target was reported resident")
	}

	if err := sys.Spawn(ctx, "two", chart.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	if sys.IsResident("one") || sys.IsResident("one@host-a") {
		t.Fatal("paged-out actor was reported resident")
	}
	if !sys.IsResident("two") || !sys.IsResident("two@host-a") {
		t.Fatal("newly resident actor was not reported resident")
	}
	if sys.IsResident("one") || !sys.IsResident("two") {
		t.Fatal("residency query paged an actor in or evicted another actor")
	}
	if err := sys.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestResidencyObserverReportsHydrationAndEviction(t *testing.T) {
	ctx := context.Background()
	chart, err := statecharts.Build(statecharts.Atomic("worker"), statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatal(err)
	}
	log := openTestLog(t)
	var mu sync.Mutex
	var changes []ResidencyChange
	sys := NewSystem(
		WithStorage(log), WithMaxResident(1),
		WithResidencyObserver(func(change ResidencyChange) {
			mu.Lock()
			changes = append(changes, change)
			mu.Unlock()
		}),
	)
	if err := sys.Register(chart); err != nil {
		t.Fatal(err)
	}
	if err := sys.Spawn(ctx, "one", chart.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	if err := sys.Spawn(ctx, "two", chart.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	changes = nil
	mu.Unlock()

	if err := sys.Tell(ctx, "one", statecharts.Event{Name: "wake", Type: statecharts.EventExternal}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := append([]ResidencyChange(nil), changes...)
	mu.Unlock()
	want := []ResidencyChange{
		{ActorID: "one", State: ResidencyHydrating},
		{ActorID: "two", State: ResidencyPagedOut},
		{ActorID: "one", State: ResidencyResident},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("residency changes = %#v, want %#v", got, want)
	}
	if err := sys.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestNodeNameQualifiesAdvertisedIOProcessorLocation(t *testing.T) {
	ctx := context.Background()
	var dms []*locationModel
	chart := buildLocationChart(&dms)
	sys := NewSystem(WithNodeName("warehouse-a"))
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "tools.locator-1", chart.ID()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if err := sys.Tell(ctx, "tools.locator-1", statecharts.Event{Name: "check", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell: %v", err)
	}
	if got := dms[len(dms)-1].Location; got != "tools.locator-1@warehouse-a" {
		t.Fatalf("IOProcessor location = %q, want %q", got, "tools.locator-1@warehouse-a")
	}
	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestMultiSegmentNodeNameRoundTripsThroughActorAddress(t *testing.T) {
	ctx := context.Background()
	var dms []*callerModel
	responder := buildResponderChart()
	caller := buildCallerChart(&dms, "responder-1@eu.warehouse-a")

	sys := NewSystem(WithNodeName("eu.warehouse-a"))
	if err := sys.Register(responder); err != nil {
		t.Fatalf("Register responder: %v", err)
	}
	if err := sys.Register(caller); err != nil {
		t.Fatalf("Register caller: %v", err)
	}
	if err := sys.Spawn(ctx, "responder-1", responder.ID()); err != nil {
		t.Fatalf("Spawn responder: %v", err)
	}
	if err := sys.Spawn(ctx, "caller-1", caller.ID()); err != nil {
		t.Fatalf("Spawn caller: %v", err)
	}
	if err := sys.Tell(ctx, "caller-1@eu.warehouse-a", statecharts.Event{Name: "go", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell by multi-segment qualified address: %v", err)
	}
	callerInstance := testInstanceFor(sys, "caller-1")
	waitFor(t, 2*time.Second, func() bool { return hasStateID(callerInstance.Configuration(), "done") })
	if got := dms[len(dms)-1].ReceivedFrom; got != "responder-1@eu.warehouse-a" {
		t.Fatalf("reply Origin = %q, want %q", got, "responder-1@eu.warehouse-a")
	}
	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestConcurrentActivationCannotExceedMaxResident(t *testing.T) {
	ctx := context.Background()
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	blockStart := statecharts.Action(func(_ *struct{}, _ statecharts.ExecContext) error {
		enteredOnce.Do(func() { close(entered) })
		<-release
		return nil
	})
	chart, err := statecharts.Build(
		statecharts.Atomic("blocked", statecharts.OnEntry(blockStart)),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	sys := NewSystem(WithMaxResident(1))
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}

	const count = 32
	errs := make(chan error, count)
	go func() { errs <- sys.Spawn(ctx, "blocked-0", chart.ID()) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatalf("first activation did not enter its chart")
	}
	for i := 1; i < count; i++ {
		go func(i int) {
			errs <- sys.Spawn(ctx, statecharts.Identifier(fmt.Sprintf("blocked-%d", i)), chart.ID())
		}(i)
	}
	// Give the competing activations time to reach admission while the first
	// activation has not yet published its Instance as resident.
	time.Sleep(50 * time.Millisecond)
	close(release)

	succeeded := 0
	for i := 0; i < count; i++ {
		err := <-errs
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrResidencyExhausted):
		default:
			t.Fatalf("Spawn error = %v, want nil or ErrResidencyExhausted", err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("successful concurrent activations = %d, want exactly 1 under WithMaxResident(1)", succeeded)
	}
	if got := sys.residentCount(); got != 1 {
		t.Fatalf("resident count = %d, want 1", got)
	}
	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestPeerDispatchDoesNotCreateOneBlockedGoroutinePerMessage(t *testing.T) {
	ctx := context.Background()
	receiver, err := statecharts.Build(
		statecharts.Atomic("slow-receiver", statecharts.On("message")),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatalf("Build receiver: %v", err)
	}
	const messages = 200
	sendBurst := statecharts.Action(func(_ *struct{}, ec statecharts.ExecContext) error {
		for i := 0; i < messages; i++ {
			ec.Send("message", statecharts.SendOptions{Target: "slow"})
		}
		return nil
	})
	sender, err := statecharts.Build(
		statecharts.Atomic("burst-sender", statecharts.On("go", statecharts.Then(sendBurst))),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatalf("Build sender: %v", err)
	}
	sys := NewSystem()
	if err := sys.Register(receiver); err != nil {
		t.Fatalf("Register receiver: %v", err)
	}
	if err := sys.Register(sender); err != nil {
		t.Fatalf("Register sender: %v", err)
	}
	if err := sys.Spawn(ctx, "slow", receiver.ID()); err != nil {
		t.Fatalf("Spawn receiver: %v", err)
	}
	if err := sys.Spawn(ctx, "burst", sender.ID()); err != nil {
		t.Fatalf("Spawn sender: %v", err)
	}

	entry, _ := sys.resolve("slow")
	entry.mu.Lock()
	baseline := runtime.NumGoroutine()
	if err := sys.Tell(ctx, "burst", statecharts.Event{Name: "go", Type: statecharts.EventExternal}); err != nil {
		entry.mu.Unlock()
		t.Fatalf("Tell: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	delta := runtime.NumGoroutine() - baseline
	entry.mu.Unlock()
	if delta > 16 {
		t.Fatalf("blocked peer burst created %d additional goroutines, want a bounded dispatcher", delta)
	}
	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestPeerDispatchQueueOverflowRaisesCommunicationError(t *testing.T) {
	ctx := context.Background()
	receiver, err := statecharts.Build(
		statecharts.Atomic("overflow-receiver", statecharts.On("message")),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatalf("Build receiver: %v", err)
	}
	sendBurst := statecharts.Action(func(_ *struct{}, ec statecharts.ExecContext) error {
		ec.Send("message", statecharts.SendOptions{Target: "receiver"})
		ec.Send("message", statecharts.SendOptions{Target: "receiver", SendID: "overflowed"})
		return nil
	})
	sender, err := statecharts.Build(
		statecharts.Compound("overflow-sender", "idle",
			statecharts.Children(
				statecharts.Atomic("idle",
					statecharts.On("go", statecharts.Target("waiting"), statecharts.Then(sendBurst)),
				),
				statecharts.Atomic("waiting",
					statecharts.On(string(statecharts.ErrEventCommunication), statecharts.Target("failed")),
				),
				statecharts.Atomic("failed"),
			),
		),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatalf("Build sender: %v", err)
	}
	sys := NewSystem(WithDispatchLimit(1))
	if err := sys.Register(receiver); err != nil {
		t.Fatalf("Register receiver: %v", err)
	}
	if err := sys.Register(sender); err != nil {
		t.Fatalf("Register sender: %v", err)
	}
	if err := sys.Spawn(ctx, "receiver", receiver.ID()); err != nil {
		t.Fatalf("Spawn receiver: %v", err)
	}
	if err := sys.Spawn(ctx, "sender", sender.ID()); err != nil {
		t.Fatalf("Spawn sender: %v", err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	if err := sys.enqueueDispatch(func() {
		close(entered)
		<-release
	}); err != nil {
		t.Fatalf("enqueue blocking dispatch: %v", err)
	}
	<-entered
	if err := sys.Tell(ctx, "sender", statecharts.Event{Name: "go", Type: statecharts.EventExternal}); err != nil {
		close(release)
		t.Fatalf("Tell: %v", err)
	}
	inst := testInstanceFor(sys, "sender")
	if !hasStateID(inst.Configuration(), "failed") {
		close(release)
		t.Fatalf("sender configuration = %v, want failed after dispatch queue overflow", inst.Configuration())
	}
	close(release)
	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

type registeredIOProcessor struct {
	requests []statecharts.SendRequest
	infos    []statecharts.IOProcessorInfo
}

func (*registeredIOProcessor) Attach(statecharts.Dispatcher) {}

func (p *registeredIOProcessor) Send(_ context.Context, req statecharts.SendRequest) error {
	p.requests = append(p.requests, req)
	return nil
}

func (p *registeredIOProcessor) IOProcessors() []statecharts.IOProcessorInfo {
	return append([]statecharts.IOProcessorInfo(nil), p.infos...)
}

func TestFallbackRoutesRegisteredIOProcessorTypeInsteadOfLocalActor(t *testing.T) {
	ctx := context.Background()
	hello := mustStringValue(t, "hello")
	var localDeliveries int
	receiver, err := statecharts.Build(
		statecharts.Atomic("typed-receiver", statecharts.On("frame", statecharts.Then(
			statecharts.Action(func(_ *struct{}, _ statecharts.ExecContext) error {
				localDeliveries++
				return nil
			}),
		))),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatalf("Build receiver: %v", err)
	}
	sender, err := statecharts.Build(
		statecharts.Atomic("typed-sender", statecharts.On("go", statecharts.Then(
			statecharts.SendEvent("frame", statecharts.SendOptions{Target: "output", Type: "browser", Data: hello}),
		))),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatalf("Build sender: %v", err)
	}

	processor := &registeredIOProcessor{}
	sys := NewSystem(WithIOProcessor("browser", func() statecharts.IOProcessor { return processor }))
	if err := sys.Register(receiver); err != nil {
		t.Fatalf("Register receiver: %v", err)
	}
	if err := sys.Register(sender); err != nil {
		t.Fatalf("Register sender: %v", err)
	}
	if err := sys.Spawn(ctx, "output", receiver.ID()); err != nil {
		t.Fatalf("Spawn receiver: %v", err)
	}
	if err := sys.Spawn(ctx, "source", sender.ID()); err != nil {
		t.Fatalf("Spawn sender: %v", err)
	}
	if err := sys.Tell(ctx, "source", statecharts.Event{Name: "go", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell: %v", err)
	}

	if len(processor.requests) != 1 {
		t.Fatalf("registered IOProcessor received %d requests, want 1", len(processor.requests))
	}
	if got := processor.requests[0]; got.Type != "browser" || got.Target != "output" || got.Event != "frame" || !got.Data.Equal(hello) {
		t.Fatalf("registered IOProcessor request = %#v, want browser frame to output", got)
	}
	if localDeliveries != 0 {
		t.Fatalf("custom-typed send reached local actor %d times, want 0", localDeliveries)
	}
	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestActorDeliveryIsolatesMutablePayloadAndUsesSCXMLOriginType(t *testing.T) {
	ctx := context.Background()
	original := map[string]statecharts.Value{"count": statecharts.Int64Value(1)}
	payload := mustMapValue(t, original)
	received := make(chan statecharts.Event, 1)
	receiver, err := statecharts.Build(
		statecharts.Atomic("payload-receiver", statecharts.On("payload", statecharts.Then(func(ec statecharts.ExecContext) error {
			ev, _ := ec.Event()
			got, ok := ev.Data.AsMap()
			if !ok {
				return fmt.Errorf("payload data is not a map: %#v", ev.Data)
			}
			got["count"] = statecharts.Int64Value(9)
			received <- ev
			return nil
		}))),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatalf("Build receiver: %v", err)
	}
	sender, err := statecharts.Build(
		statecharts.Atomic("payload-sender", statecharts.OnEntry(statecharts.SendEvent("payload", statecharts.SendOptions{Target: "receiver", Data: payload}))),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatalf("Build sender: %v", err)
	}
	sys := NewSystem()
	if err := sys.Register(receiver); err != nil {
		t.Fatalf("Register receiver: %v", err)
	}
	if err := sys.Register(sender); err != nil {
		t.Fatalf("Register sender: %v", err)
	}
	if err := sys.Spawn(ctx, "receiver", receiver.ID()); err != nil {
		t.Fatalf("Spawn receiver: %v", err)
	}
	if err := sys.Spawn(ctx, "sender", sender.ID()); err != nil {
		t.Fatalf("Spawn sender: %v", err)
	}
	select {
	case ev := <-received:
		if ev.OriginType != statecharts.SCXMLEventProcessorAlias {
			t.Fatalf("received OriginType = %q, want %q", ev.OriginType, statecharts.SCXMLEventProcessorAlias)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for actor payload")
	}
	if !original["count"].Equal(statecharts.Int64Value(1)) {
		t.Fatalf("sender payload was mutated through receiver: %#v", original)
	}
	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestFallbackAdvertisesRegisteredIOProcessor(t *testing.T) {
	ctx := context.Background()
	want, err := statecharts.NewLocation("browser://connection")
	if err != nil {
		t.Fatalf("NewLocation: %v", err)
	}
	processor := &registeredIOProcessor{infos: []statecharts.IOProcessorInfo{{Type: "browser", Location: want}}}
	var got statecharts.Location
	var ok bool
	chart, err := statecharts.Build(
		statecharts.Atomic("describer", statecharts.On("check", statecharts.Then(
			statecharts.Action(func(_ *struct{}, ec statecharts.ExecContext) error {
				got, ok = ec.IOProcessorLocation("browser")
				return nil
			}),
		))),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	sys := NewSystem(WithIOProcessor("browser", func() statecharts.IOProcessor { return processor }))
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "source", chart.ID()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if err := sys.Tell(ctx, "source", statecharts.Event{Name: "check", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell: %v", err)
	}

	if !ok || got.String() != want.String() {
		t.Fatalf("IOProcessorLocation(browser) = (%q, %v), want (%q, true)", got, ok, want)
	}
	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestUnsupportedPeerIOProcessorTypeRaisesExecutionError(t *testing.T) {
	ctx := context.Background()
	var deliveries int
	receiver, err := statecharts.Build(
		statecharts.Atomic("typed-receiver", statecharts.On("ping", statecharts.Then(
			statecharts.Action(func(_ *struct{}, _ statecharts.ExecContext) error {
				deliveries++
				return nil
			}),
		))),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatalf("Build receiver: %v", err)
	}
	send := statecharts.Action(func(_ *struct{}, ec statecharts.ExecContext) error {
		ec.Send("ping", statecharts.SendOptions{Target: "typed-target", Type: "unsupported"})
		return nil
	})
	sender, err := statecharts.Build(
		statecharts.Compound("typed-sender", "idle",
			statecharts.Children(
				statecharts.Atomic("idle", statecharts.On("go", statecharts.Target("waiting"), statecharts.Then(send))),
				statecharts.Atomic("waiting", statecharts.On(string(statecharts.ErrEventExecution), statecharts.Target("failed"))),
				statecharts.Atomic("failed"),
			),
		),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatalf("Build sender: %v", err)
	}
	sys := NewSystem()
	if err := sys.Register(receiver); err != nil {
		t.Fatalf("Register receiver: %v", err)
	}
	if err := sys.Register(sender); err != nil {
		t.Fatalf("Register sender: %v", err)
	}
	if err := sys.Spawn(ctx, "typed-target", receiver.ID()); err != nil {
		t.Fatalf("Spawn receiver: %v", err)
	}
	if err := sys.Spawn(ctx, "typed-source", sender.ID()); err != nil {
		t.Fatalf("Spawn sender: %v", err)
	}
	if err := sys.Tell(ctx, "typed-source", statecharts.Event{Name: "go", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell: %v", err)
	}
	senderInstance := testInstanceFor(sys, "typed-source")
	if !hasStateID(senderInstance.Configuration(), "failed") {
		t.Fatalf("sender configuration = %v, want failed after unsupported I/O processor type", senderInstance.Configuration())
	}
	time.Sleep(20 * time.Millisecond)
	if deliveries != 0 {
		t.Fatalf("unsupported typed send reached target %d time(s), want 0", deliveries)
	}
	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestAsyncDeliveryFailurePreservesPlatformMetadataAndDurability(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)
	store := &toggleLoadSnapshotStore{Storage: log}
	target, err := statecharts.Build(
		statecharts.Atomic("async-failure-target"),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatalf("Build target: %v", err)
	}
	var dms []*asyncFailureModel
	sender := buildAsyncFailureSender(&dms, "target")
	sys := NewSystem(WithStorage(store), WithIdleTimeout(0))
	if err := sys.Register(target); err != nil {
		t.Fatalf("Register target: %v", err)
	}
	if err := sys.Register(sender); err != nil {
		t.Fatalf("Register sender: %v", err)
	}
	if err := sys.Spawn(ctx, "target", target.ID(), Durable()); err != nil {
		t.Fatalf("Spawn target: %v", err)
	}
	if err := sys.Spawn(ctx, "sender", sender.ID(), Durable()); err != nil {
		t.Fatalf("Spawn sender: %v", err)
	}
	targetEntry, _ := sys.resolve("target")
	targetEntry.mu.Lock()
	if err := sys.evictLocked(ctx, targetEntry); err != nil {
		targetEntry.mu.Unlock()
		t.Fatalf("evict target: %v", err)
	}
	targetEntry.mu.Unlock()
	store.setFail(true)

	if err := sys.Tell(ctx, "sender", statecharts.Event{Name: "go", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell sender: %v", err)
	}
	senderInstance := testInstanceFor(sys, "sender")
	waitFor(t, 2*time.Second, func() bool { return hasStateID(senderInstance.Configuration(), "failed") })
	live := dms[len(dms)-1]
	if !live.Seen {
		t.Fatalf("sender did not observe error.communication")
	}
	if live.Event.Type != statecharts.EventPlatform {
		t.Fatalf("failure event Type = %s, want platform", live.Event.Type)
	}
	if live.Event.SendID != "request-7" {
		t.Fatalf("failure event SendID = %q, want request-7", live.Event.SendID)
	}
	if live.Event.Origin != "" {
		t.Fatalf("failure event Origin = %q, want empty platform origin", live.Event.Origin)
	}
	if seq, err := log.LastSeq(ctx, "sender"); err != nil || seq != 3 {
		t.Fatalf("sender LastSeq = %d, %v, want 3, nil (start, go, async failure)", seq, err)
	}
	store.setFail(false)
	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestPeerMessagesFromOneSenderRetainFIFOOrder(t *testing.T) {
	ctx := context.Background()
	const messages = 128
	type orderModel struct {
		mu    sync.Mutex
		order []int
	}
	var receiverModels []*orderModel
	record := statecharts.Action(func(d *orderModel, ec statecharts.ExecContext) error {
		ev, _ := ec.Event()
		nText, ok := ev.Data.AsNumber()
		if !ok {
			return fmt.Errorf("item data is not a number: %#v", ev.Data)
		}
		number, err := strconv.ParseFloat(nText, 64)
		if err != nil {
			return fmt.Errorf("parse item data %q: %w", nText, err)
		}
		n := int(number)
		d.mu.Lock()
		d.order = append(d.order, n)
		d.mu.Unlock()
		return nil
	})
	receiver, err := statecharts.Build(
		statecharts.Atomic("fifo-receiver", statecharts.On("item", statecharts.Then(record))),
		statecharts.WithNewDatamodel(func() any {
			d := &orderModel{}
			receiverModels = append(receiverModels, d)
			return d
		}), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatalf("Build receiver: %v", err)
	}
	send := statecharts.Action(func(_ *struct{}, ec statecharts.ExecContext) error {
		for i := 0; i < messages; i++ {
			ec.Send("item", statecharts.SendOptions{Target: "receiver", Data: statecharts.Int64Value(int64(i))})
		}
		return nil
	})
	sender, err := statecharts.Build(
		statecharts.Atomic("fifo-sender", statecharts.On("go", statecharts.Then(send))),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }), statecharts.WithVersion("test-v1"))
	if err != nil {
		t.Fatalf("Build sender: %v", err)
	}
	sys := NewSystem()
	if err := sys.Register(receiver); err != nil {
		t.Fatalf("Register receiver: %v", err)
	}
	if err := sys.Register(sender); err != nil {
		t.Fatalf("Register sender: %v", err)
	}
	if err := sys.Spawn(ctx, "receiver", receiver.ID()); err != nil {
		t.Fatalf("Spawn receiver: %v", err)
	}
	if err := sys.Spawn(ctx, "sender", sender.ID()); err != nil {
		t.Fatalf("Spawn sender: %v", err)
	}
	receiverEntry, _ := sys.resolve("receiver")
	receiverEntry.mu.Lock()
	if err := sys.Tell(ctx, "sender", statecharts.Event{Name: "go", Type: statecharts.EventExternal}); err != nil {
		receiverEntry.mu.Unlock()
		t.Fatalf("Tell: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	receiverEntry.mu.Unlock()
	live := receiverModels[len(receiverModels)-1]
	waitFor(t, 2*time.Second, func() bool {
		live.mu.Lock()
		defer live.mu.Unlock()
		return len(live.order) == messages
	})
	live.mu.Lock()
	defer live.mu.Unlock()
	for i, got := range live.order {
		if got != i {
			t.Fatalf("delivery order[%d] = %d, want %d; full order = %v", i, got, i, live.order)
		}
	}
	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
