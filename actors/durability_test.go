package actors

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/sqllog"
)

func openTestLog(t *testing.T) *sqllog.Log {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	// database/sql's own connection pool may open more than one physical
	// connection under concurrent load; each fresh connection to a plain
	// ":memory:" DSN (with no shared cache) is a distinct, empty database
	// with no schema in it. Pinning the pool to a single connection is
	// what makes ":memory:" behave like the single logical database every
	// test here assumes -- without it, tests that hammer this Log
	// concurrently (see TestConcurrentTellsSurviveRacingIdleSweep)
	// intermittently see "no such table" from a connection that never ran
	// sqllog.New's schema-creation statements.
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	log, err := sqllog.New(db, sqllog.SQLite)
	if err != nil {
		t.Fatalf("sqllog.New: %v", err)
	}
	return log
}

func TestDurableSpawnPersistsAndResumesViaLogWithoutDoubleApplying(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)

	var dms1 []*counterModel
	chart1 := buildLadderChart(&dms1)

	sys1 := NewSystem(WithLog(log), WithSnapshotStore(log))
	if err := sys1.Register(chart1); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys1.Spawn(ctx, "counter-1", chart1.ID(), Durable()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := sys1.Tell(ctx, "counter-1", statecharts.Event{Name: "inc", Type: statecharts.EventExternal}); err != nil {
			t.Fatalf("Tell %d: %v", i, err)
		}
	}
	// Register's own check that chart has a datamodel factory
	// (chart.NewDatamodel()'s ok) necessarily calls that factory once,
	// producing a throwaway entry in dms1 before Spawn's own -- so the
	// live datamodel actually wired into the running actor is the last one
	// produced, not dms1[0].
	live := dms1[len(dms1)-1]
	if live.Applied != 3 {
		t.Fatalf("live Applied = %d, want 3", live.Applied)
	}
	inst1 := testInstanceFor(sys1, "counter-1")
	if !hasStateID(inst1.Configuration(), "s3") {
		t.Fatalf("live configuration = %v, want 's3'", inst1.Configuration())
	}

	// Stop checkpoints the durable actor (Snapshot, Save, Stop) before
	// dropping it -- this is the same mechanism idle/residency eviction use.
	if err := sys1.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// A second, independent System against the same Log: this is what
	// "resumes exactly where it left off, even in a different process"
	// means in practice.
	var dms2 []*counterModel
	chart2 := buildLadderChart(&dms2)

	sys2 := NewSystem(WithLog(log), WithSnapshotStore(log))
	if err := sys2.Register(chart2); err != nil {
		t.Fatalf("Register (sys2): %v", err)
	}
	if err := sys2.Spawn(ctx, "counter-1", chart2.ID(), Durable()); err != nil {
		t.Fatalf("Spawn (sys2): %v", err)
	}

	inst2 := testInstanceFor(sys2, "counter-1")
	if inst2 == nil {
		t.Fatalf("counter-1 not resident in sys2 after Spawn")
	}
	if !hasStateID(inst2.Configuration(), "s3") {
		t.Fatalf("resumed configuration = %v, want 's3'", inst2.Configuration())
	}
	// The checkpoint taken by sys1.Stop already reflects all 3 "inc"
	// events; replaying them again from the log (rather than skipping to
	// the checkpoint) would double-apply their actions, driving Applied to
	// 3 immediately. Applied==0 here proves the resumed actor's actions
	// were not re-run. As above, the live datamodel is the last one
	// produced (Register's own ok-check consumes the first).
	resumed := dms2[len(dms2)-1]
	if resumed.Applied != 0 {
		t.Fatalf("resumed Applied = %d, want 0 (no double-apply)", resumed.Applied)
	}

	// The resumed actor keeps working going forward.
	if err := sys2.Tell(ctx, "counter-1", statecharts.Event{Name: "inc", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell (sys2): %v", err)
	}
	if resumed.Applied != 1 {
		t.Fatalf("Applied after one more Tell = %d, want 1", resumed.Applied)
	}

	if err := sys2.Stop(ctx); err != nil {
		t.Fatalf("Stop (sys2): %v", err)
	}
}

// TestDurableActorSurvivesCrashWithoutGracefulStop is the reviewer's
// throwaway probe that found finding #1 (Tell/deliverAsync never
// write-ahead-logging a durable actor's messages), turned into a
// permanent regression test. Without the fix, none of the 3 "inc" events
// below are ever appended to the Log -- only a graceful Stop (or an idle
// sweep) would have checkpointed them -- so Log.LastSeq stays 0 and a
// second System paging the same name back in resumes as if nothing had
// ever been sent to it.
func TestDurableActorSurvivesCrashWithoutGracefulStop(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)

	var dms1 []*counterModel
	chart1 := buildLadderChart(&dms1)

	sys1 := NewSystem(WithLog(log), WithSnapshotStore(log))
	if err := sys1.Register(chart1); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys1.Spawn(ctx, "crash-1", chart1.ID(), Durable()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := sys1.Tell(ctx, "crash-1", statecharts.Event{Name: "inc", Type: statecharts.EventExternal}); err != nil {
			t.Fatalf("Tell %d: %v", i, err)
		}
	}

	// Simulate a crash: no graceful Stop is ever called on sys1 (so no
	// checkpoint is ever taken), and sys1 is simply abandoned -- the
	// closest a single-process test can get to "the process died between
	// messages". If Tell properly write-ahead-logs before applying, the
	// Log alone, untouched by any checkpoint, must already hold the session
	// start marker followed by all 3 events.
	if seq, err := log.LastSeq(ctx, "crash-1"); err != nil || seq != 4 {
		t.Fatalf("LastSeq = %d, %v, want 4, nil -- start/messages were not write-ahead-logged before being applied", seq, err)
	}

	// A second, independent System against the same Log, standing in for
	// "the process restarted": since sys1 never checkpointed, this Spawn
	// must replay all 3 "inc" events straight from the Log to land in the
	// same state sys1 was in.
	var dms2 []*counterModel
	chart2 := buildLadderChart(&dms2)

	sys2 := NewSystem(WithLog(log), WithSnapshotStore(log))
	if err := sys2.Register(chart2); err != nil {
		t.Fatalf("Register (sys2): %v", err)
	}
	if err := sys2.Spawn(ctx, "crash-1", chart2.ID(), Durable()); err != nil {
		t.Fatalf("Spawn (sys2): %v", err)
	}

	inst2 := testInstanceFor(sys2, "crash-1")
	if inst2 == nil {
		t.Fatalf("crash-1 not resident in sys2 after Spawn")
	}
	if !hasStateID(inst2.Configuration(), "s3") {
		t.Fatalf("resumed configuration = %v, want 's3' (all 3 sent events must survive the crash)", inst2.Configuration())
	}
	// With no checkpoint ever taken, Rehydrate replays from the very
	// beginning of the Log, applying "inc"'s action once per replayed
	// event -- Applied must be exactly 3, proving replay actually
	// recovered every message Tell claimed to have delivered, not merely
	// that Spawn succeeded.
	resumed := dms2[len(dms2)-1]
	if resumed.Applied != 3 {
		t.Fatalf("resumed Applied = %d, want 3 (all 3 crashed-and-never-checkpointed events replayed)", resumed.Applied)
	}

	if err := sys2.Stop(ctx); err != nil {
		t.Fatalf("Stop (sys2): %v", err)
	}
}

// TestConcurrentTellsSurviveRacingIdleSweep hammers concurrent Tells
// against a single durable name while a real idle-timeout sweep keeps
// firing in the background, trying to page the actor out mid-flight
// (finding #2/#3: eviction racing an in-flight Deliver for the same
// name). With entry.mu now serializing the entire acquire+Log.Append+
// Deliver sequence against eviction for a name (see System.deliver's own
// doc comment), no message should ever be lost or double-applied no
// matter how delivery and eviction interleave. Run with -race and
// -count=N: a single clean pass is not strong evidence for a test like
// this.
func TestConcurrentTellsSurviveRacingIdleSweep(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)

	var dms []*counterModel
	chart := buildLadderChart(&dms)

	sys := NewSystem(
		WithLog(log), WithSnapshotStore(log),
		WithIdleTimeout(200*time.Microsecond), // fires continuously, racing every Tell
	)
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "hammer-1", chart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	const n = 150
	errCh := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- sys.Tell(ctx, "hammer-1", statecharts.Event{Name: "inc", Type: statecharts.EventExternal})
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("Tell: %v", err)
		}
	}

	// Every one of the n "inc" events must appear exactly once in the Log,
	// regardless of how many times idle-timeout eviction raced a Tell to
	// page the actor back in mid-flight: no message lost, none logged
	// twice. sqllog.Log.Append assigns Seq as MAX(seq)+1 inside one
	// transaction per call, so LastSeq landing on exactly n+1 (the start
	// marker plus n events) proves exactly n event Appends happened.
	seq, err := log.LastSeq(ctx, "hammer-1")
	if err != nil {
		t.Fatalf("LastSeq: %v", err)
	}
	if seq != n+1 {
		t.Fatalf("LastSeq = %d, want %d (start marker plus every Tell exactly once)", seq, n+1)
	}

	// Read every entry back and confirm Seq is exactly the gapless run
	// 1..n+1 with no duplicates: one start marker, then only "inc" events -- a
	// stronger check than LastSeq alone, which could in principle land on
	// n by coincidence (e.g. one lost + one duplicated).
	seen := map[uint64]bool{}
	count := 0
	for entry, err := range log.Read(ctx, "hammer-1", 1) {
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if entry.Kind == statecharts.KindSessionStarted {
			if entry.Seq != 1 {
				t.Fatalf("session-start marker Seq = %d, want 1", entry.Seq)
			}
			continue
		}
		if entry.Event.Name != "inc" {
			t.Fatalf("entry %d event = %q, want %q", entry.Seq, entry.Event.Name, "inc")
		}
		if seen[entry.Seq] {
			t.Fatalf("Seq %d appended more than once", entry.Seq)
		}
		seen[entry.Seq] = true
		count++
	}
	if count != n {
		t.Fatalf("Read returned %d entries, want %d", count, n)
	}

	// The live actor must actually have processed at least 3 of them to
	// reach 's3' (a self-loop for every further "inc") -- proving delivery
	// itself, not just logging, kept up. Applied (the in-memory action
	// counter) is deliberately not checked against n here: Snapshot
	// excludes the datamodel by design (snapshot.go), so every
	// checkpoint/page-in cycle this hammering triggers starts the next
	// live datamodel back at Applied==0, counting only increments since
	// the *last* checkpoint, not the grand total -- the Log itself (above)
	// is the only reliable total-count witness across eviction cycles.
	inst := testInstanceFor(sys, "hammer-1")
	if inst == nil {
		if err := sys.Tell(ctx, "hammer-1", statecharts.Event{Name: "inc", Type: statecharts.EventExternal}); err != nil {
			t.Fatalf("Tell (repage for final check): %v", err)
		}
		seq, err = log.LastSeq(ctx, "hammer-1")
		if err != nil {
			t.Fatalf("LastSeq (after repage): %v", err)
		}
		if seq != n+2 {
			t.Fatalf("LastSeq after repage = %d, want %d", seq, n+2)
		}
		inst = testInstanceFor(sys, "hammer-1")
	}
	if inst == nil || !hasStateID(inst.Configuration(), "s3") {
		cfg := []statecharts.Identifier(nil)
		if inst != nil {
			cfg = inst.Configuration()
		}
		t.Fatalf("final configuration = %v, want to contain 's3'", cfg)
	}

	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestIdleTimeoutPagesOutAndTransparentlyPagesBackIn(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)
	clock := statecharts.NewManualClock(time.Unix(0, 0))

	var dms []*counterModel
	chart := buildLadderChart(&dms)

	sys := NewSystem(
		WithLog(log), WithSnapshotStore(log),
		WithIdleTimeout(time.Minute),
		WithClock(clock),
	)
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "ticker-1", chart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if err := sys.Tell(ctx, "ticker-1", statecharts.Event{Name: "inc", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell: %v", err)
	}
	if !testResident(sys, "ticker-1") {
		t.Fatalf("expected ticker-1 resident right after Spawn+Tell")
	}

	// Advancing the clock past the idle timeout synchronously fires the
	// system's sweep (armed via the same Clock), evicting the actor.
	clock.Advance(2 * time.Minute)

	if testResident(sys, "ticker-1") {
		t.Fatalf("expected ticker-1 paged out after idle timeout")
	}

	// The next message pages it transparently back in.
	if err := sys.Tell(ctx, "ticker-1", statecharts.Event{Name: "inc", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell after page-out: %v", err)
	}
	if !testResident(sys, "ticker-1") {
		t.Fatalf("expected ticker-1 resident again after Tell")
	}

	inst := testInstanceFor(sys, "ticker-1")
	if !hasStateID(inst.Configuration(), "s2") {
		t.Fatalf("configuration after page-back-in = %v, want 's2' (one inc before eviction, one after)", inst.Configuration())
	}

	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestOverdueDelayedSendFiresDuringPageIn(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)
	clock := statecharts.NewManualClock(time.Unix(0, 0))
	abortChart := buildInitAbortChart()
	otherChart := buildFinishingChart()

	sys := NewSystem(
		WithLog(log), WithSnapshotStore(log),
		WithClock(clock),
		WithIdleTimeout(0),
		WithMaxResident(1),
	)
	if err := sys.Register(abortChart); err != nil {
		t.Fatalf("Register abort chart: %v", err)
	}
	if err := sys.Register(otherChart); err != nil {
		t.Fatalf("Register other chart: %v", err)
	}
	if err := sys.Spawn(ctx, "operation-1", abortChart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn operation: %v", err)
	}
	if err := sys.Tell(ctx, "operation-1", statecharts.Event{Name: "init", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell init: %v", err)
	}
	if inst := testInstanceFor(sys, "operation-1"); inst == nil || !hasStateID(inst.Configuration(), "running") {
		var cfg []statecharts.Identifier
		if inst != nil {
			cfg = inst.Configuration()
		}
		t.Fatalf("configuration after init = %v, want 'running'", cfg)
	}

	// Admitting the only other actor checkpoints and evicts operation-1.
	if err := sys.Spawn(ctx, "other", otherChart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn other: %v", err)
	}
	if testResident(sys, "operation-1") {
		t.Fatalf("expected operation-1 to be paged out")
	}

	clock.Advance(5 * time.Second)

	// Page operation-1 back in without delivering another chart event. Its
	// persisted abort deadline is already past, so hydration itself must
	// apply the delayed self-send before Spawn returns.
	if err := sys.Spawn(ctx, "operation-1", abortChart.ID(), Durable()); err != nil {
		t.Fatalf("page operation back in: %v", err)
	}
	inst := testInstanceFor(sys, "operation-1")
	if inst == nil || !hasStateID(inst.Configuration(), "aborted") {
		var cfg []statecharts.Identifier
		if inst != nil {
			cfg = inst.Configuration()
		}
		t.Fatalf("configuration after page-in = %v, want 'aborted'", cfg)
	}
	seq, err := log.LastSeq(ctx, "operation-1")
	if err != nil {
		t.Fatalf("LastSeq after overdue timer fired: %v", err)
	}
	if seq != 3 {
		t.Fatalf("LastSeq after overdue timer fired = %d, want 3 (start, init, timer_fired)", seq)
	}

	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

type timerBetweenSnapshotAndSeqLog struct {
	*sqllog.Log
	clock *statecharts.ManualClock
	once  sync.Once
	fired chan struct{}
}

func (l *timerBetweenSnapshotAndSeqLog) Append(ctx context.Context, entry statecharts.LogEntry) (uint64, error) {
	seq, err := l.Log.Append(ctx, entry)
	if err == nil && entry.Kind == statecharts.KindTimerFired {
		select {
		case <-l.fired:
		default:
			close(l.fired)
		}
	}
	return seq, err
}

func (l *timerBetweenSnapshotAndSeqLog) LastSeq(ctx context.Context, sessionID statecharts.SessionID) (uint64, error) {
	l.once.Do(func() {
		l.clock.Advance(5 * time.Second)
		// In the broken implementation LastSeq runs after Snapshot while the
		// actor is free to process the timer, so wait long enough for its
		// append to land before returning the sequence. In the fixed
		// implementation LastSeq runs inside Instance.Checkpoint on the actor
		// goroutine itself; the timer can enqueue but cannot append until the
		// checkpoint boundary has committed and stopped the actor.
		select {
		case <-l.fired:
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	})
	return l.Log.LastSeq(ctx, sessionID)
}

func TestCheckpointCannotClaimTimerFireMissingFromSnapshot(t *testing.T) {
	ctx := context.Background()
	baseLog := openTestLog(t)
	clock := statecharts.NewManualClock(time.Unix(0, 0))
	log := &timerBetweenSnapshotAndSeqLog{Log: baseLog, clock: clock, fired: make(chan struct{})}
	abortChart := buildInitAbortChart()
	otherChart := buildFinishingChart()

	sys := NewSystem(
		WithLog(log), WithSnapshotStore(log),
		WithClock(clock), WithIdleTimeout(0), WithMaxResident(1),
	)
	if err := sys.Register(abortChart); err != nil {
		t.Fatalf("Register abort chart: %v", err)
	}
	if err := sys.Register(otherChart); err != nil {
		t.Fatalf("Register other chart: %v", err)
	}
	if err := sys.Spawn(ctx, "operation-race", abortChart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn operation: %v", err)
	}
	if err := sys.Tell(ctx, "operation-race", statecharts.Event{Name: "init", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell init: %v", err)
	}

	// LastSeq advances the timer while eviction is checkpointing. The saved
	// checkpoint must either include both the timer's state change and seq,
	// or neither; it must never skip the log entry while retaining the timer.
	if err := sys.Spawn(ctx, "other-race", otherChart.ID(), Durable()); err != nil {
		t.Fatalf("evict operation: %v", err)
	}
	if err := sys.Spawn(ctx, "operation-race", abortChart.ID(), Durable()); err != nil {
		t.Fatalf("page operation back in: %v", err)
	}
	inst := testInstanceFor(sys, "operation-race")
	if inst == nil || !hasStateID(inst.Configuration(), "aborted") {
		var cfg []statecharts.Identifier
		if inst != nil {
			cfg = inst.Configuration()
		}
		t.Fatalf("configuration after page-in = %v, want 'aborted'", cfg)
	}
	seq, err := log.LastSeq(ctx, "operation-race")
	if err != nil {
		t.Fatalf("LastSeq: %v", err)
	}
	if seq != 3 {
		t.Fatalf("LastSeq after page-in = %d, want 3; a fourth entry means the checkpoint skipped and re-fired timer_fired", seq)
	}
	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestLogOnlyRecoveryUsesSystemClockTimestamps(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)
	liveClock := statecharts.NewManualClock(time.Unix(0, 0))
	chart := buildInitAbortChart()

	sys1 := NewSystem(
		WithLog(log), WithSnapshotStore(log),
		WithClock(liveClock), WithIdleTimeout(0),
	)
	if err := sys1.Register(chart); err != nil {
		t.Fatalf("Register sys1: %v", err)
	}
	if err := sys1.Spawn(ctx, "operation-clock", chart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn sys1: %v", err)
	}
	if err := sys1.Tell(ctx, "operation-clock", statecharts.Event{Name: "init", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell init: %v", err)
	}

	// Simulate a crash: stop the live instance without creating a checkpoint.
	entry, _ := sys1.resolve("operation-clock")
	if err := entry.instance.Load().Stop(ctx); err != nil {
		t.Fatalf("stop crashed instance: %v", err)
	}
	entry.instance.Store(nil)

	recoveryClock := statecharts.NewManualClock(time.Unix(5, 0))
	sys2 := NewSystem(
		WithLog(log), WithSnapshotStore(log),
		WithClock(recoveryClock), WithIdleTimeout(0),
	)
	if err := sys2.Register(chart); err != nil {
		t.Fatalf("Register sys2: %v", err)
	}
	if err := sys2.Spawn(ctx, "operation-clock", chart.ID(), Durable()); err != nil {
		t.Fatalf("rehydrate sys2: %v", err)
	}
	inst := testInstanceFor(sys2, "operation-clock")
	if inst == nil || !hasStateID(inst.Configuration(), "aborted") {
		var cfg []statecharts.Identifier
		if inst != nil {
			cfg = inst.Configuration()
		}
		t.Fatalf("configuration after log-only recovery = %v, want 'aborted' from the clock-relative overdue timer", cfg)
	}
	if err := sys2.Stop(ctx); err != nil {
		t.Fatalf("Stop sys2: %v", err)
	}
}

func TestTimerFireLogPreservesDispatchMetadata(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)
	startedAt := time.Date(2030, time.January, 2, 3, 4, 5, 0, time.UTC)
	clock := statecharts.NewManualClock(startedAt)
	sender, err := statecharts.Build(
		statecharts.Compound("metadata-sender", "idle",
			statecharts.Children(
				statecharts.Atomic("idle", statecharts.On("go", statecharts.Then(
					statecharts.SendEvent("work.abort", statecharts.SendOptions{
						SendID: "abort-work", Target: "metadata-receiver", Type: "actors", Delay: 2 * time.Second,
					}),
				))),
			),
		),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }),
	)
	if err != nil {
		t.Fatalf("Build sender: %v", err)
	}
	receiver, err := statecharts.Build(
		statecharts.Compound("metadata-receiver-chart", "waiting",
			statecharts.Children(
				statecharts.Atomic("waiting", statecharts.On("work.abort", statecharts.Target("aborted"))),
				statecharts.Atomic("aborted"),
			),
		),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }),
	)
	if err != nil {
		t.Fatalf("Build receiver: %v", err)
	}

	sys := NewSystem(WithLog(log), WithSnapshotStore(log), WithClock(clock), WithIdleTimeout(0))
	if err := sys.Register(sender); err != nil {
		t.Fatalf("Register sender: %v", err)
	}
	if err := sys.Register(receiver); err != nil {
		t.Fatalf("Register receiver: %v", err)
	}
	if err := sys.Spawn(ctx, "metadata-sender", sender.ID(), Durable()); err != nil {
		t.Fatalf("Spawn sender: %v", err)
	}
	if err := sys.Spawn(ctx, "metadata-receiver", receiver.ID(), Durable()); err != nil {
		t.Fatalf("Spawn receiver: %v", err)
	}
	if err := sys.Tell(ctx, "metadata-sender", statecharts.Event{Name: "go", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell go: %v", err)
	}
	clock.Advance(2 * time.Second)
	waitFor(t, 2*time.Second, func() bool {
		inst := testInstanceFor(sys, "metadata-receiver")
		return inst != nil && hasStateID(inst.Configuration(), "aborted")
	})

	var timerEntry statecharts.LogEntry
	for entry, err := range log.Read(ctx, "metadata-sender", 1) {
		if err != nil {
			t.Fatalf("Read sender log: %v", err)
		}
		if entry.Kind == statecharts.KindTimerFired {
			timerEntry = entry
		}
	}
	if timerEntry.Kind == "" {
		t.Fatalf("sender log has no timer_fired entry")
	}
	if timerEntry.Target != "metadata-receiver" {
		t.Fatalf("timer_fired Target = %q, want metadata-receiver", timerEntry.Target)
	}
	if timerEntry.Type != "actors" {
		t.Fatalf("timer_fired Type = %q, want actors", timerEntry.Type)
	}
	if want := startedAt.Add(2 * time.Second); !timerEntry.Timestamp.Equal(want) {
		t.Fatalf("timer_fired Timestamp = %s, want configured-clock time %s", timerEntry.Timestamp, want)
	}
	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestResidencyLimitEvictsLeastRecentlyActive(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)
	clock := statecharts.NewManualClock(time.Unix(0, 0))

	var dms []*counterModel
	chart := buildLadderChart(&dms)

	sys := NewSystem(
		WithLog(log), WithSnapshotStore(log),
		WithMaxResident(1),
		WithClock(clock),
	)
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := sys.Spawn(ctx, "a1", chart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn(a1): %v", err)
	}
	clock.Advance(time.Second) // give a1 and a2 distinct lastActive timestamps
	if err := sys.Spawn(ctx, "a2", chart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn(a2): %v", err)
	}

	if testResident(sys, "a1") {
		t.Fatalf("expected a1 evicted to make room for a2 under WithMaxResident(1)")
	}
	if !testResident(sys, "a2") {
		t.Fatalf("expected a2 resident")
	}

	// a1 is still independently reachable -- just not simultaneously
	// resident with a2.
	if err := sys.Tell(ctx, "a1", statecharts.Event{Name: "inc", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell(a1): %v", err)
	}
	if !testResident(sys, "a1") {
		t.Fatalf("expected a1 resident after Tell")
	}
	if testResident(sys, "a2") {
		t.Fatalf("expected a2 evicted after a1 paged back in (still capped at 1 resident)")
	}

	if err := sys.Tell(ctx, "a2", statecharts.Event{Name: "inc", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell(a2): %v", err)
	}
	if !testResident(sys, "a2") {
		t.Fatalf("expected a2 resident after Tell")
	}

	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestResidencyLimitNeverEvictsActorWithActiveInvoke confirms
// pickEvictionVictim skips a durable resident actor with a running
// <invoke>, even though it's the only eviction candidate and the least
// recently active: Rehydrate cannot resume a real invocation (ADR 0010), so
// paging one out and back in would strand or silently disrupt it.
func TestResidencyLimitNeverEvictsActorWithActiveInvoke(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)
	clock := statecharts.NewManualClock(time.Unix(0, 0))

	invokingChart := buildInvokingChart()
	var dms []*counterModel
	ladderChart := buildLadderChart(&dms)

	sys := NewSystem(
		WithLog(log), WithSnapshotStore(log),
		WithMaxResident(1),
		WithClock(clock),
	)
	if err := sys.Register(invokingChart); err != nil {
		t.Fatalf("Register(invokingChart): %v", err)
	}
	if err := sys.Register(ladderChart); err != nil {
		t.Fatalf("Register(ladderChart): %v", err)
	}

	if err := sys.Spawn(ctx, "invoker", invokingChart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn(invoker): %v", err)
	}
	inst := testInstanceFor(sys, "invoker")
	if hasInvokes, err := inst.HasActiveInvokes(ctx); err != nil || !hasInvokes {
		t.Fatalf("HasActiveInvokes(invoker) = (%v, %v), want (true, nil) right after Spawn", hasInvokes, err)
	}

	clock.Advance(time.Second) // distinct lastActive from "invoker"
	err := sys.Spawn(ctx, "other", ladderChart.ID(), Durable())
	if !errors.Is(err, ErrResidencyExhausted) {
		t.Fatalf("Spawn(other) error = %v, want ErrResidencyExhausted (invoker is the only candidate, and it's not evictable)", err)
	}
	if !testResident(sys, "invoker") {
		t.Fatalf("expected invoker to remain resident (never evicted while its invoke is active)")
	}
	if testResident(sys, "other") {
		t.Fatalf("expected other to never have activated, given Spawn failed")
	}

	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestIdleTimeoutNeverEvictsActorWithActiveInvoke is runSweep's counterpart
// to TestResidencyLimitNeverEvictsActorWithActiveInvoke: an actor with a
// running <invoke> stays resident past idleTimeout, however long the clock
// advances.
func TestIdleTimeoutNeverEvictsActorWithActiveInvoke(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)
	clock := statecharts.NewManualClock(time.Unix(0, 0))

	invokingChart := buildInvokingChart()

	sys := NewSystem(
		WithLog(log), WithSnapshotStore(log),
		WithIdleTimeout(time.Minute),
		WithClock(clock),
	)
	if err := sys.Register(invokingChart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "invoker", invokingChart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if !testResident(sys, "invoker") {
		t.Fatalf("expected invoker resident right after Spawn")
	}

	clock.Advance(2 * time.Minute) // well past idleTimeout; sweep fires synchronously
	if !testResident(sys, "invoker") {
		t.Fatalf("expected invoker to remain resident past idleTimeout while its invoke is still active")
	}

	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestDurableActorReachingFinalStateIsEvictedImmediately covers github
// issue #6: a durable actor that reaches its own top-level final state
// while processing a Tell is freed right away -- not left resident
// hogging memory until idle-timeout or residency pressure happens to
// notice -- and Stop afterward reports no error for it (Instance.Snapshot
// only works on a still-running actor, so evicting an already-finished one
// must not go through the normal checkpoint path).
func TestDurableActorReachingFinalStateIsEvictedImmediately(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)

	chart := buildFinishingChart()
	sys := NewSystem(WithLog(log), WithSnapshotStore(log))
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "finisher", chart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if !testResident(sys, "finisher") {
		t.Fatalf("expected finisher resident right after Spawn")
	}

	if err := sys.Tell(ctx, "finisher", statecharts.Event{Name: "finish", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell: %v", err)
	}
	if testResident(sys, "finisher") {
		t.Fatalf("expected finisher evicted immediately after reaching its top-level final state")
	}

	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestNonDurableActorReachingFinalStateIsEvictedImmediately is the
// non-durable counterpart: even though non-durable actors are otherwise
// kept resident for the system's whole lifetime (they have no Log to
// rebuild themselves from), one that finishes on its own is still freed --
// there is nothing left to lose by doing so.
func TestNonDurableActorReachingFinalStateIsEvictedImmediately(t *testing.T) {
	ctx := context.Background()
	chart := buildFinishingChart()
	sys := NewSystem()
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "finisher", chart.ID()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if !testResident(sys, "finisher") {
		t.Fatalf("expected finisher resident right after Spawn")
	}

	if err := sys.Tell(ctx, "finisher", statecharts.Event{Name: "finish", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell: %v", err)
	}
	if testResident(sys, "finisher") {
		t.Fatalf("expected finisher evicted immediately after reaching its top-level final state")
	}

	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestSweepReapsActorThatFinishedViaInternalTimerWithNoFurtherTell
// confirms runSweep's own reaping catches a finished actor that deliver's
// inline check never had a chance to: one that reaches its top-level final
// state entirely from an internal delayed <send>, with nothing ever Told
// to it afterward.
func TestSweepReapsActorThatFinishedViaInternalTimerWithNoFurtherTell(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)
	clock := statecharts.NewManualClock(time.Unix(0, 0))

	chart := buildDelayedFinishingChart(30 * time.Second)
	sys := NewSystem(
		WithLog(log), WithSnapshotStore(log),
		WithIdleTimeout(time.Minute),
		WithClock(clock),
	)
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "delayed", chart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	// Past the delayed send's own 30s delay, but short of idleTimeout (1m):
	// the internal timer fires and the machine reaches "done" entirely
	// inside the actor's own goroutine, with no System.deliver call
	// involved at all, so it's still resident until a sweep notices.
	// Firing the timer only enqueues onto the actor's own inbox (see
	// actorClock.AfterFunc) -- Advance returns as soon as that enqueue
	// succeeds, before the actor's goroutine has necessarily processed it
	// -- so wait for Instance.Done() directly rather than racing it.
	clock.Advance(35 * time.Second)
	inst := testInstanceFor(sys, "delayed")
	if inst == nil {
		t.Fatalf("expected delayed still resident right after Advance")
	}
	select {
	case <-inst.Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("delayed's internal timer never fired / never reached its final state")
	}
	if !testResident(sys, "delayed") {
		t.Fatalf("expected delayed still resident right after reaching its final state (nothing has reaped it yet)")
	}

	// Past idleTimeout: the periodic sweep fires and reaps it regardless
	// of its actual idle time, since it has already finished.
	clock.Advance(30 * time.Second)
	if testResident(sys, "delayed") {
		t.Fatalf("expected delayed reaped once the periodic sweep ran")
	}

	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestStopDoesNotErrorForActorThatAlreadyFinished is a regression test for
// a bug evictLocked's finished-instance fast path also fixes: before it,
// evicting a durable actor whose Instance had already stopped on its own
// went through the normal Snapshot-then-checkpoint path, and
// Instance.Snapshot always fails (ErrInstanceStopped) against an
// already-stopped Instance -- so Stop would have reported a spurious
// failure for an actor that had simply, legitimately finished. Idle-timeout
// sweeping is disabled here so nothing reaps the actor before Stop itself
// does, isolating that path specifically.
func TestStopDoesNotErrorForActorThatAlreadyFinished(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)
	clock := statecharts.NewManualClock(time.Unix(0, 0))

	chart := buildDelayedFinishingChart(time.Second)
	sys := NewSystem(
		WithLog(log), WithSnapshotStore(log),
		WithIdleTimeout(0), // disables sweeping entirely
		WithClock(clock),
	)
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "delayed", chart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	clock.Advance(2 * time.Second)
	inst := testInstanceFor(sys, "delayed")
	if inst == nil {
		t.Fatalf("expected delayed resident right after Advance")
	}
	select {
	case <-inst.Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("delayed never reached its final state")
	}
	if !testResident(sys, "delayed") {
		t.Fatalf("expected delayed still resident (sweeping disabled, nothing else has touched it)")
	}

	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v, want nil (an already-finished durable actor must not error Stop)", err)
	}
}

func TestDurableInvokeCompletionIsWriteAheadLogged(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)
	complete := make(chan struct{})
	chart, err := statecharts.Build(
		statecharts.Compound("durable-invoke", "working",
			statecharts.Children(
				statecharts.Atomic("working",
					statecharts.Invoke(func(context.Context, any, statecharts.InvokeIO) (any, error) {
						<-complete
						return nil, nil
					}, statecharts.WithInvokeID("job")),
					statecharts.On("done.invoke.job", statecharts.Target("completed")),
				),
				statecharts.Atomic("completed"),
			),
		),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	sys := NewSystem(WithLog(log), WithSnapshotStore(log), WithIdleTimeout(0))
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "job-1", chart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	close(complete)
	inst := testInstanceFor(sys, "job-1")
	waitFor(t, 2*time.Second, func() bool { return hasStateID(inst.Configuration(), "completed") })
	seq, err := log.LastSeq(ctx, "job-1")
	if err != nil {
		t.Fatalf("LastSeq: %v", err)
	}
	if seq != 2 {
		t.Fatalf("LastSeq after invoke completion = %d, want 2 (start plus completion)", seq)
	}
	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestFinishedDurableActorDoesNotAppendUndeliverableMessage(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)
	clock := statecharts.NewManualClock(time.Unix(0, 0))
	chart := buildDelayedFinishingChart(time.Second)
	sys := NewSystem(
		WithLog(log), WithSnapshotStore(log), WithClock(clock), WithIdleTimeout(0),
	)
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "finished", chart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	clock.Advance(2 * time.Second)
	inst := testInstanceFor(sys, "finished")
	select {
	case <-inst.Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("actor did not finish from its internal timer")
	}

	if err := sys.Tell(ctx, "finished", statecharts.Event{Name: "too-late", Type: statecharts.EventExternal}); !errors.Is(err, statecharts.ErrInstanceStopped) {
		t.Fatalf("Tell after final state = %v, want ErrInstanceStopped", err)
	}
	seq, err := log.LastSeq(ctx, "finished")
	if err != nil {
		t.Fatalf("LastSeq: %v", err)
	}
	if seq != 2 {
		t.Fatalf("LastSeq after rejected post-final Tell = %d, want 2 (start plus timer only)", seq)
	}
	if err := sys.Spawn(ctx, "finished", chart.ID(), Durable()); err != nil {
		t.Fatalf("re-Spawn after rejected message poisoned replay: %v", err)
	}
	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestNodeQualifiedAddressIsDurableSessionID(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)
	var dms []*counterModel
	chart := buildLadderChart(&dms)
	sys := NewSystem(
		WithNodeName("warehouse-a"), WithLog(log), WithSnapshotStore(log),
	)
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "counter-1", chart.ID(), Durable()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if err := sys.Tell(ctx, "warehouse-a.counter-1", statecharts.Event{Name: "inc", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Tell: %v", err)
	}
	if seq, err := log.LastSeq(ctx, "warehouse-a.counter-1"); err != nil || seq != 2 {
		t.Fatalf("qualified LastSeq = %d, %v, want 2, nil", seq, err)
	}
	if seq, err := log.LastSeq(ctx, "counter-1"); err != nil || seq != 0 {
		t.Fatalf("unqualified LastSeq = %d, %v, want 0, nil", seq, err)
	}
	if err := sys.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestDurableActorDoesNotRestartInitialInvokeAfterCrashBeforeFirstMessage(t *testing.T) {
	ctx := context.Background()
	log := openTestLog(t)
	var mu sync.Mutex
	starts := 0
	started := make(chan struct{})
	var startedOnce sync.Once
	buildChart := func() *statecharts.Chart {
		chart, err := statecharts.Build(
			statecharts.Compound("initial-invoke", "invoking",
				statecharts.Children(
					statecharts.Atomic("invoking",
						statecharts.Invoke(func(ctx context.Context, _ any, _ statecharts.InvokeIO) (any, error) {
							mu.Lock()
							starts++
							mu.Unlock()
							startedOnce.Do(func() { close(started) })
							<-ctx.Done()
							return nil, nil
						}, statecharts.WithInvokeID("work")),
						statecharts.On(string(statecharts.ErrEventCommunication), statecharts.Target("recovered")),
					),
					statecharts.Atomic("recovered"),
				),
			),
			statecharts.WithNewDatamodel(func() any { return &struct{}{} }),
		)
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return chart
	}

	sys1 := NewSystem(WithLog(log), WithSnapshotStore(log), WithIdleTimeout(0))
	chart1 := buildChart()
	if err := sys1.Register(chart1); err != nil {
		t.Fatalf("Register sys1: %v", err)
	}
	if err := sys1.Spawn(ctx, "worker", chart1.ID(), Durable()); err != nil {
		t.Fatalf("Spawn sys1: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatalf("initial invoke never started")
	}
	if seq, err := log.LastSeq(ctx, "worker"); err != nil || seq != 1 {
		t.Fatalf("LastSeq after durable start = %d, %v, want 1, nil (session-start marker)", seq, err)
	}
	// Simulate a process crash without checkpointing the System. Stopping the
	// captured Instance merely prevents the test's stand-in invoke goroutine
	// from leaking; no System checkpoint or actor message is written.
	if err := testInstanceFor(sys1, "worker").Stop(ctx); err != nil {
		t.Fatalf("stop crashed instance: %v", err)
	}

	sys2 := NewSystem(WithLog(log), WithSnapshotStore(log), WithIdleTimeout(0))
	chart2 := buildChart()
	if err := sys2.Register(chart2); err != nil {
		t.Fatalf("Register sys2: %v", err)
	}
	if err := sys2.Spawn(ctx, "worker", chart2.ID(), Durable()); err != nil {
		t.Fatalf("Spawn sys2: %v", err)
	}
	mu.Lock()
	gotStarts := starts
	mu.Unlock()
	if gotStarts != 1 {
		t.Fatalf("initial invoke starts = %d, want 1 across crash recovery", gotStarts)
	}
	if inst := testInstanceFor(sys2, "worker"); inst == nil || !hasStateID(inst.Configuration(), "recovered") {
		var cfg []statecharts.Identifier
		if inst != nil {
			cfg = inst.Configuration()
		}
		t.Fatalf("recovered configuration = %v, want recovered", cfg)
	}
	if err := sys1.Stop(ctx); err != nil {
		t.Fatalf("Stop sys1: %v", err)
	}
	if err := sys2.Stop(ctx); err != nil {
		t.Fatalf("Stop sys2: %v", err)
	}
}
