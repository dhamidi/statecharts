package actors

import (
	"context"
	"database/sql"
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
	// Log alone, untouched by any checkpoint, must already hold all 3
	// events.
	if seq, err := log.LastSeq(ctx, "crash-1"); err != nil || seq != 3 {
		t.Fatalf("LastSeq = %d, %v, want 3, nil -- messages were not write-ahead-logged before being applied", seq, err)
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
	// transaction per call, so LastSeq landing on exactly n already proves
	// exactly n successful, distinct Append calls happened -- not n-1 (a
	// lost message) and not n+1 or more (a duplicate).
	seq, err := log.LastSeq(ctx, "hammer-1")
	if err != nil {
		t.Fatalf("LastSeq: %v", err)
	}
	if seq != n {
		t.Fatalf("LastSeq = %d, want %d (every Tell must be logged exactly once)", seq, n)
	}

	// Read every entry back and confirm Seq is exactly the gapless run
	// 1..n with no duplicates, each one the "inc" event Tell sent -- a
	// stronger check than LastSeq alone, which could in principle land on
	// n by coincidence (e.g. one lost + one duplicated).
	seen := map[uint64]bool{}
	count := 0
	for entry, err := range log.Read(ctx, "hammer-1", 1) {
		if err != nil {
			t.Fatalf("Read: %v", err)
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
		if seq != n+1 {
			t.Fatalf("LastSeq after repage = %d, want %d", seq, n+1)
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
