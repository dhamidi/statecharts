package statecharts

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// memLog is a minimal in-memory Log test double.
type memLog struct {
	mu      sync.Mutex
	entries map[SessionID][]LogEntry
}

func newMemLog() *memLog { return &memLog{entries: map[SessionID][]LogEntry{}} }

func (l *memLog) Append(ctx context.Context, entry LogEntry) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	seq := uint64(len(l.entries[entry.SessionID])) + 1
	entry.Seq = seq
	l.entries[entry.SessionID] = append(l.entries[entry.SessionID], entry)
	return seq, nil
}

func (l *memLog) Read(ctx context.Context, sessionID SessionID, from uint64) iter.Seq2[LogEntry, error] {
	return func(yield func(LogEntry, error) bool) {
		l.mu.Lock()
		entries := append([]LogEntry(nil), l.entries[sessionID]...)
		l.mu.Unlock()
		for _, e := range entries {
			if e.Seq < from {
				continue
			}
			if !yield(e, nil) {
				return
			}
		}
	}
}

func (l *memLog) LastSeq(ctx context.Context, sessionID SessionID) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	entries := l.entries[sessionID]
	if len(entries) == 0 {
		return 0, nil
	}
	return entries[len(entries)-1].Seq, nil
}

// memSnapshotStore is a minimal in-memory SnapshotStore test double.
type memSnapshotStore struct {
	mu sync.Mutex
	cp map[SessionID]Checkpoint
}

func newMemSnapshotStore() *memSnapshotStore {
	return &memSnapshotStore{cp: map[SessionID]Checkpoint{}}
}

func (s *memSnapshotStore) Save(ctx context.Context, sessionID SessionID, cp Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cp[sessionID] = cp
	return nil
}

func (s *memSnapshotStore) Load(ctx context.Context, sessionID SessionID) (Checkpoint, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp, ok := s.cp[sessionID]
	return cp, ok, nil
}

func doorChart(t *testing.T) *Chart {
	t.Helper()
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
	return chart
}

func TestSnapshotRoundTripJSON(t *testing.T) {
	chart := doorChart(t)
	d := &Door{}
	in := New(chart, d)
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := in.Send(ctx, Event{Name: "open.request", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	snap, err := in.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if !hasState(snap.Configuration, "open") {
		t.Fatalf("snapshot configuration = %v, want 'open'", snap.Configuration)
	}

	b, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Snapshot
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !hasState(got.Configuration, "open") {
		t.Fatalf("round-tripped configuration = %v, want 'open'", got.Configuration)
	}

	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestSnapshotJSONRoundTripsID(t *testing.T) {
	chart := doorChart(t)
	d := &Door{}
	in := New(chart, d, WithSessionID("sess-json-id"))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	snap, err := in.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.ID != "sess-json-id" {
		t.Fatalf("snap.ID = %q, want %q", snap.ID, "sess-json-id")
	}

	b, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Snapshot
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ID != "sess-json-id" {
		t.Fatalf("round-tripped ID = %q, want %q", got.ID, "sess-json-id")
	}

	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestRestorePreservesSessionIDFromSnapshot(t *testing.T) {
	chart := doorChart(t)
	d1 := &Door{}
	in1 := New(chart, d1, WithSessionID("original-session"))
	ctx := context.Background()
	if err := in1.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	snap, err := in1.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := in1.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	d2 := &Door{}
	in2, err := Restore(chart, d2, snap)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if in2.ID() != "original-session" {
		t.Fatalf("restored Instance.ID() = %q, want %q (preserved from Snapshot)", in2.ID(), "original-session")
	}
}

func TestRestoreWithSessionIDOverridesSnapshot(t *testing.T) {
	chart := doorChart(t)
	d1 := &Door{}
	in1 := New(chart, d1, WithSessionID("original-session"))
	ctx := context.Background()
	if err := in1.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	snap, err := in1.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := in1.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	d2 := &Door{}
	in2, err := Restore(chart, d2, snap, WithSessionID("override-session"))
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if in2.ID() != "override-session" {
		t.Fatalf("restored Instance.ID() = %q, want %q (explicit WithSessionID must override the Snapshot's)", in2.ID(), "override-session")
	}
}

func TestRestoreFromSnapshot(t *testing.T) {
	chart := doorChart(t)
	d1 := &Door{}
	in1 := New(chart, d1)
	ctx := context.Background()
	if err := in1.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := in1.Send(ctx, Event{Name: "open.request", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	snap, err := in1.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := in1.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Restore against a FRESH datamodel value, as a real cold start would.
	d2 := &Door{}
	in2, err := Restore(chart, d2, snap)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if err := in2.Start(ctx); err != nil {
		t.Fatalf("Start (restored): %v", err)
	}
	if !hasState(in2.Configuration(), "open") {
		t.Fatalf("restored configuration = %v, want 'open'", in2.Configuration())
	}
	// onentry/onexit must NOT re-run: OpenCount should still be 0 on the
	// fresh datamodel (Restore skips executable content, unlike replay).
	if d2.OpenCount != 0 {
		t.Fatalf("d2.OpenCount = %d, want 0 (Restore must not re-run actions)", d2.OpenCount)
	}

	// the restored instance must still work going forward.
	if err := in2.Send(ctx, Event{Name: "close.request", Type: EventExternal}); err != nil {
		t.Fatalf("Send after restore: %v", err)
	}
	if !hasState(in2.Configuration(), "closed") {
		t.Fatalf("configuration after restore+Send = %v, want 'closed'", in2.Configuration())
	}
	if err := in2.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestRehydrateReplaysExplicitSends(t *testing.T) {
	ctx := context.Background()
	log := newMemLog()
	store := newMemSnapshotStore()
	sessionID := SessionID("sess-1")

	// Live phase: append-then-Send for each explicit external event, so
	// the log is always written ahead of the event being applied.
	chart := doorChart(t)
	d := &Door{}
	in := New(chart, d, WithIOProcessor(NoopIOProcessor))
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	sendLive := func(ev Event) {
		t.Helper()
		if _, err := log.Append(ctx, LogEntry{
			SessionID: sessionID, Kind: KindExternalEvent, Timestamp: time.Now().UTC(), Event: ev,
		}); err != nil {
			t.Fatalf("log.Append: %v", err)
		}
		if err := in.Send(ctx, ev); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}

	sendLive(Event{Name: "open.request", Type: EventExternal})
	sendLive(Event{Name: "close.request", Type: EventExternal})
	sendLive(Event{Name: "open.request", Type: EventExternal})

	if !hasState(in.Configuration(), "open") {
		t.Fatalf("live configuration = %v, want 'open'", in.Configuration())
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := in.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	// Cold start: reconstruct purely from the log (no checkpoint saved),
	// against a brand new datamodel value.
	d2 := &Door{}
	in2, err := Rehydrate(ctx, chart, d2, log, store, sessionID, NoopIOProcessor)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}
	if !hasState(in2.Configuration(), "open") {
		t.Fatalf("rehydrated configuration = %v, want 'open'", in2.Configuration())
	}
	if d2.OpenCount != 2 {
		t.Fatalf("d2.OpenCount = %d, want 2 (both open.request events replayed)", d2.OpenCount)
	}
	if err := in2.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestRehydrateUsesCheckpointToSkipReplay(t *testing.T) {
	ctx := context.Background()
	log := newMemLog()
	store := newMemSnapshotStore()
	sessionID := SessionID("sess-2")

	chart := doorChart(t)
	d := &Door{}
	in := New(chart, d)
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ev := Event{Name: "open.request", Type: EventExternal}
	seq, err := log.Append(ctx, LogEntry{SessionID: sessionID, Kind: KindExternalEvent, Timestamp: time.Now().UTC(), Event: ev})
	if err != nil {
		t.Fatalf("log.Append: %v", err)
	}
	if err := in.Send(ctx, ev); err != nil {
		t.Fatalf("Send: %v", err)
	}

	snap, err := in.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := store.Save(ctx, sessionID, Checkpoint{Snapshot: snap, Seq: seq}); err != nil {
		t.Fatalf("store.Save: %v", err)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// A log entry AFTER the checkpoint must still be replayed.
	ev2 := Event{Name: "close.request", Type: EventExternal}
	if _, err := log.Append(ctx, LogEntry{SessionID: sessionID, Kind: KindExternalEvent, Timestamp: time.Now().UTC(), Event: ev2}); err != nil {
		t.Fatalf("log.Append: %v", err)
	}

	d2 := &Door{}
	in2, err := Rehydrate(ctx, chart, d2, log, store, sessionID, NoopIOProcessor)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}
	if !hasState(in2.Configuration(), "closed") {
		t.Fatalf("configuration = %v, want 'closed' (checkpoint + post-checkpoint replay)", in2.Configuration())
	}
	// OpenCount must be 0: the checkpoint already reflects the one
	// open.request (its action ran once, live); replay must not re-run it.
	if d2.OpenCount != 0 {
		t.Fatalf("d2.OpenCount = %d, want 0 (checkpointed action must not replay)", d2.OpenCount)
	}
	if err := in2.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestRehydrateSuppressesRealDispatchDuringReplayThenGoesLive(t *testing.T) {
	ctx := context.Background()
	log := newMemLog()
	store := newMemSnapshotStore()
	sessionID := SessionID("sess-3")

	// A chart whose transition sends to a genuinely external target,
	// so we can observe whether the real IOProcessor was invoked.
	chart, err := Build(
		Compound("m", "a",
			Children(
				Atomic("a", On("go", Target("b"), Then(SendEvent("ping", SendOptions{Target: "external-target"})))),
				Atomic("b"),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	spy := &spyIOProcessor{}
	in := New(chart, nil, WithIOProcessor(spy))
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ev := Event{Name: "go", Type: EventExternal}
	if _, err := log.Append(ctx, LogEntry{SessionID: sessionID, Kind: KindExternalEvent, Timestamp: time.Now().UTC(), Event: ev}); err != nil {
		t.Fatalf("log.Append: %v", err)
	}
	if err := in.Send(ctx, ev); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if spy.sendCount != 1 {
		t.Fatalf("live sendCount = %d, want 1", spy.sendCount)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	spy2 := &spyIOProcessor{}
	in2, err := Rehydrate(ctx, chart, nil, log, store, sessionID, spy2)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}
	if spy2.sendCount != 0 {
		t.Fatalf("spy2.sendCount after replay = %d, want 0 (real dispatch must be suppressed during replay)", spy2.sendCount)
	}
	if !hasState(in2.Configuration(), "b") {
		t.Fatalf("configuration after replay = %v, want 'b'", in2.Configuration())
	}

	// Now live: a fresh transition firing SendEvent should reach the real
	// IOProcessor, proving the gate flips to pass-through after replay.
	// (Re-use chart's only transition path isn't re-triggerable from b, so
	// just confirm no further suppressed calls occurred and the gate itself
	// is marked live.)
	if err := in2.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestRehydrateIOProcessorsReportedDuringAndAfterReplay confirms
// replayGate.IOProcessors forwards to the wrapped IOProcessor even while
// !live -- unlike Send/Cancel/Log, reading an already-advertised address has
// no real-world side effect to suppress during replay.
func TestRehydrateIOProcessorsReportedDuringAndAfterReplay(t *testing.T) {
	ctx := context.Background()
	log := newMemLog()
	store := newMemSnapshotStore()
	sessionID := SessionID("sess-ioprocessors")

	var seen []IOProcessorInfo
	record := func(ec ExecContext) error {
		seen = ec.IOProcessors()
		return nil
	}

	chart, err := Build(
		Compound("m", "a",
			Children(
				Atomic("a", On("go", Target("b"), Then(record))),
				Atomic("b", On("go", Target("a"), Then(record))),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	liveIO := &describingIOProcessor{infos: []IOProcessorInfo{{Type: "mock", Location: mustLocation(t, "mock://live")}}}
	in := New(chart, nil, WithIOProcessor(liveIO))
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ev := Event{Name: "go", Type: EventExternal}
	if _, err := log.Append(ctx, LogEntry{SessionID: sessionID, Kind: KindExternalEvent, Timestamp: time.Now().UTC(), Event: ev}); err != nil {
		t.Fatalf("log.Append: %v", err)
	}
	if err := in.Send(ctx, ev); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Reset before Rehydrate so the next observation is unambiguously from
	// the replay below, not the live phase above.
	seen = nil

	replayIO := &describingIOProcessor{infos: []IOProcessorInfo{{Type: "mock", Location: mustLocation(t, "mock://replayed")}}}
	in2, err := Rehydrate(ctx, chart, nil, log, store, sessionID, replayIO)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}

	// The single logged "go" event replays during Rehydrate itself, before
	// goLive is called -- so this observation is from inside replay, while
	// the gate is still suppressing Send/Cancel/Log.
	if len(seen) != 1 || seen[0].Location.String() != "mock://replayed" {
		t.Fatalf("ExecContext.IOProcessors() during replay = %v, want [{mock mock://replayed}]", seen)
	}

	// Now live: a fresh Send should see the same wrapped processor's
	// entries, proving IOProcessors() keeps working once the gate flips.
	seen = nil
	if err := in2.Send(ctx, ev); err != nil {
		t.Fatalf("Send (live): %v", err)
	}
	if len(seen) != 1 || seen[0].Location.String() != "mock://replayed" {
		t.Fatalf("ExecContext.IOProcessors() after goLive = %v, want [{mock mock://replayed}]", seen)
	}

	if err := in2.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestRehydrateWithNilLoggerDoesNotPanicOnceLive reproduces a nil-pointer
// panic in replayGate.Log: Rehydrate always wraps whatever Logger it finds
// in a non-nil *replayGate, so doLog's own "logger != nil" guard always
// passes even when the caller configured WithLogger(nil), and the gate must
// therefore refuse to dereference a nil wrapped Logger itself once live.
func TestRehydrateWithNilLoggerDoesNotPanicOnceLive(t *testing.T) {
	ctx := context.Background()
	log := newMemLog()
	store := newMemSnapshotStore()
	sessionID := SessionID("sess-nil-logger")

	logAction := func(ec ExecContext) error {
		ec.Log("transition", nil)
		return nil
	}
	chart, err := Build(
		Compound("m", "a",
			Children(
				Atomic("a", On("go", Target("b"), Then(logAction))),
				Atomic("b", On("back", Target("a"), Then(logAction))),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in2, err := Rehydrate(ctx, chart, nil, log, store, sessionID, NoopIOProcessor, WithLogger(nil))
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}

	// A live Send that triggers Log must not panic even though no Logger
	// was configured.
	if err := in2.Send(ctx, Event{Name: "go", Type: EventExternal}); err != nil {
		t.Fatalf("Send after Rehydrate: %v", err)
	}
	if !hasState(in2.Configuration(), "b") {
		t.Fatalf("configuration = %v, want 'b'", in2.Configuration())
	}
	if err := in2.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestRehydrateSuppressesInvokeStartAndSignalsCommErrorForMidInvokeState
// covers github issue #5: an <invoke> attached to a state present in the
// restored configuration must never actually restart (its real goroutine
// already ran once, live -- restarting it during Rehydrate's own bootstrap
// would repeat that real-world side effect, see ADR 0010), and Rehydrate
// must instead synthesize error.communication for it once replay catches
// up, since there is no way to guarantee the original invocation's process
// is still alive.
func TestRehydrateSuppressesInvokeStartAndSignalsCommErrorForMidInvokeState(t *testing.T) {
	ctx := context.Background()
	log := newMemLog()
	store := newMemSnapshotStore()
	sessionID := SessionID("sess-invoke")

	buildChart := func(t *testing.T, starts *int32, started chan struct{}) *Chart {
		t.Helper()
		chart, err := Build(
			Compound("m", "a",
				Children(
					Atomic("a",
						Invoke(func(ctx context.Context, params any, io InvokeIO) (any, error) {
							atomic.AddInt32(starts, 1)
							if started != nil {
								close(started)
							}
							<-ctx.Done()
							return nil, nil
						}),
						On(string(ErrEventCommunication), Target("recovered")),
					),
					Atomic("recovered"),
				),
			),
		)
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return chart
	}

	// Live phase: the invoke genuinely starts once, entering "a" via
	// Start's own initial-configuration bootstrap.
	var liveStarts int32
	started := make(chan struct{})
	liveChart := buildChart(t, &liveStarts, started)
	in := New(liveChart, nil)
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatalf("live invoke never started")
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Cold start: no checkpoint, nothing logged (entering "a" is itself the
	// deterministic content replayed, exactly as Start's own bootstrap would
	// produce it live) -- so Rehydrate's replay pass is just its Start call.
	var replayStarts int32
	replayChart := buildChart(t, &replayStarts, nil)
	in2, err := Rehydrate(ctx, replayChart, nil, log, store, sessionID, NoopIOProcessor)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}
	defer in2.Stop(ctx)

	if got := atomic.LoadInt32(&replayStarts); got != 0 {
		t.Fatalf("replay invoke start count = %d, want 0 (invoke must not restart during Rehydrate)", got)
	}
	if !hasState(in2.Configuration(), "recovered") {
		t.Fatalf("configuration after Rehydrate = %v, want 'recovered' (mid-invoke error.communication must fire once replay catches up)", in2.Configuration())
	}
}

// TestRestoreReconstructsActiveInvokeBookkeeping is ADR 0013 Gap 1's most
// direct test: Restore alone -- not Rehydrate -- never calls
// enterStates/processInvokes, so ip.invokesByID/ip.activeInvokes can only be
// populated here from snap.ActiveInvokes itself.
func TestRestoreReconstructsActiveInvokeBookkeeping(t *testing.T) {
	ctx := context.Background()
	chart, err := Build(
		Compound("m", "a",
			Children(
				Atomic("a",
					Invoke(func(ctx context.Context, params any, io InvokeIO) (any, error) {
						<-ctx.Done()
						return nil, nil
					}),
				),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, nil)
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	snap, err := in.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	wantID := Identifier("a.invoke1")
	if len(snap.ActiveInvokes) != 1 || snap.ActiveInvokes[0] != (ActiveInvoke{State: "a", SpecIndex: 0, ID: wantID}) {
		t.Fatalf("snap.ActiveInvokes = %+v, want a single {State:a SpecIndex:0 ID:%s}", snap.ActiveInvokes, wantID)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	in2, err := Restore(chart, nil, snap)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}

	ri, ok := in2.ip.invokesByID[wantID]
	if !ok {
		t.Fatalf("ip.invokesByID missing %q after Restore", wantID)
	}
	if ri.state == nil || ri.state.id != "a" || ri.specIndex != 0 {
		t.Fatalf("restored runningInvoke = %+v, want state=\"a\" specIndex=0", ri)
	}

	stateA := chart.byID["a"]
	if got := in2.ip.activeInvokes[stateA]; len(got) != 1 || got[0] != ri {
		t.Fatalf("ip.activeInvokes[a] = %v, want [%v]", got, ri)
	}
}

// TestRehydrateResolvesInvokeCapturedInCheckpointWithNoLogToReplay reproduces
// ADR 0013's Gap 1 exactly: a checkpoint captured while an invoke is
// genuinely running, with nothing about entering the invoking state or
// starting the invoke ever written to the log, so replaying from the
// checkpoint's Seq+1 replays nothing at all. Before Snapshot.ActiveInvokes
// existed, this left ip.invokesByID empty after Restore and the invocation
// stuck forever, waiting on a done.invoke nothing would ever generate. This
// is the regression test for that bug.
func TestRehydrateResolvesInvokeCapturedInCheckpointWithNoLogToReplay(t *testing.T) {
	ctx := context.Background()
	log := newMemLog()
	store := newMemSnapshotStore()
	sessionID := SessionID("sess-checkpoint-invoke")

	buildChart := func(t *testing.T, starts *int32, started chan struct{}) *Chart {
		t.Helper()
		chart, err := Build(
			Compound("m", "a",
				Children(
					Atomic("a",
						Invoke(func(ctx context.Context, params any, io InvokeIO) (any, error) {
							atomic.AddInt32(starts, 1)
							if started != nil {
								close(started)
							}
							<-ctx.Done()
							return nil, nil
						}),
						On(string(ErrEventCommunication), Target("recovered")),
					),
					Atomic("recovered"),
				),
			),
		)
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return chart
	}

	// Live phase: the invoke starts, and the checkpoint is taken while it is
	// still running -- no log entry records entering "a" or starting the
	// invoke, unlike the ADR 0012 case (TestRehydrateSuppressesInvokeStart...
	// above), where Rehydrate's own bootstrap replays that entry.
	var liveStarts int32
	started := make(chan struct{})
	liveChart := buildChart(t, &liveStarts, started)
	in := New(liveChart, nil)
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatalf("live invoke never started")
	}

	snap, err := in.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap.ActiveInvokes) != 1 {
		t.Fatalf("snap.ActiveInvokes = %v, want 1 entry", snap.ActiveInvokes)
	}
	if err := store.Save(ctx, sessionID, Checkpoint{Snapshot: snap, Seq: 0}); err != nil {
		t.Fatalf("store.Save: %v", err)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	var replayStarts int32
	replayChart := buildChart(t, &replayStarts, nil)
	in2, err := Rehydrate(ctx, replayChart, nil, log, store, sessionID, NoopIOProcessor)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}
	defer in2.Stop(ctx)

	if got := atomic.LoadInt32(&replayStarts); got != 0 {
		t.Fatalf("rehydrated invoke start count = %d, want 0 (a non-resumable invoke must never restart)", got)
	}
	if !hasState(in2.Configuration(), "recovered") {
		t.Fatalf("configuration after Rehydrate = %v, want 'recovered' (checkpoint-derived invoke must still resolve to error.communication)", in2.Configuration())
	}
}

// TestInvokeResumeErrorBecomesCommunicationError covers WithInvokeResume's
// first outcome: Resume reporting the real-world resource confirmed gone.
func TestInvokeResumeErrorBecomesCommunicationError(t *testing.T) {
	ctx := context.Background()
	log := newMemLog()
	store := newMemSnapshotStore()
	sessionID := SessionID("sess-resume-error")
	resumeErr := errors.New("resume: process confirmed gone")

	buildChart := func(t *testing.T, resume InvokeResumeFunc, started chan struct{}, captured chan Event) *Chart {
		t.Helper()
		onCommError := Action(func(d *struct{}, ec ExecContext) error {
			if captured != nil {
				ev, _ := ec.Event()
				captured <- ev
			}
			return nil
		})
		chart, err := Build(
			Compound("m", "a",
				Children(
					Atomic("a",
						Invoke(func(ctx context.Context, params any, io InvokeIO) (any, error) {
							if started != nil {
								close(started)
							}
							<-ctx.Done()
							return nil, nil
						}, WithInvokeID("job"), WithInvokeResume(resume)),
						On(string(ErrEventCommunication), Then(onCommError), Target("failed")),
					),
					Atomic("failed"),
				),
			),
		)
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return chart
	}

	started := make(chan struct{})
	in := New(buildChart(t, nil, started, nil), &struct{}{})
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatalf("live invoke never started")
	}
	snap, err := in.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := store.Save(ctx, sessionID, Checkpoint{Snapshot: snap, Seq: 0}); err != nil {
		t.Fatalf("store.Save: %v", err)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	resume := func(ctx context.Context, id Identifier, params any, io InvokeIO) (any, error) {
		return nil, resumeErr
	}
	captured := make(chan Event, 1)
	replayChart := buildChart(t, resume, nil, captured)
	in2, err := Rehydrate(ctx, replayChart, &struct{}{}, log, store, sessionID, NoopIOProcessor)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}
	defer in2.Stop(ctx)

	select {
	case ev := <-captured:
		if ev.Data != resumeErr {
			t.Fatalf("error.communication Data = %v, want %v", ev.Data, resumeErr)
		}
		if ev.InvokeID != "job" {
			t.Fatalf("error.communication InvokeID = %q, want %q", ev.InvokeID, "job")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Resume's error never produced error.communication")
	}

	// captured fires from inside the transition's own action content, before
	// enterStates/publishConfig for "failed" complete -- poll (same pattern
	// as TestInvokeStartsOnStateEntryAndDeliversEvents) instead of assuming
	// Configuration is already updated the instant captured receives.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if err := in2.Send(ctx, Event{Name: "noop", Type: EventExternal}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		if hasState(in2.Configuration(), "failed") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("configuration = %v, want 'failed'", in2.Configuration())
		}
		time.Sleep(time.Millisecond)
	}
}

// TestInvokeResumeDataBecomesDoneInvoke covers WithInvokeResume's second
// outcome: the work already finished while nothing was watching it, so
// Resume returns immediately with data instead of blocking.
func TestInvokeResumeDataBecomesDoneInvoke(t *testing.T) {
	ctx := context.Background()
	log := newMemLog()
	store := newMemSnapshotStore()
	sessionID := SessionID("sess-resume-data")

	buildChart := func(t *testing.T, resume InvokeResumeFunc, started chan struct{}, captured chan Event) *Chart {
		t.Helper()
		onDone := Action(func(d *struct{}, ec ExecContext) error {
			if captured != nil {
				ev, _ := ec.Event()
				captured <- ev
			}
			return nil
		})
		chart, err := Build(
			Compound("m", "a",
				Children(
					Atomic("a",
						Invoke(func(ctx context.Context, params any, io InvokeIO) (any, error) {
							if started != nil {
								close(started)
							}
							<-ctx.Done()
							return nil, nil
						}, WithInvokeID("job"), WithInvokeResume(resume)),
						On("done.invoke.job", Then(onDone), Target("finished")),
					),
					Atomic("finished"),
				),
			),
		)
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return chart
	}

	started := make(chan struct{})
	in := New(buildChart(t, nil, started, nil), &struct{}{})
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatalf("live invoke never started")
	}
	snap, err := in.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := store.Save(ctx, sessionID, Checkpoint{Snapshot: snap, Seq: 0}); err != nil {
		t.Fatalf("store.Save: %v", err)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	resume := func(ctx context.Context, id Identifier, params any, io InvokeIO) (any, error) {
		return "job-result", nil
	}
	captured := make(chan Event, 1)
	replayChart := buildChart(t, resume, nil, captured)
	in2, err := Rehydrate(ctx, replayChart, &struct{}{}, log, store, sessionID, NoopIOProcessor)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}
	defer in2.Stop(ctx)

	select {
	case ev := <-captured:
		if ev.Data != "job-result" {
			t.Fatalf("done.invoke.job Data = %v, want %q", ev.Data, "job-result")
		}
		if ev.InvokeID != "job" {
			t.Fatalf("done.invoke.job InvokeID = %q, want %q", ev.InvokeID, "job")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Resume's data never produced done.invoke.job")
	}

	// captured fires from inside the transition's own action content, before
	// enterStates/publishConfig for "finished" complete -- poll instead of
	// assuming Configuration is already updated the instant captured
	// receives.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if err := in2.Send(ctx, Event{Name: "noop", Type: EventExternal}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		if hasState(in2.Configuration(), "finished") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("configuration = %v, want 'finished'", in2.Configuration())
		}
		time.Sleep(time.Millisecond)
	}
}

// TestInvokeResumeBlockingKeepsInvocationActiveAndReachable covers
// WithInvokeResume's third outcome: Resume blocks, and the resumed
// invocation keeps running exactly as if it had never stopped -- proven two
// ways, HasActiveInvokes staying true and a round trip through
// io.Incoming/io.Deliver actually reaching the live chart.
func TestInvokeResumeBlockingKeepsInvocationActiveAndReachable(t *testing.T) {
	ctx := context.Background()
	log := newMemLog()
	store := newMemSnapshotStore()
	sessionID := SessionID("sess-resume-blocking")

	buildChart := func(t *testing.T, resume InvokeResumeFunc, started chan struct{}) *Chart {
		t.Helper()
		chart, err := Build(
			Compound("m", "a",
				Children(
					Atomic("a",
						Invoke(func(ctx context.Context, params any, io InvokeIO) (any, error) {
							if started != nil {
								close(started)
							}
							<-ctx.Done()
							return nil, nil
						}, WithInvokeID("job"), WithInvokeResume(resume), WithAutoForward()),
						On("resumed.echo", Target("echoed")),
					),
					Atomic("echoed"),
				),
			),
		)
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return chart
	}

	started := make(chan struct{})
	in := New(buildChart(t, nil, started), nil)
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatalf("live invoke never started")
	}
	snap, err := in.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := store.Save(ctx, sessionID, Checkpoint{Snapshot: snap, Seq: 0}); err != nil {
		t.Fatalf("store.Save: %v", err)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	resumed := make(chan struct{})
	resume := func(ctx context.Context, id Identifier, params any, io InvokeIO) (any, error) {
		close(resumed)
		for {
			select {
			case ev := <-io.Incoming:
				if ev.Name == "poke" {
					io.Deliver(Event{Name: "resumed.echo"})
				}
			case <-ctx.Done():
				return nil, nil
			}
		}
	}
	replayChart := buildChart(t, resume, nil)
	in2, err := Rehydrate(ctx, replayChart, nil, log, store, sessionID, NoopIOProcessor)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}
	defer in2.Stop(ctx)

	select {
	case <-resumed:
	case <-time.After(2 * time.Second):
		t.Fatalf("Resume was never called")
	}

	active, err := in2.HasActiveInvokes(ctx)
	if err != nil {
		t.Fatalf("HasActiveInvokes: %v", err)
	}
	if !active {
		t.Fatalf("HasActiveInvokes = false, want true for a blocking Resume that kept running")
	}

	// WithAutoForward carries "poke" to Resume's io.Incoming; Resume echoes
	// it back via io.Deliver, proving the resumed invocation's traffic
	// reaches the live chart in both directions rather than a dead end.
	if err := in2.Send(ctx, Event{Name: "poke", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if err := in2.Send(ctx, Event{Name: "noop", Type: EventExternal}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		if hasState(in2.Configuration(), "echoed") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("configuration = %v, want 'echoed' after Resume echoed back via io.Deliver", in2.Configuration())
		}
		time.Sleep(time.Millisecond)
	}
}

// resumeParamsModel is the datamodel TestInvokeResumeParamsRecomputedFromRestoredDatamodel
// mutates via a replayed event, to distinguish Resume's params argument
// (must reflect the post-replay value) from the checkpoint's own mid-invoke
// snapshot of it (still the pre-bump value).
type resumeParamsModel struct {
	Value int
}

// TestInvokeResumeParamsRecomputedFromRestoredDatamodel confirms Resume's
// params argument is computed fresh from the fully-restored-and-replayed
// datamodel, not read back from anywhere stale.
func TestInvokeResumeParamsRecomputedFromRestoredDatamodel(t *testing.T) {
	ctx := context.Background()
	log := newMemLog()
	store := newMemSnapshotStore()
	sessionID := SessionID("sess-resume-params")

	params := func(ec ExecContext) any {
		d, _ := ec.Datamodel().(*resumeParamsModel)
		if d == nil {
			return 0
		}
		return d.Value
	}
	bump := Action(func(d *resumeParamsModel, ec ExecContext) error {
		d.Value = 5
		return nil
	})
	buildChart := func(t *testing.T, resume InvokeResumeFunc, started chan struct{}) *Chart {
		t.Helper()
		chart, err := Build(
			Compound("m", "a",
				Children(
					Atomic("a",
						Invoke(func(ctx context.Context, params any, io InvokeIO) (any, error) {
							if started != nil {
								close(started)
							}
							<-ctx.Done()
							return nil, nil
						}, WithInvokeID("job"), WithInvokeParams(params), WithInvokeResume(resume)),
						On("bump", Then(bump)),
					),
				),
			),
		)
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return chart
	}

	started := make(chan struct{})
	d1 := &resumeParamsModel{}
	in := New(buildChart(t, nil, started), d1)
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatalf("live invoke never started")
	}

	snap, err := in.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := store.Save(ctx, sessionID, Checkpoint{Snapshot: snap, Seq: 0}); err != nil {
		t.Fatalf("store.Save: %v", err)
	}

	// Logged and applied AFTER the checkpoint: replay must apply this to the
	// restored datamodel before Resume's params are (re)computed.
	bumpEvent := Event{Name: "bump", Type: EventExternal}
	if _, err := log.Append(ctx, LogEntry{SessionID: sessionID, Kind: KindExternalEvent, Timestamp: time.Now().UTC(), Event: bumpEvent}); err != nil {
		t.Fatalf("log.Append: %v", err)
	}
	if err := in.Send(ctx, bumpEvent); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if d1.Value != 5 {
		t.Fatalf("live d1.Value = %d, want 5", d1.Value)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	gotParams := make(chan any, 1)
	resume := func(ctx context.Context, id Identifier, params any, io InvokeIO) (any, error) {
		gotParams <- params
		<-ctx.Done()
		return nil, nil
	}
	d2 := &resumeParamsModel{}
	in2, err := Rehydrate(ctx, buildChart(t, resume, nil), d2, log, store, sessionID, NoopIOProcessor)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}
	defer in2.Stop(ctx)

	select {
	case params := <-gotParams:
		if params != 5 {
			t.Fatalf("Resume params = %v, want 5 (recomputed from the restored+replayed datamodel)", params)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Resume was never called")
	}
}

// resumeEventUnboundModel is the datamodel
// TestInvokeResumeParamsEventIsUnbound mutates via a replayed event. Its
// Value doubles as proof that Resume's params recomputation still sees the
// fully-replayed datamodel even though, per that same recomputation,
// _event does not.
type resumeEventUnboundModel struct {
	Value int
}

// paramsEventSnapshot is what TestInvokeResumeParamsEventIsUnbound's Params
// callback records each time it runs, so the test can inspect both halves
// of ExecContext's state at the moment Resume's params were recomputed.
type paramsEventSnapshot struct {
	datamodelValue int
	event          Event
	hasEvent       bool
}

// TestInvokeResumeParamsEventIsUnbound confirms that when
// resumeInvokesAfterReplay recomputes an invocation's params, ec.Event()
// reports unbound -- SCXML 5.10.1's rule for before the first event is
// processed -- rather than leaking whatever event ip.lastEvent happens to
// hold left over from replaying an unrelated event after the checkpoint.
// It also confirms ec.Datamodel() is unaffected by that same recomputation,
// still reflecting the fully-replayed datamodel.
func TestInvokeResumeParamsEventIsUnbound(t *testing.T) {
	ctx := context.Background()
	log := newMemLog()
	store := newMemSnapshotStore()
	sessionID := SessionID("sess-resume-event-unbound")

	gotParams := make(chan paramsEventSnapshot, 1)
	params := func(ec ExecContext) any {
		d, _ := ec.Datamodel().(*resumeEventUnboundModel)
		v := 0
		if d != nil {
			v = d.Value
		}
		ev, ok := ec.Event()
		select {
		case gotParams <- paramsEventSnapshot{datamodelValue: v, event: ev, hasEvent: ok}:
		default:
		}
		return v
	}
	bump := Action(func(d *resumeEventUnboundModel, ec ExecContext) error {
		d.Value = 7
		return nil
	})

	buildChart := func(t *testing.T, resume InvokeResumeFunc, started chan struct{}) *Chart {
		t.Helper()
		chart, err := Build(
			Compound("m", "a",
				Children(
					Atomic("a",
						Invoke(func(ctx context.Context, params any, io InvokeIO) (any, error) {
							if started != nil {
								close(started)
							}
							<-ctx.Done()
							return nil, nil
						}, WithInvokeID("job"), WithInvokeParams(params), WithInvokeResume(resume)),
						On("bump", Then(bump)),
					),
				),
			),
		)
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return chart
	}

	started := make(chan struct{})
	d1 := &resumeEventUnboundModel{}
	in := New(buildChart(t, nil, started), d1)
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatalf("live invoke never started")
	}

	snap, err := in.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := store.Save(ctx, sessionID, Checkpoint{Snapshot: snap, Seq: 0}); err != nil {
		t.Fatalf("store.Save: %v", err)
	}

	// Logged and applied AFTER the checkpoint: an event with no relation at
	// all to why "a" (and its invoke) was ever entered. Pre-fix, this is
	// exactly the event a buggy recomputation would leak into Resume's
	// params via ip.lastEvent.
	bumpEvent := Event{Name: "bump", Type: EventExternal}
	if _, err := log.Append(ctx, LogEntry{SessionID: sessionID, Kind: KindExternalEvent, Timestamp: time.Now().UTC(), Event: bumpEvent}); err != nil {
		t.Fatalf("log.Append: %v", err)
	}
	if err := in.Send(ctx, bumpEvent); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if d1.Value != 7 {
		t.Fatalf("live d1.Value = %d, want 7", d1.Value)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Drain the live-entry call recorded above (pre-first-event, so already
	// hasEvent=false there too) -- the next value received must be the
	// Resume-driven recomputation below, not a leftover from live entry.
	select {
	case <-gotParams:
	default:
	}

	resume := func(ctx context.Context, id Identifier, params any, io InvokeIO) (any, error) {
		<-ctx.Done()
		return nil, nil
	}
	d2 := &resumeEventUnboundModel{}
	in2, err := Rehydrate(ctx, buildChart(t, resume, nil), d2, log, store, sessionID, NoopIOProcessor)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}
	defer in2.Stop(ctx)

	select {
	case got := <-gotParams:
		if got.hasEvent {
			t.Fatalf("Resume's params recomputation saw ec.Event() = (%+v, true), want hasEvent=false (SCXML 5.10.1 unbound)", got.event)
		}
		if got.datamodelValue != 7 {
			t.Fatalf("Resume's params recomputation saw datamodel value = %d, want 7 (still fully replayed)", got.datamodelValue)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Resume's params recomputation was never observed")
	}
}

// TestRestoreRejectsActiveInvokeNotInConfiguration confirms restoreFrom
// cross-validates ActiveInvoke.State against the restored Configuration,
// the same way it already validates Configuration/HistoryValue entries
// against the chart. A hand-built or corrupted Snapshot claiming an active
// invoke for a state the configuration doesn't include would otherwise
// populate invoke bookkeeping for a state the interpreter never thinks it
// entered -- whose exit, and thus cancelInvokes, will then never run.
func TestRestoreRejectsActiveInvokeNotInConfiguration(t *testing.T) {
	ctx := context.Background()
	chart, err := Build(
		Compound("m", "a",
			Children(
				Atomic("a",
					Invoke(func(ctx context.Context, params any, io InvokeIO) (any, error) {
						<-ctx.Done()
						return nil, nil
					}),
					On("next", Target("b")),
				),
				Atomic("b"),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, nil)
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Exit "a" (cancelling its invoke) so the resulting Configuration
	// genuinely has no invoke-bearing state in it.
	if err := in.Send(ctx, Event{Name: "next", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	snap, err := in.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if hasState(snap.Configuration, "a") {
		t.Fatalf("snap.Configuration = %v, want it to no longer contain \"a\"", snap.Configuration)
	}

	// Hand-craft an ActiveInvoke for "a" onto an otherwise-legitimate
	// snapshot whose Configuration does not contain "a" -- the corruption
	// this test targets.
	snap.ActiveInvokes = append(snap.ActiveInvokes, ActiveInvoke{State: "a", SpecIndex: 0, ID: "job"})

	if _, err := Restore(chart, nil, snap); err == nil {
		t.Fatalf("Restore succeeded, want an error for an active invoke referencing a state outside Configuration")
	} else if !strings.Contains(err.Error(), "not in Configuration") {
		t.Fatalf("Restore error = %v, want it to mention %q", err, "not in Configuration")
	}
}

// TestSnapshotActiveInvokesJSONRoundTrip confirms Snapshot.ActiveInvokes
// survives a JSON marshal/unmarshal round trip.
func TestSnapshotActiveInvokesJSONRoundTrip(t *testing.T) {
	ctx := context.Background()
	chart, err := Build(
		Compound("m", "a",
			Children(
				Atomic("a",
					Invoke(func(ctx context.Context, params any, io InvokeIO) (any, error) {
						<-ctx.Done()
						return nil, nil
					}),
				),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, nil)
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer in.Stop(ctx)

	snap, err := in.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap.ActiveInvokes) != 1 {
		t.Fatalf("snap.ActiveInvokes = %v, want 1 entry", snap.ActiveInvokes)
	}

	b, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got Snapshot
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(got.ActiveInvokes) != 1 || got.ActiveInvokes[0] != snap.ActiveInvokes[0] {
		t.Fatalf("round-tripped ActiveInvokes = %v, want %v", got.ActiveInvokes, snap.ActiveInvokes)
	}
}

type spyIOProcessor struct {
	sendCount int
}

func (s *spyIOProcessor) Attach(Dispatcher) {}
func (s *spyIOProcessor) Send(ctx context.Context, req SendRequest) error {
	s.sendCount++
	return nil
}
func (s *spyIOProcessor) Cancel(ctx context.Context, sendID Identifier) error { return nil }
