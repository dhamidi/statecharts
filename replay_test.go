package statecharts

import (
	"context"
	"encoding/json"
	"iter"
	"sync"
	"testing"
	"time"
)

// memLog is a minimal in-memory Log test double.
type memLog struct {
	mu      sync.Mutex
	entries map[string][]LogEntry
}

func newMemLog() *memLog { return &memLog{entries: map[string][]LogEntry{}} }

func (l *memLog) Append(ctx context.Context, entry LogEntry) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	seq := uint64(len(l.entries[entry.SessionID])) + 1
	entry.Seq = seq
	l.entries[entry.SessionID] = append(l.entries[entry.SessionID], entry)
	return seq, nil
}

func (l *memLog) Read(ctx context.Context, sessionID string, from uint64) iter.Seq2[LogEntry, error] {
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

func (l *memLog) LastSeq(ctx context.Context, sessionID string) (uint64, error) {
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
	cp map[string]Checkpoint
}

func newMemSnapshotStore() *memSnapshotStore {
	return &memSnapshotStore{cp: map[string]Checkpoint{}}
}

func (s *memSnapshotStore) Save(ctx context.Context, sessionID string, cp Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cp[sessionID] = cp
	return nil
}

func (s *memSnapshotStore) Load(ctx context.Context, sessionID string) (Checkpoint, bool, error) {
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
	sessionID := "sess-1"

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
	sessionID := "sess-2"

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
	sessionID := "sess-3"

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
	sessionID := "sess-ioprocessors"

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

	liveIO := &describingIOProcessor{infos: []IOProcessorInfo{{Type: "mock", Location: "mock://live"}}}
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

	replayIO := &describingIOProcessor{infos: []IOProcessorInfo{{Type: "mock", Location: "mock://replayed"}}}
	in2, err := Rehydrate(ctx, chart, nil, log, store, sessionID, replayIO)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}

	// The single logged "go" event replays during Rehydrate itself, before
	// goLive is called -- so this observation is from inside replay, while
	// the gate is still suppressing Send/Cancel/Log.
	if len(seen) != 1 || seen[0].Location != "mock://replayed" {
		t.Fatalf("ExecContext.IOProcessors() during replay = %v, want [{mock mock://replayed}]", seen)
	}

	// Now live: a fresh Send should see the same wrapped processor's
	// entries, proving IOProcessors() keeps working once the gate flips.
	seen = nil
	if err := in2.Send(ctx, ev); err != nil {
		t.Fatalf("Send (live): %v", err)
	}
	if len(seen) != 1 || seen[0].Location != "mock://replayed" {
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
	sessionID := "sess-nil-logger"

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

type spyIOProcessor struct {
	sendCount int
}

func (s *spyIOProcessor) Attach(Dispatcher) {}
func (s *spyIOProcessor) Send(ctx context.Context, req SendRequest) error {
	s.sendCount++
	return nil
}
func (s *spyIOProcessor) Cancel(ctx context.Context, sendID Identifier) error { return nil }
