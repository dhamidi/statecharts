package statecharts

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"sync"
	"testing"
	"time"
)

type memLog struct {
	mu      sync.Mutex
	entries map[SessionID][]LogEntry
}

func newMemLog() *memLog { return &memLog{entries: map[SessionID][]LogEntry{}} }

func (l *memLog) Append(_ context.Context, entry LogEntry) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry.Seq = uint64(len(l.entries[entry.SessionID])) + 1
	l.entries[entry.SessionID] = append(l.entries[entry.SessionID], entry)
	return entry.Seq, nil
}

func (l *memLog) Read(_ context.Context, sessionID SessionID, from uint64) iter.Seq2[LogEntry, error] {
	return func(yield func(LogEntry, error) bool) {
		l.mu.Lock()
		entries := append([]LogEntry(nil), l.entries[sessionID]...)
		l.mu.Unlock()
		for _, entry := range entries {
			if entry.Seq >= from && !yield(entry, nil) {
				return
			}
		}
	}
}

func (l *memLog) LastSeq(_ context.Context, sessionID SessionID) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return uint64(len(l.entries[sessionID])), nil
}

type memSnapshotStore struct {
	mu sync.Mutex
	cp map[SessionID]Checkpoint
}

func newMemSnapshotStore() *memSnapshotStore {
	return &memSnapshotStore{cp: map[SessionID]Checkpoint{}}
}

func (s *memSnapshotStore) Save(_ context.Context, sessionID SessionID, checkpoint Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cp[sessionID] = checkpoint
	return nil
}

func (s *memSnapshotStore) Load(_ context.Context, sessionID SessionID) (Checkpoint, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	checkpoint, ok := s.cp[sessionID]
	return checkpoint, ok, nil
}

type replayDoor struct {
	Locked    bool
	OpenCount int
}

func replayDoorChart(t *testing.T, created *[]*replayDoor, observations ...chan<- int) *Chart {
	t.Helper()
	b := newTestBuilder(t, func() *replayDoor {
		data := &replayDoor{}
		*created = append(*created, data)
		return data
	})
	notLocked := b.condition("door-is-not-locked", func(data *replayDoor, _ ExecContext) bool { return !data.Locked })
	recordOpen := b.action("record-door-open", func(data *replayDoor, _ ExecContext) error {
		data.OpenCount++
		return nil
	})
	observe := b.action("observe-door-open-count", func(data *replayDoor, _ ExecContext) error {
		if len(observations) > 0 {
			observations[0] <- data.OpenCount
		}
		return nil
	})
	chart, err := b.build(Compound("door", "closed", Children(
		Atomic("closed", On("open.request", Target("open"), If(notLocked), Then(recordOpen))),
		Atomic("open", On("close.request", Target("closed")), On("inspect", Then(observe))),
	)), WithRevisionSalt("replay-door-v1"))
	if err != nil {
		t.Fatal(err)
	}
	return chart
}

func TestSnapshotRoundTripRestoresConfigurationModelAndSessionID(t *testing.T) {
	var created []*replayDoor
	observed := make(chan int, 1)
	chart := replayDoorChart(t, &created, observed)
	instance, err := chart.NewInstance(WithSessionID("door-42"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := instance.Send(ctx, Event{Name: "open.request"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := instance.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	wire, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Snapshot
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	restored, err := chart.Restore(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if restored.ID() != "door-42" {
		t.Fatalf("restored ID = %q", restored.ID())
	}
	if err := restored.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer restored.Stop(ctx)
	active := restored.Configuration()
	if !hasState(active, "open") {
		t.Fatalf("restored active = %v", active)
	}
	if err := restored.Send(ctx, Event{Name: "inspect"}); err != nil {
		t.Fatal(err)
	}
	if count := <-observed; count != 1 {
		t.Fatalf("restored open count = %d", count)
	}
	if err := restored.Send(ctx, Event{Name: "close.request"}); err != nil {
		t.Fatal(err)
	}
	waitActive(t, restored, "closed")
}

func TestRestoreWithSessionIDOverridesSnapshot(t *testing.T) {
	var created []*replayDoor
	chart := replayDoorChart(t, &created)
	instance, err := chart.NewInstance(WithSessionID("snapshot-session"))
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot, err := instance.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	restored, err := chart.Restore(snapshot, WithSessionID("option-session"))
	if err != nil {
		t.Fatal(err)
	}
	if restored.ID() != "option-session" {
		t.Fatalf("restored ID = %q, want option-session", restored.ID())
	}
}

func TestSnapshotQueuesAndPendingSendsRoundTripCanonicalValues(t *testing.T) {
	nested, err := MapValue(map[string]Value{"items": ListValue([]Value{Int64Value(1), testStringValue("two")})})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := TaggedValue("snapshot.payload/v1", nested)
	if err != nil {
		t.Fatal(err)
	}
	want := Snapshot{
		Version:       snapshotVersion,
		InternalQueue: []Event{{Name: "internal", Type: EventInternal, Data: payload}},
		ExternalQueue: []Event{{Name: "external", Type: EventExternal, Data: payload, DeliveryID: "delivery-1"}},
		PendingSends: []PendingSend{{
			SendID: "send-1", Target: "worker", Type: SCXMLEventProcessor,
			Event: Event{Name: "later", Data: payload, SendID: "send-1"}, FireAt: time.Unix(123, 456).UTC(),
		}},
	}
	wire, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got Snapshot
	if err := json.Unmarshal(wire, &got); err != nil {
		t.Fatal(err)
	}
	if !got.InternalQueue[0].Data.Equal(payload) || !got.ExternalQueue[0].Data.Equal(payload) || !got.PendingSends[0].Event.Data.Equal(payload) {
		t.Fatalf("snapshot payloads did not round-trip: %#v", got)
	}
}

func TestRestoreRearmsPendingSendAgainstConfiguredClock(t *testing.T) {
	type model struct{ Aborts int }
	var models []*model
	b := newTestBuilder(t, func() *model { data := &model{}; models = append(models, data); return data })
	aborted := make(chan int, 1)
	recordAbort := b.action("record-delayed-abort", func(data *model, _ ExecContext) error {
		data.Aborts++
		aborted <- data.Aborts
		return nil
	})
	chart, err := b.build(Compound("operation", "idle", Children(
		Atomic("idle", On("start", Target("running"), Then(Send("abort", SendID("abort-operation"), SendDelay(2*time.Second))))),
		Atomic("running", On("abort", Target("aborted"), Then(recordAbort))),
		Atomic("aborted"),
	)), WithRevisionSalt("delayed-abort-v1"))
	if err != nil {
		t.Fatal(err)
	}
	clock := NewManualClock(time.Unix(0, 0))
	instance, err := chart.NewInstance(WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := instance.Send(ctx, Event{Name: "start"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := instance.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	restored, err := chart.Restore(snapshot, WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer restored.Stop(ctx)
	clock.Advance(time.Second)
	if err := restored.Send(ctx, Event{Name: "sync"}); err != nil {
		t.Fatal(err)
	}
	waitActive(t, restored, "running")
	clock.Advance(time.Second)
	if err := restored.Send(ctx, Event{Name: "sync"}); err != nil {
		t.Fatal(err)
	}
	waitActive(t, restored, "aborted")
	if count := <-aborted; count != 1 {
		t.Fatalf("abort count = %d", count)
	}
}

func TestRestorePreservesAutoGeneratedSendSequence(t *testing.T) {
	b := newTestBuilder(t, func() *struct{} { return &struct{}{} })
	chart, err := b.build(Atomic("running",
		OnEntry(Send("first", SendDelay(time.Hour))),
		On("again", Then(Send("second", SendDelay(time.Hour)))),
	), WithRevisionSalt("send-sequence-v1"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	in, err := chart.NewInstance()
	if err != nil {
		t.Fatal(err)
	}
	if err := in.Start(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := in.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var persisted Snapshot
	if err := json.Unmarshal(wire, &persisted); err != nil {
		t.Fatal(err)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	restored, err := chart.Restore(persisted)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer restored.Stop(ctx)
	if err := restored.Send(ctx, Event{Name: "again"}); err != nil {
		t.Fatal(err)
	}
	got, err := restored.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.PendingSends) != 2 || got.PendingSends[0].SendID != "send.1" || got.PendingSends[1].SendID != "send.2" {
		t.Fatalf("pending sends = %#v, want send.1 and send.2", got.PendingSends)
	}
}

func TestRestorePreservesAutoGeneratedInvokeSequence(t *testing.T) {
	model := NewGoModel(func() *struct{} { return &struct{}{} })
	chart, err := Build(Compound("machine", "invoking", Children(
		Atomic("invoking", Invoke("worker", "jobs", WithInvokeDefinitionID("worker-definition")), On("leave", Target("idle"))),
		Atomic("idle", On("enter", Target("invoking"))),
	)), model, WithRevisionSalt("invoke-sequence-v1"))
	if err != nil {
		t.Fatal(err)
	}
	handler := WithInvokeHandler("worker", func() InvokeHandler {
		return InvokeHandlerFunc(func(ctx context.Context, _ InvokeRequest, _ InvokeIO) (Value, error) {
			<-ctx.Done()
			return NullValue(), ctx.Err()
		})
	})
	ctx := context.Background()
	in, err := chart.NewInstance(handler)
	if err != nil {
		t.Fatal(err)
	}
	if err := in.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := in.Send(ctx, Event{Name: "leave", Type: EventExternal}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := in.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var persisted Snapshot
	if err := json.Unmarshal(wire, &persisted); err != nil {
		t.Fatal(err)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	restored, err := chart.Restore(persisted, handler)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer restored.Stop(ctx)
	if err := restored.Send(ctx, Event{Name: "enter", Type: EventExternal}); err != nil {
		t.Fatal(err)
	}
	got, err := restored.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.ActiveInvokes) != 1 || got.ActiveInvokes[0].ID != "invoking.invoke2" {
		t.Fatalf("active invokes after restore = %v, want session-unique id invoking.invoke2", got.ActiveInvokes)
	}
}

func TestSnapshotActiveInvokesJSONRoundTrip(t *testing.T) {
	want := Snapshot{Version: snapshotVersion, ActiveInvokes: []ActiveInvoke{{
		State: "running", DefinitionID: "worker-definition", ID: "worker.invoke7",
		Type: "worker", Source: "jobs/42",
	}}}
	wire, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got Snapshot
	if err := json.Unmarshal(wire, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.ActiveInvokes) != 1 || got.ActiveInvokes[0] != want.ActiveInvokes[0] {
		t.Fatalf("active invokes = %#v, want %#v", got.ActiveInvokes, want.ActiveInvokes)
	}
}

func TestRestoreRejectsActiveInvokeNotInConfiguration(t *testing.T) {
	b := newTestBuilder(t, func() *struct{} { return &struct{}{} })
	chart, err := b.build(Compound("root", "idle", Children(
		Atomic("idle"), Atomic("running", Invoke("worker", "jobs", WithInvokeDefinitionID("worker-definition"))),
	)), WithRevisionSalt("invalid-active-invoke-v1"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = chart.Restore(Snapshot{
		Version: snapshotVersion, Revision: chart.Revision(), Running: true,
		Configuration: []Identifier{"idle"},
		ActiveInvokes: []ActiveInvoke{{State: "running", DefinitionID: "worker-definition", ID: "worker-1", Type: "worker", Source: "jobs"}},
	}, WithInvokeHandler("worker", func() InvokeHandler {
		return InvokeHandlerFunc(func(context.Context, InvokeRequest, InvokeIO) (Value, error) {
			return NullValue(), nil
		})
	}))
	if !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("Restore error = %v, want ErrInvalidSnapshot", err)
	}
}

func TestRestoreRejectsUnsupportedSnapshotVersionAndRevision(t *testing.T) {
	var created []*replayDoor
	chart := replayDoorChart(t, &created)
	if _, err := chart.Restore(Snapshot{Version: snapshotVersion + 1}); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("unsupported version error = %v", err)
	}
	if _, err := chart.Restore(Snapshot{Version: snapshotVersion, Revision: RevisionID("sha256:other")}); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("revision mismatch error = %v", err)
	}
}

func TestRehydrateReplaysLogIntoFreshSession(t *testing.T) {
	ctx := context.Background()
	log := newMemLog()
	sessionID := SessionID("replay-door")
	_, _ = log.Append(ctx, LogEntry{SessionID: sessionID, Kind: KindExternalEvent, Timestamp: time.Unix(1, 0), Event: Event{Name: "open.request"}})
	var created []*replayDoor
	chart := replayDoorChart(t, &created)
	instance, err := chart.Rehydrate(ctx, log, newMemSnapshotStore(), sessionID, NoopIOProcessor)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	waitActive(t, instance, "open")
	if len(created) != 1 || created[0].OpenCount != 1 {
		t.Fatalf("replayed models = %#v", created)
	}
}

func TestRehydrateRejectsBadCheckpointAndReplaysFromBeginning(t *testing.T) {
	ctx := context.Background()
	log := newMemLog()
	store := newMemSnapshotStore()
	sessionID := SessionID("bad-checkpoint")
	_, _ = log.Append(ctx, LogEntry{SessionID: sessionID, Kind: KindExternalEvent, Timestamp: time.Unix(1, 0), Event: Event{Name: "open.request"}})
	store.cp[sessionID] = Checkpoint{Seq: 1, Snapshot: Snapshot{Version: snapshotVersion + 1}}
	var created []*replayDoor
	chart := replayDoorChart(t, &created)
	instance, err := chart.Rehydrate(ctx, log, store, sessionID, NoopIOProcessor)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	waitActive(t, instance, "open")
	if created[len(created)-1].OpenCount != 1 {
		t.Fatalf("OpenCount = %d", created[len(created)-1].OpenCount)
	}
}

func TestRehydrateRejectedSnapshotDecodeCannotMutateReplaySession(t *testing.T) {
	type modelState struct{ Applied int }
	var factorySessions []*modelState
	var decodedSessions []*modelState
	model := NewGoModel(func() *modelState {
		data := &modelState{Applied: 99}
		factorySessions = append(factorySessions, data)
		return data
	}, WithGoSnapshotCodec(GoSnapshotCodec[modelState]{
		Encode: func(data *modelState) ([]byte, error) { return json.Marshal(data) },
		Decode: func(data []byte) (*modelState, error) {
			var decoded modelState
			if err := json.Unmarshal(data, &decoded); err != nil {
				return nil, err
			}
			decodedSessions = append(decodedSessions, &decoded)
			return nil, errors.New("reject decoded snapshot after partial mutation")
		},
	}))
	apply, err := model.Action("apply", "v1", func(data *modelState, _ ExecContext, _ []Value) error {
		data.Applied++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	chart, err := Build(Compound("root", "ready", Children(Atomic("ready", On("apply", Then(apply.Do()))))), model, WithRevisionSalt("rejected-snapshot-v1"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	log := newMemLog()
	store := newMemSnapshotStore()
	sessionID := SessionID("rejected-snapshot-model")
	seq, err := log.Append(ctx, LogEntry{SessionID: sessionID, Kind: KindExternalEvent, Timestamp: time.Unix(1, 0), Event: Event{Name: "apply", Type: EventExternal}})
	if err != nil {
		t.Fatal(err)
	}
	badModel, err := json.Marshal(&modelState{Applied: 500})
	if err != nil {
		t.Fatal(err)
	}
	badModel, err = json.Marshal(goSnapshot{Version: 1, Data: badModel, Values: map[Identifier]Value{}})
	if err != nil {
		t.Fatal(err)
	}
	store.cp[sessionID] = Checkpoint{Seq: seq, Snapshot: Snapshot{
		Version: snapshotVersion, Revision: chart.Revision(), Datamodel: badModel,
		Configuration: []Identifier{"ready"}, Running: true,
	}}
	instance, err := chart.Rehydrate(ctx, log, store, sessionID, NoopIOProcessor)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	if len(decodedSessions) != 1 || decodedSessions[0].Applied != 500 {
		t.Fatalf("decoded snapshot sessions = %#v, want one rejected session with Applied=500", decodedSessions)
	}
	if len(factorySessions) != 2 || factorySessions[0].Applied != 99 || factorySessions[1].Applied != 100 || factorySessions[0] == factorySessions[1] {
		t.Fatalf("factory sessions = %#v, want discarded restore session at 99 and independent replay session at 100", factorySessions)
	}
}

type replayIO struct {
	mu       sync.Mutex
	requests []SendRequest
}

func (*replayIO) Attach(Dispatcher) {}
func (p *replayIO) Send(_ context.Context, request SendRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, request)
	return nil
}
func (p *replayIO) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.requests)
}

func TestRehydrateSuppressesExternalEffectsThenGoesLive(t *testing.T) {
	b := newTestBuilder(t, func() *struct{} { return &struct{}{} })
	chart, err := b.build(Atomic("active", On("dispatch", Then(Send("work", SendTarget("worker"))))), WithRevisionSalt("dispatch-v1"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	log := newMemLog()
	sessionID := SessionID("dispatch-replay")
	_, _ = log.Append(ctx, LogEntry{SessionID: sessionID, Kind: KindExternalEvent, Timestamp: time.Unix(1, 0), Event: Event{Name: "dispatch"}})
	processor := &replayIO{}
	instance, err := chart.Rehydrate(ctx, log, newMemSnapshotStore(), sessionID, processor, WithIOProcessor(SCXMLEventProcessor, processor))
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	if processor.count() != 0 {
		t.Fatalf("replay dispatched %d effects", processor.count())
	}
	if err := instance.Send(ctx, Event{Name: "dispatch"}); err != nil {
		t.Fatal(err)
	}
	if processor.count() != 1 {
		t.Fatalf("live dispatch count = %d", processor.count())
	}
}

func TestRehydratePreservesLoggedPlatformEventSemantics(t *testing.T) {
	model := NewGoModel(func() *struct{} { return &struct{}{} })
	var observed Event
	record, err := model.Action("record-platform-event", "v1", func(_ *struct{}, ec ExecContext, _ []Value) error {
		observed, _ = ec.Event()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	chart, err := Build(Compound("root", "waiting", Children(
		Atomic("waiting", On(string(ErrEventExecution), Target("recovered"), Then(record.Do()))),
		Atomic("recovered"),
	)), model, WithRevisionSalt("platform-replay-v1"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	log := newMemLog()
	sessionID := SessionID("platform-replay")
	payload := PlatformErrorValue(ErrEventExecution, errors.New("historical execution failure"))
	_, _ = log.Append(ctx, LogEntry{SessionID: sessionID, Kind: KindExternalEvent, Timestamp: time.Unix(1, 0), Event: Event{Name: ErrEventExecution, Type: EventPlatform, Data: payload}})
	ingressCalls := 0
	instance, err := chart.Rehydrate(ctx, log, newMemSnapshotStore(), sessionID, NoopIOProcessor, WithIngressHook(func(Event) error {
		ingressCalls++
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	waitActive(t, instance, "recovered")
	if observed.Type != EventPlatform {
		t.Fatalf("replayed event Type = %v, want platform", observed.Type)
	}
	if !observed.Data.Equal(payload) {
		t.Fatalf("replayed event payload = %#v, want %#v", observed.Data, payload)
	}
	if ingressCalls != 0 {
		t.Fatalf("ingress hook calls during replay = %d, want 0", ingressCalls)
	}
	if err := instance.Send(ctx, Event{Name: "live", Type: EventExternal}); err != nil {
		t.Fatal(err)
	}
	if ingressCalls != 1 {
		t.Fatalf("ingress hook calls after live event = %d, want 1", ingressCalls)
	}
}

func TestRehydrateWithNilLoggerDoesNotPanicOnceLive(t *testing.T) {
	model := NewGoModel(func() *struct{} { return &struct{}{} })
	chart, err := Build(Compound("machine", "idle", Children(
		Atomic("idle", On("go", Target("done"), Then(LogValue("transition", GoLiteral(testStringValue("live")))))),
		Atomic("done"),
	)), model, WithRevisionSalt("nil-logger-v1"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	instance, err := chart.Rehydrate(ctx, newMemLog(), newMemSnapshotStore(), "nil-logger", NoopIOProcessor, WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	if err := instance.Send(ctx, Event{Name: "go", Type: EventExternal}); err != nil {
		t.Fatal(err)
	}
	waitActive(t, instance, "done")
}

type replayInvokeHandler struct {
	start  func(context.Context, InvokeRequest, InvokeIO) (Value, error)
	resume func(context.Context, InvokeRequest, InvokeIO) (Value, error)
}

func (h *replayInvokeHandler) Start(ctx context.Context, request InvokeRequest, io InvokeIO) (Value, error) {
	return h.start(ctx, request, io)
}

func (h *replayInvokeHandler) Resume(ctx context.Context, request InvokeRequest, io InvokeIO) (Value, error) {
	return h.resume(ctx, request, io)
}

func replayInvokeChart(t *testing.T, captured chan<- Event, autoForward bool) *Chart {
	t.Helper()
	model := NewGoModel(func() *struct{} { return &struct{}{} })
	record, err := model.Action("record-resumed-invoke-event", "v1", func(_ *struct{}, ec ExecContext, _ []Value) error {
		if captured != nil {
			event, _ := ec.Event()
			captured <- event
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	options := []InvokeOption{
		WithInvokeDefinitionID("job-definition"), WithInvokeID("job"),
		WithInvokeContent(GoLiteral(testStringValue("persisted-input"))),
	}
	if autoForward {
		options = append(options, WithAutoForward())
	}
	chart, err := Build(Compound("machine", "running", Children(
		Atomic("running", Invoke("worker", "queue:jobs", options...),
			On(string(ErrEventCommunication), Target("failed"), Then(record.Do())),
			On("done.invoke.job", Target("finished"), Then(record.Do())),
			On("invoke.echo", Target("finished"), Then(record.Do())),
			On("send.direct", Then(Send("direct", SendTarget("#_job"))))),
		Atomic("failed"), Atomic("finished"),
	)), model, WithRevisionSalt("replay-invoke-canonical-v1"))
	if err != nil {
		t.Fatal(err)
	}
	return chart
}

func checkpointActiveInvoke(t *testing.T, chart *Chart, store *memSnapshotStore, sessionID SessionID, factory InvokeHandlerFactory) {
	t.Helper()
	ctx := context.Background()
	started := make(chan InvokeRequest, 1)
	instance, err := chart.NewInstance(WithInvokeHandler("worker", func() InvokeHandler {
		h := factory().(*replayInvokeHandler)
		h.start = func(ctx context.Context, request InvokeRequest, _ InvokeIO) (Value, error) {
			started <- request
			<-ctx.Done()
			return Value{}, ctx.Err()
		}
		return h
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case request := <-started:
		if request.DefinitionID != "job-definition" || request.ID != "job" || request.Type != "worker" || request.Source != "queue:jobs" || !request.Data.Equal(testStringValue("persisted-input")) {
			t.Fatalf("live InvokeRequest = %+v", request)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("live invoke did not start")
	}
	snapshot, err := instance.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.ActiveInvokes) != 1 {
		t.Fatalf("active invokes = %+v", snapshot.ActiveInvokes)
	}
	if err := store.Save(ctx, sessionID, Checkpoint{Snapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	if err := instance.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRehydrateCheckpointActiveNonResumableInvokeSignalsCommunicationError(t *testing.T) {
	ctx := context.Background()
	store := newMemSnapshotStore()
	chart := replayInvokeChart(t, nil, false)
	startedAgain := make(chan struct{}, 1)
	checkpointActiveInvoke(t, chart, store, "checkpoint-non-resumable", func() InvokeHandler {
		return &replayInvokeHandler{resume: func(context.Context, InvokeRequest, InvokeIO) (Value, error) { return Value{}, nil }}
	})
	instance, err := chart.Rehydrate(ctx, newMemLog(), store, "checkpoint-non-resumable", NoopIOProcessor,
		WithInvokeHandler("worker", func() InvokeHandler {
			return InvokeHandlerFunc(func(context.Context, InvokeRequest, InvokeIO) (Value, error) {
				startedAgain <- struct{}{}
				return NullValue(), nil
			})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	waitActive(t, instance, "failed")
	select {
	case <-startedAgain:
		t.Fatal("non-resumable handler was started again")
	default:
	}
}

func TestInvokeResumeErrorBecomesCommunicationError(t *testing.T) {
	ctx := context.Background()
	store := newMemSnapshotStore()
	captured := make(chan Event, 1)
	chart := replayInvokeChart(t, captured, false)
	factory := func() InvokeHandler { return &replayInvokeHandler{} }
	checkpointActiveInvoke(t, chart, store, "resume-error", factory)
	wantErr := errors.New("resume: process confirmed gone")
	instance, err := chart.Rehydrate(ctx, newMemLog(), store, "resume-error", NoopIOProcessor, WithInvokeHandler("worker", func() InvokeHandler {
		return &replayInvokeHandler{start: func(context.Context, InvokeRequest, InvokeIO) (Value, error) {
			t.Fatal("Start called during rehydrate")
			return Value{}, nil
		}, resume: func(_ context.Context, request InvokeRequest, _ InvokeIO) (Value, error) {
			if request.ID != "job" || request.DefinitionID != "job-definition" || request.Type != "worker" || request.Source != "queue:jobs" || !request.Data.Equal(testStringValue("persisted-input")) {
				t.Errorf("resumed InvokeRequest = %+v", request)
			}
			return Value{}, wantErr
		}}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	select {
	case event := <-captured:
		classification, message, ok := PlatformErrorDetails(event.Data)
		if event.Name != ErrEventCommunication || event.InvokeID != "job" || !ok || classification != ErrEventCommunication || message != wantErr.Error() {
			t.Fatalf("communication event = %+v, details = %q/%q/%v", event, classification, message, ok)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Resume error was not delivered")
	}
}

func TestInvokeResumeValueBecomesDoneInvoke(t *testing.T) {
	ctx := context.Background()
	store := newMemSnapshotStore()
	captured := make(chan Event, 1)
	chart := replayInvokeChart(t, captured, false)
	checkpointActiveInvoke(t, chart, store, "resume-value", func() InvokeHandler { return &replayInvokeHandler{} })
	want := testStringValue("completed-payload")
	instance, err := chart.Rehydrate(ctx, newMemLog(), store, "resume-value", NoopIOProcessor, WithInvokeHandler("worker", func() InvokeHandler {
		return &replayInvokeHandler{start: func(context.Context, InvokeRequest, InvokeIO) (Value, error) {
			t.Fatal("Start called during rehydrate")
			return Value{}, nil
		}, resume: func(_ context.Context, request InvokeRequest, _ InvokeIO) (Value, error) {
			if request.ID != "job" || !request.Data.Equal(testStringValue("persisted-input")) {
				t.Errorf("resumed InvokeRequest = %+v", request)
			}
			return want, nil
		}}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	select {
	case event := <-captured:
		if event.Name != "done.invoke.job" || event.InvokeID != "job" || !event.Data.Equal(want) {
			t.Fatalf("done event = %+v, want exact payload %v", event, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Resume value was not delivered")
	}
}

func TestInvokeResumeBlockingRemainsReachableAndCancels(t *testing.T) {
	ctx := context.Background()
	store := newMemSnapshotStore()
	captured := make(chan Event, 1)
	chart := replayInvokeChart(t, captured, true)
	checkpointActiveInvoke(t, chart, store, "resume-blocking", func() InvokeHandler { return &replayInvokeHandler{} })
	resumed := make(chan struct{})
	cancelled := make(chan struct{})
	incoming := make(chan Identifier, 3)
	instance, err := chart.Rehydrate(ctx, newMemLog(), store, "resume-blocking", NoopIOProcessor, WithInvokeHandler("worker", func() InvokeHandler {
		return &replayInvokeHandler{start: func(context.Context, InvokeRequest, InvokeIO) (Value, error) {
			t.Fatal("Start called during rehydrate")
			return Value{}, nil
		}, resume: func(ctx context.Context, request InvokeRequest, io InvokeIO) (Value, error) {
			close(resumed)
			for {
				select {
				case event := <-io.Incoming:
					incoming <- event.Name
					if event.Name == "direct" {
						io.Deliver(Event{Name: "invoke.echo", Data: testStringValue("from-resume")})
					}
				case <-ctx.Done():
					close(cancelled)
					return Value{}, ctx.Err()
				}
			}
		}}
	}))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-resumed:
	case <-time.After(2 * time.Second):
		t.Fatal("Resume was not called")
	}
	active, err := instance.HasActiveInvokes(ctx)
	if err != nil || !active {
		t.Fatalf("HasActiveInvokes = %v, %v", active, err)
	}
	if err := instance.Send(ctx, Event{Name: "poke", Type: EventExternal}); err != nil {
		t.Fatal(err)
	}
	if err := instance.Send(ctx, Event{Name: "send.direct", Type: EventExternal}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []Identifier{"poke", "send.direct", "direct"} {
		select {
		case got := <-incoming:
			if got != want {
				t.Fatalf("incoming event = %q, want %q", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("incoming event %q was not delivered", want)
		}
	}
	select {
	case event := <-captured:
		if event.Name != "invoke.echo" || event.InvokeID != "job" || !event.Data.Equal(testStringValue("from-resume")) {
			t.Fatalf("invoke-to-parent event = %+v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("invoke-to-parent delivery was not received")
	}
	if err := instance.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("blocking Resume was not cancelled")
	}
}

type capturingReplayIO struct {
	dispatcher Dispatcher
}

func (p *capturingReplayIO) Attach(dispatcher Dispatcher) { p.dispatcher = dispatcher }
func (*capturingReplayIO) Send(context.Context, SendRequest) error {
	return nil
}

func TestRehydrateStopsStartedInstanceWhenReplayFails(t *testing.T) {
	model := NewGoModel(func() *struct{} { return &struct{}{} })
	chart, err := Build(Atomic("live"), model, WithRevisionSalt("failed-replay-cleanup-v1"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	log := newMemLog()
	const sessionID SessionID = "broken-replay"
	if _, err := log.Append(ctx, LogEntry{
		SessionID: sessionID,
		Kind:      EntryKind("unknown"),
		Timestamp: time.Unix(1, 0),
	}); err != nil {
		t.Fatal(err)
	}
	io := &capturingReplayIO{}
	if _, err := chart.Rehydrate(ctx, log, newMemSnapshotStore(), sessionID, io); err == nil {
		t.Fatal("Rehydrate succeeded, want replay error")
	}
	instance, ok := io.dispatcher.(*Instance)
	if !ok || instance == nil {
		t.Fatalf("captured Dispatcher = %T, want *Instance", io.dispatcher)
	}
	select {
	case <-instance.Done():
	case <-time.After(time.Second):
		t.Fatal("started instance remained running after replay failed")
	}
}

func TestRehydrateCleanupCompletesAfterCallerCancellation(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	model := NewGoModel(func() *struct{} { return &struct{}{} })
	blockStart, err := model.Action("block-start", "v1", func(_ *struct{}, _ ExecContext, _ []Value) error {
		close(entered)
		<-release
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	chart, err := Build(Atomic("blocked-start", OnEntry(blockStart.Do())), model, WithRevisionSalt("cancelled-cleanup-v1"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	io := &capturingReplayIO{}
	returned := make(chan error, 1)
	go func() {
		_, err := chart.Rehydrate(ctx, newMemLog(), newMemSnapshotStore(), "blocked-start", io)
		returned <- err
	}()
	select {
	case <-entered:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("initial entry action was not called")
	}
	select {
	case err := <-returned:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Rehydrate error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Rehydrate cleanup ignored its canceled caller context")
	}
	instance, ok := io.dispatcher.(*Instance)
	if !ok || instance == nil {
		t.Fatalf("captured Dispatcher = %T, want *Instance", io.dispatcher)
	}
	close(release)
	select {
	case <-instance.Done():
	case <-time.After(time.Second):
		t.Fatal("cleanup did not stop the instance after the action unwound")
	}
}
