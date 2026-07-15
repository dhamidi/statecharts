package statecharts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
)

type recordingDatamodel struct {
	program *recordingProgram
}

func (m recordingDatamodel) Name() Identifier { return "recording" }

func (m recordingDatamodel) Compile(definition *Definition) (DatamodelProgram, error) {
	if err := definition.Validate(); err != nil {
		return nil, err
	}
	return m.program, nil
}

type recordingProgram struct {
	mu             sync.Mutex
	sessions       []*recordingSession
	newErr         error
	closeErr       error
	decodeErr      error
	executeStarted chan struct{}
	executeRelease chan struct{}
}

func (p *recordingProgram) Fingerprint() []byte { return []byte("recording/v1") }

func (p *recordingProgram) NewSession(SessionOptions) (DatamodelSession, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.newErr != nil {
		return nil, p.newErr
	}
	session := &recordingSession{
		values:         make(map[string]Value),
		closeErr:       p.closeErr,
		decodeErr:      p.decodeErr,
		executeStarted: p.executeStarted,
		executeRelease: p.executeRelease,
	}
	p.sessions = append(p.sessions, session)
	return session, nil
}

func (p *recordingProgram) allSessions() []*recordingSession {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*recordingSession(nil), p.sessions...)
}

type modelCall struct {
	operation  string
	expression string
	event      Identifier
	hasEvent   bool
	active     bool
}

type recordingSession struct {
	mu             sync.Mutex
	values         map[string]Value
	calls          []modelCall
	closeCount     int
	closeErr       error
	decodeErr      error
	executeStarted chan struct{}
	executeRelease chan struct{}
}

func (s *recordingSession) record(operation string, ec ExecContext, expression CompiledExpression) string {
	name, _ := expression.(string)
	event, hasEvent := ec.Event()
	s.mu.Lock()
	s.calls = append(s.calls, modelCall{
		operation: operation, expression: name,
		event: event.Name, hasEvent: hasEvent, active: ec.In("active"),
	})
	s.mu.Unlock()
	return name
}

func (s *recordingSession) EvaluateBoolean(ec ExecContext, expression CompiledExpression) (bool, error) {
	switch s.record("boolean", ec, expression) {
	case "error":
		return false, errors.New("condition failed")
	case "panic":
		panic("condition panicked")
	default:
		return true, nil
	}
}

func (s *recordingSession) EvaluateValue(ec ExecContext, expression CompiledExpression) (Value, error) {
	name := s.record("value", ec, expression)
	s.mu.Lock()
	defer s.mu.Unlock()
	if value, ok := s.values[name]; ok {
		return value.Clone(), nil
	}
	return Int64Value(42), nil
}

func (s *recordingSession) Assign(ec ExecContext, location CompiledExpression, value Value) error {
	name := s.record("assign", ec, location)
	s.mu.Lock()
	s.values[name] = value.Clone()
	s.mu.Unlock()
	return nil
}

func (s *recordingSession) Execute(ec ExecContext, expression CompiledExpression) error {
	switch s.record("execute", ec, expression) {
	case "block":
		close(s.executeStarted)
		<-s.executeRelease
	case "error":
		return errors.New("action failed")
	}
	return nil
}

func (s *recordingSession) ForEach(ec ExecContext, expression CompiledExpression, bindings IterationBindings, body func() error) error {
	s.record("foreach", ec, expression)
	for i := int64(0); i < 3; i++ {
		if err := s.Assign(ec, bindings.Item, Int64Value(i)); err != nil {
			return err
		}
		if bindings.Index != nil {
			if err := s.Assign(ec, bindings.Index, Int64Value(i)); err != nil {
				return err
			}
		}
		if err := body(); err != nil {
			return err
		}
	}
	return nil
}

func (s *recordingSession) EncodeSnapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	wire := make(map[string][]byte, len(s.values))
	for name, value := range s.values {
		encoded, err := value.MarshalBinary()
		if err != nil {
			return nil, err
		}
		wire[name] = encoded
	}
	return json.Marshal(wire)
}

func (s *recordingSession) DecodeSnapshot(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values["partial"] = Int64Value(99)
	if s.decodeErr != nil {
		return s.decodeErr
	}
	var wire map[string][]byte
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	decoded := make(map[string]Value, len(wire))
	for name, encoded := range wire {
		var value Value
		if err := value.UnmarshalBinary(encoded); err != nil {
			return err
		}
		decoded[name] = value
	}
	s.values = decoded
	return nil
}

func (s *recordingSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCount++
	return s.closeErr
}

func (s *recordingSession) callSnapshot() []modelCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]modelCall(nil), s.calls...)
}

func (s *recordingSession) closes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCount
}

func datamodelTestChart(t *testing.T, program DatamodelProgram) *Chart {
	t.Helper()
	chart, err := Build(Compound("root", "active", Children(
		Atomic("active",
			On("go", Target("bad")),
			On("go", Target("done")),
		),
		Atomic("bad"),
		Final("done"),
	)), WithDatamodelProgram(program))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	active := chart.byID["active"]
	active.transitions[0].modelCondition = "error"
	active.transitions[0].hasModelCondition = true
	active.transitions[1].modelCondition = "ready"
	active.transitions[1].hasModelCondition = true
	active.transitions[1].actions = []actionBlock{{modelAction("transition-action")}}
	chart.byID["done"].modelDone = "result"
	chart.byID["done"].hasModelDone = true
	return chart
}

func TestDatamodelProgramCreatesIsolatedSessionsAndDelegatesCurrentContext(t *testing.T) {
	program := &recordingProgram{}
	chart := datamodelTestChart(t, program)
	in, err := chart.NewInstance()
	if err != nil {
		t.Fatalf("NewInstance: %v", err)
	}

	ec := in.ip.execContext()
	if err := in.ip.assignModel(ec, "counter", Int64Value(7)); err != nil {
		t.Fatalf("assignModel: %v", err)
	}
	value, err := in.ip.evaluateModelValue(ec, "counter")
	if err != nil {
		t.Fatalf("evaluateModelValue: %v", err)
	}
	if got, ok := value.AsInt64(); !ok || got != 7 {
		t.Fatalf("evaluated value = (%d, %v), want (7, true)", got, ok)
	}
	iterations := 0
	if err := in.ip.forEachModel(ec, "items", IterationBindings{Item: "item", Index: "index"}, func() error {
		iterations++
		return nil
	}); err != nil {
		t.Fatalf("forEachModel: %v", err)
	}
	if iterations != 3 {
		t.Fatalf("foreach body calls = %d, want 3", iterations)
	}

	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := in.Send(ctx, Event{Name: "go"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := in.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	result, err := in.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if got, ok := result.AsInt64(); !ok || got != 42 {
		t.Fatalf("result = (%d, %v), want (42, true)", got, ok)
	}
	if got := len(in.ip.internalQueue); got != 1 || in.ip.internalQueue[0].Name != ErrEventExecution {
		t.Fatalf("condition error queue = %+v, want one %q", in.ip.internalQueue, ErrEventExecution)
	}

	sessions := program.allSessions()
	if len(sessions) != 1 {
		t.Fatalf("sessions = %d, want 1", len(sessions))
	}
	if sessions[0].closes() != 1 {
		t.Fatalf("terminal session closes = %d, want 1", sessions[0].closes())
	}
	var sawCondition, sawAction, sawDone bool
	for _, call := range sessions[0].callSnapshot() {
		if call.expression == "ready" && call.operation == "boolean" {
			sawCondition = call.hasEvent && call.event == "go" && call.active
		}
		if call.expression == "transition-action" && call.operation == "execute" {
			// Transition content runs after active has been exited.
			sawAction = call.hasEvent && call.event == "go" && !call.active
		}
		if call.expression == "result" && call.operation == "value" {
			sawDone = call.hasEvent && call.event == "go"
		}
	}
	if !sawCondition || !sawAction || !sawDone {
		t.Fatalf("session did not receive current interpreter context: condition=%v action=%v done=%v calls=%+v", sawCondition, sawAction, sawDone, sessions[0].callSnapshot())
	}
}

func TestDatamodelSessionsAreDistinctAndCloseExactlyOnce(t *testing.T) {
	program := &recordingProgram{}
	chart, err := Build(Atomic("active"), WithDatamodelProgram(program))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	first, err := chart.NewInstance()
	if err != nil {
		t.Fatalf("first instance: %v", err)
	}
	second, err := chart.NewInstance()
	if err != nil {
		t.Fatalf("second instance: %v", err)
	}
	sessions := program.allSessions()
	if len(sessions) != 2 || sessions[0] == sessions[1] {
		t.Fatalf("sessions = %+v, want two distinct sessions", sessions)
	}
	if err := first.ip.assignModel(first.ip.execContext(), "value", Int64Value(1)); err != nil {
		t.Fatal(err)
	}
	if value, err := second.ip.evaluateModelValue(second.ip.execContext(), "value"); err != nil {
		t.Fatal(err)
	} else if got, _ := value.AsInt64(); got == 1 {
		t.Fatal("session state leaked between instances")
	}
	ctx := context.Background()
	for _, instance := range []*Instance{first, second} {
		if err := instance.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}
		if err := instance.Stop(ctx); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		if err := instance.Stop(ctx); err != nil {
			t.Fatalf("second Stop: %v", err)
		}
	}
	for i, session := range sessions {
		if got := session.closes(); got != 1 {
			t.Fatalf("session %d closes = %d, want 1", i, got)
		}
	}
}

func TestDatamodelSessionClosesWhenInstanceIsStoppedBeforeStart(t *testing.T) {
	program := &recordingProgram{}
	chart, err := Build(Atomic("active"), WithDatamodelProgram(program))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	in, err := chart.NewInstance()
	if err != nil {
		t.Fatalf("newProgramInstance: %v", err)
	}
	if err := in.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got := program.allSessions()[0].closes(); got != 1 {
		t.Fatalf("close calls = %d, want 1", got)
	}
	if err := in.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}

func TestDatamodelSessionClosesAfterStartContextExpires(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	program := &recordingProgram{executeStarted: started, executeRelease: release}
	chart, err := Build(Atomic("active", OnEntry(func(ExecContext) error { return nil })), WithDatamodelProgram(program))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	chart.byID["active"].onEntry = []actionBlock{{modelAction("block")}}
	in, err := chart.NewInstance()
	if err != nil {
		t.Fatalf("NewInstance: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	startResult := make(chan error, 1)
	go func() { startResult <- in.Start(ctx) }()
	<-started
	cancel()
	if err := <-startResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("Start error = %v, want context.Canceled", err)
	}
	close(release)

	if err := in.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := program.allSessions()[0].closes(); got != 1 {
		t.Fatalf("close calls = %d, want 1", got)
	}
}

func TestDatamodelSnapshotUsesFreshSessionAndClosesRejectedDecode(t *testing.T) {
	program := &recordingProgram{}
	chart, err := Build(Atomic("active"), WithVersion("v1"), WithDatamodelProgram(program))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	original, err := chart.NewInstance(WithSessionID("session"))
	if err != nil {
		t.Fatalf("newProgramInstance: %v", err)
	}
	if err := original.ip.assignModel(original.ip.execContext(), "count", Int64Value(11)); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := original.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	snapshot, err := original.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := original.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	restored, err := chart.Restore(snapshot)
	if err != nil {
		t.Fatalf("restoreProgramInstance: %v", err)
	}
	value, err := restored.ip.evaluateModelValue(restored.ip.execContext(), "count")
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := value.AsInt64(); !ok || got != 11 {
		t.Fatalf("restored count = (%d, %v), want (11, true)", got, ok)
	}
	if err := restored.Start(ctx); err != nil {
		t.Fatalf("restored Start: %v", err)
	}
	if err := restored.Stop(ctx); err != nil {
		t.Fatalf("restored Stop: %v", err)
	}

	program.decodeErr = errors.New("incompatible snapshot")
	if _, err := chart.Restore(snapshot); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("rejected restore error = %v, want ErrInvalidSnapshot", err)
	}
	sessions := program.allSessions()
	if len(sessions) != 3 {
		t.Fatalf("sessions = %d, want original, restored, rejected", len(sessions))
	}
	if sessions[2].closes() != 1 {
		t.Fatalf("rejected session closes = %d, want 1", sessions[2].closes())
	}
	if _, ok := sessions[1].values["partial"]; ok {
		t.Fatal("rejected decode state leaked into previously restored session")
	}
}

func TestDatamodelRejectedCheckpointIsDiscardedBeforeFreshReplaySession(t *testing.T) {
	program := &recordingProgram{decodeErr: errors.New("stale model snapshot")}
	chart, err := Build(Atomic("active"), WithVersion("v1"), WithDatamodelProgram(program))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	ctx := context.Background()
	store := newMemSnapshotStore()
	log := newMemLog()
	sessionID := SessionID("program-replay")
	snapshot := Snapshot{
		Version: snapshotVersion, ChartVersion: chart.Version(), Datamodel: []byte(`{}`),
		ID: sessionID, Configuration: []Identifier{"active"}, Running: true,
	}
	if err := store.Save(ctx, sessionID, Checkpoint{Snapshot: snapshot}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	in, err := chart.Rehydrate(ctx, log, store, sessionID, NoopIOProcessor)
	if err != nil {
		t.Fatalf("rehydrateProgramInstance: %v", err)
	}
	sessions := program.allSessions()
	if len(sessions) != 2 {
		t.Fatalf("sessions = %d, want rejected restore and fresh replay", len(sessions))
	}
	if got := sessions[0].closes(); got != 1 {
		t.Fatalf("rejected restore closes = %d, want 1", got)
	}
	if _, ok := sessions[0].values["partial"]; !ok {
		t.Fatal("test session did not partially mutate before rejecting decode")
	}
	if _, ok := sessions[1].values["partial"]; ok {
		t.Fatal("rejected snapshot state leaked into fresh replay session")
	}
	if got := sessions[1].closes(); got != 0 {
		t.Fatalf("live replay session closes = %d, want 0", got)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got := sessions[1].closes(); got != 1 {
		t.Fatalf("stopped replay session closes = %d, want 1", got)
	}
}

func TestDatamodelReplayFailureClosesSession(t *testing.T) {
	program := &recordingProgram{}
	chart, err := Build(Atomic("active"), WithVersion("v1"), WithDatamodelProgram(program))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	ctx := context.Background()
	log := newMemLog()
	sessionID := SessionID("failed-program-replay")
	if _, err := log.Append(ctx, LogEntry{SessionID: sessionID, Kind: "unknown"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := chart.Rehydrate(ctx, log, newMemSnapshotStore(), sessionID, NoopIOProcessor); err == nil {
		t.Fatal("rehydrateProgramInstance accepted unknown log entry")
	}
	sessions := program.allSessions()
	if len(sessions) != 1 {
		t.Fatalf("failed replay sessions = %d, want 1", len(sessions))
	}
	if got := sessions[0].closes(); got != 1 {
		t.Fatalf("failed replay closes = %d, want 1", got)
	}
}

func TestDatamodelSessionCreationAndCloseErrorsAreReported(t *testing.T) {
	program := &recordingProgram{newErr: errors.New("cannot create session")}
	chart, err := Build(Atomic("active"), WithDatamodelProgram(program))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if _, err := chart.NewInstance(); err == nil || err.Error() != "statecharts: create datamodel session: cannot create session" {
		t.Fatalf("creation error = %v", err)
	}

	program.newErr = nil
	program.closeErr = errors.New("cannot close session")
	in, err := chart.NewInstance()
	if err != nil {
		t.Fatalf("newProgramInstance: %v", err)
	}
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop request: %v", err)
	}
	if err := in.Wait(ctx); err == nil || err.Error() != "statecharts: close datamodel session: cannot close session" {
		t.Fatalf("Wait error = %v", err)
	}
	if got := program.allSessions()[0].closes(); got != 1 {
		t.Fatalf("close calls = %d, want 1", got)
	}
}

func TestDatamodelInterfaceRejectsModelSpecificSwitchesByConstruction(t *testing.T) {
	program := &recordingProgram{}
	model := recordingDatamodel{program: program}
	definition := Definition{
		ID: "model-contract", Datamodel: model.Name(),
		Root: StateDefinition{ID: StateDefinitionID{Value: "active"}, Kind: KindAtomic},
	}
	compiled, err := model.Compile(&definition)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if got := string(compiled.Fingerprint()); got != "recording/v1" {
		t.Fatalf("fingerprint = %q", got)
	}
	if _, ok := compiled.(*recordingProgram); !ok {
		t.Fatalf("compiled program = %T", compiled)
	}
	_ = fmt.Sprintf("%T", compiled)
}
