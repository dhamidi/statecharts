package statecharts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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

func (*recordingProgram) ResolveExpression(expression Expression) (CompiledExpression, error) {
	value, ok := expression.Data.AsString()
	if !ok {
		return nil, fmt.Errorf("recording expression is not a string")
	}
	return value, nil
}

func (*recordingProgram) ResolveFunction(function FunctionRef) (CompiledExpression, error) {
	return string(function.Name), nil
}

func (*recordingProgram) ResolveDataLocation(id Identifier) (CompiledExpression, error) {
	return string(id), nil
}

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

func datamodelTestChart(t *testing.T, program *recordingProgram, salt string) *Chart {
	t.Helper()
	chart, err := Build(Compound("root", "active", Children(
		Atomic("active", On("finish", Target("done"))), Final("done"),
	)), recordingDatamodel{program: program}, WithRevisionSalt(salt))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return chart
}

func TestDatamodelProgramCreatesIsolatedSessionsAndDelegatesValues(t *testing.T) {
	program := &recordingProgram{}
	chart := datamodelTestChart(t, program, "recording-v1")
	first, err := chart.NewInstance()
	if err != nil {
		t.Fatal(err)
	}
	second, err := chart.NewInstance()
	if err != nil {
		t.Fatal(err)
	}
	if first.session == second.session {
		t.Fatal("instances shared a datamodel session")
	}
	location, err := chart.program.ResolveDataLocation("counter")
	if err != nil {
		t.Fatal(err)
	}
	if err := first.session.Assign(ExecContext{}, location, Int64Value(7)); err != nil {
		t.Fatal(err)
	}
	value, err := first.session.EvaluateValue(ExecContext{}, location)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := value.AsInt64(); got != 7 {
		t.Fatalf("value = %d, want 7", got)
	}
	for _, instance := range []*Instance{first, second} {
		if err := instance.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		if err := instance.Stop(context.Background()); err != nil {
			t.Fatalf("repeated Stop: %v", err)
		}
	}
	for i, session := range program.allSessions() {
		if got := session.closes(); got != 1 {
			t.Fatalf("session %d closes = %d, want 1", i, got)
		}
	}
}

func TestDatamodelSnapshotRestoreUsesFreshSession(t *testing.T) {
	program := &recordingProgram{}
	chart := datamodelTestChart(t, program, "recording-v1")
	instance, err := chart.NewInstance(WithSessionID("recording-session"))
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	location, _ := chart.program.ResolveDataLocation("counter")
	if err := instance.session.Assign(ExecContext{}, location, Int64Value(9)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := instance.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	restored, err := chart.Restore(snapshot)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	value, err := restored.session.EvaluateValue(ExecContext{}, location)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := value.AsInt64(); got != 9 {
		t.Fatalf("restored value = %d, want 9", got)
	}
	if len(program.allSessions()) != 2 {
		t.Fatalf("sessions = %d, want 2", len(program.allSessions()))
	}
	if err := restored.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	program.decodeErr = errors.New("incompatible snapshot")
	if _, err := chart.Restore(snapshot); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("rejected Restore error = %v, want ErrInvalidSnapshot", err)
	}
	sessions := program.allSessions()
	if len(sessions) != 3 || sessions[0].closes() != 1 || sessions[1].closes() != 1 || sessions[2].closes() != 1 {
		t.Fatalf("session close counts = %v, want [1 1 1]", []int{sessions[0].closes(), sessions[1].closes(), sessions[2].closes()})
	}
}

func TestDatamodelDefinitionIsADeepCopy(t *testing.T) {
	chart := datamodelTestChart(t, &recordingProgram{}, "recording-v2")
	first := chart.Definition()
	first.Root.Children[0].ID.Value = "changed"
	second := chart.Definition()
	if second.Root.Children[0].ID.Value != "active" {
		t.Fatalf("definition mutated: %+v", second)
	}
	wire, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Definition
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	recompiled, err := Compile(decoded, recordingDatamodel{program: &recordingProgram{}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(recompiled.Definition(), second) {
		t.Fatal("definition round trip changed")
	}
}

func TestDatamodelSessionClosesWhenStoppedBeforeStart(t *testing.T) {
	program := &recordingProgram{}
	in, err := datamodelTestChart(t, program, "stop-before-start").NewInstance()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := in.Stop(context.Background()); err != nil {
			t.Fatalf("Stop %d: %v", i+1, err)
		}
	}
	if got := program.allSessions()[0].closes(); got != 1 {
		t.Fatalf("close calls = %d, want 1", got)
	}
}

func TestDatamodelSessionClosesAfterCanceledStart(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	program := &recordingProgram{executeStarted: started, executeRelease: release}
	expr := Expression{Kind: "recording", Data: testStringValue("block")}
	chart, err := Build(Atomic("active", OnEntry(NewScriptExecutable(ScriptDefinition{Expr: expr}))), recordingDatamodel{program}, WithRevisionSalt("canceled-start"))
	if err != nil {
		t.Fatal(err)
	}
	in, err := chart.NewInstance()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- in.Start(ctx) }()
	<-started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
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

func TestDatamodelReplayFailureClosesCreatedSession(t *testing.T) {
	program := &recordingProgram{}
	chart := datamodelTestChart(t, program, "failed-replay")
	log := newMemLog()
	ctx := context.Background()
	const id SessionID = "failed-datamodel-replay"
	if _, err := log.Append(ctx, LogEntry{SessionID: id, Kind: EntryKind("unknown")}); err != nil {
		t.Fatal(err)
	}
	if _, err := chart.Rehydrate(ctx, log, newMemSnapshotStore(), id, NoopIOProcessor); err == nil {
		t.Fatal("Rehydrate succeeded, want replay error")
	}
	if sessions := program.allSessions(); len(sessions) != 1 || sessions[0].closes() != 1 {
		t.Fatalf("failed replay sessions/closes = %d/%v, want 1/1", len(sessions), func() int {
			if len(sessions) == 0 {
				return 0
			}
			return sessions[0].closes()
		}())
	}
}

func TestDatamodelSessionCreationAndCloseErrorsAreSurfaced(t *testing.T) {
	program := &recordingProgram{newErr: errors.New("cannot create session")}
	chart := datamodelTestChart(t, program, "session-errors")
	if _, err := chart.NewInstance(); err == nil || err.Error() != "statecharts: create datamodel session: cannot create session" {
		t.Fatalf("creation error = %v", err)
	}
	program.newErr = nil
	program.closeErr = errors.New("cannot close session")
	in, err := chart.NewInstance()
	if err != nil {
		t.Fatal(err)
	}
	if err := in.Stop(context.Background()); err == nil || err.Error() != "statecharts: close datamodel session: cannot close session" {
		t.Fatalf("Stop error = %v", err)
	}
	if got := program.allSessions()[0].closes(); got != 1 {
		t.Fatalf("close calls = %d, want 1", got)
	}
}

func TestDatamodelEvaluationsReceiveLiveExecContextAndTerminalResult(t *testing.T) {
	program := &recordingProgram{}
	expr := func(name string) Expression {
		return Expression{Kind: "recording", Data: testStringValue(name)}
	}
	chart, err := Build(Compound("root", "active", Children(
		Atomic("active",
			On("go", Target("bad"), If(expr("error"))),
			On("go", Target("done"), If(expr("ready")), Then(NewScriptExecutable(ScriptDefinition{Expr: expr("transition-action")}))),
		),
		Atomic("bad"),
		Final("done", WithDone(expr("result"))),
	)), recordingDatamodel{program}, WithRevisionSalt("live-context"))
	if err != nil {
		t.Fatal(err)
	}
	in, err := chart.NewInstance()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := in.Send(ctx, Event{Name: "go"}); err != nil {
		t.Fatal(err)
	}
	if err := in.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := in.Result()
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := result.AsInt64(); !ok || got != 42 {
		t.Fatalf("result = (%d, %v), want (42, true)", got, ok)
	}
	session := program.allSessions()[0]
	if got := session.closes(); got != 1 {
		t.Fatalf("terminal close calls = %d, want 1", got)
	}
	var condition, action, done bool
	for _, call := range session.callSnapshot() {
		switch {
		case call.operation == "boolean" && call.expression == "ready":
			condition = call.hasEvent && call.event == "go" && call.active
		case call.operation == "execute" && call.expression == "transition-action":
			action = call.hasEvent && call.event == "go" && !call.active
		case call.operation == "value" && call.expression == "result":
			done = call.hasEvent && call.event == "go"
		}
	}
	if !condition || !action || !done {
		t.Fatalf("live context missing: condition=%v action=%v done=%v calls=%+v", condition, action, done, session.callSnapshot())
	}
}
