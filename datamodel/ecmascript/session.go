package ecmascript

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/dhamidi/statecharts"
	"modernc.org/quickjs"
)

type session struct {
	program *program
	vm      *quickjs.VM
	current *statecharts.ExecContext

	interrupted atomic.Bool
	closed      atomic.Bool
	closeOnce   sync.Once
	closeErr    error
	done        chan struct{}
	watcher     sync.WaitGroup
	vmMu        sync.Mutex
}

func newSession(program *program) (_ *session, resultErr error) {
	vm, err := quickjs.NewVM()
	if err != nil {
		return nil, fmt.Errorf("ecmascript: create VM: %w", err)
	}
	s := &session{program: program, vm: vm, done: make(chan struct{})}
	keepVM := false
	defer func() {
		if !keepVM {
			_ = vm.Close()
		}
	}()
	if program.config.memoryLimit != 0 {
		vm.SetMemoryLimit(program.config.memoryLimit)
	}
	if program.config.stackLimit != 0 {
		vm.SetMaxStackSize(program.config.stackLimit)
	}
	if program.config.gcThreshold != 0 {
		vm.SetGCThreshold(program.config.gcThreshold)
	}
	if err := s.registerHostFunctions(); err != nil {
		return nil, err
	}
	if _, err := vm.Eval(bridgeSource, quickjs.EvalGlobal); err != nil {
		return nil, fmt.Errorf("ecmascript: initialize VM bridge: %w", err)
	}
	for _, id := range program.declarations {
		direct := directDataName.MatchString(string(id))
		declaration := "__sc_declare(" + jsString(string(id)) + "," + strconv.FormatBool(direct) + ")"
		if _, err := vm.Eval(declaration, quickjs.EvalGlobal); err != nil {
			return nil, fmt.Errorf("ecmascript: declare data %q: %w", id, err)
		}
	}
	if _, err := vm.Eval("__sc_capture_globals()", quickjs.EvalGlobal); err != nil {
		return nil, fmt.Errorf("ecmascript: capture VM globals: %w", err)
	}
	if err := vm.SetEvalTimeout(program.config.evaluationTimeout); err != nil {
		return nil, fmt.Errorf("ecmascript: set evaluation timeout: %w", err)
	}
	if signal := program.config.interrupt; signal != nil {
		s.watcher.Add(1)
		go s.watchInterrupt(signal)
	}
	keepVM = true
	return s, nil
}

func (s *session) registerHostFunctions() error {
	registrations := []struct {
		name string
		fn   quickjs.HostFunc
	}{
		{"__statecharts_event", func([]any) (any, error) { return s.eventWire() }},
		{"__statecharts_session_id", func([]any) (any, error) {
			context, err := s.execContext()
			if err != nil {
				return nil, err
			}
			return context.SessionID(), nil
		}},
		{"__statecharts_name", func([]any) (any, error) {
			context, err := s.execContext()
			if err != nil {
				return nil, err
			}
			return context.Name(), nil
		}},
		{"__statecharts_ioprocessors", func([]any) (any, error) { return s.ioProcessorsWire() }},
		{"__statecharts_platform", func([]any) (any, error) { return s.platformWire() }},
		{"__statecharts_in", func(arguments []any) (any, error) {
			context, err := s.execContext()
			if err != nil {
				return nil, err
			}
			if len(arguments) != 1 {
				return nil, fmt.Errorf("In requires exactly one state ID")
			}
			id, ok := arguments[0].(string)
			if !ok {
				return nil, fmt.Errorf("In state ID is %T, want string", arguments[0])
			}
			if context.In(statecharts.Identifier(id)) {
				return 1, nil
			}
			return 0, nil
		}},
	}
	for _, registration := range registrations {
		if err := s.vm.RegisterHostFunc(registration.name, registration.fn); err != nil {
			return fmt.Errorf("ecmascript: register host function %q: %w", registration.name, err)
		}
	}
	return nil
}

func (s *session) watchInterrupt(signal <-chan struct{}) {
	defer s.watcher.Done()
	select {
	case <-signal:
		s.interrupted.Store(true)
		s.vmMu.Lock()
		if s.vm != nil {
			s.vm.Interrupt()
		}
		s.vmMu.Unlock()
	case <-s.done:
	}
}

func (s *session) execContext() (statecharts.ExecContext, error) {
	if s.current == nil {
		return statecharts.ExecContext{}, fmt.Errorf("ecmascript: system binding accessed outside an evaluation")
	}
	return *s.current, nil
}

func asCompiled(expression statecharts.CompiledExpression, owner *programOwner) (*compiledExpression, error) {
	compiled, ok := expression.(*compiledExpression)
	if !ok || compiled == nil || compiled.owner != owner {
		return nil, fmt.Errorf("ecmascript: compiled expression belongs to another program")
	}
	return compiled, nil
}

func (s *session) evaluate(context statecharts.ExecContext, source string) (any, error) {
	return s.evaluateTurn(&context, source)
}

func (s *session) evaluateWithoutContext(source string) (any, error) {
	return s.evaluateTurn(nil, source)
}

func (s *session) evaluateTurn(context *statecharts.ExecContext, source string) (any, error) {
	if s.closed.Load() {
		return nil, fmt.Errorf("ecmascript: session is closed")
	}
	if s.interrupted.Load() {
		return nil, fmt.Errorf("ecmascript: session was interrupted")
	}
	s.current = context
	defer func() { s.current = nil }()
	result, evalErr := s.vm.Eval(source, quickjs.EvalGlobal)
	if s.interrupted.Load() {
		return nil, errors.Join(evalErr, fmt.Errorf("ecmascript: session was interrupted"))
	}
	_, jobsErr := s.vm.ExecutePendingJobs()
	if err := errors.Join(evalErr, jobsErr); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *session) EvaluateBoolean(context statecharts.ExecContext, expression statecharts.CompiledExpression) (bool, error) {
	compiled, err := asCompiled(expression, s.program.owner)
	if err != nil {
		return false, err
	}
	if compiled.kind != compiledSource {
		return false, fmt.Errorf("ecmascript: boolean expression is not source")
	}
	wrapper := `(() => { "use strict"; return Boolean((` + compiled.source + `)); })()`
	result, err := s.evaluate(context, wrapper)
	if err != nil {
		return false, err
	}
	boolean, ok := result.(bool)
	if !ok {
		return false, fmt.Errorf("ecmascript: boolean result is %T", result)
	}
	return boolean, nil
}

func (s *session) EvaluateValue(context statecharts.ExecContext, expression statecharts.CompiledExpression) (statecharts.Value, error) {
	compiled, err := asCompiled(expression, s.program.owner)
	if err != nil {
		return statecharts.Value{}, err
	}
	var source string
	switch compiled.kind {
	case compiledSource:
		source = compiled.source
	case compiledData:
		source = `__sc_read_data(` + jsString(string(compiled.dataID)) + `)`
	default:
		return statecharts.Value{}, fmt.Errorf("ecmascript: expression is not readable")
	}
	wrapper := `(() => { "use strict"; return __sc_export((` + source + `)); })()`
	result, err := s.evaluate(context, wrapper)
	if err != nil {
		return statecharts.Value{}, err
	}
	wire, ok := result.(string)
	if !ok {
		return statecharts.Value{}, fmt.Errorf("ecmascript: exported value is %T, want string", result)
	}
	return decodeValue(wire)
}

func (s *session) Assign(context statecharts.ExecContext, expression statecharts.CompiledExpression, value statecharts.Value) error {
	compiled, err := asCompiled(expression, s.program.owner)
	if err != nil {
		return err
	}
	wire, err := value.MarshalBinary()
	if err != nil {
		return fmt.Errorf("ecmascript: encode assignment value: %w", err)
	}
	imported := `__sc_import(` + jsString(string(wire)) + `)`
	var source string
	switch compiled.kind {
	case compiledData:
		source = `__sc_assign_data(` + jsString(string(compiled.dataID)) + `,` + imported + `)`
	case compiledSource:
		source = `(() => { "use strict"; (` + compiled.source + `) = ` + imported + `; })()`
	default:
		return fmt.Errorf("ecmascript: expression is not assignable")
	}
	_, err = s.evaluate(context, source)
	return err
}

func (s *session) Execute(context statecharts.ExecContext, expression statecharts.CompiledExpression) error {
	compiled, err := asCompiled(expression, s.program.owner)
	if err != nil {
		return err
	}
	if compiled.kind != compiledSource && compiled.kind != compiledFunction {
		return fmt.Errorf("ecmascript: expression is not executable")
	}
	wrapper := "(() => { \"use strict\";\n" + compiled.source + "\n})()"
	_, err = s.evaluate(context, wrapper)
	return err
}

func (s *session) ForEach(context statecharts.ExecContext, expression statecharts.CompiledExpression, bindings statecharts.IterationBindings, body func() error) error {
	if body == nil {
		return fmt.Errorf("ecmascript: nil foreach body")
	}
	value, err := s.EvaluateValue(context, expression)
	if err != nil {
		return err
	}
	list, ok := value.AsList()
	if !ok {
		return fmt.Errorf("ecmascript: foreach value is %s, want list", value.Kind())
	}
	for index, item := range list {
		if err := s.Assign(context, bindings.Item, item); err != nil {
			return err
		}
		if bindings.Index != nil {
			if err := s.Assign(context, bindings.Index, statecharts.Int64Value(int64(index))); err != nil {
				return err
			}
		}
		if err := body(); err != nil {
			return err
		}
	}
	return nil
}

// SnapshotIncompatibleError reports declared ECMAScript data that cannot be
// represented as canonical Value data. Replay remains the source of truth.
type SnapshotIncompatibleError struct {
	Reason string
}

func (err *SnapshotIncompatibleError) Error() string {
	return "ecmascript: snapshot is incompatible: " + err.Reason
}

type snapshotWire struct {
	Version int            `json:"version"`
	Values  []snapshotItem `json:"values"`
}

type snapshotItem struct {
	ID    statecharts.Identifier `json:"id"`
	Value statecharts.Value      `json:"value"`
}

func (s *session) EncodeSnapshot() ([]byte, error) {
	unchanged, err := s.evaluateWithoutContext("__sc_globals_unchanged()")
	if err != nil {
		return nil, &SnapshotIncompatibleError{Reason: fmt.Sprintf("inspect VM globals: %v", err)}
	}
	if safe, _ := unchanged.(bool); !safe {
		return nil, &SnapshotIncompatibleError{Reason: "scripts changed undeclared VM globals"}
	}
	items := make([]snapshotItem, 0, len(s.program.declarations))
	for _, id := range sortedDeclarations(s.program.declarations) {
		value, err := s.exportData(id)
		if err != nil {
			return nil, &SnapshotIncompatibleError{Reason: fmt.Sprintf("data %q: %v", id, err)}
		}
		items = append(items, snapshotItem{ID: id, Value: value})
	}
	result, err := json.Marshal(snapshotWire{Version: 1, Values: items})
	if err != nil {
		return nil, fmt.Errorf("ecmascript: encode snapshot: %w", err)
	}
	return result, nil
}

func (s *session) exportData(id statecharts.Identifier) (statecharts.Value, error) {
	wrapper := `__sc_export(__sc_read_data(` + jsString(string(id)) + `))`
	result, err := s.evaluateWithoutContext(wrapper)
	if err != nil {
		return statecharts.Value{}, err
	}
	wire, ok := result.(string)
	if !ok {
		return statecharts.Value{}, fmt.Errorf("exported data is %T, want string", result)
	}
	return decodeValue(wire)
}

func (s *session) DecodeSnapshot(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var wire snapshotWire
	if err := decoder.Decode(&wire); err != nil {
		return fmt.Errorf("ecmascript: decode snapshot: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return err
	}
	if wire.Version != 1 {
		return fmt.Errorf("ecmascript: unsupported snapshot version %d", wire.Version)
	}
	want := make(map[statecharts.Identifier]bool, len(s.program.declarations))
	for _, id := range s.program.declarations {
		want[id] = true
	}
	seen := make(map[statecharts.Identifier]bool, len(wire.Values))
	for _, item := range wire.Values {
		if !want[item.ID] {
			return fmt.Errorf("ecmascript: snapshot contains unknown data ID %q", item.ID)
		}
		if seen[item.ID] {
			return fmt.Errorf("ecmascript: snapshot contains duplicate data ID %q", item.ID)
		}
		seen[item.ID] = true
	}
	for id := range want {
		if !seen[id] {
			return fmt.Errorf("ecmascript: snapshot is missing data ID %q", id)
		}
	}
	restore, err := json.Marshal(wire.Values)
	if err != nil {
		return fmt.Errorf("ecmascript: encode snapshot restore input: %w", err)
	}
	_, err = s.evaluateWithoutContext(`__sc_restore(` + jsString(string(restore)) + `)`)
	if err != nil {
		return fmt.Errorf("ecmascript: restore snapshot: %w", err)
	}
	return nil
}

func requireEOF(decoder *json.Decoder) error {
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("ecmascript: unexpected data after snapshot")
		}
		return fmt.Errorf("ecmascript: decode snapshot trailing data: %w", err)
	}
	return nil
}

func decodeValue(wire string) (statecharts.Value, error) {
	var value statecharts.Value
	if err := value.UnmarshalBinary([]byte(wire)); err != nil {
		return statecharts.Value{}, fmt.Errorf("ecmascript: decode exported Value: %w", err)
	}
	return value, nil
}

func jsString(value string) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err) // encoding/json cannot fail while marshaling a string.
	}
	return string(encoded)
}

func (s *session) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		close(s.done)
		s.watcher.Wait()
		s.vmMu.Lock()
		defer s.vmMu.Unlock()
		if s.vm != nil {
			s.closeErr = s.vm.Close()
			s.vm = nil
		}
	})
	return s.closeErr
}
