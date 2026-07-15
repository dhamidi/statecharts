package statecharts

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
)

type goModelState struct{ Count int }

func baseGoDefinition(data []DataDefinition, actions ...Executable) *Definition {
	var blocks []ExecutableBlock
	if len(actions) != 0 {
		blocks = []ExecutableBlock{actions}
	}
	return &Definition{ID: "test", Datamodel: "go", Data: data, Root: StateDefinition{ID: StateDefinitionID{Value: "root"}, Kind: KindAtomic, OnEntry: blocks}}
}

func resolvedExpression(t *testing.T, program DatamodelProgram, expression Expression) CompiledExpression {
	t.Helper()
	compiled, err := program.ResolveExpression(expression)
	if err != nil {
		t.Fatalf("ResolveExpression: %v", err)
	}
	return compiled

}

func resolvedDataLocation(t *testing.T, program DatamodelProgram, id Identifier) CompiledExpression {
	t.Helper()
	compiled, err := program.ResolveDataLocation(id)
	if err != nil {
		t.Fatalf("ResolveDataLocation: %v", err)
	}
	return compiled
}

func resolvedFunction(t *testing.T, program DatamodelProgram, function FunctionRef) CompiledExpression {
	t.Helper()
	compiled, err := program.ResolveFunction(function)
	if err != nil {
		t.Fatalf("ResolveFunction: %v", err)
	}
	return compiled
}

func TestGoModelReusableParameterizedRefsAndRegistrySnapshot(t *testing.T) {
	m := NewGoModel(func() *goModelState { return &goModelState{} })
	r, err := m.Action("add", "v1", func(d *goModelState, _ ExecContext, a []Value) error {
		n, _ := a[0].AsNumber()
		if n == "1" {
			d.Count++
		} else {
			d.Count += 2
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	one, two := r.Expression(GoLiteral(Int64Value(1))), r.Expression(GoLiteral(Int64Value(2)))
	call := r.Function(GoLiteral(Int64Value(1)))
	d := baseGoDefinition(nil, NewScriptExecutable(ScriptDefinition{Expr: one}), NewScriptExecutable(ScriptDefinition{Expr: two}), NewCallExecutable(CallDefinition{Function: call}))
	p, err := m.Compile(d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.Action("later", "v1", func(*goModelState, ExecContext, []Value) error { return nil }); err != nil {
		t.Fatal(err)
	}
	s, _ := p.NewSession(SessionOptions{})
	for _, e := range []Expression{one, two} {
		if err := s.Execute(ExecContext{}, resolvedExpression(t, p, e)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Execute(ExecContext{}, resolvedFunction(t, p, call)); err != nil {
		t.Fatal(err)
	}
	if s.(*goSession[goModelState]).data.Count != 4 {
		t.Fatal("parameterized references did not differ")
	}
}

func TestGoModelTypedConditionActionValueAndLocation(t *testing.T) {
	model := NewGoModel(func() *goModelState { return &goModelState{} })
	valueRef, err := model.Value("argument", "v1", func(_ *goModelState, _ ExecContext, args []Value) (Value, error) {
		return args[0].Clone(), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	locationRef, err := model.Location(
		"count", "v1",
		func(data *goModelState, _ ExecContext, _ []Value) (Value, error) {
			return Int64Value(int64(data.Count)), nil
		},
		func(data *goModelState, _ ExecContext, value Value, _ []Value) error {
			count, ok := value.AsInt64()
			if !ok {
				return errors.New("count is not an integer")
			}
			data.Count = int(count)
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	conditionRef, err := model.Condition("at-least", "v1", func(data *goModelState, _ ExecContext, args []Value) (bool, error) {
		minimum, _ := args[0].AsInt64()
		return int64(data.Count) >= minimum, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	actionRef, err := model.Action("add", "v1", func(data *goModelState, _ ExecContext, args []Value) error {
		amount, _ := args[0].AsInt64()
		data.Count += int(amount)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	valueExpression := valueRef.Expression(GoLiteral(Int64Value(5)))
	locationExpression := locationRef.Expression()
	conditionExpression := conditionRef.Expression(GoLiteral(Int64Value(4)))
	actionExpression := actionRef.Expression(GoLiteral(Int64Value(2)))
	definition := baseGoDefinition(
		[]DataDefinition{{ID: "initial", Expr: &valueExpression}},
		NewAssignExecutable(AssignDefinition{Location: locationExpression, Expr: valueExpression}),
		NewScriptExecutable(ScriptDefinition{Expr: actionExpression}),
	)
	definition.Root.Transitions = []TransitionDefinition{{Events: []Identifier{"check"}, Condition: &conditionExpression}}
	program, err := model.Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	session, err := program.NewSession(SessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	value, err := session.EvaluateValue(ExecContext{}, resolvedExpression(t, program, valueExpression))
	if err != nil {
		t.Fatal(err)
	}
	location := resolvedExpression(t, program, locationExpression)
	if err := session.Assign(ExecContext{}, location, value); err != nil {
		t.Fatal(err)
	}
	if got, err := session.EvaluateValue(ExecContext{}, location); err != nil {
		t.Fatal(err)
	} else if count, _ := got.AsInt64(); count != 5 {
		t.Fatalf("location read = %d, want 5", count)
	}
	if matches, err := session.EvaluateBoolean(ExecContext{}, resolvedExpression(t, program, conditionExpression)); err != nil || !matches {
		t.Fatalf("condition = %v, %v", matches, err)
	}
	if err := session.Execute(ExecContext{}, resolvedExpression(t, program, actionExpression)); err != nil {
		t.Fatal(err)
	}
	if session.(*goSession[goModelState]).data.Count != 7 {
		t.Fatalf("count = %d, want 7", session.(*goSession[goModelState]).data.Count)
	}
}

func TestGoModelReferencesRoundTripAndCompileValidation(t *testing.T) {
	model := NewGoModel(func() *goModelState { return &goModelState{} })
	action, err := model.Action("record", "v2", func(*goModelState, ExecContext, []Value) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := model.Action("record", "v2", func(*goModelState, ExecContext, []Value) error { return nil }); err == nil {
		t.Fatal("duplicate registration succeeded")
	}
	argument := GoLiteral(Int64Value(7))
	function := action.Function(argument)
	definition := baseGoDefinition(nil, NewCallExecutable(CallDefinition{Function: function}))
	wire, err := json.Marshal(definition)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Definition
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	got := decoded.Root.OnEntry[0][0].Call.Function
	if got.Name != "record" || got.Version != "v2" || len(got.Args) != 1 || got.Args[0].Kind != goLiteralKind {
		t.Fatalf("round-tripped function = %+v", got)
	}

	equivalent := NewGoModel(func() *goModelState { return &goModelState{} })
	if _, err := equivalent.Action("record", "v2", func(*goModelState, ExecContext, []Value) error { return nil }); err != nil {
		t.Fatal(err)
	}
	program, err := equivalent.Compile(&decoded)
	if err != nil {
		t.Fatalf("compile round trip: %v", err)
	}
	fingerprint := program.Fingerprint()
	fingerprint[0] ^= 0xff
	if bytes.Equal(fingerprint, program.Fingerprint()) {
		t.Fatal("Fingerprint returned mutable program storage")
	}

	unknown := NewGoModel(func() *goModelState { return &goModelState{} })
	if _, err := unknown.Compile(&decoded); err == nil || !strings.Contains(err.Error(), "unknown Go function") {
		t.Fatalf("unknown reference error = %v", err)
	}
	wrongKind := NewGoModel(func() *goModelState { return &goModelState{} })
	if _, err := wrongKind.Value("record", "v2", func(*goModelState, ExecContext, []Value) (Value, error) { return NullValue(), nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := wrongKind.Compile(&decoded); err == nil || !strings.Contains(err.Error(), "wrong kind") {
		t.Fatalf("wrong-kind error = %v", err)
	}
	decoded.Root.OnEntry[0][0].Call.Function.Version = "v3"
	if _, err := equivalent.Compile(&decoded); err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("version mismatch error = %v", err)
	}
	if _, err := model.Action("bad", string([]byte{0xff}), func(*goModelState, ExecContext, []Value) error { return nil }); err == nil {
		t.Fatal("invalid UTF-8 version registered")
	}
}

func TestGoModelDeclaredDataReadWriteForeachAndSnapshot(t *testing.T) {
	item, array := GoData("item"), GoData("array")
	list := GoLiteral(ListValue([]Value{Int64Value(4), Int64Value(5)}))
	fe := NewForEachExecutable(ForEachDefinition{Array: array, Item: "item", Index: "index", Actions: []ExecutableBlock{{NewAssignExecutable(AssignDefinition{Location: item, Expr: item})}}})
	d := baseGoDefinition([]DataDefinition{{ID: "item"}, {ID: "index"}, {ID: "array", Expr: &list}}, fe)
	p, err := NewGoModel(func() *goModelState { return &goModelState{} }).Compile(d)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := p.NewSession(SessionOptions{})
	arrayValue, err := s.EvaluateValue(ExecContext{}, resolvedExpression(t, p, list))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Assign(ExecContext{}, resolvedDataLocation(t, p, "array"), arrayValue); err != nil {
		t.Fatal(err)
	}
	seen := 0
	if err := s.ForEach(ExecContext{}, resolvedExpression(t, p, array), IterationBindings{Item: resolvedDataLocation(t, p, "item"), Index: resolvedDataLocation(t, p, "index")}, func() error { seen++; return nil }); err != nil {
		t.Fatal(err)
	}
	if seen != 2 {
		t.Fatalf("iterations=%d", seen)
	}
	s.(*goSession[goModelState]).data.Count = 9
	snap, err := s.EncodeSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	s2, _ := p.NewSession(SessionOptions{})
	if err := s2.DecodeSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	v, _ := s2.EvaluateValue(ExecContext{}, resolvedExpression(t, p, item))
	n, _ := v.AsNumber()
	if n != "5" || s2.(*goSession[goModelState]).data.Count != 9 {
		t.Fatal("full snapshot not restored")
	}
	before, _ := s2.EncodeSnapshot()
	if err := s2.DecodeSnapshot([]byte(`{"data":{},"values":{"alien":null}}`)); err == nil {
		t.Fatal("invalid envelope accepted")
	}
	after, _ := s2.EncodeSnapshot()
	if !bytes.Equal(before, after) {
		t.Fatal("failed decode was not atomic")
	}
	bad := baseGoDefinition([]DataDefinition{{ID: "x", Expr: func() *Expression { e := GoData("missing"); return &e }()}})
	if _, err := NewGoModel(func() *goModelState { return &goModelState{} }).Compile(bad); err == nil {
		t.Fatal("unknown ID compiled")
	}
}

func TestGoModelFingerprintReferencedOnlyAndRegistryIsolation(t *testing.T) {
	makeModel := func(delta int) (*GoModel[goModelState], GoActionRef) {
		m := NewGoModel(func() *goModelState { return &goModelState{} })
		r, _ := m.Action("same", "v1", func(d *goModelState, _ ExecContext, _ []Value) error { d.Count += delta; return nil })
		return m, r
	}
	m1, r1 := makeModel(1)
	m2, r2 := makeModel(2)
	e1, e2 := r1.Expression(), r2.Expression()
	d1, d2 := baseGoDefinition(nil, NewScriptExecutable(ScriptDefinition{Expr: e1})), baseGoDefinition(nil, NewScriptExecutable(ScriptDefinition{Expr: e2}))
	p1, _ := m1.Compile(d1)
	p2, _ := m2.Compile(d2)
	if !bytes.Equal(p1.Fingerprint(), p2.Fingerprint()) {
		t.Fatal("behavior pointers affected fingerprint")
	}
	m3, r3 := makeModel(1)
	_, _ = m3.Action("unused", "v9", func(*goModelState, ExecContext, []Value) error { return nil })
	e3 := r3.Expression()
	p3, _ := m3.Compile(baseGoDefinition(nil, NewScriptExecutable(ScriptDefinition{Expr: e3})))
	if !bytes.Equal(p1.Fingerprint(), p3.Fingerprint()) {
		t.Fatal("unused registration affected fingerprint")
	}
	for p, e := range map[DatamodelProgram]struct {
		e Expression
		n int
	}{p1: {e1, 1}, p2: {e2, 2}} {
		s, _ := p.NewSession(SessionOptions{})
		_ = s.Execute(ExecContext{}, resolvedExpression(t, p, e.e))
		if s.(*goSession[goModelState]).data.Count != e.n {
			t.Fatal("registry interference")
		}
	}
}

func TestGoModelRejectsNilAndVersionMismatch(t *testing.T) {
	m := NewGoModel(func() *goModelState { return &goModelState{} })
	if _, err := m.Action("nil", "v1", nil); err == nil {
		t.Fatal("nil callback accepted")
	}
	r, _ := m.Action("a", "v1", func(*goModelState, ExecContext, []Value) error { return nil })
	ref := r.Function()
	ref.Version = "v2"
	if _, err := m.Compile(baseGoDefinition(nil, NewCallExecutable(CallDefinition{Function: ref}))); err == nil {
		t.Fatal("version mismatch compiled")
	}
	if _, err := NewGoModel[goModelState](nil).Compile(baseGoDefinition(nil)); err == nil {
		t.Fatal("nil factory compiled")
	}
	withoutCodec := NewGoModel(func() *goModelState { return &goModelState{} }, WithGoSnapshotCodec(GoSnapshotCodec[goModelState]{}))
	if _, err := withoutCodec.Compile(baseGoDefinition(nil)); err == nil {
		t.Fatal("nil snapshot codec compiled")
	}
}

func TestGoModelCallbackFailuresAndTypeMismatch(t *testing.T) {
	m := NewGoModel(func() *goModelState { return &goModelState{} })
	a, _ := m.Action("err", "v1", func(*goModelState, ExecContext, []Value) error { return errors.New("bad") })
	v, _ := m.Value("panic", "v1", func(*goModelState, ExecContext, []Value) (Value, error) { panic("boom") })
	c, _ := m.Condition("panic-condition", "v1", func(*goModelState, ExecContext, []Value) (bool, error) { panic("condition boom") })
	l, _ := m.Location(
		"panic-location", "v1",
		func(*goModelState, ExecContext, []Value) (Value, error) { panic("read boom") },
		func(*goModelState, ExecContext, Value, []Value) error { panic("write boom") },
	)
	ae, ve, ce, le := a.Expression(), v.Expression(), c.Expression(), l.Expression()
	d := baseGoDefinition(
		[]DataDefinition{{ID: "v", Expr: &ve}, {ID: "location", Expr: &le}},
		NewScriptExecutable(ScriptDefinition{Expr: ae}),
		NewAssignExecutable(AssignDefinition{Location: le, Expr: GoLiteral(NullValue())}),
	)
	d.Root.Transitions = []TransitionDefinition{{Events: []Identifier{"check"}, Condition: &ce}}
	p, err := m.Compile(d)
	if err != nil {
		t.Fatal(err)
	}
	s, err := p.NewSession(SessionOptions{})
	if err != nil {
		t.Fatalf("NewSession evaluated a data initializer: %v", err)
	}
	if _, err := s.EvaluateValue(ExecContext{}, resolvedExpression(t, p, ve)); err == nil {
		t.Fatal("value callback panic not converted")
	}
	if _, err := s.EvaluateBoolean(ExecContext{}, resolvedExpression(t, p, ce)); err == nil {
		t.Fatal("condition callback panic not converted")
	}
	location := resolvedExpression(t, p, le)
	if _, err := s.EvaluateValue(ExecContext{}, location); err == nil {
		t.Fatal("location reader panic not converted")
	}
	if err := s.Assign(ExecContext{}, location, NullValue()); err == nil {
		t.Fatal("location writer panic not converted")
	}
	// Runtime callback errors and foreign compiled handles are ordinary errors.
	d = baseGoDefinition(nil, NewScriptExecutable(ScriptDefinition{Expr: ae}))
	p, _ = m.Compile(d)
	s, _ = p.NewSession(SessionOptions{})
	if err := s.Execute(ExecContext{}, resolvedExpression(t, p, ae)); err == nil {
		t.Fatal("callback error lost")
	}
	if _, err := s.EvaluateValue(ExecContext{}, struct{}{}); err == nil {
		t.Fatal("type mismatch accepted")
	}
}

func TestGoModelCustomCodecAndConcurrentCompileSessions(t *testing.T) {
	codec := GoSnapshotCodec[goModelState]{Encode: func(d *goModelState) ([]byte, error) { return json.Marshal(d.Count) }, Decode: func(b []byte) (*goModelState, error) {
		var n int
		if err := json.Unmarshal(b, &n); err != nil {
			return nil, err
		}
		return &goModelState{Count: n}, nil
	}}
	m := NewGoModel(func() *goModelState { return &goModelState{} }, WithGoSnapshotCodec(codec))
	r, _ := m.Action("inc", "v1", func(d *goModelState, _ ExecContext, _ []Value) error { d.Count++; return nil })
	e := r.Expression()
	d := baseGoDefinition(nil, NewScriptExecutable(ScriptDefinition{Expr: e}))
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := m.Compile(d)
			if err != nil {
				t.Error(err)
				return
			}
			s, err := p.NewSession(SessionOptions{})
			if err != nil {
				t.Error(err)
				return
			}
			if err := s.Execute(ExecContext{}, resolvedExpression(t, p, e)); err != nil {
				t.Error(err)
			}
			b, err := s.EncodeSnapshot()
			if err != nil {
				t.Error(err)
			}
			fresh, _ := p.NewSession(SessionOptions{})
			if err := fresh.DecodeSnapshot(b); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

func TestGoModelSnapshotCodecPanicsBecomeErrors(t *testing.T) {
	codec := GoSnapshotCodec[goModelState]{
		Encode: func(*goModelState) ([]byte, error) { panic("encode") },
		Decode: func([]byte) (*goModelState, error) { panic("decode") },
	}
	model := NewGoModel(func() *goModelState { return &goModelState{} }, WithGoSnapshotCodec(codec))
	program, err := model.Compile(baseGoDefinition(nil))
	if err != nil {
		t.Fatal(err)
	}
	session, err := program.NewSession(SessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.EncodeSnapshot(); err == nil || !strings.Contains(err.Error(), "snapshot codec panicked") {
		t.Fatalf("EncodeSnapshot error = %v", err)
	}
	wire, err := json.Marshal(goSnapshot{Version: 1, Data: []byte("data"), Values: map[Identifier]Value{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.DecodeSnapshot(wire); err == nil || !strings.Contains(err.Error(), "snapshot codec panicked") {
		t.Fatalf("DecodeSnapshot error = %v", err)
	}
}

func TestGoModelLateDataInitializationIsDeferredAndGenericallyResolvable(t *testing.T) {
	model := NewGoModel(func() *goModelState { return &goModelState{} })
	initializer, err := model.Value("initialize", "v1", func(data *goModelState, ec ExecContext, _ []Value) (Value, error) {
		data.Count++
		if ec.SessionID() != "session-1" {
			return Value{}, errors.New("initializer did not receive interpreter context")
		}
		return Int64Value(41), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	expression := initializer.Expression()
	definition := baseGoDefinition([]DataDefinition{{ID: "answer", Expr: &expression}})
	definition.DataBinding = DataBindingLate
	program, err := model.Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	session, err := program.NewSession(SessionOptions{})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if session.(*goSession[goModelState]).data.Count != 0 {
		t.Fatal("late data initializer ran during session creation")
	}

	context := ExecContext{sessionID: "session-1"}
	value, err := session.EvaluateValue(context, resolvedExpression(t, program, expression))
	if err != nil {
		t.Fatal(err)
	}
	location := resolvedDataLocation(t, program, "answer")
	if err := session.Assign(context, location, value); err != nil {
		t.Fatal(err)
	}
	got, err := session.EvaluateValue(context, location)
	if err != nil {
		t.Fatal(err)
	}
	if number, _ := got.AsInt64(); number != 41 {
		t.Fatalf("initialized answer = %d, want 41", number)
	}
}

func TestGoModelDataInitializersCanRunInDefinitionOrder(t *testing.T) {
	model := NewGoModel(func() *goModelState { return &goModelState{} })
	copyValue, err := model.Value("copy", "v1", func(_ *goModelState, _ ExecContext, args []Value) (Value, error) {
		return args[0].Clone(), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	first := GoLiteral(Int64Value(9))
	second := copyValue.Expression(GoData("first"))
	definition := baseGoDefinition([]DataDefinition{
		{ID: "first", Expr: &first},
		{ID: "second", Expr: &second},
	})
	program, err := model.Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	session, err := program.NewSession(SessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range definition.Data {
		value, err := session.EvaluateValue(ExecContext{}, resolvedExpression(t, program, *declaration.Expr))
		if err != nil {
			t.Fatal(err)
		}
		if err := session.Assign(ExecContext{}, resolvedDataLocation(t, program, declaration.ID), value); err != nil {
			t.Fatal(err)
		}
	}
	value, err := session.EvaluateValue(ExecContext{}, resolvedDataLocation(t, program, "second"))
	if err != nil {
		t.Fatal(err)
	}
	if number, _ := value.AsInt64(); number != 9 {
		t.Fatalf("second = %d, want 9", number)
	}
}

func TestGoModelSessionRejectsHandlesFromAnotherProgram(t *testing.T) {
	build := func(delta int, dataID Identifier) (DatamodelProgram, Expression, FunctionRef) {
		model := NewGoModel(func() *goModelState { return &goModelState{} })
		action, err := model.Action("change", "v1", func(data *goModelState, _ ExecContext, _ []Value) error {
			data.Count += delta
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		expression := action.Expression()
		function := action.Function()
		definition := baseGoDefinition(
			[]DataDefinition{{ID: dataID}},
			NewScriptExecutable(ScriptDefinition{Expr: expression}),
			NewCallExecutable(CallDefinition{Function: function}),
		)
		program, err := model.Compile(definition)
		if err != nil {
			t.Fatal(err)
		}
		return program, expression, function
	}
	first, _, _ := build(1, "first")
	second, secondExpression, secondFunction := build(100, "second")
	session, err := first.NewSession(SessionOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := session.Execute(ExecContext{}, resolvedExpression(t, second, secondExpression)); err == nil {
		t.Fatal("session executed an expression handle from another program")
	}
	if err := session.Execute(ExecContext{}, resolvedFunction(t, second, secondFunction)); err == nil {
		t.Fatal("session executed a function handle from another program")
	}
	if err := session.Assign(ExecContext{}, resolvedDataLocation(t, second, "second"), Int64Value(1)); err == nil {
		t.Fatal("session assigned through a data handle from another program")
	}
	if got := session.(*goSession[goModelState]).data.Count; got != 0 {
		t.Fatalf("foreign callbacks changed session data to %d", got)
	}
	if _, exists := session.(*goSession[goModelState]).values["second"]; exists {
		t.Fatal("foreign data location was inserted into session")
	}
}
