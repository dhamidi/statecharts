package statecharts

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

type compileTestModel struct {
	Trace            []string
	InitializerCalls int
}

type compileParityModel struct {
	Trace []string
}

func compileTestDefinition(root StateDefinition) Definition {
	return Definition{ID: "compiled-test", Datamodel: "go", Root: root}
}

func compileTestState(id Identifier, kind StateKind, children ...StateDefinition) StateDefinition {
	return StateDefinition{ID: StateDefinitionID{Value: id}, Kind: kind, Children: children}
}

func compileTestAction(t *testing.T, model *GoModel[compileTestModel], name string, action GoAction[compileTestModel]) GoActionRef {
	t.Helper()
	reference, err := model.Action(Identifier(name), "v1", action)
	if err != nil {
		t.Fatalf("register action %q: %v", name, err)
	}
	return reference
}

func TestCompileNestedBlocksPreserveIndependentErrorBoundaries(t *testing.T) {
	model := NewGoModel(func() *compileTestModel { return &compileTestModel{} })
	mark := compileTestAction(t, model, "mark", func(data *compileTestModel, _ ExecContext, args []Value) error {
		label, ok := args[0].AsString()
		if !ok {
			if number, numberOK := args[0].AsInt64(); numberOK {
				label = strconv.FormatInt(number, 10)
			}
		}
		data.Trace = append(data.Trace, label)
		return nil
	})
	fail := compileTestAction(t, model, "fail", func(*compileTestModel, ExecContext, []Value) error {
		return errors.New("expected failure")
	})
	condition, err := model.Condition("true", "v1", func(*compileTestModel, ExecContext, []Value) (bool, error) {
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	item := GoData("item")
	definition := compileTestDefinition(compileTestState("root", KindAtomic))
	definition.Data = []DataDefinition{{ID: "item"}}
	definition.Root.OnEntry = []ExecutableBlock{{
		NewChooseExecutable(ChooseDefinition{Branches: []ChooseBranchDefinition{{
			Condition: condition.Expression(),
			Actions: []ExecutableBlock{
				{NewScriptExecutable(ScriptDefinition{Expr: fail.Expression()}), NewScriptExecutable(ScriptDefinition{Expr: mark.Expression(GoLiteral(mustTestString(t, "choose-skipped")))})},
				{NewScriptExecutable(ScriptDefinition{Expr: mark.Expression(GoLiteral(mustTestString(t, "choose-next-block")))})},
			},
		}}}),
		NewForEachExecutable(ForEachDefinition{
			Array: GoLiteral(ListValue([]Value{Int64Value(1), Int64Value(2)})), Item: "item",
			Actions: []ExecutableBlock{
				{NewScriptExecutable(ScriptDefinition{Expr: fail.Expression()}), NewScriptExecutable(ScriptDefinition{Expr: mark.Expression(GoLiteral(mustTestString(t, "foreach-skipped")))})},
				{NewScriptExecutable(ScriptDefinition{Expr: mark.Expression(item)})},
			},
		}),
		NewScriptExecutable(ScriptDefinition{Expr: mark.Expression(GoLiteral(mustTestString(t, "outer-continues")))}),
	}}
	definition.Root.Transitions = []TransitionDefinition{{
		Events:  []Identifier{ErrEventExecution},
		Actions: []ExecutableBlock{{NewScriptExecutable(ScriptDefinition{Expr: mark.Expression(GoLiteral(mustTestString(t, "error")))})}},
	}}

	chart, err := Compile(definition, model)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	instance, err := chart.NewInstance()
	if err != nil {
		t.Fatalf("NewInstance: %v", err)
	}
	if err := instance.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	data := instance.session.(*goSession[compileTestModel]).data
	if got, want := strings.Join(data.Trace, ","), "choose-next-block,1,2,outer-continues,error,error,error"; got != want {
		t.Fatalf("trace = %q, want %q", got, want)
	}
	if err := instance.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestCompileLateDataInitializationSurvivesSnapshotJSON(t *testing.T) {
	model := NewGoModel(func() *compileTestModel { return &compileTestModel{} })
	initializer, err := model.Value("initialize", "v1", func(data *compileTestModel, _ ExecContext, _ []Value) (Value, error) {
		data.InitializerCalls++
		return Int64Value(int64(data.InitializerCalls)), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	value := initializer.Expression()
	a := compileTestState("a", KindAtomic)
	a.Data = []DataDefinition{{ID: "a-value", Expr: &value}}
	a.Transitions = []TransitionDefinition{{Events: []Identifier{"next"}, Targets: []Identifier{"b"}}}
	b := compileTestState("b", KindAtomic)
	b.Transitions = []TransitionDefinition{{Events: []Identifier{"back"}, Targets: []Identifier{"a"}}}
	root := compileTestState("root", KindCompound, a, b)
	root.Initial = &TransitionDefinition{Targets: []Identifier{"a"}}
	definition := compileTestDefinition(root)
	definition.DataBinding = DataBindingLate

	chart, err := Compile(definition, model)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	instance, err := chart.NewInstance(WithSessionID("late-data"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := instance.Send(ctx, Event{Name: "next"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := instance.Snapshot(ctx)
	if err != nil {
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
	if err := instance.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	restored, err := chart.Restore(decoded)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if err := restored.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restored.Send(ctx, Event{Name: "back"}); err != nil {
		t.Fatal(err)
	}
	if got := restored.session.(*goSession[compileTestModel]).data.InitializerCalls; got != 1 {
		t.Fatalf("initializer calls after restore/re-entry = %d, want 1", got)
	}
	if err := restored.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

type compileTestDatamodel struct {
	name    Identifier
	program DatamodelProgram
}

func (m compileTestDatamodel) Name() Identifier { return m.name }
func (m compileTestDatamodel) Compile(*Definition) (DatamodelProgram, error) {
	return m.program, nil
}

func TestCompileRejectsInvalidDatamodelBoundariesAndSources(t *testing.T) {
	definition := compileTestDefinition(compileTestState("root", KindAtomic))
	model := compileTestDatamodel{name: "other", program: &recordingProgram{}}
	if _, err := Compile(definition, model); err == nil || !strings.Contains(err.Error(), "datamodel") {
		t.Fatalf("name mismatch error = %v", err)
	}

	definition.Datamodel = "nil-program"
	if _, err := Compile(definition, compileTestDatamodel{name: "nil-program"}); err == nil {
		t.Fatal("nil datamodel program was accepted")
	}

	definition.Datamodel = "source"
	definition.Data = []DataDefinition{{ID: "config", Source: "asset:config"}}
	if _, err := Compile(definition, compileTestDatamodel{name: "source", program: &recordingProgram{}}); err == nil || !strings.Contains(err.Error(), "source") {
		t.Fatalf("unsupported source error = %v", err)
	}
}

func TestCompileDefinitionIsNormalizedAndIsolated(t *testing.T) {
	model := NewGoModel(func() *compileTestModel { return &compileTestModel{} })
	definition := compileTestDefinition(StateDefinition{
		ID: StateDefinitionID{Value: "root"}, Kind: KindCompound,
		Children: []StateDefinition{
			{ID: StateDefinitionID{Generated: true}, Kind: KindAtomic},
			compileTestState("explicit", KindAtomic),
		},
	})
	chart, err := Compile(definition, model)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	first := chart.Definition()
	if first.Root.Initial == nil || first.Root.Children[0].ID.Value == "" {
		t.Fatalf("Definition was not normalized: %+v", first)
	}
	generated := first.Root.Children[0].ID.Value
	first.Root.Children[0].ID.Value = "mutated"
	first.Root.Initial.Targets[0] = "mutated"
	definition.Root.Children[1].ID.Value = "caller-mutated"
	second := chart.Definition()
	if second.Root.Children[0].ID.Value != generated || second.Root.Initial.Targets[0] != generated || second.Root.Children[1].ID.Value != "explicit" {
		t.Fatalf("compiled definition was mutated: %+v", second)
	}
	wire, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Definition
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	recompiled, err := Compile(decoded, model)
	if err != nil {
		t.Fatalf("recompile encoded definition: %v", err)
	}
	if got, want := recompiled.States(), chart.States(); !reflect.DeepEqual(got, want) {
		t.Fatalf("recompiled states = %v, want %v", got, want)
	}
	for _, candidate := range []*Chart{chart, recompiled} {
		instance, err := candidate.NewInstance()
		if err != nil {
			t.Fatal(err)
		}
		if err := instance.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := instance.Configuration(); !containsID(got, generated) {
			t.Fatalf("encoded/recompiled configuration = %v, want %q", got, generated)
		}
		_ = instance.Stop(context.Background())
	}
}

func TestCompileConditionErrorFallsThroughToLaterTransition(t *testing.T) {
	model := NewGoModel(func() *compileTestModel { return &compileTestModel{} })
	broken, err := model.Condition("broken", "v1", func(*compileTestModel, ExecContext, []Value) (bool, error) {
		return false, errors.New("condition failed")
	})
	if err != nil {
		t.Fatal(err)
	}
	active := compileTestState("active", KindAtomic)
	condition := broken.Expression()
	active.Transitions = []TransitionDefinition{
		{Events: []Identifier{"go"}, Targets: []Identifier{"wrong"}, Condition: &condition},
		{Events: []Identifier{"go"}, Targets: []Identifier{"fallback"}},
	}
	fallback := compileTestState("fallback", KindAtomic)
	fallback.Transitions = []TransitionDefinition{{Events: []Identifier{ErrEventExecution}, Targets: []Identifier{"recovered"}}}
	root := compileTestState("root", KindCompound,
		active,
		compileTestState("wrong", KindAtomic),
		fallback,
		compileTestState("recovered", KindAtomic),
	)
	root.Initial = &TransitionDefinition{Targets: []Identifier{"active"}}
	chart, err := Compile(compileTestDefinition(root), model)
	if err != nil {
		t.Fatal(err)
	}
	instance, err := chart.NewInstance()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := instance.Send(ctx, Event{Name: "go"}); err != nil {
		t.Fatal(err)
	}
	if got := instance.Configuration(); !containsID(got, "recovered") || containsID(got, "wrong") {
		t.Fatalf("configuration = %v, want recovered", got)
	}
	if err := instance.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCompileRaiseSendCancelLogAndPayloads(t *testing.T) {
	model := NewGoModel(func() *compileTestModel { return &compileTestModel{} })
	mark := compileTestAction(t, model, "record-raised", func(data *compileTestModel, ec ExecContext, _ []Value) error {
		event, _ := ec.Event()
		if value, ok := event.Data.AsInt64(); ok {
			data.Trace = append(data.Trace, strconv.FormatInt(value, 10))
		}
		return nil
	})
	generatedID := GoData("generated-id")
	definition := compileTestDefinition(compileTestState("root", KindAtomic))
	definition.Data = []DataDefinition{{ID: "generated-id"}}
	definition.Root.OnEntry = []ExecutableBlock{{
		NewLogExecutable(LogDefinition{LabelExpr: expressionPointer(GoLiteral(mustTestString(t, "compiled"))), Expr: expressionPointer(GoLiteral(Int64Value(5)))}),
		NewRaiseExecutable(RaiseDefinition{EventExpr: expressionPointer(GoLiteral(mustTestString(t, "raised"))), Data: expressionPointer(GoLiteral(Int64Value(6)))}),
		NewSendExecutable(SendDefinition{Event: "kept", Target: "peer", Type: "custom", ID: "kept", Delay: "1s", Content: expressionPointer(GoLiteral(Int64Value(7)))}),
		NewSendExecutable(SendDefinition{Event: "cancelled", Target: "peer", Type: "custom", ID: "cancelled", Delay: "1s"}),
		NewCancelExecutable(CancelDefinition{SendIDExpr: expressionPointer(GoLiteral(mustTestString(t, "cancelled")))}),
		NewSendExecutable(SendDefinition{
			EventExpr:  expressionPointer(GoLiteral(mustTestString(t, "generated"))),
			TargetExpr: expressionPointer(GoLiteral(mustTestString(t, "peer"))),
			TypeExpr:   expressionPointer(GoLiteral(mustTestString(t, "custom"))),
			IDLocation: &generatedID,
			DelayExpr:  expressionPointer(GoLiteral(mustTestString(t, "1s"))),
			Params:     []ParamDefinition{{Name: "n", Expr: expressionPointer(GoLiteral(Int64Value(9)))}},
		}),
	}}
	definition.Root.Transitions = []TransitionDefinition{{
		Events:  []Identifier{"raised"},
		Actions: []ExecutableBlock{{NewCallExecutable(CallDefinition{Function: mark.Function()})}},
	}}
	chart, err := Compile(definition, model)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	clock := NewManualClock(time.Unix(0, 0))
	processor := &captureIOProcessor{}
	logger := &recordingLogger{}
	instance, err := chart.NewInstance(
		WithClock(clock),
		WithLogger(logger),
		WithIOProcessor("custom", processor),
		WithSessionID("compiled-send"),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if got := instance.session.(*goSession[compileTestModel]).data.Trace; len(got) != 1 || got[0] != "6" {
		t.Fatalf("raise trace = %v, want [6]", got)
	}
	if len(logger.calls) != 1 || logger.calls[0].label != "compiled" || !logger.calls[0].data.Equal(Int64Value(5)) {
		t.Fatalf("logger calls = %+v", logger.calls)
	}
	clock.Advance(time.Second)
	if err := instance.Send(ctx, Event{Name: "barrier"}); err != nil {
		t.Fatal(err)
	}
	if len(processor.requests) != 2 {
		t.Fatalf("requests = %+v, want kept and generated", processor.requests)
	}
	if first := processor.requests[0]; first.Event != "kept" || first.EventSendID != "kept" || !first.Data.Equal(Int64Value(7)) {
		t.Fatalf("kept request = %+v", first)
	}
	second := processor.requests[1]
	if second.Event != "generated" || second.EventSendID == "" || second.EventSendID != second.SendID {
		t.Fatalf("generated request IDs = %+v", second)
	}
	payload, ok := second.Data.AsMap()
	if !ok || !payload["n"].Equal(Int64Value(9)) {
		t.Fatalf("generated payload = %#v", second.Data)
	}
	location, err := chart.program.ResolveDataLocation("generated-id")
	if err != nil {
		t.Fatal(err)
	}
	stored, err := instance.session.EvaluateValue(ExecContext{}, location)
	if err != nil {
		t.Fatal(err)
	}
	storedID, _ := stored.AsString()
	if storedID == "" || Identifier(storedID) != second.EventSendID {
		t.Fatalf("stored generated ID = %q, request = %q", storedID, second.EventSendID)
	}
	if err := instance.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCompileDoneDataParamsProduceResult(t *testing.T) {
	model := NewGoModel(func() *compileTestModel { return &compileTestModel{} })
	root := compileTestState("done", KindFinal)
	root.DoneData = &DoneDataDefinition{Params: []ParamDefinition{
		{Name: "answer", Expr: expressionPointer(GoLiteral(Int64Value(42)))},
		{Name: "status", Expr: expressionPointer(GoLiteral(mustTestString(t, "ok")))},
	}}
	chart, err := Compile(compileTestDefinition(root), model)
	if err != nil {
		t.Fatal(err)
	}
	instance, err := chart.NewInstance()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := instance.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := instance.Result()
	if err != nil {
		t.Fatal(err)
	}
	values, ok := result.AsMap()
	if !ok || !values["answer"].Equal(Int64Value(42)) {
		t.Fatalf("result = %#v", result)
	}
	status, _ := values["status"].AsString()
	if status != "ok" {
		t.Fatalf("status = %q", status)
	}
}

func TestCompileStateKindsHistoryAndInitialActionOrder(t *testing.T) {
	t.Run("initial actions", func(t *testing.T) {
		model := NewGoModel(func() *compileTestModel { return &compileTestModel{} })
		mark := compileTestAction(t, model, "mark-order", func(data *compileTestModel, _ ExecContext, args []Value) error {
			value, _ := args[0].AsString()
			data.Trace = append(data.Trace, value)
			return nil
		})
		leaf := compileTestState("leaf", KindAtomic)
		leaf.OnEntry = []ExecutableBlock{{NewScriptExecutable(ScriptDefinition{Expr: mark.Expression(GoLiteral(mustTestString(t, "leaf")))})}}
		container := compileTestState("container", KindCompound, leaf)
		container.OnEntry = []ExecutableBlock{{NewScriptExecutable(ScriptDefinition{Expr: mark.Expression(GoLiteral(mustTestString(t, "container")))})}}
		container.Initial = &TransitionDefinition{
			Targets: []Identifier{"leaf"},
			Actions: []ExecutableBlock{{NewScriptExecutable(ScriptDefinition{Expr: mark.Expression(GoLiteral(mustTestString(t, "initial")))})}},
		}
		root := compileTestState("root", KindCompound, container)
		root.Initial = &TransitionDefinition{Targets: []Identifier{"container"}}
		chart, err := Compile(compileTestDefinition(root), model)
		if err != nil {
			t.Fatal(err)
		}
		instance, err := chart.NewInstance()
		if err != nil {
			t.Fatal(err)
		}
		if err := instance.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(instance.session.(*goSession[compileTestModel]).data.Trace, ","); got != "container,initial,leaf" {
			t.Fatalf("entry trace = %q", got)
		}
		_ = instance.Stop(context.Background())
	})

	t.Run("parallel", func(t *testing.T) {
		region := func(id, leaf string) StateDefinition {
			state := compileTestState(Identifier(id), KindCompound, compileTestState(Identifier(leaf), KindAtomic))
			state.Initial = &TransitionDefinition{Targets: []Identifier{Identifier(leaf)}}
			return state
		}
		definition := compileTestDefinition(compileTestState("parallel", KindParallel, region("left", "left-active"), region("right", "right-active")))
		chart, err := Compile(definition, NewGoModel(func() *compileTestModel { return &compileTestModel{} }))
		if err != nil {
			t.Fatal(err)
		}
		instance, err := chart.NewInstance()
		if err != nil {
			t.Fatal(err)
		}
		if err := instance.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := instance.Configuration(); !containsID(got, "left-active") || !containsID(got, "right-active") {
			t.Fatalf("parallel configuration = %v", got)
		}
		_ = instance.Stop(context.Background())
	})

	t.Run("shallow history", func(t *testing.T) {
		a := compileTestState("a", KindAtomic)
		a.Transitions = []TransitionDefinition{{Events: []Identifier{"next"}, Targets: []Identifier{"b"}}}
		b := compileTestState("b", KindAtomic)
		b.Transitions = []TransitionDefinition{{Events: []Identifier{"leave"}, Targets: []Identifier{"outside"}}}
		history := StateDefinition{
			ID: StateDefinitionID{Value: "recent"}, Kind: KindHistory, History: Shallow,
			Initial: &TransitionDefinition{Targets: []Identifier{"a"}},
		}
		group := compileTestState("group", KindCompound, a, b, history)
		group.Initial = &TransitionDefinition{Targets: []Identifier{"a"}}
		outside := compileTestState("outside", KindAtomic)
		outside.Transitions = []TransitionDefinition{{Events: []Identifier{"back"}, Targets: []Identifier{"recent"}}}
		root := compileTestState("root", KindCompound, group, outside)
		root.Initial = &TransitionDefinition{Targets: []Identifier{"group"}}
		chart, err := Compile(compileTestDefinition(root), NewGoModel(func() *compileTestModel { return &compileTestModel{} }))
		if err != nil {
			t.Fatal(err)
		}
		instance, err := chart.NewInstance()
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		if err := instance.Start(ctx); err != nil {
			t.Fatal(err)
		}
		for _, event := range []Identifier{"next", "leave", "back"} {
			if err := instance.Send(ctx, Event{Name: event}); err != nil {
				t.Fatal(err)
			}
		}
		if got := instance.Configuration(); !containsID(got, "b") {
			t.Fatalf("history configuration = %v, want b", got)
		}
		_ = instance.Stop(ctx)
	})
}

func TestCompileDataInitializersUseRealContextAndDefinitionOrder(t *testing.T) {
	model := NewGoModel(func() *compileTestModel { return &compileTestModel{} })
	firstRef, err := model.Value("first", "v1", func(data *compileTestModel, ec ExecContext, _ []Value) (Value, error) {
		if _, hasEvent := ec.Event(); hasEvent {
			return Value{}, errors.New("initializer unexpectedly has an event")
		}
		data.Trace = append(data.Trace, ec.SessionID())
		return Int64Value(11), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	copyRef, err := model.Value("copy", "v1", func(_ *compileTestModel, _ ExecContext, args []Value) (Value, error) {
		return args[0].Clone(), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	first := firstRef.Expression()
	second := copyRef.Expression(GoData("first"))
	definition := compileTestDefinition(compileTestState("root", KindAtomic))
	definition.Data = []DataDefinition{{ID: "first", Expr: &first}, {ID: "second", Expr: &second}}
	chart, err := Compile(definition, model)
	if err != nil {
		t.Fatal(err)
	}
	instance, err := chart.NewInstance(WithSessionID("initializer-session"))
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	session := instance.session.(*goSession[compileTestModel])
	if got := strings.Join(session.data.Trace, ","); got != "initializer-session" {
		t.Fatalf("initializer context trace = %q", got)
	}
	location, err := chart.program.ResolveDataLocation("second")
	if err != nil {
		t.Fatal(err)
	}
	value, err := session.EvaluateValue(ExecContext{}, location)
	if err != nil {
		t.Fatal(err)
	}
	if number, _ := value.AsInt64(); number != 11 {
		t.Fatalf("second initializer = %d, want 11", number)
	}
	_ = instance.Stop(context.Background())
}

func expressionPointer(expression Expression) *Expression { return &expression }

func mustTestString(t *testing.T, value string) Value {
	t.Helper()
	result, err := StringValue(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
