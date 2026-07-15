package scxml_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/datamodel/ecmascript"
	statescxml "github.com/dhamidi/statecharts/syntax/scxml"
)

type wireExpressionCodec struct{}

func (wireExpressionCodec) ParseExpression(_ statecharts.TextExpressionRole, text string) (statecharts.Expression, error) {
	var expression statecharts.Expression
	err := json.Unmarshal([]byte(text), &expression)
	return expression, err
}

func (wireExpressionCodec) FormatExpression(_ statecharts.TextExpressionRole, expression statecharts.Expression) (string, error) {
	wire, err := json.Marshal(expression)
	return string(wire), err
}

func stringValue(t testing.TB, text string) statecharts.Value {
	t.Helper()
	value, err := statecharts.StringValue(text)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func expression(t testing.TB, kind, text string) statecharts.Expression {
	t.Helper()
	return statecharts.Expression{Kind: statecharts.Identifier(kind), Data: stringValue(t, text)}
}

func expressionPointer(expression statecharts.Expression) *statecharts.Expression { return &expression }

func stateID(id statecharts.Identifier) statecharts.StateDefinitionID {
	return statecharts.StateDefinitionID{Value: id}
}

func completeDefinition(t testing.TB) statecharts.Definition {
	t.Helper()
	condition := expression(t, "test.condition", "ready")
	value := expression(t, "test.value", "payload")
	location := expression(t, "test.location", "result")
	array := expression(t, "test.array", "items")
	dynamicEvent := expression(t, "test.event", "dynamic")
	dynamicType := expression(t, "test.type", "worker")
	dynamicSource := expression(t, "test.source", "queue")
	number, _ := statecharts.NumberValue("123.45")
	payload, _ := statecharts.MapValue(map[string]statecharts.Value{
		"number": number,
		"list":   statecharts.ListValue([]statecharts.Value{stringValue(t, "item")}),
	})
	tagged, _ := statecharts.TaggedValue("example/value-v1", payload)
	actions := statecharts.ExecutableBlock{
		statecharts.NewRaiseExecutable(statecharts.RaiseDefinition{Event: "raised", Data: &value}),
		statecharts.NewRaiseExecutable(statecharts.RaiseDefinition{EventExpr: &dynamicEvent}),
		statecharts.NewSendExecutable(statecharts.SendDefinition{Event: "sent", Target: "peer@node", Type: "custom", ID: "timer", Delay: "1s", Content: &value}),
		statecharts.NewSendExecutable(statecharts.SendDefinition{EventExpr: &dynamicEvent, TargetExpr: &value, TypeExpr: &dynamicType, IDLocation: &location, DelayExpr: &value, Params: []statecharts.ParamDefinition{{Name: "job", Location: &location}}}),
		statecharts.NewCancelExecutable(statecharts.CancelDefinition{SendID: "timer"}),
		statecharts.NewCancelExecutable(statecharts.CancelDefinition{SendIDExpr: &value}),
		statecharts.NewLogExecutable(statecharts.LogDefinition{Label: "observed", Expr: &value}),
		statecharts.NewLogExecutable(statecharts.LogDefinition{LabelExpr: &value}),
		statecharts.NewAssignExecutable(statecharts.AssignDefinition{Location: location, Expr: value}),
		statecharts.NewChooseExecutable(statecharts.ChooseDefinition{
			Branches: []statecharts.ChooseBranchDefinition{
				{Condition: condition, Actions: []statecharts.ExecutableBlock{{statecharts.NewScriptExecutable(statecharts.ScriptDefinition{Expr: value})}}},
				{Condition: value, Actions: []statecharts.ExecutableBlock{{statecharts.NewLogExecutable(statecharts.LogDefinition{Label: "second branch"})}}},
			},
			Else: []statecharts.ExecutableBlock{{statecharts.NewExtensionExecutable(statecharts.ExtensionDefinition{Namespace: "urn:example", Name: "fallback", Data: stringValue(t, "extension")})}},
		}),
		statecharts.NewForEachExecutable(statecharts.ForEachDefinition{Array: array, Item: "item", Index: "index", Actions: []statecharts.ExecutableBlock{{
			statecharts.NewCallExecutable(statecharts.CallDefinition{Function: statecharts.FunctionRef{Name: "visit", Version: "v1", Args: []statecharts.Expression{value}}}),
		}}}),
		statecharts.NewScriptExecutable(statecharts.ScriptDefinition{Expr: value}),
		statecharts.NewCallExecutable(statecharts.CallDefinition{Function: statecharts.FunctionRef{Name: "record", Version: "v1", Args: []statecharts.Expression{value}}}),
		statecharts.NewExtensionExecutable(statecharts.ExtensionDefinition{Namespace: "urn:example", Name: "audit", Data: tagged}),
	}
	return statecharts.Definition{
		ID: "demo", Name: "Demo", Datamodel: "test", RevisionSalt: "test-salt", DataBinding: statecharts.DataBindingLate,
		Data: []statecharts.DataDefinition{{ID: "root-data", Expr: &value}, {ID: "remote-data", Source: "memory://remote"}, {ID: "empty-data"}},
		Root: statecharts.StateDefinition{
			ID: stateID("root"), Kind: statecharts.KindCompound,
			Initial: &statecharts.TransitionDefinition{Targets: []statecharts.Identifier{"idle"}},
			Children: []statecharts.StateDefinition{
				{
					ID: stateID("idle"), Kind: statecharts.KindAtomic,
					OnEntry: []statecharts.ExecutableBlock{actions, {statecharts.NewRaiseExecutable(statecharts.RaiseDefinition{Event: "second-block"})}},
					OnExit:  []statecharts.ExecutableBlock{{statecharts.NewLogExecutable(statecharts.LogDefinition{Label: "exit", Expr: &value})}},
					Transitions: []statecharts.TransitionDefinition{
						{Events: []statecharts.Identifier{"start.*", "resume"}, Targets: []statecharts.Identifier{"a", "b"}, Condition: &condition, Actions: []statecharts.ExecutableBlock{{statecharts.NewRaiseExecutable(statecharts.RaiseDefinition{Event: "transitioned"})}, {statecharts.NewLogExecutable(statecharts.LogDefinition{Label: "second transition block"})}}},
						{Targets: []statecharts.Identifier{"done"}, Condition: &condition, Type: statecharts.TransitionInternal},
					},
					Invokes: []statecharts.InvokeDefinition{
						{DefinitionID: "idle.service", ID: "service", Type: "worker", Src: "queue://jobs", AutoForward: true, Params: []statecharts.ParamDefinition{{Name: "job", Expr: &value}}, Finalize: []statecharts.ExecutableBlock{{statecharts.NewAssignExecutable(statecharts.AssignDefinition{Location: location, Expr: value})}}},
						{DefinitionID: "idle.dynamic-service", IDLocation: &location, TypeExpr: &dynamicType, SrcExpr: &dynamicSource, Content: &value},
					},
					Data: []statecharts.DataDefinition{{ID: "local", Content: &value}},
				},
				{ID: stateID("work"), Kind: statecharts.KindParallel, Children: []statecharts.StateDefinition{
					{ID: stateID("region-a"), Kind: statecharts.KindCompound, Initial: &statecharts.TransitionDefinition{Targets: []statecharts.Identifier{"a"}, Actions: []statecharts.ExecutableBlock{{statecharts.NewLogExecutable(statecharts.LogDefinition{Label: "initial", Expr: &value})}}}, Children: []statecharts.StateDefinition{
						{ID: stateID("a"), Kind: statecharts.KindAtomic},
						{ID: stateID("a-history"), Kind: statecharts.KindHistory, History: statecharts.Shallow, Initial: &statecharts.TransitionDefinition{Targets: []statecharts.Identifier{"a"}}},
					}},
					{ID: stateID("region-b"), Kind: statecharts.KindCompound, Initial: &statecharts.TransitionDefinition{Targets: []statecharts.Identifier{"b"}}, Children: []statecharts.StateDefinition{{ID: stateID("b"), Kind: statecharts.KindAtomic}}},
				}},
				{ID: stateID("history"), Kind: statecharts.KindHistory, History: statecharts.Deep, Initial: &statecharts.TransitionDefinition{Targets: []statecharts.Identifier{"idle"}}},
				{ID: stateID("done"), Kind: statecharts.KindFinal, DoneData: &statecharts.DoneDataDefinition{Content: &value}},
				{ID: statecharts.StateDefinitionID{Generated: true}, Kind: statecharts.KindFinal, DoneData: &statecharts.DoneDataDefinition{Params: []statecharts.ParamDefinition{{Name: "result", Location: &location}}}},
			},
		},
	}
}

func TestGoldenSimpleDefinition(t *testing.T) {
	definition := statecharts.Definition{
		ID: "simple", Datamodel: "go",
		Root: statecharts.StateDefinition{
			ID: stateID("root"), Kind: statecharts.KindCompound,
			Initial: &statecharts.TransitionDefinition{Targets: []statecharts.Identifier{"waiting"}},
			Children: []statecharts.StateDefinition{
				{ID: stateID("waiting"), Kind: statecharts.KindAtomic, Transitions: []statecharts.TransitionDefinition{{Events: []statecharts.Identifier{"finish"}, Targets: []statecharts.Identifier{"done"}}}},
				{ID: stateID("done"), Kind: statecharts.KindFinal},
			},
		},
	}
	wire, err := statescxml.MarshalIndent(definition, "  ")
	if err != nil {
		t.Fatal(err)
	}
	want := `<?xml version="1.0" encoding="UTF-8"?>
<scxml xmlns="http://www.w3.org/2005/07/scxml" xmlns:stc="https://statecharts.dev/ns/definition" version="1.0" initial="root" datamodel="go" stc:definition-id="simple">
  <state id="root" initial="waiting">
    <state id="waiting">
      <transition event="finish" target="done"></transition>
    </state>
    <final id="done"></final>
  </state>
</scxml>`
	if string(wire) != want {
		t.Fatalf("golden mismatch\n got:\n%s\nwant:\n%s", wire, want)
	}
}

func TestRoundTripPreservesCompleteDefinition(t *testing.T) {
	want := completeDefinition(t)
	before := want.Clone()
	options := []statescxml.Option{statescxml.WithTextExpressionCodec(wireExpressionCodec{})}
	wire, err := statescxml.Marshal(want, options...)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, marker := range []string{"<parallel", "<history", "<invoke", "<foreach", "<if", "<elseif", "<stc:extension", "<stc:block"} {
		if !bytes.Contains(wire, []byte(marker)) {
			t.Errorf("encoded document lacks %s", marker)
		}
	}
	got, err := statescxml.Unmarshal(wire, options...)
	if err != nil {
		t.Fatalf("Unmarshal: %v\n%s", err, wire)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip differs\n got: %#v\nwant: %#v", got, want)
	}
	wireAgain, err := statescxml.Marshal(got, options...)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wireAgain, wire) {
		t.Fatalf("encoding is not deterministic\nfirst:  %s\nsecond: %s", wire, wireAgain)
	}
	if !reflect.DeepEqual(want, before) {
		t.Fatal("Marshal mutated caller-owned definition")
	}
}

func TestECMAScriptSourceRoundTripsAndExecutes(t *testing.T) {
	model, err := ecmascript.New()
	if err != nil {
		t.Fatal(err)
	}
	count, _ := ecmascript.Source("0")
	condition, _ := ecmascript.Source("count === 0")
	location, _ := ecmascript.Source("count")
	increment, _ := ecmascript.Source("count + 1")
	result, _ := ecmascript.Source("count")
	chart, err := statecharts.Build(
		statecharts.Compound("root", "active", statecharts.Children(
			statecharts.Atomic("active", statecharts.On("finish", statecharts.If(condition), statecharts.Target("done"), statecharts.Then(
				statecharts.NewAssignExecutable(statecharts.AssignDefinition{Location: location, Expr: increment}),
			))),
			statecharts.Final("done", statecharts.WithDone(result)),
		)),
		model,
		statecharts.WithData(statecharts.DataDefinition{ID: "count", Expr: &count}),
	)
	if err != nil {
		t.Fatal(err)
	}
	options := []statescxml.Option{statescxml.WithTextExpressionCodec(model.TextExpressionCodec())}
	wire, err := statescxml.Marshal(chart.Definition(), options...)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := statescxml.Unmarshal(wire, options...)
	if err != nil {
		t.Fatal(err)
	}
	recompiled, err := statecharts.Compile(decoded, model)
	if err != nil {
		t.Fatal(err)
	}
	instance, err := recompiled.NewInstance()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := instance.Send(ctx, statecharts.Event{Name: "finish"}); err != nil {
		t.Fatal(err)
	}
	if err := instance.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	value, err := instance.Result()
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := value.AsInt64(); !ok || got != 1 {
		t.Fatalf("result = %v (integer=%v), want 1", value, ok)
	}
}

type goCounter struct{ Count int64 }

func TestGoFunctionReferencesRoundTripAndExecute(t *testing.T) {
	model := statecharts.NewGoModel(func() *goCounter { return &goCounter{} })
	increment, err := model.Action("increment", "v1", func(data *goCounter, _ statecharts.ExecContext, arguments []statecharts.Value) error {
		amount, ok := arguments[0].AsInt64()
		if !ok {
			return errors.New("amount is not an integer")
		}
		data.Count += amount
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	count, err := model.Value("count", "v1", func(data *goCounter, _ statecharts.ExecContext, _ []statecharts.Value) (statecharts.Value, error) {
		return statecharts.Int64Value(data.Count), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	chart, err := statecharts.Build(
		statecharts.Compound("root", "waiting", statecharts.Children(
			statecharts.Atomic("waiting", statecharts.On("finish", statecharts.Target("done"), statecharts.Then(
				statecharts.NewCallExecutable(statecharts.CallDefinition{Function: increment.Function(statecharts.GoLiteral(statecharts.Int64Value(2)))}),
			))),
			statecharts.Final("done", statecharts.WithDone(count.Get())),
		)),
		model,
	)
	if err != nil {
		t.Fatal(err)
	}
	options := []statescxml.Option{statescxml.WithTextExpressionCodec(model.TextExpressionCodec())}
	wire, err := statescxml.Marshal(chart.Definition(), options...)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := statescxml.Unmarshal(wire, options...)
	if err != nil {
		t.Fatal(err)
	}
	recompiled, err := statecharts.Compile(decoded, model)
	if err != nil {
		t.Fatal(err)
	}
	instance, err := recompiled.NewInstance()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := instance.Send(ctx, statecharts.Event{Name: "finish"}); err != nil {
		t.Fatal(err)
	}
	if err := instance.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := instance.Result()
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := result.AsInt64(); !ok || got != 2 {
		t.Fatalf("result = %v (integer=%v), want 2", result, ok)
	}
}

func TestGoTextCodecRejectsForeignExpressionAtExactPath(t *testing.T) {
	definition := completeDefinition(t)
	_, err := statescxml.Marshal(definition, statescxml.WithTextExpressionCodec(statecharts.GoTextExpressionCodec{}))
	var pathError *statescxml.Error
	if !errors.As(err, &pathError) || pathError.Path != "definition.data[0].expr" {
		t.Fatalf("error = %T %v, want foreign expression error at first expression", err, err)
	}
}

func TestEmptyECMAScriptSourceIsNotDropped(t *testing.T) {
	empty, err := ecmascript.Source("")
	if err != nil {
		t.Fatal(err)
	}
	definition := statecharts.Definition{
		ID: "empty-source", Datamodel: "ecmascript",
		Data: []statecharts.DataDefinition{{ID: "empty", Expr: &empty}},
		Root: statecharts.StateDefinition{ID: stateID("root"), Kind: statecharts.KindAtomic},
	}
	options := []statescxml.Option{statescxml.WithTextExpressionCodec(ecmascript.TextExpressionCodec{})}
	wire, err := statescxml.Marshal(definition, options...)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(wire, []byte(`expr=""`)) {
		t.Fatalf("empty expression attribute was dropped: %s", wire)
	}
	got, err := statescxml.Unmarshal(wire, options...)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, definition) {
		t.Fatalf("round trip differs: %#v != %#v", got, definition)
	}
}

func TestXMLIllegalCharactersFailInsteadOfChangingDefinition(t *testing.T) {
	invalid, err := ecmascript.Source("before\x00after")
	if err != nil {
		t.Fatal(err)
	}
	definition := statecharts.Definition{
		ID: "invalid-source", Datamodel: "ecmascript",
		Data: []statecharts.DataDefinition{{ID: "value", Expr: &invalid}},
		Root: statecharts.StateDefinition{ID: stateID("root"), Kind: statecharts.KindAtomic},
	}
	wire, err := statescxml.Marshal(definition, statescxml.WithTextExpressionCodec(ecmascript.TextExpressionCodec{}))
	if err == nil || wire != nil {
		t.Fatalf("Marshal = %q, %v; want no document", wire, err)
	}
	var pathError *statescxml.Error
	if !errors.As(err, &pathError) || pathError.Path != "definition.data[0].expr" {
		t.Fatalf("error = %T %v, want exact expression path", err, err)
	}

	decoded, err := statescxml.Unmarshal([]byte("<scxml xmlns=\"http://www.w3.org/2005/07/scxml\" version=\"1.0\" datamodel=\"go\"><state id=\"root\x00\"/></scxml>"))
	if err == nil || !reflect.DeepEqual(decoded, statecharts.Definition{}) {
		t.Fatalf("Unmarshal invalid XML character = %#v, %v", decoded, err)
	}
}

func TestHistoryDefaultUsesSCXMLTransitionChild(t *testing.T) {
	wire := []byte(`<scxml xmlns="http://www.w3.org/2005/07/scxml" version="1.0" initial="root" datamodel="go"><state id="root" initial="active"><state id="active"/><history id="recent" type="deep"><transition target="active"/></history></state></scxml>`)
	definition, err := statescxml.Unmarshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	history := definition.Root.Children[1]
	if history.Kind != statecharts.KindHistory || history.Initial == nil || !reflect.DeepEqual(history.Initial.Targets, []statecharts.Identifier{"active"}) || len(history.Transitions) != 0 {
		t.Fatalf("history state = %#v", history)
	}
	encoded, err := statescxml.Marshal(definition)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte(`<history id="recent" type="deep" initial=`)) || !bytes.Contains(encoded, []byte(`<history id="recent" type="deep"><transition target="active"></transition></history>`)) {
		t.Fatalf("history default is not an SCXML transition child: %s", encoded)
	}
}

func TestDecodeConventionalMultipleTopLevelSCXMLStates(t *testing.T) {
	wire := []byte(`<scxml xmlns="http://www.w3.org/2005/07/scxml" version="1.0" initial="waiting" datamodel="go"><state id="waiting"><transition event="finish" target="done"/></state><final id="done"/></scxml>`)
	definition, err := statescxml.Unmarshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	if definition.Root.Kind != statecharts.KindCompound || !definition.Root.ID.Generated || len(definition.Root.Children) != 2 || definition.Root.Children[0].ID.Value != "waiting" || definition.Root.Children[1].ID.Value != "done" {
		t.Fatalf("synthetic root = %#v", definition.Root)
	}
	model := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	chart, err := statecharts.Compile(definition, model)
	if err != nil {
		t.Fatal(err)
	}
	instance, err := chart.NewInstance()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := instance.Send(ctx, statecharts.Event{Name: "finish"}); err != nil {
		t.Fatal(err)
	}
	if err := instance.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeMarksOmittedSCXMLStateIDAsGenerated(t *testing.T) {
	definition, err := statescxml.Unmarshal([]byte(`<scxml xmlns="http://www.w3.org/2005/07/scxml" version="1.0" datamodel="go"><state/></scxml>`))
	if err != nil {
		t.Fatal(err)
	}
	if definition.ID != "scxml.document" || !definition.Root.ID.Generated || definition.Root.ID.Value != "" {
		t.Fatalf("definition with omitted state ID = %#v", definition)
	}
}

func TestExpressionWithoutCodecFailsWithPath(t *testing.T) {
	definition := completeDefinition(t)
	_, err := statescxml.Marshal(definition)
	if err == nil {
		t.Fatal("Marshal succeeded without expression codec")
	}
	var pathError *statescxml.Error
	if !errors.As(err, &pathError) || pathError.Path != "definition.data[0].expr" {
		t.Fatalf("error = %T %v, want expression path", err, err)
	}
}

func TestUnknownExecutableExtensionIsRejectedWithoutPartialDefinition(t *testing.T) {
	wire := []byte(`<scxml xmlns="http://www.w3.org/2005/07/scxml" xmlns:vendor="urn:vendor" version="1.0" initial="root" datamodel="go"><state id="root"><onentry><vendor:action/></onentry></state></scxml>`)
	definition, err := statescxml.Unmarshal(wire)
	if err == nil {
		t.Fatal("Unmarshal succeeded")
	}
	if !reflect.DeepEqual(definition, statecharts.Definition{}) {
		t.Fatalf("failed decode returned partial definition: %#v", definition)
	}
	var pathError *statescxml.Error
	if !errors.As(err, &pathError) || pathError.Line != 1 || !strings.Contains(pathError.Path, "onEntry") {
		t.Fatalf("error = %#v, want line and onEntry path", err)
	}
}

func TestMalformedInputReturnsLineAndPathWithoutPartialDefinition(t *testing.T) {
	wire := []byte("<scxml xmlns=\"http://www.w3.org/2005/07/scxml\" version=\"1.0\">\n  <state id=\"root\">\n</scxml>")
	definition, err := statescxml.Unmarshal(wire)
	if err == nil {
		t.Fatal("Unmarshal succeeded")
	}
	if !reflect.DeepEqual(definition, statecharts.Definition{}) {
		t.Fatalf("failed decode returned partial definition: %#v", definition)
	}
	var pathError *statescxml.Error
	if !errors.As(err, &pathError) || pathError.Line == 0 || pathError.Path == "" {
		t.Fatalf("error = %#v, want line and path", err)
	}
}

func TestStrictDecodeRejectsUnknownDuplicateAndTrailingInput(t *testing.T) {
	tests := []struct {
		name string
		wire string
	}{
		{"unknown attribute", `<scxml xmlns="http://www.w3.org/2005/07/scxml" version="1.0" datamodel="go" mystery="x"><state id="root"/></scxml>`},
		{"unknown child", `<scxml xmlns="http://www.w3.org/2005/07/scxml" version="1.0" datamodel="go"><state id="root"><mystery/></state></scxml>`},
		{"duplicate attribute", `<scxml xmlns="http://www.w3.org/2005/07/scxml" version="1.0" version="1.0" datamodel="go"><state id="root"/></scxml>`},
		{"trailing root", `<scxml xmlns="http://www.w3.org/2005/07/scxml" version="1.0" datamodel="go"><state id="root"/></scxml><scxml/>`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition, err := statescxml.Unmarshal([]byte(test.wire))
			if err == nil {
				t.Fatal("Unmarshal succeeded")
			}
			if !reflect.DeepEqual(definition, statecharts.Definition{}) {
				t.Fatalf("failed decode returned partial definition: %#v", definition)
			}
			var pathError *statescxml.Error
			if !errors.As(err, &pathError) || pathError.Path == "" {
				t.Fatalf("error = %T %v, want path error", err, err)
			}
		})
	}
}

func FuzzUnmarshal(f *testing.F) {
	valid, err := statescxml.Marshal(completeDefinition(f), statescxml.WithTextExpressionCodec(wireExpressionCodec{}))
	if err == nil {
		f.Add(valid)
	}
	for _, seed := range []string{"", "<scxml>", "<state/>", "<scxml></state>", "\x00"} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		definition, err := statescxml.Unmarshal(data, statescxml.WithTextExpressionCodec(wireExpressionCodec{}))
		if err != nil {
			if !reflect.DeepEqual(definition, statecharts.Definition{}) {
				t.Fatal("failed decode returned partial definition")
			}
			return
		}
		wire, err := statescxml.Marshal(definition, statescxml.WithTextExpressionCodec(wireExpressionCodec{}))
		if err != nil {
			t.Fatal(err)
		}
		second, err := statescxml.Unmarshal(wire, statescxml.WithTextExpressionCodec(wireExpressionCodec{}))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(second, definition) {
			t.Fatal("decode/encode/decode changed definition")
		}
	})
}
