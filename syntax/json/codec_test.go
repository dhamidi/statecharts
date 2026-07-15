package json_test

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/dhamidi/statecharts"
	statejson "github.com/dhamidi/statecharts/syntax/json"
)

func stringValue(t testing.TB, value string) statecharts.Value {
	t.Helper()
	result, err := statecharts.StringValue(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func expression(t testing.TB, kind, value string) statecharts.Expression {
	t.Helper()
	return statecharts.Expression{Kind: statecharts.Identifier(kind), Data: stringValue(t, value)}
}

func allValueKinds(t testing.TB) statecharts.Value {
	t.Helper()
	number, err := statecharts.NumberValue("123.4500")
	if err != nil {
		t.Fatal(err)
	}
	object, err := statecharts.MapValue(map[string]statecharts.Value{
		"null":   statecharts.NullValue(),
		"bool":   statecharts.BoolValue(true),
		"number": number,
		"list":   statecharts.ListValue([]statecharts.Value{stringValue(t, "item")}),
	})
	if err != nil {
		t.Fatal(err)
	}
	tagged, err := statecharts.TaggedValue("example/value-v1", object)
	if err != nil {
		t.Fatal(err)
	}
	return tagged
}

func stateID(id statecharts.Identifier) statecharts.StateDefinitionID {
	return statecharts.StateDefinitionID{Value: id}
}

func completeDefinition(t testing.TB) statecharts.Definition {
	t.Helper()
	condition := expression(t, "go.condition", "ready")
	value := expression(t, "go.value", "payload")
	location := expression(t, "go.location", "result")
	array := expression(t, "go.array", "items")
	dynamicEvent := expression(t, "go.event", "dynamic")
	dynamicType := expression(t, "go.type", "worker")
	dynamicSource := expression(t, "go.source", "queue")
	actions := statecharts.ExecutableBlock{
		statecharts.NewRaiseExecutable(statecharts.RaiseDefinition{Event: "raised", Data: &value}),
		statecharts.NewSendExecutable(statecharts.SendDefinition{Event: "sent", Target: "peer@node", Type: "custom", ID: "timer", Delay: "1s", Content: &value}),
		statecharts.NewSendExecutable(statecharts.SendDefinition{EventExpr: &dynamicEvent, TargetExpr: &value, TypeExpr: &dynamicType, IDLocation: &location, DelayExpr: &value, Params: []statecharts.ParamDefinition{{Name: "job", Location: &location}}}),
		statecharts.NewCancelExecutable(statecharts.CancelDefinition{SendID: "timer"}),
		statecharts.NewCancelExecutable(statecharts.CancelDefinition{SendIDExpr: &value}),
		statecharts.NewLogExecutable(statecharts.LogDefinition{Label: "observed", Expr: &value}),
		statecharts.NewLogExecutable(statecharts.LogDefinition{LabelExpr: &value}),
		statecharts.NewAssignExecutable(statecharts.AssignDefinition{Location: location, Expr: value}),
		statecharts.NewChooseExecutable(statecharts.ChooseDefinition{
			Branches: []statecharts.ChooseBranchDefinition{{Condition: condition, Actions: []statecharts.ExecutableBlock{{statecharts.NewScriptExecutable(statecharts.ScriptDefinition{Expr: value})}}}},
			Else:     []statecharts.ExecutableBlock{{statecharts.NewExtensionExecutable(statecharts.ExtensionDefinition{Namespace: "urn:example", Name: "fallback", Data: stringValue(t, "extension")})}},
		}),
		statecharts.NewForEachExecutable(statecharts.ForEachDefinition{Array: array, Item: "item", Index: "index", Actions: []statecharts.ExecutableBlock{{
			statecharts.NewCallExecutable(statecharts.CallDefinition{Function: statecharts.FunctionRef{Name: "visit", Version: "v1", Args: []statecharts.Expression{value}}}),
		}}}),
		statecharts.NewScriptExecutable(statecharts.ScriptDefinition{Expr: value}),
		statecharts.NewCallExecutable(statecharts.CallDefinition{Function: statecharts.FunctionRef{Name: "record", Version: "v1", Args: []statecharts.Expression{value}}}),
		statecharts.NewExtensionExecutable(statecharts.ExtensionDefinition{Namespace: "urn:example", Name: "audit", Data: allValueKinds(t)}),
	}
	return statecharts.Definition{
		ID: "demo", Name: "Demo", Datamodel: "go", RevisionSalt: "test-salt", DataBinding: statecharts.DataBindingLate,
		Data: []statecharts.DataDefinition{{ID: "root-data", Expr: &value}, {ID: "remote-data", Source: "memory://remote"}, {ID: "empty-data"}},
		Root: statecharts.StateDefinition{
			ID: stateID("root"), Kind: statecharts.KindCompound,
			Initial: &statecharts.TransitionDefinition{Targets: []statecharts.Identifier{"idle"}},
			Children: []statecharts.StateDefinition{
				{
					ID: stateID("idle"), Kind: statecharts.KindAtomic,
					OnEntry: []statecharts.ExecutableBlock{actions, {statecharts.NewRaiseExecutable(statecharts.RaiseDefinition{EventExpr: &dynamicEvent})}},
					OnExit:  []statecharts.ExecutableBlock{{statecharts.NewLogExecutable(statecharts.LogDefinition{Label: "exit", Expr: &value})}},
					Transitions: []statecharts.TransitionDefinition{
						{Events: []statecharts.Identifier{"start.*"}, Targets: []statecharts.Identifier{"work"}, Condition: &condition, Actions: []statecharts.ExecutableBlock{{statecharts.NewRaiseExecutable(statecharts.RaiseDefinition{Event: "transitioned"})}}},
						{Targets: []statecharts.Identifier{"done"}, Condition: &condition, Type: statecharts.TransitionInternal},
					},
					Invokes: []statecharts.InvokeDefinition{
						{DefinitionID: "idle.service", ID: "service", Type: "worker", Src: "queue://jobs", AutoForward: true, Params: []statecharts.ParamDefinition{{Name: "job", Expr: &value}}, Finalize: []statecharts.ExecutableBlock{{statecharts.NewAssignExecutable(statecharts.AssignDefinition{Location: location, Expr: value})}}},
						{DefinitionID: "idle.dynamic-service", IDLocation: &location, TypeExpr: &dynamicType, SrcExpr: &dynamicSource, Content: &value},
					},
					Data: []statecharts.DataDefinition{{ID: "local", Content: &value}},
				},
				{ID: stateID("work"), Kind: statecharts.KindParallel, Children: []statecharts.StateDefinition{
					{ID: stateID("region-a"), Kind: statecharts.KindCompound, Initial: &statecharts.TransitionDefinition{Targets: []statecharts.Identifier{"a"}}, Children: []statecharts.StateDefinition{
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

func TestDefinitionJSONRoundTripsEveryNodeDeterministically(t *testing.T) {
	want := completeDefinition(t)
	before := want.Clone()
	compact, err := statejson.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	again, err := statejson.Marshal(want)
	if err != nil || !bytes.Equal(compact, again) {
		t.Fatalf("second Marshal differs: %v\n%s\n%s", err, compact, again)
	}
	if !bytes.Contains(compact, []byte(`"kind":"compound"`)) || bytes.Contains(compact, []byte(`"kind":1`)) {
		t.Fatalf("state kinds are not human-readable strings: %s", compact)
	}
	got, err := statejson.Unmarshal(compact)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip differs\n got: %#v\nwant: %#v", got, want)
	}
	indented, err := statejson.MarshalIndent(want, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(indented, []byte("\n  \"id\"")) {
		t.Fatalf("MarshalIndent did not indent output:\n%s", indented)
	}
	if !reflect.DeepEqual(want, before) {
		t.Fatal("marshaling mutated the caller-owned definition")
	}
	fromIndented, err := statejson.Unmarshal(indented)
	if err != nil || !reflect.DeepEqual(fromIndented, want) {
		t.Fatalf("indented round trip = %#v, %v", fromIndented, err)
	}
}

func TestDefinitionJSONRejectsUnknownFieldsAndMalformedUnionsWithPaths(t *testing.T) {
	valid, err := statejson.Marshal(completeDefinition(t))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		data []byte
		path string
	}{
		{name: "nested unknown field", data: bytes.Replace(valid, []byte(`"kind":"compound"`), []byte(`"kind":"compound","bogus":true`), 1), path: "definition.root.bogus"},
		{name: "unknown executable kind", data: bytes.Replace(valid, []byte(`"kind":"raise"`), []byte(`"kind":"future"`), 1), path: "definition.root.children[0].onEntry[0][0].kind"},
		{name: "mismatched executable payload", data: bytes.Replace(valid, []byte(`"kind":"raise","raise":`), []byte(`"kind":"send","raise":`), 1), path: "definition.root.children[0].onEntry[0][0]"},
		{name: "unknown value field", data: bytes.Replace(valid, []byte(`"version":1,"kind":"string"`), []byte(`"version":1,"kind":"string","bogus":true`), 1), path: "definition.data[0].expr.data.bogus"},
		{name: "trailing value", data: append(append([]byte(nil), valid...), []byte(` {}`)...), path: "definition"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := statejson.Unmarshal(test.data)
			if err == nil {
				t.Fatal("Unmarshal succeeded")
			}
			if !reflect.DeepEqual(got, statecharts.Definition{}) {
				t.Fatalf("failed decode returned partial definition: %#v", got)
			}
			var pathErr *statejson.Error
			if !errors.As(err, &pathErr) {
				t.Fatalf("error type = %T, want *json.Error: %v", err, err)
			}
			if pathErr.Path != test.path {
				t.Fatalf("error path = %q, want %q: %v", pathErr.Path, test.path, err)
			}
		})
	}
}

func TestDefinitionJSONRejectsNullsAndDuplicateFieldsWithPaths(t *testing.T) {
	valid, err := statejson.Marshal(completeDefinition(t))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		data []byte
		path string
	}{
		{name: "null scalar", data: []byte(`{"id":"demo","name":null,"datamodel":"go","root":{"id":{"value":"root"},"kind":"atomic"}}`), path: "definition.name"},
		{name: "null list", data: []byte(`{"id":"demo","datamodel":"go","data":null,"root":{"id":{"value":"root"},"kind":"atomic"}}`), path: "definition.data"},
		{name: "duplicate root field", data: bytes.Replace(valid, []byte(`"name":"Demo"`), []byte(`"name":"Demo","name":"Other"`), 1), path: "definition.name"},
		{name: "duplicate nested field", data: bytes.Replace(valid, []byte(`"kind":"compound"`), []byte(`"kind":"compound","kind":"compound"`), 1), path: "definition.root.kind"},
		{name: "duplicate value field", data: bytes.Replace(valid, []byte(`"kind":"string"`), []byte(`"kind":"string","kind":"string"`), 1), path: "definition.data[0].expr.data.kind"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := statejson.Unmarshal(test.data)
			if err == nil {
				t.Fatal("Unmarshal succeeded")
			}
			if !reflect.DeepEqual(got, statecharts.Definition{}) {
				t.Fatalf("failed decode returned partial definition: %#v", got)
			}
			var pathErr *statejson.Error
			if !errors.As(err, &pathErr) {
				t.Fatalf("error type = %T, want *json.Error: %v", err, err)
			}
			if pathErr.Path != test.path {
				t.Fatalf("error path = %q, want %q: %v", pathErr.Path, test.path, err)
			}
		})
	}
}

func TestDefinitionJSONRejectsWrongScalarTypesWithPaths(t *testing.T) {
	valid, err := statejson.Marshal(completeDefinition(t))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		data []byte
		path string
	}{
		{name: "definition string", data: bytes.Replace(valid, []byte(`"name":"Demo"`), []byte(`"name":false`), 1), path: "definition.name"},
		{name: "state id", data: bytes.Replace(valid, []byte(`"id":{"value":"root"}`), []byte(`"id":42`), 1), path: "definition.root.id"},
		{name: "transition event", data: bytes.Replace(valid, []byte(`"events":["start.*"]`), []byte(`"events":[42]`), 1), path: "definition.root.children[0].transitions[0].events[0]"},
		{name: "invoke boolean", data: bytes.Replace(valid, []byte(`"autoForward":true`), []byte(`"autoForward":"yes"`), 1), path: "definition.root.children[0].invokes[0].autoForward"},
		{name: "executable static field", data: bytes.Replace(valid, []byte(`"event":"raised"`), []byte(`"event":42`), 1), path: "definition.root.children[0].onEntry[0][0].raise.event"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := statejson.Unmarshal(test.data)
			if err == nil {
				t.Fatal("Unmarshal succeeded")
			}
			if !reflect.DeepEqual(got, statecharts.Definition{}) {
				t.Fatalf("failed decode returned partial definition: %#v", got)
			}
			var pathErr *statejson.Error
			if !errors.As(err, &pathErr) {
				t.Fatalf("error type = %T, want *json.Error: %v", err, err)
			}
			if pathErr.Path != test.path {
				t.Fatalf("error path = %q, want %q: %v", pathErr.Path, test.path, err)
			}
		})
	}
}

func TestDefinitionJSONCompileParityForGoBuiltDefinition(t *testing.T) {
	model := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	chart, err := statecharts.Build(
		statecharts.Compound("flow", "waiting", statecharts.Children(
			statecharts.Atomic("waiting", statecharts.On("finish", statecharts.Target("done"))),
			statecharts.Final("done"),
		)),
		model,
		statecharts.WithRevisionSalt("json-parity-v1"),
	)
	if err != nil {
		t.Fatal(err)
	}
	data, err := statejson.Marshal(chart.Definition())
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := statejson.Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	recompiled, err := statecharts.Compile(decoded, model)
	if err != nil {
		t.Fatal(err)
	}
	if recompiled.Revision() != chart.Revision() {
		t.Fatalf("revision after JSON round trip = %q, want %q", recompiled.Revision(), chart.Revision())
	}
}

func FuzzDefinitionJSONUnmarshal(f *testing.F) {
	valid, err := statejson.Marshal(completeDefinition(f))
	if err == nil {
		f.Add(valid)
	}
	for _, seed := range []string{``, `{}`, `null`, `{"root":`, `[]`, `{"unknown":true}`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		definition, err := statejson.Unmarshal(data)
		if err != nil {
			if !reflect.DeepEqual(definition, statecharts.Definition{}) {
				t.Fatal("failed decode returned partial definition")
			}
			return
		}
		encoded, err := statejson.Marshal(definition)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := statejson.Unmarshal(encoded); err != nil {
			t.Fatal(err)
		}
	})
}

func TestDefinitionJSONErrorsAreUseful(t *testing.T) {
	_, err := statejson.Unmarshal([]byte(`{"id":"demo"}`))
	if err == nil || !strings.Contains(err.Error(), "definition") {
		t.Fatalf("error = %v, want definition context", err)
	}
}
