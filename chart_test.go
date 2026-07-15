package statecharts

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

type Door struct {
	OpenCount int
	Locked    bool
}

func testStringValue(s string) Value {
	value, err := StringValue(s)
	if err != nil {
		panic(err)
	}
	return value
}

func TestBuildGoChartDefinitionIsolationAndRoundTrip(t *testing.T) {
	model := NewGoModel(func() *Door { return &Door{} })
	notLocked, err := model.Condition("door-not-locked", "v1", func(d *Door, _ ExecContext, _ []Value) (bool, error) {
		return !d.Locked, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	recordOpen, err := model.Action("record-door-open", "v1", func(d *Door, _ ExecContext, _ []Value) error {
		d.OpenCount++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	root := Compound("door", "closed", Children(
		Atomic("closed", On("open.request", Target("open"), If(notLocked.If()), Then(recordOpen.Do()))),
		Atomic("open", On("close.request", Target("closed"))),
	))
	chart, err := Build(root, model, WithName("Door workflow"), WithRevisionSalt("door-v1"))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if got, want := chart.States(), []Identifier{"door", "closed", "open"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("States = %v, want %v", got, want)
	}
	definition := chart.Definition()
	definition.Root.Children[0].ID.Value = "mutated"
	if got := chart.Definition().Root.Children[0].ID.Value; got != "closed" {
		t.Fatalf("Definition was not deep copied: %q", got)
	}
	wire, err := json.Marshal(chart.Definition())
	if err != nil {
		t.Fatal(err)
	}
	var decoded Definition
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	recompiled, err := Compile(decoded, model)
	if err != nil {
		t.Fatalf("Compile round trip: %v", err)
	}
	if !reflect.DeepEqual(recompiled.Definition(), chart.Definition()) {
		t.Fatal("definition changed across JSON round trip")
	}
}

func TestBuildStateKindsAndDefaultInitials(t *testing.T) {
	model := NewGoModel(func() *struct{} { return &struct{}{} })
	chart, err := Build(Parallel("machine", Children(
		Compound("motor", "", Children(Atomic("off"), Atomic("on"))),
		Compound("light", "dark", Children(Atomic("dark"), Atomic("lit"), History("light.history", Shallow, "dark"))),
	)), model)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if chart.root.kind != KindParallel || chart.byID["light.history"].historyKind != Shallow {
		t.Fatalf("compiled kinds were not preserved")
	}
	instance, err := chart.NewInstance()
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	configuration := instance.Configuration()
	if !containsID(configuration, "off") || !containsID(configuration, "dark") {
		t.Fatalf("default configuration = %v", configuration)
	}
	if err := instance.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func containsID(ids []Identifier, wanted Identifier) bool {
	for _, id := range ids {
		if id == wanted {
			return true
		}
	}
	return false
}

func TestBuildRejectsInvalidDefinitions(t *testing.T) {
	model := NewGoModel(func() *struct{} { return &struct{}{} })
	for name, root := range map[string]StateDefinition{
		"duplicate IDs":  Compound("root", "a", Children(Atomic("a"), Atomic("a"))),
		"missing target": Compound("root", "a", Children(Atomic("a", On("go", Target("missing"))))),
		"empty compound": Compound("root", "a"),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Build(root, model); err == nil {
				t.Fatal("Build succeeded")
			}
		})
	}
}

func TestBuildRejectsNilDatamodel(t *testing.T) {
	if _, err := Build(Atomic("root"), nil); err == nil || !strings.Contains(err.Error(), "nil datamodel") {
		t.Fatalf("Build nil datamodel error = %v", err)
	}
}

// This is deliberately a single, dense authoring example: besides making the
// expected document order obvious, it catches builder options which silently
// fail to survive normalization.
func TestBuildCanonicalBuilderSurface(t *testing.T) {
	model := NewGoModel(func() *Door { return &Door{} })
	guard, err := model.Condition("unlocked", "v1", func(d *Door, _ ExecContext, _ []Value) (bool, error) { return !d.Locked, nil })
	if err != nil {
		t.Fatal(err)
	}
	action, err := model.Action("count", "v2", func(d *Door, _ ExecContext, _ []Value) error { d.OpenCount++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	one, two := GoLiteral(Int64Value(1)), GoLiteral(Int64Value(2))
	location := GoData("seed")
	param := ParamDefinition{Name: "p", Expr: &one}
	data := []DataDefinition{{ID: "seed", Expr: &two}}
	root := Compound("root", "", WithInitial(Target("work")),
		Children(
			Atomic("work",
				OnEntry(Raise("entered", one), LogValue("entry", two)), OnExit(CancelSend("timer")),
				On("go next", Target("regions"), If(guard.If()), Then(action.Do()), AsInternal()),
				Eventless(Target("done"), If(guard.If())),
				Invoke("worker", "static", WithInvokeDefinitionID("job-def"), WithInvokeID("job"), WithInvokeParams(param), WithFinalize(action.Do()), WithAutoForward()),
				Invoke("", "", WithInvokeDefinitionID("dynamic-def"), WithInvokeIDLocation(location), WithInvokeTypeExpression(one), WithInvokeSourceExpression(two), WithInvokeContent(two)),
				OnEntry(Send("tick", SendTarget("#_internal"), SendType("scxml"), SendID("timer"), SendDelay(25*time.Millisecond), SendParams(param)),
					Send("payload", SendIDLocation(location), SendContent(one)))),
			Parallel("regions", Children(
				Compound("left", "idle", Children(Atomic("idle"), Final("left-done", WithDone(one)))),
				Compound("right", "", WithInitial(Target("ready"), Then(action.Do())), Children(Atomic("ready"), History("remember", Deep, "ready"))),
			)),
			Final("done", WithDoneParams(param)),
		),
	)
	chart, err := Build(root, model, WithName("Builder chart"), WithRevisionSalt("rev-7"), WithDataBinding(DataBindingLate), WithData(data...))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	d := chart.Definition()
	if got, want := []any{d.ID, d.Name, d.Datamodel, d.RevisionSalt, d.DataBinding}, []any{Identifier("root"), "Builder chart", Identifier("go"), "rev-7", DataBindingLate}; !reflect.DeepEqual(got, want) {
		t.Fatalf("header = %#v, want %#v", got, want)
	}
	if got, want := chart.States(), []Identifier{"root", "work", "regions", "left", "idle", "left-done", "right", "ready", "remember", "done"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state order = %v, want %v", got, want)
	}
	if d.Root.Kind != KindCompound || !reflect.DeepEqual(d.Root.Initial.Targets, []Identifier{"work"}) || d.Root.Initial.Type != TransitionExternal || len(d.Root.Initial.Actions) != 0 {
		t.Fatalf("root initial = %#v", d.Root.Initial)
	}
	work := d.Root.Children[0]
	if work.Kind != KindAtomic || len(work.Transitions) != 2 || !reflect.DeepEqual(work.Transitions[0].Events, []Identifier{"go", "next"}) || work.Transitions[0].Type != TransitionInternal || work.Transitions[0].Condition == nil || len(work.Transitions[0].Actions) != 1 {
		t.Fatalf("work transitions = %#v", work.Transitions)
	}
	if len(work.Invokes) != 2 || work.Invokes[0].DefinitionID != "job-def" || work.Invokes[0].ID != "job" || len(work.Invokes[0].Params) != 1 || !work.Invokes[0].AutoForward || len(work.Invokes[0].Finalize) != 1 || work.Invokes[1].IDLocation == nil || work.Invokes[1].TypeExpr == nil || work.Invokes[1].SrcExpr == nil || work.Invokes[1].Content == nil {
		t.Fatalf("invokes = %#v", work.Invokes)
	}
	if len(work.OnEntry) != 2 || len(work.OnEntry[0]) != 2 || work.OnEntry[0][0].Kind != ExecutableRaise || work.OnEntry[0][1].Kind != ExecutableLog || work.OnEntry[1][0].Kind != ExecutableSend || work.OnEntry[1][1].Send.IDLocation == nil || len(work.OnExit) != 1 || work.OnExit[0][0].Kind != ExecutableCancel {
		t.Fatalf("work executable blocks = %#v / %#v", work.OnEntry, work.OnExit)
	}
	send := work.OnEntry[1][0].Send
	if send.Target != "#_internal" || send.Type != "scxml" || send.ID != "timer" || send.Delay != "25ms" || len(send.Params) != 1 {
		t.Fatalf("send = %#v", send)
	}
	regions := d.Root.Children[1]
	if regions.Kind != KindParallel || regions.Children[0].Initial.Targets[0] != "idle" || regions.Children[1].Initial.Targets[0] != "ready" || len(regions.Children[1].Initial.Actions) != 1 || regions.Children[1].Children[1].History != Deep || regions.Children[1].Children[1].Initial.Targets[0] != "ready" {
		t.Fatalf("normalized regions = %#v", regions)
	}
	if d.Root.Children[2].DoneData == nil || len(d.Root.Children[2].DoneData.Params) != 1 || regions.Children[0].Children[1].DoneData.Content == nil || len(d.Data) != 1 {
		t.Fatal("done-data or document data missing")
	}
}

func TestBuildOwnsInputsAndDefinitionsDeeply(t *testing.T) {
	model := NewGoModel(func() *struct{} { return &struct{}{} })
	nested := ListValue([]Value{Int64Value(1)})
	expr := GoLiteral(nested)
	targets := []Identifier{"done"}
	params := []ParamDefinition{{Name: "result", Expr: &expr}}
	root := Compound("root", "start", Children(Atomic("start", On("go", Target(targets...), Then(Raise("raised", expr)))), Final("done", WithDoneParams(params...))))
	chart, err := Build(root, model, WithData(DataDefinition{ID: "input", Expr: &expr}))
	if err != nil {
		t.Fatal(err)
	}
	want := chart.Definition()
	targets[0] = "start"
	params[0].Name = "changed"
	expr.Data = Int64Value(99)
	root.Children[0].Transitions[0].Targets[0] = "start"
	if got := chart.Definition(); !reflect.DeepEqual(got, want) {
		t.Fatal("caller mutation changed chart definition")
	}
	returned := chart.Definition()
	returned.Root.Children[0].Transitions[0].Targets[0] = "start"
	returned.Root.Children[0].Transitions[0].Actions[0][0].Raise.Data.Data = Int64Value(88)
	returned.Root.Children[1].DoneData.Params[0].Expr.Data = Int64Value(77)
	returned.Data[0].Expr.Data = Int64Value(66)
	if got := chart.Definition(); !reflect.DeepEqual(got, want) {
		t.Fatal("nested mutation of Definition changed chart")
	}
}

func TestBuildGeneratesStableUniqueInvokeDefinitionIDs(t *testing.T) {
	model := NewGoModel(func() *struct{} { return &struct{}{} })
	chart, err := Build(Atomic("root",
		Invoke("worker", "first"),
		Invoke("worker", "second"),
	), model)
	if err != nil {
		t.Fatal(err)
	}
	definition := chart.Definition()
	invokes := definition.Root.Invokes
	if len(invokes) != 2 || invokes[0].DefinitionID == "" || invokes[1].DefinitionID == "" || invokes[0].DefinitionID == invokes[1].DefinitionID {
		t.Fatalf("generated invoke definition IDs = %#v", invokes)
	}
	definition.Root.Invokes[1].DefinitionID = invokes[0].DefinitionID
	var definitionErr *DefinitionError
	if err := definition.Validate(); !errors.As(err, &definitionErr) || !strings.Contains(definitionErr.Path, "definitionId") {
		t.Fatalf("duplicate invoke definition ID error = %T %v", err, err)
	}
}

func TestBuildJSONCompileRuntimeParity(t *testing.T) {
	type flow struct{ Trace []string }
	model := NewGoModel(func() *flow { return &flow{} })
	guard, _ := model.Condition("yes", "v1", func(*flow, ExecContext, []Value) (bool, error) { return true, nil })
	mark, _ := model.Action("mark", "v1", func(d *flow, _ ExecContext, _ []Value) error { d.Trace = append(d.Trace, "mark"); return nil })
	root := Compound("flow", "waiting", Children(Atomic("waiting", On("go", Target("done"), If(guard.If()), Then(mark.Do()))), Final("done", WithDone(GoLiteral(Int64Value(42))))))
	built, err := Build(root, model)
	if err != nil {
		t.Fatal(err)
	}
	wire, _ := json.Marshal(built.Definition())
	var decoded Definition
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	compiled, err := Compile(decoded, model)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(built.Definition(), compiled.Definition()) {
		t.Fatal("canonical definitions differ")
	}
	type outcome struct {
		configuration []Identifier
		trace         []string
		result        Value
	}
	var outcomes []outcome
	for _, chart := range []*Chart{built, compiled} {
		in, err := chart.NewInstance()
		if err != nil {
			t.Fatal(err)
		}
		if err := in.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := in.Send(t.Context(), Event{Name: "go"}); err != nil {
			t.Fatal(err)
		}
		if err := in.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		trace := in.session.(*goSession[flow]).data.Trace
		if !reflect.DeepEqual(trace, []string{"mark"}) {
			t.Fatalf("trace = %v", trace)
		}
		result, err := in.Result()
		if err != nil || !result.Equal(Int64Value(42)) {
			t.Fatalf("result = %#v, %v", result, err)
		}
		outcomes = append(outcomes, outcome{in.Configuration(), append([]string(nil), trace...), result})
	}
	if !reflect.DeepEqual(outcomes[0], outcomes[1]) {
		t.Fatalf("runtime outcomes differ: %#v", outcomes)
	}
}

func TestBuildRejectsStructuralEdgeCases(t *testing.T) {
	model := NewGoModel(func() *struct{} { return &struct{}{} })
	badID := Atomic("bad id")
	badEvent := Atomic("a", On("bad event!", Target("a")))
	badKind := Atomic("a")
	badKind.Kind = StateKind(99)
	badHistory := History("h", HistoryKind(99), "a")
	cases := map[string]StateDefinition{
		"initial outside subtree": Compound("r", "outside", Children(Compound("box", "outside", Children(Atomic("in"))), Atomic("outside"))),
		"atomic children":         Atomic("r", Children(Atomic("a"))), "final children": Final("r", Children(Atomic("a"))),
		"unresolved history default":  Compound("r", "a", Children(Atomic("a"), History("h", Shallow, "missing"))),
		"cyclic history default":      Compound("r", "a", Children(Atomic("a"), History("h1", Deep, "h2"), History("h2", Deep, "h1"))),
		"shallow non-child":           Compound("r", "box", Children(Compound("box", "leaf", Children(Atomic("leaf"))), History("h", Shallow, "leaf"))),
		"history outside parent":      Compound("r", "box", Children(Compound("box", "leaf", Children(Atomic("leaf"), History("h", Deep, "outside"))), Atomic("outside"))),
		"sibling multi-target":        Compound("r", "a", Children(Atomic("a", On("go", Target("a", "b"))), Atomic("b"))),
		"ancestor descendant targets": Compound("r", "box", Children(Compound("box", "leaf", Children(Atomic("leaf", On("go", Target("box", "leaf"))))))),
		"parallel final child":        Parallel("r", Children(Final("done"))),
		"final transition":            Final("r", On("go", Target("r"))), "final invoke": Final("r", Invoke("x", "y")),
		"empty eventless": Compound("r", "a", Children(Atomic("a", Eventless()))),
		"invalid state":   badID, "invalid event": Compound("r", "a", Children(badEvent)), "invalid kind": badKind,
		"invalid history kind": Compound("r", "a", Children(Atomic("a"), badHistory)),
		"root onexit":          Compound("r", "a", OnExit(Raise("x")), Children(Atomic("a"))), "root invoke": Compound("r", "a", Invoke("x", "y"), Children(Atomic("a"))),
		"root initial executable": Compound("r", "a", WithInitial(Then(Raise("x"))), Children(Atomic("a"))),
		"invoke id and location":  Compound("r", "a", Children(Atomic("a", Invoke("x", "y", WithInvokeID("i"), WithInvokeIDLocation(GoLiteral(Int64Value(1))))))),
		"duplicate targets":       Compound("r", "a", Children(Atomic("a", On("go", Target("a", "a"))))),
	}
	for name, root := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Build(root, model)
			if err == nil {
				t.Fatal("Build succeeded")
			}
			var de *DefinitionError
			if !errors.As(err, &de) {
				t.Fatalf("Build error = %T %v, want *DefinitionError", err, err)
			}
		})
	}
}
