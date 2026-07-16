package statecharts

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

type authoredCounter struct {
	Count   int64
	Enabled bool
}

func TestNewProvidesConciseGoAuthoringWithStableQualifiedReferences(t *testing.T) {
	counter := New("counter", func() *authoredCounter {
		return &authoredCounter{Enabled: true}
	}, Version("v3"))

	enabled := counter.Condition("enabled", func(data *authoredCounter, _ ExecContext, _ []Value) (bool, error) {
		return data.Enabled, nil
	})
	increment := counter.Action("increment", func(data *authoredCounter, _ ExecContext, _ []Value) error {
		data.Count++
		return nil
	})
	count := counter.Value("count", func(data *authoredCounter, _ ExecContext, _ []Value) (Value, error) {
		return Int64Value(data.Count), nil
	})

	chart, err := counter.Build(
		Compound("root", "active", Children(
			Atomic("active", On("increment", If(enabled.If()), Then(increment.Do()), Target("done"))),
			Final("done", WithDone(count.Get())),
		)),
		WithName("Counter"),
	)
	if err != nil {
		t.Fatal(err)
	}
	definition := chart.Definition()
	if definition.ID != "counter" || definition.Name != "Counter" || definition.Datamodel != "go" || definition.RevisionSalt != "v3" {
		t.Fatalf("definition header = %#v", definition)
	}
	transition := definition.Root.Children[0].Transitions[0]
	condition, err := decodeGoRef(transition.Condition.Data)
	if err != nil {
		t.Fatal(err)
	}
	action, err := decodeGoRef(transition.Actions[0][0].Script.Expr.Data)
	if err != nil {
		t.Fatal(err)
	}
	result, err := decodeGoRef(definition.Root.Children[1].DoneData.Content.Data)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := []FunctionRef{condition, action, result}, []FunctionRef{
		{Name: "counter.enabled", Version: "v3", Args: []Expression{}},
		{Name: "counter.increment", Version: "v3", Args: []Expression{}},
		{Name: "counter.count", Version: "v3", Args: []Expression{}},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("qualified references = %#v, want %#v", got, want)
	}

	instance, err := chart.NewInstance()
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := instance.Send(t.Context(), Event{Name: "increment", Type: EventExternal}); err != nil {
		t.Fatal(err)
	}
	if err := instance.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err := instance.Result()
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := value.AsInt64(); !ok || got != 1 {
		t.Fatalf("result = %v, integer = %v", value, ok)
	}
}

func TestBuilderAccumulatesRegistrationErrorsUntilBuild(t *testing.T) {
	builder := New("broken", func() *struct{} { return &struct{}{} })
	builder.Action("missing", nil)
	builder.Condition("also-missing", nil)
	builder.Value("still-missing", nil)
	builder.Location("location", nil, nil)

	chart, err := builder.Build(Atomic("active"))
	if chart != nil || err == nil {
		t.Fatalf("Build = %#v, %v; want accumulated error", chart, err)
	}
	for _, fragment := range []string{"action \"missing\"", "condition \"also-missing\"", "value \"still-missing\"", "location \"location\""} {
		if !strings.Contains(err.Error(), fragment) {
			t.Errorf("error %q lacks %q", err, fragment)
		}
	}
}

func TestBuilderSupportsRetainedExplicitBehaviorVersionsAndCallSugar(t *testing.T) {
	builder := New("counter", func() *authoredCounter { return &authoredCounter{} }, Version("v1"))
	v1 := builder.Action("increment", func(*authoredCounter, ExecContext, []Value) error { return nil })
	v2 := builder.ActionVersion("increment", "v2", func(*authoredCounter, ExecContext, []Value) error { return nil })
	argument := GoLiteral(Int64Value(2))

	chart, err := builder.Build(Atomic("active", OnEntry(v1.Call(argument))))
	if err != nil {
		t.Fatal(err)
	}
	call := chart.Definition().Root.OnEntry[0][0]
	if call.Kind != ExecutableCall || call.Call == nil {
		t.Fatalf("executable = %#v, want call", call)
	}
	if got, want := call.Call.Function, (FunctionRef{Name: "counter.increment", Version: "v1", Args: []Expression{argument}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("call = %#v, want %#v", got, want)
	}
	if got := v2.Function(); got.Name != "counter.increment" || got.Version != "v2" {
		t.Fatalf("retained reference = %#v", got)
	}
}

func TestBuilderDefaultsBehaviorAndRevisionVersionToV1(t *testing.T) {
	builder := New("default-version", func() *struct{} { return &struct{}{} })
	action := builder.Action("run", func(*struct{}, ExecContext, []Value) error { return nil })

	chart, err := builder.Build(Atomic("active", OnEntry(action.Do())))
	if err != nil {
		t.Fatal(err)
	}
	if got := chart.Definition().RevisionSalt; got != "v1" {
		t.Fatalf("revision salt = %q, want v1", got)
	}
	if got := action.Function(); got.Name != "default-version.run" || got.Version != "v1" {
		t.Fatalf("default reference = %#v", got)
	}
}

func TestBuildersOwnIndependentBehaviorRegistries(t *testing.T) {
	var leftCalls, rightCalls int
	left := New("same-chart", func() *struct{} { return &struct{}{} })
	right := New("same-chart", func() *struct{} { return &struct{}{} })
	leftRun := left.Action("run", func(*struct{}, ExecContext, []Value) error {
		leftCalls++
		return nil
	})
	rightRun := right.Action("run", func(*struct{}, ExecContext, []Value) error {
		rightCalls++
		return nil
	})
	leftChart, err := left.Build(Atomic("active", OnEntry(leftRun.Do())))
	if err != nil {
		t.Fatal(err)
	}
	rightChart, err := right.Build(Atomic("active", OnEntry(rightRun.Do())))
	if err != nil {
		t.Fatal(err)
	}

	for _, chart := range []*Chart{leftChart, rightChart} {
		instance, err := chart.NewInstance()
		if err != nil {
			t.Fatal(err)
		}
		if err := instance.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := instance.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if leftCalls != 1 || rightCalls != 1 {
		t.Fatalf("registry calls = left %d, right %d; want 1 each", leftCalls, rightCalls)
	}
}

func TestBuilderRejectsInvalidIdentityEvenWithoutRegistrations(t *testing.T) {
	for _, test := range []struct {
		name    string
		builder *Builder[struct{}]
		want    string
	}{
		{"chart ID", New("not valid", func() *struct{} { return &struct{}{} }), "chart ID"},
		{"version", New("valid", func() *struct{} { return &struct{}{} }, Version("")), "version"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.builder.Build(Atomic("active"))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Build error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestBuilderDoesNotHideCompileErrors(t *testing.T) {
	builder := New("invalid-chart", func() *struct{} { return &struct{}{} })
	_, err := builder.Build(Compound("root", "missing", Children(Atomic("active"))))
	var definitionError *DefinitionError
	if !errors.As(err, &definitionError) {
		t.Fatalf("Build error = %T %v, want DefinitionError", err, err)
	}
}
