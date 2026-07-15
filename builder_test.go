package statecharts

import (
	"testing"
	"time"
)

// testBuilder keeps runtime-semantic tests concise while exercising the real
// serializable GoModel registration and Definition compiler path.
type testBuilder[D any] struct {
	t     testing.TB
	model *GoModel[D]
}

func newTestBuilder[D any](t testing.TB, factory func() *D) *testBuilder[D] {
	t.Helper()
	return &testBuilder[D]{t: t, model: NewGoModel(factory)}
}

func (b *testBuilder[D]) action(name Identifier, fn func(*D, ExecContext) error) Executable {
	b.t.Helper()
	ref, err := b.model.Action(name, "v1", func(data *D, context ExecContext, _ []Value) error {
		return fn(data, context)
	})
	if err != nil {
		b.t.Fatalf("register action %q: %v", name, err)
	}
	return ref.Do()
}

func (b *testBuilder[D]) effect(name Identifier, fn func(ExecContext) error) Executable {
	b.t.Helper()
	return b.action(name, func(_ *D, context ExecContext) error { return fn(context) })
}

func (b *testBuilder[D]) condition(name Identifier, fn func(*D, ExecContext) bool) Expression {
	b.t.Helper()
	ref, err := b.model.Condition(name, "v1", func(data *D, context ExecContext, _ []Value) (bool, error) {
		return fn(data, context), nil
	})
	if err != nil {
		b.t.Fatalf("register condition %q: %v", name, err)
	}
	return ref.If()
}

func (b *testBuilder[D]) build(root StateDefinition, options ...BuildOption) (*Chart, error) {
	b.t.Helper()
	return Build(root, b.model, options...)
}

func (b *testBuilder[D]) newInstance(chart *Chart, options ...Option) *Instance {
	b.t.Helper()
	instance, err := chart.NewInstance(options...)
	if err != nil {
		b.t.Fatalf("NewInstance: %v", err)
	}
	return instance
}

func waitActive(t testing.TB, instance *Instance, state Identifier) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !hasState(instance.Configuration(), state) {
		if time.Now().After(deadline) {
			t.Fatalf("configuration = %v, want %q", instance.Configuration(), state)
		}
		time.Sleep(time.Millisecond)
	}
}
