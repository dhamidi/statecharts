package ecmascript_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/datamodel/ecmascript"
	statejson "github.com/dhamidi/statecharts/syntax/json"
)

const testProcessorType statecharts.Identifier = "example.processor"

type describedProcessor struct{}

func (*describedProcessor) Attach(statecharts.Dispatcher) {}
func (*describedProcessor) Send(context.Context, statecharts.SendRequest) error {
	return nil
}
func (*describedProcessor) IOProcessors() []statecharts.IOProcessorInfo {
	location, _ := statecharts.NewLocation("processor://host-a/session-7")
	return []statecharts.IOProcessorInfo{{Type: testProcessorType, Location: location}}
}

func source(t *testing.T, text string) statecharts.Expression {
	t.Helper()
	expression, err := ecmascript.Source(text)
	if err != nil {
		t.Fatalf("Source(%q): %v", text, err)
	}
	return expression
}

func model(t *testing.T, options ...ecmascript.Option) *ecmascript.Model {
	t.Helper()
	result, err := ecmascript.New(options...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return result
}

func runToResult(t *testing.T, chart *statecharts.Chart, events ...statecharts.Event) statecharts.Value {
	t.Helper()
	instance, err := chart.NewInstance()
	if err != nil {
		t.Fatalf("NewInstance: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := instance.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, event := range events {
		if err := instance.Send(ctx, event); err != nil {
			t.Fatalf("Send(%q): %v", event.Name, err)
		}
	}
	if err := instance.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	result, err := instance.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	return result
}

func TestExpressionsAssignmentsScriptsBranchesAndIteration(t *testing.T) {
	assign := func(location, expression string) statecharts.Executable {
		return statecharts.NewAssignExecutable(statecharts.AssignDefinition{
			Location: source(t, location),
			Expr:     source(t, expression),
		})
	}
	chart, err := statecharts.Build(
		statecharts.Compound("root", "active", statecharts.Children(
			statecharts.Atomic("active", statecharts.On("run",
				statecharts.If(source(t, "count === 0")),
				statecharts.Target("done"),
				statecharts.Then(
					assign("count", "count + 1"),
					statecharts.NewScriptExecutable(statecharts.ScriptDefinition{Expr: source(t, "count += 1;")}),
					statecharts.NewForEachExecutable(statecharts.ForEachDefinition{
						Array: source(t, "[2, 3]"), Item: "item", Index: "index",
						Actions: []statecharts.ExecutableBlock{{assign("count", "count + item + index")}},
					}),
					statecharts.NewChooseExecutable(statecharts.ChooseDefinition{
						Branches: []statecharts.ChooseBranchDefinition{{
							Condition: source(t, "count === 8"),
							Actions:   []statecharts.ExecutableBlock{{assign("count", "count + 1")}},
						}},
					}),
				),
			)),
			statecharts.Final("done", statecharts.WithDone(source(t, "count"))),
		)),
		model(t),
		statecharts.WithData(
			statecharts.DataDefinition{ID: "count", Expr: ptr(source(t, "0"))},
			statecharts.DataDefinition{ID: "item"},
			statecharts.DataDefinition{ID: "index"},
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	result := runToResult(t, chart, statecharts.Event{Name: "run"})
	if got, ok := result.AsInt64(); !ok || got != 9 {
		t.Fatalf("result = %v (integer=%v), want 9", result, ok)
	}
}

func TestEvaluationExceptionBecomesOrderedPlatformEvent(t *testing.T) {
	chart, err := statecharts.Build(
		statecharts.Compound("root", "active", statecharts.Children(
			statecharts.Atomic("active",
				statecharts.OnEntry(
					statecharts.NewScriptExecutable(statecharts.ScriptDefinition{Expr: source(t, `throw new Error("boom")`)}),
					statecharts.NewAssignExecutable(statecharts.AssignDefinition{Location: source(t, "untouched"), Expr: source(t, "false")}),
				),
				statecharts.On("error.execution", statecharts.Target("done")),
			),
			statecharts.Final("done", statecharts.WithDone(source(t, `({name: _event.name, untouched})`))),
		)),
		model(t),
		statecharts.WithData(statecharts.DataDefinition{ID: "untouched", Expr: ptr(source(t, "true"))}),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	result := runToResult(t, chart)
	fields, ok := result.AsMap()
	if !ok {
		t.Fatalf("result kind = %s, want map", result.Kind())
	}
	name, _ := fields["name"].AsString()
	untouched, _ := fields["untouched"].AsBool()
	if name != "error.execution" || !untouched {
		t.Fatalf("result = %#v, want ordered error.execution before second action", fields)
	}
}

func TestSystemBindingsAreCurrentAndProtected(t *testing.T) {
	chart, err := statecharts.Build(
		statecharts.Compound("root", "active", statecharts.Children(
			statecharts.Atomic("active", statecharts.On("inspect", statecharts.Target("done"))),
			statecharts.Final("done", statecharts.WithDone(source(t, `({
				before,
				event: _event.name,
				type: _event.type,
				session: _sessionid,
				name: _name,
				active: In("done"),
				platform: _x.region,
				processor: _ioprocessors.find(item => item.type === "example.processor").location
			})`))),
		)),
		model(t),
		statecharts.WithName("bindings"),
		statecharts.WithData(statecharts.DataDefinition{ID: "before", Expr: ptr(source(t, "typeof _event"))}),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	instance, err := chart.NewInstance(
		statecharts.WithSessionID("session-7"),
		statecharts.WithPlatformVariables(map[string]any{"region": "west"}),
		statecharts.WithIOProcessor(testProcessorType, &describedProcessor{}),
	)
	if err != nil {
		t.Fatalf("NewInstance: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := instance.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := instance.Send(ctx, statecharts.Event{Name: "inspect", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := instance.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	result, err := instance.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	fields, ok := result.AsMap()
	if !ok {
		t.Fatalf("result kind = %s, want map", result.Kind())
	}
	wantStrings := map[string]string{
		"before": "undefined", "event": "inspect", "type": "external",
		"session": "session-7", "name": "bindings", "platform": "west",
		"processor": "processor://host-a/session-7",
	}
	for key, want := range wantStrings {
		if got, _ := fields[key].AsString(); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
	if got, _ := fields["active"].AsBool(); !got {
		t.Error("In(\"done\") = false, want true")
	}

	protected, err := statecharts.Build(
		statecharts.Compound("root", "active", statecharts.Children(
			statecharts.Atomic("active",
				statecharts.OnEntry(statecharts.NewScriptExecutable(statecharts.ScriptDefinition{Expr: source(t, `_name = "changed"`)})),
				statecharts.On("error.execution", statecharts.Target("done")),
			),
			statecharts.Final("done", statecharts.WithDone(source(t, "_name"))),
		)),
		model(t), statecharts.WithName("protected"),
	)
	if err != nil {
		t.Fatalf("Build protected chart: %v", err)
	}
	if got, _ := runToResult(t, protected).AsString(); got != "protected" {
		t.Fatalf("protected _name = %q, want protected", got)
	}
}

func TestGeneratedIDLocationAndSelfSendUseModelLocationsAndValues(t *testing.T) {
	chart, err := statecharts.Build(
		statecharts.Compound("root", "active", statecharts.Children(
			statecharts.Atomic("active",
				statecharts.On("start", statecharts.Then(
					statecharts.Send("forward",
						statecharts.SendIDLocation(source(t, "generated")),
						statecharts.SendContent(source(t, "_event.data")),
					),
				)),
				statecharts.On("forward", statecharts.Target("done")),
			),
			statecharts.Final("done", statecharts.WithDone(source(t, `({generated, data: _event.data})`))),
		)),
		model(t),
		statecharts.WithData(statecharts.DataDefinition{ID: "generated"}),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	payload := mustString(t, "payload")
	result := runToResult(t, chart, statecharts.Event{Name: "start", Data: payload})
	fields, ok := result.AsMap()
	if !ok {
		t.Fatalf("result kind = %s, want map", result.Kind())
	}
	generated, _ := fields["generated"].AsString()
	if generated == "" {
		t.Fatal("generated send ID location remained empty")
	}
	if !fields["data"].Equal(payload) {
		t.Fatalf("forwarded data = %#v, want %#v", fields["data"], payload)
	}
}

func TestFunctionReferenceKeepsCommaExpressionAsOneArgument(t *testing.T) {
	chart, err := statecharts.Build(
		statecharts.Compound("root", "active", statecharts.Children(
			statecharts.Atomic("active", statecharts.On("run", statecharts.Target("done"), statecharts.Then(
				statecharts.NewScriptExecutable(statecharts.ScriptDefinition{Expr: source(t, `globalThis.capture = (...args) => { count = args.length }`)}),
				statecharts.NewCallExecutable(statecharts.CallDefinition{Function: statecharts.FunctionRef{
					Name: "capture", Version: "v1", Args: []statecharts.Expression{source(t, "1, 2")},
				}}),
			))),
			statecharts.Final("done", statecharts.WithDone(source(t, "count"))),
		)),
		model(t),
		statecharts.WithData(statecharts.DataDefinition{ID: "count", Expr: ptr(source(t, "0"))}),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	result := runToResult(t, chart, statecharts.Event{Name: "run"})
	if got, ok := result.AsInt64(); !ok || got != 1 {
		t.Fatalf("captured argument count = %v (integer=%v), want 1", result, ok)
	}
}

func TestCanonicalValuesCrossTheVMWithoutEscapingVMValues(t *testing.T) {
	largeInteger, err := statecharts.NumberValue("9007199254740993")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := statecharts.MapValue(map[string]statecharts.Value{
		"message": mustString(t, "hello"),
		"flags":   statecharts.ListValue([]statecharts.Value{statecharts.BoolValue(true), statecharts.Int64Value(42)}),
		"big":     largeInteger,
	})
	if err != nil {
		t.Fatal(err)
	}
	tagged, err := statecharts.TaggedValue("example/message-v1", payload)
	if err != nil {
		t.Fatal(err)
	}
	chart, err := statecharts.Build(
		statecharts.Compound("root", "active", statecharts.Children(
			statecharts.Atomic("active", statecharts.On("finish", statecharts.Target("done"))),
			statecharts.Final("done", statecharts.WithDone(source(t, "_event.data"))),
		)),
		model(t),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	result := runToResult(t, chart, statecharts.Event{Name: "finish", Data: tagged})
	if !result.Equal(tagged) {
		t.Fatalf("result = %#v, want tagged payload %#v", result, tagged)
	}
}

func TestSnapshotRestoreRecreatesVMAndDeclaredData(t *testing.T) {
	chart, err := statecharts.Build(
		statecharts.Compound("root", "active", statecharts.Children(
			statecharts.Atomic("active",
				statecharts.On("increment", statecharts.Then(statecharts.NewScriptExecutable(statecharts.ScriptDefinition{Expr: source(t, "count++")}))),
				statecharts.On("store", statecharts.Then(statecharts.NewAssignExecutable(statecharts.AssignDefinition{
					Location: source(t, "text"), Expr: source(t, "_event.data"),
				}))),
				statecharts.On("finish", statecharts.Target("done")),
			),
			statecharts.Final("done", statecharts.WithDone(source(t, "({count, text})"))),
		)),
		model(t),
		statecharts.WithData(
			statecharts.DataDefinition{ID: "count", Expr: ptr(source(t, "1"))},
			statecharts.DataDefinition{ID: "text"},
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	instance, err := chart.NewInstance()
	if err != nil {
		t.Fatalf("NewInstance: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := instance.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := instance.Send(ctx, statecharts.Event{Name: "increment"}); err != nil {
		t.Fatalf("Send increment: %v", err)
	}
	exotic := mustString(t, "astral:\U000E0001")
	if err := instance.Send(ctx, statecharts.Event{Name: "store", Data: exotic}); err != nil {
		t.Fatalf("Send store: %v", err)
	}
	snapshot, err := instance.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := instance.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	restored, err := chart.Restore(snapshot)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if err := restored.Start(ctx); err != nil {
		t.Fatalf("Start restored: %v", err)
	}
	if err := restored.Send(ctx, statecharts.Event{Name: "finish"}); err != nil {
		t.Fatalf("Send finish: %v", err)
	}
	if err := restored.Wait(ctx); err != nil {
		t.Fatalf("Wait restored: %v", err)
	}
	result, err := restored.Result()
	if err != nil {
		t.Fatalf("Result restored: %v", err)
	}
	fields, ok := result.AsMap()
	if !ok {
		t.Fatalf("restored result kind = %s, want map", result.Kind())
	}
	if got, ok := fields["count"].AsInt64(); !ok || got != 2 {
		t.Fatalf("restored count = %v (integer=%v), want 2", fields["count"], ok)
	}
	if !fields["text"].Equal(exotic) {
		t.Fatalf("restored text = %#v, want %#v", fields["text"], exotic)
	}

	corrupt := snapshot
	corrupt.Datamodel = []byte(`{"version":1,"values":[]}`)
	if _, err := chart.Restore(corrupt); !errors.Is(err, statecharts.ErrInvalidSnapshot) {
		t.Fatalf("Restore corrupt error = %v, want ErrInvalidSnapshot", err)
	}
}

func TestFailedEvaluationDrainsQueuedJobsBeforeTheNextTurn(t *testing.T) {
	chart, err := statecharts.Build(
		statecharts.Compound("root", "active", statecharts.Children(
			statecharts.Atomic("active",
				statecharts.On("run", statecharts.Then(
					statecharts.NewScriptExecutable(statecharts.ScriptDefinition{Expr: source(t, `
						Promise.resolve().then(() => { count++ });
						throw new Error("after queueing");
					`)}),
				)),
				statecharts.On("error.execution", statecharts.Target("done")),
			),
			statecharts.Final("done", statecharts.WithDone(source(t, "count"))),
		)),
		model(t),
		statecharts.WithData(statecharts.DataDefinition{ID: "count", Expr: ptr(source(t, "0"))}),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	result := runToResult(t, chart, statecharts.Event{Name: "run"})
	if got, ok := result.AsInt64(); !ok || got != 1 {
		t.Fatalf("count after failed turn = %v (integer=%v), want 1", result, ok)
	}
}

func TestCompileRejectsDataIDsThatCollideWithEngineGlobals(t *testing.T) {
	_, err := statecharts.Build(
		statecharts.Atomic("root"),
		model(t),
		statecharts.WithData(statecharts.DataDefinition{ID: "Math"}),
	)
	if err == nil || !strings.Contains(err.Error(), "conflicts with an ECMAScript global") {
		t.Fatalf("Build error = %v, want global collision", err)
	}
}

func TestUnsupportedDeclaredDataMakesSnapshotUnavailableWithoutStoppingInstance(t *testing.T) {
	chart, err := statecharts.Build(
		statecharts.Compound("root", "active", statecharts.Children(
			statecharts.Atomic("active",
				statecharts.On("make-function", statecharts.Then(
					statecharts.NewScriptExecutable(statecharts.ScriptDefinition{Expr: source(t, "value = () => 42")}),
				)),
				statecharts.On("finish", statecharts.Target("done")),
			),
			statecharts.Final("done", statecharts.WithDone(source(t, `"still-running"`))),
		)),
		model(t),
		statecharts.WithData(statecharts.DataDefinition{ID: "value"}),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	instance, err := chart.NewInstance()
	if err != nil {
		t.Fatalf("NewInstance: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := instance.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := instance.Send(ctx, statecharts.Event{Name: "make-function"}); err != nil {
		t.Fatalf("Send make-function: %v", err)
	}
	_, err = instance.Snapshot(ctx)
	var incompatible *ecmascript.SnapshotIncompatibleError
	if !errors.As(err, &incompatible) {
		t.Fatalf("Snapshot error = %v, want SnapshotIncompatibleError", err)
	}
	if err := instance.Send(ctx, statecharts.Event{Name: "finish"}); err != nil {
		t.Fatalf("Send finish after snapshot failure: %v", err)
	}
	if err := instance.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	result, err := instance.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if got, _ := result.AsString(); got != "still-running" {
		t.Fatalf("result = %q, want still-running", got)
	}
}

func TestUndeclaredGlobalMutationMakesSnapshotUnavailable(t *testing.T) {
	chart, err := statecharts.Build(
		statecharts.Compound("root", "active", statecharts.Children(
			statecharts.Atomic("active", statecharts.On("mutate", statecharts.Then(
				statecharts.NewScriptExecutable(statecharts.ScriptDefinition{Expr: source(t, "globalThis.runtimeFunction = () => 42")}),
			))),
		)),
		model(t),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	instance, err := chart.NewInstance()
	if err != nil {
		t.Fatalf("NewInstance: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := instance.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := instance.Send(ctx, statecharts.Event{Name: "mutate"}); err != nil {
		t.Fatalf("Send mutate: %v", err)
	}
	_, err = instance.Snapshot(ctx)
	var incompatible *ecmascript.SnapshotIncompatibleError
	if !errors.As(err, &incompatible) || !strings.Contains(err.Error(), "undeclared VM globals") {
		t.Fatalf("Snapshot error = %v, want undeclared-global SnapshotIncompatibleError", err)
	}
	if err := instance.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestSynchronousPromiseJobsDrainWithinTheSameTurn(t *testing.T) {
	chart, err := statecharts.Build(
		statecharts.Compound("root", "active", statecharts.Children(
			statecharts.Atomic("active", statecharts.On("run", statecharts.Target("done"), statecharts.Then(
				statecharts.NewScriptExecutable(statecharts.ScriptDefinition{Expr: source(t, `Promise.resolve().then(() => { count = 41 }).then(() => { count++ })`)}),
			))),
			statecharts.Final("done", statecharts.WithDone(source(t, "count"))),
		)),
		model(t),
		statecharts.WithData(statecharts.DataDefinition{ID: "count", Expr: ptr(source(t, "0"))}),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if got, ok := runToResult(t, chart, statecharts.Event{Name: "run"}).AsInt64(); !ok || got != 42 {
		t.Fatalf("result = %d (integer=%v), want 42", got, ok)
	}
}

func TestEvaluationTimeoutInterruptsRunawayCode(t *testing.T) {
	chart, err := statecharts.Build(
		statecharts.Compound("root", "active", statecharts.Children(
			statecharts.Atomic("active",
				statecharts.OnEntry(statecharts.NewScriptExecutable(statecharts.ScriptDefinition{Expr: source(t, "for (;;) {}")})),
				statecharts.On("error.execution", statecharts.Target("done")),
			),
			statecharts.Final("done", statecharts.WithDone(source(t, "_event.name"))),
		)),
		model(t, ecmascript.WithEvaluationTimeout(20*time.Millisecond)),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	started := time.Now()
	result := runToResult(t, chart)
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("runaway evaluation took %s", elapsed)
	}
	if got, _ := result.AsString(); got != "error.execution" {
		t.Fatalf("result = %q, want error.execution", got)
	}
}

func TestSessionsIsolateGlobalsWhileSharingAProgram(t *testing.T) {
	chart, err := statecharts.Build(
		statecharts.Compound("root", "active", statecharts.Children(
			statecharts.Atomic("active",
				statecharts.On("increment", statecharts.Then(statecharts.NewScriptExecutable(statecharts.ScriptDefinition{Expr: source(t, "count++")}))),
				statecharts.On("finish", statecharts.Target("done")),
			),
			statecharts.Final("done", statecharts.WithDone(source(t, "count"))),
		)),
		model(t),
		statecharts.WithData(statecharts.DataDefinition{ID: "count", Expr: ptr(source(t, "0"))}),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	results := make(chan int64, 2)
	errs := make(chan error, 2)
	for writes := 1; writes <= 2; writes++ {
		writes := writes
		go func() {
			instance, err := chart.NewInstance()
			if err != nil {
				errs <- err
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := instance.Start(ctx); err != nil {
				errs <- err
				return
			}
			for range writes {
				if err := instance.Send(ctx, statecharts.Event{Name: "increment"}); err != nil {
					errs <- err
					return
				}
			}
			if err := instance.Send(ctx, statecharts.Event{Name: "finish"}); err != nil {
				errs <- err
				return
			}
			if err := instance.Wait(ctx); err != nil {
				errs <- err
				return
			}
			value, err := instance.Result()
			if err != nil {
				errs <- err
				return
			}
			result, ok := value.AsInt64()
			if !ok {
				errs <- errors.New("result was not an integer")
				return
			}
			results <- result
		}()
	}
	got := make([]int64, 0, 2)
	for len(got) < 2 {
		select {
		case result := <-results:
			got = append(got, result)
		case err := <-errs:
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(got, []int64{1, 2}) && !reflect.DeepEqual(got, []int64{2, 1}) {
		t.Fatalf("isolated results = %v, want [1 2] in either order", got)
	}
}

func TestTextExpressionAndDefinitionRoundTripPreserveRevision(t *testing.T) {
	codec := ecmascript.TextExpressionCodec{}
	expression, err := codec.ParseExpression(ecmascript.SourceExpression, "value + 1\n")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	formatted, err := codec.FormatExpression(ecmascript.SourceExpression, expression)
	if err != nil {
		t.Fatalf("FormatExpression: %v", err)
	}
	if formatted != "value + 1\n" {
		t.Fatalf("formatted source = %q", formatted)
	}

	chart, err := statecharts.Build(
		statecharts.Compound("root", "done", statecharts.Children(
			statecharts.Final("done", statecharts.WithDone(source(t, "value"))),
		)),
		model(t),
		statecharts.WithData(statecharts.DataDefinition{ID: "value", Expr: ptr(source(t, "41"))}),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	wire, err := statejson.MarshalIndent(chart.Definition(), "", "  ")
	if err != nil {
		t.Fatalf("MarshalIndent: %v", err)
	}
	decoded, err := statejson.Unmarshal(wire)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	recompiled, err := statecharts.Compile(decoded, model(t))
	if err != nil {
		t.Fatalf("Compile decoded: %v", err)
	}
	if chart.Revision() != recompiled.Revision() {
		t.Fatalf("revision changed across source round trip: %s != %s", chart.Revision(), recompiled.Revision())
	}
	if strings.Contains(string(chart.DefinitionArtifact().ProgramFingerprint), "bytecode") {
		t.Fatalf("program fingerprint contains engine cache material: %q", chart.DefinitionArtifact().ProgramFingerprint)
	}
}

func ptr[T any](value T) *T { return &value }

func mustString(t *testing.T, value string) statecharts.Value {
	t.Helper()
	result, err := statecharts.StringValue(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
