package statecharts

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type recordingInvokeHandler struct {
	requests chan InvokeRequest
	resume   bool
}

type invokeLifecycleBinding struct {
	request   InvokeRequest
	io        InvokeIO
	cancelled <-chan struct{}
}

type invokeLifecycleHandler struct {
	bindings chan<- invokeLifecycleBinding
}

func (h *invokeLifecycleHandler) Start(ctx context.Context, request InvokeRequest, io InvokeIO) (Value, error) {
	cancelled := make(chan struct{})
	h.bindings <- invokeLifecycleBinding{request: request, io: io, cancelled: cancelled}
	<-ctx.Done()
	close(cancelled)
	return Value{}, ctx.Err()
}

func (h *recordingInvokeHandler) Start(ctx context.Context, request InvokeRequest, _ InvokeIO) (Value, error) {
	h.requests <- request
	<-ctx.Done()
	return Value{}, ctx.Err()
}

func (h *recordingInvokeHandler) Resume(ctx context.Context, request InvokeRequest, _ InvokeIO) (Value, error) {
	h.resume = true
	h.requests <- request
	<-ctx.Done()
	return Value{}, ctx.Err()
}

func invokeTestDefinition(invokes ...InvokeDefinition) Definition {
	active := compileTestState("active", KindAtomic)
	active.Invokes = invokes
	root := compileTestState("root", KindCompound, active)
	root.Initial = &TransitionDefinition{Targets: []Identifier{"active"}}
	return compileTestDefinition(root)
}

func TestInvokeDefinitionRoundTripsEveryDeclarativeField(t *testing.T) {
	model := NewGoModel(func() *compileTestModel { return &compileTestModel{} })
	finalize, err := model.Action("finalize-round-trip", "v1", func(*compileTestModel, ExecContext, []Value) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	idLocation := GoData("invoke-id")
	typeExpression := GoLiteral(mustTestString(t, "worker"))
	sourceExpression := GoLiteral(mustTestString(t, "queue:dynamic"))
	param := GoLiteral(Int64Value(4))
	content := GoLiteral(Int64Value(5))
	definition := invokeTestDefinition(
		InvokeDefinition{
			DefinitionID: "all-fields", IDLocation: &idLocation,
			TypeExpr: &typeExpression, SrcExpr: &sourceExpression,
			Params: []ParamDefinition{{Name: "n", Expr: &param}}, AutoForward: true,
			Finalize: []ExecutableBlock{{NewScriptExecutable(ScriptDefinition{Expr: finalize.Expression()})}},
		},
		InvokeDefinition{DefinitionID: "whole-content", ID: "content", Type: "content-worker", Src: "queue:static", Content: &content},
	)
	definition.Data = []DataDefinition{{ID: "invoke-id"}}
	chart, err := Compile(definition, model)
	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}
	invokes := recompiled.Definition().Root.Children[0].Invokes
	if len(invokes) != 2 || invokes[0].IDLocation == nil || invokes[0].TypeExpr == nil || invokes[0].SrcExpr == nil ||
		len(invokes[0].Params) != 1 || len(invokes[0].Finalize) != 1 || !invokes[0].AutoForward || invokes[1].Content == nil {
		t.Fatalf("round-tripped invokes = %+v", invokes)
	}
}

func TestCompileDeclarativeInvokeRoundTripsAndRunsThroughHandler(t *testing.T) {
	model := NewGoModel(func() *compileTestModel { return &compileTestModel{} })
	definition := invokeTestDefinition(InvokeDefinition{
		DefinitionID: "active.worker",
		ID:           "worker-1",
		Type:         "worker",
		Src:          "queue:primary",
		Params: []ParamDefinition{{
			Name: "count", Expr: expressionPointer(GoLiteral(Int64Value(3))),
		}},
		AutoForward: true,
	})
	chart, err := Compile(definition, model)
	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}
	if got := recompiled.Definition().Root.Children[0].Invokes[0].DefinitionID; got != "active.worker" {
		t.Fatalf("definition ID = %q", got)
	}

	requests := make(chan InvokeRequest, 1)
	instance, err := recompiled.NewInstance(WithInvokeHandler("worker", func() InvokeHandler {
		return &recordingInvokeHandler{requests: requests}
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case request := <-requests:
		if request.DefinitionID != "active.worker" || request.ID != "worker-1" || request.Type != "worker" || request.Source != "queue:primary" {
			t.Fatalf("request = %+v", request)
		}
		params, ok := request.Data.AsMap()
		if !ok || !params["count"].Equal(Int64Value(3)) {
			t.Fatalf("request data = %#v", request.Data)
		}
	case <-time.After(time.Second):
		t.Fatal("handler was not started")
	}
	_ = instance.Stop(ctx)
}

func TestStaticInvokePreparationFailsBeforeSessionCreation(t *testing.T) {
	var sessions atomic.Int64
	model := NewGoModel(func() *compileTestModel {
		sessions.Add(1)
		return &compileTestModel{}
	})
	chart, err := Compile(invokeTestDefinition(InvokeDefinition{DefinitionID: "job", Type: "missing"}), model)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chart.NewInstance(); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("NewInstance error = %v", err)
	}
	if got := sessions.Load(); got != 0 {
		t.Fatalf("datamodel sessions created before preparation = %d", got)
	}
	if err := chart.Prepare(); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("Prepare error = %v", err)
	}
}

func TestDynamicInvokeTypeFailureIsAPlatformExecutionError(t *testing.T) {
	model := NewGoModel(func() *compileTestModel { return &compileTestModel{} })
	typeExpression := GoLiteral(mustTestString(t, "missing-dynamic"))
	active := compileTestState("active", KindAtomic)
	active.Invokes = []InvokeDefinition{{DefinitionID: "dynamic", TypeExpr: &typeExpression}}
	active.Transitions = []TransitionDefinition{{Events: []Identifier{ErrEventExecution}, Targets: []Identifier{"failed"}}}
	failed := compileTestState("failed", KindFinal)
	root := compileTestState("root", KindCompound, active, failed)
	root.Initial = &TransitionDefinition{Targets: []Identifier{"active"}}
	chart, err := Compile(compileTestDefinition(root), model)
	if err != nil {
		t.Fatal(err)
	}
	instance, err := chart.NewInstance()
	if err != nil {
		t.Fatalf("dynamic type must not fail preparation: %v", err)
	}
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := instance.Wait(waitCtx); err != nil {
		t.Fatal(err)
	}
}

func TestInvokeIDLocationSourceAndContentUseDatamodel(t *testing.T) {
	model := NewGoModel(func() *compileTestModel { return &compileTestModel{} })
	idLocation := GoData("invoke-id")
	typeExpression := GoLiteral(mustTestString(t, "worker"))
	sourceExpression := GoLiteral(mustTestString(t, "resource:dynamic"))
	content := GoLiteral(Int64Value(17))
	definition := invokeTestDefinition(InvokeDefinition{
		DefinitionID: "dynamic-input", IDLocation: &idLocation,
		TypeExpr: &typeExpression, SrcExpr: &sourceExpression, Content: &content,
	})
	definition.Data = []DataDefinition{{ID: "invoke-id"}}
	chart, err := Compile(definition, model)
	if err != nil {
		t.Fatal(err)
	}
	requests := make(chan InvokeRequest, 1)
	instance, err := chart.NewInstance(WithInvokeHandler("worker", func() InvokeHandler {
		return &recordingInvokeHandler{requests: requests}
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	request := <-requests
	if request.ID == "" || request.Source != "resource:dynamic" || !request.Data.Equal(Int64Value(17)) {
		t.Fatalf("request = %+v", request)
	}
	location, err := chart.program.ResolveDataLocation("invoke-id")
	if err != nil {
		t.Fatal(err)
	}
	stored, err := instance.session.EvaluateValue(ExecContext{}, location)
	if err != nil {
		t.Fatal(err)
	}
	storedID, _ := stored.AsString()
	if storedID != string(request.ID) {
		t.Fatalf("stored ID = %q, request ID = %q", storedID, request.ID)
	}
	_ = instance.Stop(ctx)
}

func TestCompileInvokeIDLocationRejectsNonLocation(t *testing.T) {
	model := NewGoModel(func() *compileTestModel { return &compileTestModel{} })
	notALocation := GoLiteral(mustTestString(t, "not-a-location"))
	active := compileTestState("active", KindAtomic)
	active.Invokes = []InvokeDefinition{{DefinitionID: "bad-idlocation", Type: "worker", IDLocation: &notALocation}}
	root := compileTestState("root", KindCompound, active)
	root.Initial = &TransitionDefinition{Targets: []Identifier{"active"}}
	if _, err := Compile(compileTestDefinition(root), model); err == nil || !strings.Contains(err.Error(), "invalid in requested context") {
		t.Fatalf("Compile error = %v", err)
	}
}

func TestDeclarativeInvokePreservesAutoforwardFinalizeAndCancellation(t *testing.T) {
	model := NewGoModel(func() *compileTestModel { return &compileTestModel{} })
	record, err := model.Action("record-finalize", "v1", func(data *compileTestModel, ec ExecContext, _ []Value) error {
		event, _ := ec.Event()
		value, _ := event.Data.AsInt64()
		data.Trace = append(data.Trace, strconv.FormatInt(value, 10))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	active := compileTestState("active", KindAtomic)
	active.Invokes = []InvokeDefinition{{
		DefinitionID: "lifecycle", ID: "job", Type: "worker", AutoForward: true,
		Finalize: []ExecutableBlock{{NewScriptExecutable(ScriptDefinition{Expr: record.Expression()})}},
	}}
	active.Transitions = []TransitionDefinition{{Events: []Identifier{"leave"}, Targets: []Identifier{"outside"}}}
	outside := compileTestState("outside", KindAtomic)
	outside.Transitions = []TransitionDefinition{{Events: []Identifier{"late"}, Targets: []Identifier{"failed"}}}
	root := compileTestState("root", KindCompound, active, outside, compileTestState("failed", KindAtomic))
	root.Initial = &TransitionDefinition{Targets: []Identifier{"active"}}
	chart, err := Compile(compileTestDefinition(root), model)
	if err != nil {
		t.Fatal(err)
	}
	bindings := make(chan invokeLifecycleBinding, 1)
	instance, err := chart.NewInstance(WithInvokeHandler("worker", func() InvokeHandler {
		return &invokeLifecycleHandler{bindings: bindings}
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	binding := <-bindings
	forwardedData, err := MapValue(map[string]Value{"n": Int64Value(8)})
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Send(ctx, Event{Name: "forwarded", Type: EventExternal, Data: forwardedData}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-binding.io.Incoming:
		if event.Name != "forwarded" || !event.Data.Equal(forwardedData) {
			t.Fatalf("autoforwarded event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("external event was not autoforwarded")
	}
	binding.io.Deliver(Event{Name: "reply", Data: Int64Value(12)})
	if got := instance.session.(*goSession[compileTestModel]).data.Trace; !reflect.DeepEqual(got, []string{"12"}) {
		t.Fatalf("finalize trace = %v", got)
	}
	if err := instance.Send(ctx, Event{Name: "leave"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-binding.cancelled:
	case <-time.After(time.Second):
		t.Fatal("handler context was not cancelled on state exit")
	}
	binding.io.Deliver(Event{Name: "late"})
	if err := instance.Send(ctx, Event{Name: "barrier"}); err != nil {
		t.Fatal(err)
	}
	if got := instance.Configuration(); !containsID(got, "outside") || containsID(got, "failed") {
		t.Fatalf("late cancelled delivery changed configuration: %v", got)
	}
	_ = instance.Stop(ctx)
}

func TestInvokeChartHandlerRunsADeclarativelyInvokedChild(t *testing.T) {
	childModel := NewGoModel(func() *struct{} { return &struct{}{} })
	sendHello, err := childModel.Action("send-parent-hello", "v1", func(_ *struct{}, ec ExecContext, _ []Value) error {
		ec.Send("hello", SendOptions{Target: "#_parent"})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sendPong, err := childModel.Action("send-parent-pong", "v1", func(_ *struct{}, ec ExecContext, _ []Value) error {
		ec.Send("pong", SendOptions{Target: "#_parent"})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	child, err := Build(Compound("croot", "start", Children(
		Atomic("start", OnEntry(sendHello.Do()), On("ping", Target("pinged"))),
		Atomic("pinged", OnEntry(sendPong.Do())),
	)), childModel, WithRevisionSalt("ping-pong-v1"))
	if err != nil {
		t.Fatalf("Build child: %v", err)
	}
	waitingHello := compileTestState("waitingHello", KindAtomic)
	waitingHello.Transitions = []TransitionDefinition{{Events: []Identifier{"hello"}, Targets: []Identifier{"waitingPong"}}}
	waitingPong := compileTestState("waitingPong", KindAtomic)
	waitingPong.Transitions = []TransitionDefinition{{Events: []Identifier{"pong"}, Targets: []Identifier{"done"}}}
	invoking := compileTestState("invoking", KindCompound, waitingHello, waitingPong)
	invoking.Initial = &TransitionDefinition{Targets: []Identifier{"waitingHello"}}
	invoking.Invokes = []InvokeDefinition{{DefinitionID: "child-service", ID: "child", AutoForward: true}}
	root := compileTestState("proot", KindCompound, invoking, compileTestState("done", KindAtomic))
	root.Initial = &TransitionDefinition{Targets: []Identifier{"invoking"}}
	definition := compileTestDefinition(root)
	definition.ID = "proot"
	chart, err := Compile(definition, NewGoModel(func() *compileTestModel { return &compileTestModel{} }))
	if err != nil {
		t.Fatal(err)
	}
	instance, err := chart.NewInstance(WithInvokeHandler(SCXMLInvokeType,
		InvokeChartHandler(child, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitForConfiguration := func(id Identifier) {
		t.Helper()
		deadline := time.Now().Add(time.Second)
		for !containsID(instance.Configuration(), id) {
			if time.Now().After(deadline) {
				t.Fatalf("configuration = %v, want %q", instance.Configuration(), id)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitForConfiguration("waitingPong")
	if err := instance.Send(ctx, Event{Name: "ping", Type: EventExternal}); err != nil {
		t.Fatal(err)
	}
	waitForConfiguration("done")
	_ = instance.Stop(ctx)
}

func TestActiveInvokeSnapshotsResolveStableDefinitionIDAfterReorder(t *testing.T) {
	model := NewGoModel(func() *compileTestModel { return &compileTestModel{} })
	first := InvokeDefinition{DefinitionID: "first-definition", ID: "first-runtime", Type: "first"}
	second := InvokeDefinition{DefinitionID: "second-definition", ID: "second-runtime", Type: "second"}
	chart, err := Compile(invokeTestDefinition(first, second), model)
	if err != nil {
		t.Fatal(err)
	}
	chart.version = "stable-invokes-v1"
	requests := make(chan InvokeRequest, 2)
	options := []Option{
		WithInvokeHandler("first", func() InvokeHandler { return &recordingInvokeHandler{requests: requests} }),
		WithInvokeHandler("second", func() InvokeHandler { return &recordingInvokeHandler{requests: requests} }),
		WithSessionID("stable-invokes"),
	}
	instance, err := chart.NewInstance(options...)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	<-requests
	<-requests
	snapshot, err := instance.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = instance.Stop(ctx)
	if got := []Identifier{snapshot.ActiveInvokes[0].DefinitionID, snapshot.ActiveInvokes[1].DefinitionID}; !reflect.DeepEqual(got, []Identifier{"first-definition", "second-definition"}) {
		t.Fatalf("snapshot definition IDs = %v", got)
	}

	reordered, err := Compile(invokeTestDefinition(second, first), model)
	if err != nil {
		t.Fatal(err)
	}
	reordered.version = chart.version
	store := newMemSnapshotStore()
	if err := store.Save(ctx, "stable-invokes", Checkpoint{Snapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	restored, err := reordered.Rehydrate(ctx, newMemLog(), store, "stable-invokes", NoopIOProcessor, options...)
	if err != nil {
		t.Fatal(err)
	}
	resumed := map[Identifier]InvokeRequest{}
	for range 2 {
		select {
		case request := <-requests:
			resumed[request.DefinitionID] = request
		case <-time.After(time.Second):
			t.Fatal("active invocation was not resumed")
		}
	}
	if got := resumed["second-definition"].Type; got != "second" {
		t.Fatalf("resumed second handler type = %q", got)
	}
	if got := restored.ip.invokesByID["second-runtime"].spec.definitionID; got != "second-definition" {
		t.Fatalf("restored second definition ID = %q", got)
	}
	if got := restored.ip.invokesByID["second-runtime"].spec.staticType; got != "second" {
		t.Fatalf("restored second handler type = %q", got)
	}
	_ = restored.Stop(ctx)
}

func TestResumeKeepsEvaluatedDynamicTypeAndSource(t *testing.T) {
	model := NewGoModel(func() *compileTestModel { return &compileTestModel{} })
	typeLocation := GoData("handler-type")
	sourceLocation := GoData("handler-source")
	firstType := GoLiteral(mustTestString(t, "first"))
	secondType := GoLiteral(mustTestString(t, "second"))
	firstSource := GoLiteral(mustTestString(t, "source:first"))
	secondSource := GoLiteral(mustTestString(t, "source:second"))
	active := compileTestState("active", KindAtomic)
	active.Invokes = []InvokeDefinition{{
		DefinitionID: "dynamic-binding", ID: "job", TypeExpr: &typeLocation, SrcExpr: &sourceLocation,
	}}
	active.Transitions = []TransitionDefinition{{Events: []Identifier{"change-binding"}, Actions: []ExecutableBlock{{
		NewAssignExecutable(AssignDefinition{Location: typeLocation, Expr: secondType}),
		NewAssignExecutable(AssignDefinition{Location: sourceLocation, Expr: secondSource}),
	}}}}
	root := compileTestState("root", KindCompound, active)
	root.Initial = &TransitionDefinition{Targets: []Identifier{"active"}}
	definition := compileTestDefinition(root)
	definition.Data = []DataDefinition{{ID: "handler-type", Expr: &firstType}, {ID: "handler-source", Expr: &firstSource}}
	chart, err := Compile(definition, model)
	if err != nil {
		t.Fatal(err)
	}
	chart.version = "dynamic-binding-v1"
	requests := make(chan InvokeRequest, 2)
	options := []Option{
		WithInvokeHandler("first", func() InvokeHandler { return &recordingInvokeHandler{requests: requests} }),
		WithInvokeHandler("second", func() InvokeHandler { return &recordingInvokeHandler{requests: requests} }),
		WithSessionID("dynamic-binding"),
	}
	ctx := context.Background()
	instance, err := chart.NewInstance(options...)
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	started := <-requests
	if started.Type != "first" || started.Source != "source:first" {
		t.Fatalf("started request = %+v", started)
	}
	if err := instance.Send(ctx, Event{Name: "change-binding"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := instance.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshot.ActiveInvokes[0]; got.Type != "first" || got.Source != "source:first" {
		t.Fatalf("snapshotted active invoke = %+v", got)
	}
	_ = instance.Stop(ctx)
	store := newMemSnapshotStore()
	if err := store.Save(ctx, "dynamic-binding", Checkpoint{Snapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	rehydrated, err := chart.Rehydrate(ctx, newMemLog(), store, "dynamic-binding", NoopIOProcessor, options...)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case resumed := <-requests:
		if resumed.Type != "first" || resumed.Source != "source:first" {
			t.Fatalf("resumed request = %+v, want original binding", resumed)
		}
	case <-time.After(time.Second):
		t.Fatal("invoke was not resumed")
	}
	_ = rehydrated.Stop(ctx)
}
