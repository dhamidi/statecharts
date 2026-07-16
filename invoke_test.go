package statecharts

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestDeclarativeInvokeStartsDeliversAndCancelsWithItsState(t *testing.T) {
	t.Parallel()
	synctest.Test(t, testDeclarativeInvokeStartsDeliversAndCancelsWithItsState)
}

func testDeclarativeInvokeStartsDeliversAndCancelsWithItsState(t *testing.T) {
	b := newTestBuilder(t, func() *struct{} { return &struct{}{} })
	chart, err := b.build(Compound("root", "idle", Children(
		Atomic("idle", On("start", Target("running"))),
		Atomic("running",
			Invoke("worker", "jobs", WithInvokeID("job")),
			On("worker.ready", Target("observed")),
			On("stop", Target("idle")),
		),
		Atomic("observed", On("stop", Target("idle"))),
	)))
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan InvokeIO, 1)
	cancelled := make(chan struct{})
	instance := b.newInstance(chart, WithInvokeHandler("worker", func() InvokeHandler {
		return InvokeHandlerFunc(func(ctx context.Context, request InvokeRequest, io InvokeIO) (Value, error) {
			if request.ID != "job" || request.Source != "jobs" {
				t.Errorf("request = %+v", request)
			}
			started <- io
			<-ctx.Done()
			close(cancelled)
			io.Deliver(Event{Name: "worker.ready"})
			return NullValue(), nil
		})
	}))
	if err := instance.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(context.Background())
	if err := instance.Send(context.Background(), Event{Name: "start"}); err != nil {
		t.Fatal(err)
	}
	select {
	case io := <-started:
		io.Deliver(Event{Name: "worker.ready"})
	case <-time.After(time.Second):
		t.Fatal("invoke did not start")
	}
	waitActive(t, instance, "observed")
	if err := instance.Send(context.Background(), Event{Name: "stop"}); err != nil {
		t.Fatal(err)
	}
	waitActive(t, instance, "idle")
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("invoke context was not cancelled on state exit")
	}
	synctest.Wait()
	if active := instance.Configuration(); !hasState(active, "idle") {
		t.Fatalf("late invoke delivery changed configuration: active=%v", active)
	}
}

func TestInvokeIsNotStartedWhenStateExitsInSameMacrostep(t *testing.T) {
	t.Parallel()
	synctest.Test(t, testInvokeIsNotStartedWhenStateExitsInSameMacrostep)
}

func testInvokeIsNotStartedWhenStateExitsInSameMacrostep(t *testing.T) {
	b := newTestBuilder(t, func() *struct{} { return &struct{}{} })
	chart, err := b.build(Compound("root", "transient", Children(
		Atomic("transient", Invoke("worker", "jobs"), Eventless(Target("done"))),
		Final("done"),
	)))
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	instance := b.newInstance(chart, WithInvokeHandler("worker", func() InvokeHandler {
		return InvokeHandlerFunc(func(context.Context, InvokeRequest, InvokeIO) (Value, error) {
			started <- struct{}{}
			return NullValue(), nil
		})
	}))
	if err := instance.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(context.Background())
	synctest.Wait()
	select {
	case <-started:
		t.Fatal("invoke started for a state exited before the macrostep settled")
	default:
	}
}

func TestInvokeFailureRaisesCommunicationError(t *testing.T) {
	t.Parallel()
	type model struct{ failure Value }
	var data *model
	b := newTestBuilder(t, func() *model { data = &model{}; return data })
	record := b.action("record-invoke-communication-error", func(data *model, ec ExecContext) error {
		event, _ := ec.Event()
		data.failure = event.Data
		return nil
	})
	chart, err := b.build(Compound("root", "running", Children(
		Atomic("running", Invoke("worker", "jobs", WithInvokeID("job")),
			On(string(ErrEventCommunication), Then(record), Target("failed"))),
		Atomic("failed"),
	)))
	if err != nil {
		t.Fatal(err)
	}
	instance := b.newInstance(chart, WithInvokeHandler("worker", func() InvokeHandler {
		return InvokeHandlerFunc(func(context.Context, InvokeRequest, InvokeIO) (Value, error) {
			return Value{}, errors.New("service unavailable")
		})
	}))
	if err := instance.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(context.Background())
	waitActive(t, instance, "failed")
	classification, message, ok := PlatformErrorDetails(data.failure)
	if !ok || classification != ErrEventCommunication || message != "service unavailable" {
		t.Fatalf("error.communication Data = %v, want classification=%q message=%q", data.failure, ErrEventCommunication, "service unavailable")
	}
}

func TestInvokeReceivesExplicitAndAutoForwardedEvents(t *testing.T) {
	t.Parallel()
	b := newTestBuilder(t, func() *struct{} { return &struct{}{} })
	chart, err := b.build(Compound("root", "running", Children(
		Atomic("running",
			Invoke("worker", "jobs", WithInvokeID("job"), WithAutoForward()),
			On("send.explicit", Then(Send("explicit", SendTarget("#_job")))),
		),
	)))
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan Event, 4)
	instance := b.newInstance(chart, WithInvokeHandler("worker", func() InvokeHandler {
		return InvokeHandlerFunc(func(ctx context.Context, _ InvokeRequest, io InvokeIO) (Value, error) {
			for {
				select {
				case event := <-io.Incoming:
					received <- event
				case <-ctx.Done():
					return NullValue(), nil
				}
			}
		})
	}))
	if err := instance.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(context.Background())
	if err := instance.Send(context.Background(), Event{Name: "send.explicit"}); err != nil {
		t.Fatal(err)
	}
	seen := map[Identifier]bool{}
	deadline := time.After(time.Second)
	for len(seen) < 2 {
		select {
		case event := <-received:
			seen[event.Name] = true
		case <-deadline:
			t.Fatalf("received events = %v, want explicit and send.explicit", seen)
		}
	}
	if !seen["explicit"] || !seen["send.explicit"] {
		t.Fatalf("received events = %v", seen)
	}
}

func TestInvokeFinalizeRunsBeforeReturnedEventGuard(t *testing.T) {
	t.Parallel()
	type model struct{ normalized bool }
	var data *model
	b := newTestBuilder(t, func() *model { data = &model{}; return data })
	normalize := b.action("normalize-invoke-result", func(data *model, _ ExecContext) error {
		data.normalized = true
		return nil
	})
	normalized := b.condition("invoke-result-is-normalized", func(data *model, _ ExecContext) bool {
		return data.normalized
	})
	chart, err := b.build(Compound("root", "running", Children(
		Atomic("running",
			Invoke("worker", "jobs", WithInvokeID("job"), WithFinalize(normalize)),
			On("reply", If(normalized), Target("done")),
		),
		Atomic("done"),
	)))
	if err != nil {
		t.Fatal(err)
	}
	instance := b.newInstance(chart, WithInvokeHandler("worker", func() InvokeHandler {
		return InvokeHandlerFunc(func(ctx context.Context, _ InvokeRequest, io InvokeIO) (Value, error) {
			io.Deliver(Event{Name: "reply"})
			<-ctx.Done()
			return NullValue(), nil
		})
	}))
	if err := instance.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(context.Background())
	waitActive(t, instance, "done")
	if !data.normalized {
		t.Fatal("finalize did not run")
	}
}

func TestInvokeChartHandlerRoutesParentAndChildEvents(t *testing.T) {
	t.Parallel()
	childBuilder := newTestBuilder(t, func() *struct{} { return &struct{}{} })
	child, err := childBuilder.build(Compound("child", "waiting", Children(
		Atomic("waiting", On("ping", Then(Send("pong", SendTarget("#_parent"))), Target("done"))),
		Final("done", WithDone(GoLiteral(Int64Value(7)))),
	)))
	if err != nil {
		t.Fatal(err)
	}

	parentBuilder := newTestBuilder(t, func() *struct{} { return &struct{}{} })
	parent, err := parentBuilder.build(Compound("parent", "running", Children(
		Atomic("running",
			Invoke(string(SCXMLInvokeType), "child", WithInvokeID("child")),
			On("start", Then(Send("ping", SendTarget("#_child")))),
			On("pong", Target("observed")),
		),
		Atomic("observed"),
	)))
	if err != nil {
		t.Fatal(err)
	}
	instance := parentBuilder.newInstance(parent,
		WithInvokeHandler(SCXMLInvokeType, InvokeChartHandler(child, nil)))
	if err := instance.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(context.Background())
	if err := instance.Send(context.Background(), Event{Name: "start"}); err != nil {
		t.Fatal(err)
	}
	waitActive(t, instance, "observed")
}

func TestInvokeGeneratedIDsAreCanonicalFreshAndAvoidExplicitCollisions(t *testing.T) {
	type model struct{ id Identifier }
	var data *model
	b := newTestBuilder(t, func() *model { data = &model{}; return data })
	location, err := b.model.Location("generated-invoke-id", "v1",
		func(data *model, _ ExecContext, _ []Value) (Value, error) {
			value, _ := StringValue(string(data.id))
			return value, nil
		},
		func(data *model, _ ExecContext, value Value, _ []Value) error {
			text, _ := value.AsString()
			data.id = Identifier(text)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	chart, err := b.build(Compound("root", "active", Children(
		Atomic("active",
			Invoke("worker", "", WithInvokeID("active.invoke1")),
			Invoke("worker", "", WithInvokeIDLocation(location.At())),
			On("leave", Target("away")),
		),
		Atomic("away", On("return", Target("active"))),
	)))
	if err != nil {
		t.Fatal(err)
	}
	requests := make(chan InvokeRequest, 4)
	instance := b.newInstance(chart, WithInvokeHandler("worker", func() InvokeHandler {
		return InvokeHandlerFunc(func(ctx context.Context, request InvokeRequest, _ InvokeIO) (Value, error) {
			requests <- request
			<-ctx.Done()
			return NullValue(), nil
		})
	}))
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	first := map[Identifier]bool{(<-requests).ID: true, (<-requests).ID: true}
	if !first["active.invoke1"] || !first["active.invoke2"] || data.id != "active.invoke2" {
		t.Fatalf("first-entry IDs/stored ID = %v/%q, want active.invoke1, active.invoke2 / active.invoke2", first, data.id)
	}
	if err := instance.Send(ctx, Event{Name: "leave"}); err != nil {
		t.Fatal(err)
	}
	if err := instance.Send(ctx, Event{Name: "return"}); err != nil {
		t.Fatal(err)
	}
	second := map[Identifier]bool{(<-requests).ID: true, (<-requests).ID: true}
	if !second["active.invoke1"] || !second["active.invoke3"] || data.id != "active.invoke3" {
		t.Fatalf("re-entry IDs/stored ID = %v/%q, want explicit active.invoke1 and fresh active.invoke3", second, data.id)
	}
	if !strings.HasPrefix(string(data.id), "active.invoke") {
		t.Fatalf("generated ID = %q, want canonical state.invokeN format", data.id)
	}
}

func TestInvokeIDLocationRunsBeforeCanonicalParams(t *testing.T) {
	type model struct{ id Identifier }
	b := newTestBuilder(t, func() *model { return &model{} })
	location, err := b.model.Location("invoke-id-before-params", "v1",
		func(data *model, _ ExecContext, _ []Value) (Value, error) {
			value, _ := StringValue(string(data.id))
			return value, nil
		},
		func(data *model, _ ExecContext, value Value, _ []Value) error {
			text, _ := value.AsString()
			data.id = Identifier(text)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	param, err := b.model.Value("invoke-param-sees-id", "v1", func(data *model, _ ExecContext, _ []Value) (Value, error) {
		value, _ := StringValue(string(data.id))
		return value, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	chart, err := b.build(Atomic("active", Invoke("worker", "", WithInvokeIDLocation(location.At()),
		WithInvokeParams(ParamDefinition{Name: "id", Expr: expressionPointer(param.Get())}))))
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan InvokeRequest, 1)
	instance := b.newInstance(chart, WithInvokeHandler("worker", func() InvokeHandler { return &recordingInvokeHandler{requests: started} }))
	if err := instance.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(context.Background())
	request := <-started
	params, _ := request.Data.AsMap()
	text, _ := params["id"].AsString()
	if request.ID != "active.invoke1" || text != string(request.ID) {
		t.Fatalf("request ID/param = %q/%q, want active.invoke1 in both", request.ID, text)
	}
}

func TestInvokePanickingRegisteredParamRaisesExecutionErrorWithoutStarting(t *testing.T) {
	b := newTestBuilder(t, func() *struct{} { return &struct{}{} })
	broken, err := b.model.Value("panicking-invoke-param", "v1", func(*struct{}, ExecContext, []Value) (Value, error) {
		panic("boom")
	})
	if err != nil {
		t.Fatal(err)
	}
	chart, err := b.build(Compound("root", "active", Children(
		Atomic("active", Invoke("worker", "", WithInvokeParams(ParamDefinition{Name: "bad", Expr: expressionPointer(broken.Get())})),
			On(string(ErrEventExecution), Target("recovered"))),
		Atomic("recovered"),
	)))
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	instance := b.newInstance(chart, WithInvokeHandler("worker", func() InvokeHandler {
		return InvokeHandlerFunc(func(context.Context, InvokeRequest, InvokeIO) (Value, error) {
			started <- struct{}{}
			return NullValue(), nil
		})
	}))
	if err := instance.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(context.Background())
	waitActive(t, instance, "recovered")
	select {
	case <-started:
		t.Fatal("invoke handler started despite parameter expression panic")
	default:
	}
}

func TestInvokeEventQueuedDuringFinalOnExitIsDroppedAfterCancellation(t *testing.T) {
	type model struct{ ingressed []Identifier }
	b := newTestBuilder(t, func() *model { return &model{} })
	exitStarted := make(chan struct{})
	finishExit := make(chan struct{})
	blockExit := b.action("block-final-onexit", func(*model, ExecContext) error {
		close(exitStarted)
		<-finishExit
		return nil
	})
	chart, err := b.build(Compound("root", "a", Children(
		Atomic("a", Invoke("worker", "jobs", WithInvokeID("service")), OnExit(blockExit), On("leave", Target("b"))),
		Atomic("b", On("late", Target("wrong"))),
		Atomic("wrong"),
	)))
	if err != nil {
		t.Fatal(err)
	}
	deliverReady := make(chan func(Event), 1)
	var ingressed []Identifier
	instance := b.newInstance(chart,
		WithInvokeHandler("worker", func() InvokeHandler {
			return InvokeHandlerFunc(func(ctx context.Context, _ InvokeRequest, io InvokeIO) (Value, error) {
				deliverReady <- io.Deliver
				<-ctx.Done()
				return NullValue(), nil
			})
		}),
		WithIngressHook(func(event Event) error {
			ingressed = append(ingressed, event.Name)
			return nil
		}),
	)
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	deliver := <-deliverReady
	leaveDone := make(chan error, 1)
	go func() { leaveDone <- instance.Send(ctx, Event{Name: "leave"}) }()
	<-exitStarted
	deliverDone := make(chan struct{})
	go func() {
		deliver(Event{Name: "late"})
		close(deliverDone)
	}()
	close(finishExit)
	if err := <-leaveDone; err != nil {
		t.Fatal(err)
	}
	<-deliverDone
	if !hasState(instance.Configuration(), "b") || hasState(instance.Configuration(), "wrong") {
		t.Fatalf("configuration = %v, want b; late cancelled-invoke event must be dropped", instance.Configuration())
	}
	if len(ingressed) != 1 || ingressed[0] != "leave" {
		t.Fatalf("ingress events = %v, want only leave", ingressed)
	}
}

func TestInvokeNormalCompletionEmitsDoneEventWithoutInstanceError(t *testing.T) {
	b := newTestBuilder(t, func() *struct{} { return &struct{}{} })
	chart, err := b.build(Compound("root", "running", Children(
		Atomic("running", Invoke("worker", "jobs", WithInvokeID("service")), On("done.invoke.service", Target("done"))),
		Atomic("done"),
	)))
	if err != nil {
		t.Fatal(err)
	}
	instance := b.newInstance(chart, WithInvokeHandler("worker", func() InvokeHandler {
		return InvokeHandlerFunc(func(context.Context, InvokeRequest, InvokeIO) (Value, error) {
			return NullValue(), nil
		})
	}))
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	waitActive(t, instance, "done")
	if err := instance.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
}

func TestInvokeFinalizeRejectsExternalEffects(t *testing.T) {
	type model struct {
		executionErrors int
		forbiddenEvents int
		timerFired      bool
	}
	var data *model
	b := newTestBuilder(t, func() *model { data = &model{}; return data })
	countError := b.action("count-finalize-execution-error", func(data *model, _ ExecContext) error { data.executionErrors++; return nil })
	countForbidden := b.action("count-forbidden-finalize-event", func(data *model, _ ExecContext) error { data.forbiddenEvents++; return nil })
	markTimer := b.action("mark-preserved-delayed-send", func(data *model, _ ExecContext) error { data.timerFired = true; return nil })
	tryExternalEffects := b.action("try-finalize-external-effects", func(_ *model, ec ExecContext) error {
		ec.Send("forbidden-send", SendOptions{})
		ec.Raise(Event{Name: "forbidden-raise"})
		ec.Cancel("keep")
		return nil
	})
	clock := NewManualClock(time.Unix(0, 0))
	deliverReady := make(chan func(Event), 1)
	chart, err := b.build(Atomic("active",
		OnEntry(Send("timer-fired", SendID("keep"), SendDelay(time.Hour))),
		Invoke("worker", "jobs", WithInvokeID("service"), WithFinalize(tryExternalEffects)),
		On(string(ErrEventExecution), Then(countError)),
		On("forbidden-send forbidden-raise", Then(countForbidden)),
		On("timer-fired", Then(markTimer)),
	))
	if err != nil {
		t.Fatal(err)
	}
	instance := b.newInstance(chart, WithClock(clock), WithInvokeHandler("worker", func() InvokeHandler {
		return InvokeHandlerFunc(func(ctx context.Context, _ InvokeRequest, io InvokeIO) (Value, error) {
			deliverReady <- io.Deliver
			<-ctx.Done()
			return NullValue(), nil
		})
	}))
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	(<-deliverReady)(Event{Name: "reply"})
	if data.executionErrors != 3 || data.forbiddenEvents != 0 {
		t.Fatalf("after finalize: %+v, want three error.execution events and no send/raise effects", data)
	}
	clock.Advance(time.Hour)
	if err := instance.Send(ctx, Event{Name: "sync"}); err != nil {
		t.Fatal(err)
	}
	if !data.timerFired {
		t.Fatal("finalize cancelled a delayed send; cancel is an external effect")
	}
}

func TestInvokeDeliverCopiesPayloadAtServiceBoundary(t *testing.T) {
	type model struct{ received Value }
	var data *model
	b := newTestBuilder(t, func() *model { data = &model{}; return data })
	record := b.action("record-delivered-copy", func(data *model, ec ExecContext) error {
		event, _ := ec.Event()
		data.received = event.Data
		return nil
	})
	chart, err := b.build(Atomic("active", Invoke("worker", "jobs"), On("reply", Then(record))))
	if err != nil {
		t.Fatal(err)
	}
	deliverReady := make(chan func(Event), 1)
	instance := b.newInstance(chart, WithInvokeHandler("worker", func() InvokeHandler {
		return InvokeHandlerFunc(func(ctx context.Context, _ InvokeRequest, io InvokeIO) (Value, error) {
			deliverReady <- io.Deliver
			<-ctx.Done()
			return NullValue(), nil
		})
	}))
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	originalList := []Value{Int64Value(1)}
	original, _ := MapValue(map[string]Value{"values": ListValue(originalList)})
	(<-deliverReady)(Event{Name: "reply", Data: original})
	originalList[0] = Int64Value(9)
	receivedMap, _ := data.received.AsMap()
	receivedList, _ := receivedMap["values"].AsList()
	if !receivedList[0].Equal(Int64Value(1)) {
		t.Fatalf("received payload = %v after service mutation, want an isolated copy", data.received)
	}
}

func TestInvokeCompletionCopiesPayloadAtServiceBoundary(t *testing.T) {
	type model struct{ received Value }
	var data *model
	b := newTestBuilder(t, func() *model { data = &model{}; return data })
	processed := make(chan struct{}, 1)
	record := b.action("record-completion-copy", func(data *model, ec ExecContext) error {
		event, _ := ec.Event()
		data.received = event.Data
		processed <- struct{}{}
		return nil
	})
	chart, err := b.build(Atomic("active", Invoke("worker", "jobs", WithInvokeID("service")), On("done.invoke.service", Then(record))))
	if err != nil {
		t.Fatal(err)
	}
	resultList := []Value{Int64Value(1)}
	result, _ := MapValue(map[string]Value{"values": ListValue(resultList)})
	instance := b.newInstance(chart, WithInvokeHandler("worker", func() InvokeHandler {
		return InvokeHandlerFunc(func(context.Context, InvokeRequest, InvokeIO) (Value, error) { return result, nil })
	}))
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	select {
	case <-processed:
	case <-time.After(2 * time.Second):
		t.Fatal("done.invoke.service was not processed")
	}
	resultList[0] = Int64Value(9)
	copiedMap, _ := data.received.AsMap()
	copiedList, _ := copiedMap["values"].AsList()
	if !copiedList[0].Equal(Int64Value(1)) {
		t.Fatalf("completion payload = %v after service mutation, want an isolated copy", data.received)
	}
}

func TestInvokeAutoForwardStopsAfterCancellation(t *testing.T) {
	synctest.Test(t, testInvokeAutoForwardStopsAfterCancellation)
}

func testInvokeAutoForwardStopsAfterCancellation(t *testing.T) {
	b := newTestBuilder(t, func() *struct{} { return &struct{}{} })
	chart, err := b.build(Compound("root", "a", Children(
		Atomic("a", Invoke("worker", "jobs", WithAutoForward()), On("go", Target("b"))),
		Atomic("b"),
	)))
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan Event, 8)
	instance := b.newInstance(chart, WithInvokeHandler("worker", func() InvokeHandler {
		return InvokeHandlerFunc(func(ctx context.Context, _ InvokeRequest, io InvokeIO) (Value, error) {
			for {
				select {
				case event := <-io.Incoming:
					received <- event
				case <-ctx.Done():
					return NullValue(), nil
				}
			}
		})
	}))
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	if err := instance.Send(ctx, Event{Name: "go"}); err != nil {
		t.Fatal(err)
	}
	waitActive(t, instance, "b")
	if err := instance.Send(ctx, Event{Name: "after"}); err != nil {
		t.Fatal(err)
	}
	synctest.Wait()
	for {
		select {
		case event := <-received:
			if event.Name == "after" {
				t.Fatalf("received %+v after invocation cancellation", event)
			}
		default:
			return
		}
	}
}

func TestInvokeAutoForwardCopiesNestedMutablePayload(t *testing.T) {
	b := newTestBuilder(t, func() *struct{} { return &struct{}{} })
	chart, err := b.build(Atomic("active", Invoke("worker", "jobs", WithAutoForward())))
	if err != nil {
		t.Fatal(err)
	}
	mutated := make(chan struct{})
	instance := b.newInstance(chart, WithInvokeHandler("worker", func() InvokeHandler {
		return InvokeHandlerFunc(func(ctx context.Context, _ InvokeRequest, io InvokeIO) (Value, error) {
			select {
			case event := <-io.Incoming:
				values, _ := event.Data.AsMap()
				list, _ := values["values"].AsList()
				list[0] = Int64Value(9)
				close(mutated)
			case <-ctx.Done():
			}
			<-ctx.Done()
			return NullValue(), nil
		})
	}))
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	original, _ := MapValue(map[string]Value{"values": ListValue([]Value{Int64Value(1)})})
	if err := instance.Send(ctx, Event{Name: "load", Data: original}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-mutated:
	case <-time.After(2 * time.Second):
		t.Fatal("invoke did not receive autoforwarded event")
	}
	originalMap, _ := original.AsMap()
	originalList, _ := originalMap["values"].AsList()
	if !originalList[0].Equal(Int64Value(1)) {
		t.Fatalf("source payload = %v after invoked service mutation, want an isolated forwarded copy", original)
	}
}

func TestInvokeChartHandlerRoundTripSetsChildOriginMetadata(t *testing.T) {
	childBuilder := newTestBuilder(t, func() *struct{} { return &struct{}{} })
	sendHello := childBuilder.action("send-parent-hello-with-origin", func(_ *struct{}, ec ExecContext) error {
		ec.Send("hello", SendOptions{Target: "#_parent"})
		return nil
	})
	sendPong := childBuilder.action("send-parent-pong-with-origin", func(_ *struct{}, ec ExecContext) error {
		ec.Send("pong", SendOptions{Target: "#_parent"})
		return nil
	})
	child, err := childBuilder.build(Compound("child", "start", Children(
		Atomic("start", OnEntry(sendHello), On("ping", Target("pinged"))),
		Atomic("pinged", OnEntry(sendPong)),
	)))
	if err != nil {
		t.Fatal(err)
	}
	type model struct{ origin, originType Identifier }
	var data *model
	parentBuilder := newTestBuilder(t, func() *model { data = &model{}; return data })
	recordOrigin := parentBuilder.action("record-child-origin", func(data *model, ec ExecContext) error {
		event, _ := ec.Event()
		data.origin, data.originType = event.Origin, event.OriginType
		return nil
	})
	parent, err := parentBuilder.build(Compound("parent", "invoking", Children(
		Compound("invoking", "waitingHello", Children(
			Atomic("waitingHello", On("hello", Then(recordOrigin), Target("waitingPong"))),
			Atomic("waitingPong", On("pong", Target("done"))),
		), Invoke(string(SCXMLInvokeType), "child", WithInvokeID("child"), WithAutoForward())),
		Atomic("done"),
	)))
	if err != nil {
		t.Fatal(err)
	}
	instance := parentBuilder.newInstance(parent, WithInvokeHandler(SCXMLInvokeType, InvokeChartHandler(child, nil)))
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	waitActive(t, instance, "waitingPong")
	if !strings.HasPrefix(string(data.origin), "#_scxml_") || data.originType != SCXMLEventProcessorAlias {
		t.Fatalf("child #_parent event origin = %q/%q, want standard SCXML metadata", data.origin, data.originType)
	}
	if err := instance.Send(ctx, Event{Name: "ping"}); err != nil {
		t.Fatal(err)
	}
	waitActive(t, instance, "done")
}

func TestInvokeChartHandlerCancellationStopsChildAndRunsOnExit(t *testing.T) {
	childExited := make(chan struct{})
	childBuilder := newTestBuilder(t, func() *struct{} { return &struct{}{} })
	markExit := childBuilder.action("mark-invoked-child-exit", func(*struct{}, ExecContext) error { close(childExited); return nil })
	child, err := childBuilder.build(Atomic("only", OnExit(markExit)))
	if err != nil {
		t.Fatal(err)
	}
	parentBuilder := newTestBuilder(t, func() *struct{} { return &struct{}{} })
	parent, err := parentBuilder.build(Compound("parent", "a", Children(
		Atomic("a", Invoke(string(SCXMLInvokeType), "child"), On("go", Target("b"))), Atomic("b"),
	)))
	if err != nil {
		t.Fatal(err)
	}
	instance := parentBuilder.newInstance(parent, WithInvokeHandler(SCXMLInvokeType, InvokeChartHandler(child, nil)))
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	if err := instance.Send(ctx, Event{Name: "go"}); err != nil {
		t.Fatal(err)
	}
	waitActive(t, instance, "b")
	select {
	case <-childExited:
	case <-time.After(2 * time.Second):
		t.Fatal("child session was never stopped when the invoking state was exited")
	}
}

func TestInvokeChartHandlerChildIOProcessorsExcludeSyntheticParent(t *testing.T) {
	seen := make(chan []IOProcessorInfo, 1)
	childBuilder := newTestBuilder(t, func() *struct{} { return &struct{}{} })
	record := childBuilder.action("record-child-io-processors", func(_ *struct{}, ec ExecContext) error { seen <- ec.IOProcessors(); return nil })
	child, err := childBuilder.build(Atomic("only", OnEntry(record)))
	if err != nil {
		t.Fatal(err)
	}
	baseIO := &describingIOProcessor{infos: []IOProcessorInfo{{Type: "mock", Location: mustLocation(t, "mock://base")}}}
	parentBuilder := newTestBuilder(t, func() *struct{} { return &struct{}{} })
	parent, err := parentBuilder.build(Atomic("active", Invoke(string(SCXMLInvokeType), "child")))
	if err != nil {
		t.Fatal(err)
	}
	instance := parentBuilder.newInstance(parent, WithInvokeHandler(SCXMLInvokeType, InvokeChartHandler(child, baseIO)))
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	select {
	case got := <-seen:
		if len(got) != 2 || got[0].Type != SCXMLEventProcessor || !strings.HasPrefix(got[0].Location.String(), "#_scxml_") || got[1].Type != "mock" || got[1].Location.String() != "mock://base" {
			t.Fatalf("child IOProcessors = %v, want child SCXML entry followed by base entries and no #_parent", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("child OnEntry did not run")
	}
}

func TestInvokeChartHandlerPropagatesTopLevelDonePayload(t *testing.T) {
	childBuilder := newTestBuilder(t, func() *struct{} { return &struct{}{} })
	child, err := childBuilder.build(Compound("child", "done", Children(Final("done", WithDone(GoLiteral(mustTestString(t, "child result")))))))
	if err != nil {
		t.Fatal(err)
	}
	type model struct{ result Value }
	var data *model
	parentBuilder := newTestBuilder(t, func() *model { data = &model{}; return data })
	record := parentBuilder.action("record-child-done-payload", func(data *model, ec ExecContext) error { event, _ := ec.Event(); data.result = event.Data; return nil })
	parent, err := parentBuilder.build(Compound("parent", "working", Children(
		Atomic("working", Invoke(string(SCXMLInvokeType), "child", WithInvokeID("child")), On("done.invoke.child", Then(record), Target("finished"))),
		Final("finished"),
	)))
	if err != nil {
		t.Fatal(err)
	}
	instance := parentBuilder.newInstance(parent, WithInvokeHandler(SCXMLInvokeType, InvokeChartHandler(child, nil)))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := instance.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	want := mustTestString(t, "child result")
	if !data.result.Equal(want) {
		t.Fatalf("done.invoke data = %#v, want child result", data.result)
	}
}

func TestInvokeFullIncomingMailboxRaisesCommunicationError(t *testing.T) {
	type model struct{ failure Value }
	var data *model
	b := newTestBuilder(t, func() *model { data = &model{}; return data })
	fill := b.action("fill-invoke-incoming-mailbox", func(_ *model, ec ExecContext) error {
		for i := 0; i <= invokeIncomingBuffer; i++ {
			ec.Send("message", SendOptions{Target: "#_service"})
		}
		return nil
	})
	record := b.action("record-full-mailbox-error", func(data *model, ec ExecContext) error { event, _ := ec.Event(); data.failure = event.Data; return nil })
	chart, err := b.build(Compound("root", "active", Children(
		Atomic("active", Invoke("worker", "jobs", WithInvokeID("service")), On("fill", Then(fill)), On(string(ErrEventCommunication), Then(record), Target("failed"))),
		Atomic("failed"),
	)))
	if err != nil {
		t.Fatal(err)
	}
	instance := b.newInstance(chart, WithInvokeHandler("worker", func() InvokeHandler {
		return InvokeHandlerFunc(func(ctx context.Context, _ InvokeRequest, _ InvokeIO) (Value, error) {
			<-ctx.Done()
			return NullValue(), nil
		})
	}))
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer instance.Stop(ctx)
	if err := instance.Send(ctx, Event{Name: "fill"}); err != nil {
		t.Fatal(err)
	}
	waitActive(t, instance, "failed")
	classification, _, ok := PlatformErrorDetails(data.failure)
	if !ok || classification != ErrEventCommunication {
		t.Fatalf("mailbox error Data = %v, want classification %q", data.failure, ErrEventCommunication)
	}
}
