package statecharts

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestInstanceBasicLifecycle(t *testing.T) {
	notLocked := Cond(func(d *Door, ec ExecContext) bool { return !d.Locked })
	recordOpen := Action(func(d *Door, ec ExecContext) error { d.OpenCount++; return nil })

	chart, err := Build(
		Compound("door", "closed",
			Children(
				Atomic("closed", On("open.request", Target("open"), If(notLocked), Then(recordOpen))),
				Atomic("open", On("close.request", Target("closed"))),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	d := &Door{}
	in := New(chart, d)
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !hasState(in.Configuration(), "closed") {
		t.Fatalf("initial configuration = %v, want 'closed'", in.Configuration())
	}

	if err := in.Send(ctx, Event{Name: "open.request", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if !hasState(in.Configuration(), "open") {
		t.Fatalf("configuration after Send = %v, want 'open'", in.Configuration())
	}
	if d.OpenCount != 1 {
		t.Fatalf("OpenCount = %d, want 1", d.OpenCount)
	}

	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := in.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if err := in.Err(); err != nil {
		t.Fatalf("Err() after clean stop = %v, want nil", err)
	}

	// Send/Stop ordering: after Stop, further Sends must not hang, and
	// must never be silently dropped while reporting success.
	if err := in.Send(ctx, Event{Name: "open.request", Type: EventExternal}); !errors.Is(err, ErrInstanceStopped) {
		t.Fatalf("Send after Stop: got %v, want ErrInstanceStopped", err)
	}
}

func TestInstanceSendStopOrdering(t *testing.T) {
	var enteredB bool
	chart, err := Build(
		Compound("m", "a",
			Children(
				Atomic("a", On("go", Target("b"))),
				Atomic("b", OnEntry(Action(func(d *struct{}, ec ExecContext) error {
					enteredB = true
					return nil
				}))),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, &struct{}{})
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Send then immediately Stop: FIFO ordering through the same ingress
	// path guarantees the Send's effect lands before Stop takes hold. Once
	// Stop takes hold, exitInterpreter empties the configuration (SCXML
	// Appendix D), so "b" was reached is checked via a side effect of
	// entering it, not via a post-Stop Configuration() snapshot.
	if err := in.Send(ctx, Event{Name: "go", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := in.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	if !enteredB {
		t.Fatalf("expected 'b' to have been entered before Stop took hold")
	}
}

func TestInstanceStopRunsOnExitForRemainingStates(t *testing.T) {
	var exited bool
	chart, err := Build(
		Compound("m", "a",
			Children(
				Atomic("a", OnExit(Action(func(d *struct{}, ec ExecContext) error {
					exited = true
					return nil
				}))),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, &struct{}{})
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !exited {
		t.Fatalf("Stop() must run onexit for every state still active (SCXML Appendix D exitInterpreter), but it did not run")
	}
}

func TestInstanceNaturalTerminationRunsOnExitForRemainingStates(t *testing.T) {
	// The chart's root StateSpec plays the role of SCXML's own <scxml>
	// wrapper element -- never itself a member of the configuration (see
	// interpretation.start(), which stops addAncestorStatesToEnter before
	// reaching it) -- so "done" must be a direct child of root for
	// reaching it to flip running to false at all; "app" is the
	// intermediate, real ancestor whose onexit must still fire once it
	// does.
	var exitedApp, exitedFinal bool
	chart, err := Build(
		Compound("root", "app",
			Children(
				Compound("app", "a",
					Children(
						Atomic("a", On("go", Target("done"))),
					),
					OnExit(Action(func(d *struct{}, ec ExecContext) error {
						exitedApp = true
						return nil
					})),
				),
				Final("done", OnExit(Action(func(d *struct{}, ec ExecContext) error {
					exitedFinal = true
					return nil
				}))),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, &struct{}{})
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := in.Send(ctx, Event{Name: "go", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := in.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !exitedApp || !exitedFinal {
		t.Fatalf("reaching a top-level final state must run onexit for every remaining active state (app=%v, done=%v)", exitedApp, exitedFinal)
	}
}

func TestInstanceActionPanicBecomesExecutionError(t *testing.T) {
	boom := Action(func(d *struct{}, ec ExecContext) error {
		panic("boom")
	})

	chart, err := Build(
		Compound("m", "a",
			Children(
				Atomic("a",
					On("go", Target("b"), Then(boom)),
					On(string(ErrEventExecution), Target("recovered")),
				),
				Atomic("b", On(string(ErrEventExecution), Target("recovered"))),
				Atomic("recovered"),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, &struct{}{})
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := in.Send(ctx, Event{Name: "go", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if !hasState(in.Configuration(), "recovered") {
		t.Fatalf("configuration = %v, want recovered after error.execution", in.Configuration())
	}
	if err := in.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestInstanceDelayedSendFiresAndCanBeCancelled(t *testing.T) {
	scheduleTimeout := Action(func(d *struct{}, ec ExecContext) error {
		ec.Send("timeout", SendOptions{SendID: "t1", Delay: 5 * time.Second})
		return nil
	})

	chart, err := Build(
		Compound("m", "waiting",
			Children(
				Atomic("waiting", OnEntry(scheduleTimeout), On("timeout", Target("done"))),
				Atomic("done"),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	clock := NewManualClock(time.Unix(0, 0))
	in := New(chart, &struct{}{}, WithClock(clock))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	clock.Advance(5 * time.Second)
	// synchronize with the actor goroutine: since both this send and the
	// timer hand-off go through the same FIFO inbox, waiting for this
	// reply guarantees the timer-fired event was already processed.
	if err := in.Send(ctx, Event{Name: "noop", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if !hasState(in.Configuration(), "done") {
		t.Fatalf("configuration = %v, want 'done' after delayed send fired", in.Configuration())
	}
}

func TestInstanceDefaultSessionIDIsNonEmptyAndVaries(t *testing.T) {
	chart := doorChart(t)

	in1 := New(chart, &Door{})
	in2 := New(chart, &Door{})

	if in1.ID() == "" {
		t.Fatalf("Instance.ID() = %q, want non-empty", in1.ID())
	}
	if in2.ID() == "" {
		t.Fatalf("Instance.ID() = %q, want non-empty", in2.ID())
	}
	if in1.ID() == in2.ID() {
		t.Fatalf("two Instances minted the same default session id %q, want distinct ids", in1.ID())
	}
}

func TestInstanceWithIDGeneratorOverridesDefault(t *testing.T) {
	chart := doorChart(t)
	gen := &ManualIDGenerator{}

	in1 := New(chart, &Door{}, WithIDGenerator(gen))
	in2 := New(chart, &Door{}, WithIDGenerator(gen))

	if in1.ID() != "id-1" {
		t.Fatalf("in1.ID() = %q, want %q", in1.ID(), "id-1")
	}
	if in2.ID() != "id-2" {
		t.Fatalf("in2.ID() = %q, want %q", in2.ID(), "id-2")
	}
}

func TestInstanceWithSessionIDTakesPriorityOverGenerator(t *testing.T) {
	chart := doorChart(t)
	gen := &ManualIDGenerator{}

	in := New(chart, &Door{}, WithIDGenerator(gen), WithSessionID("explicit-id"))
	if in.ID() != "explicit-id" {
		t.Fatalf("in.ID() = %q, want %q (WithSessionID must win over WithIDGenerator)", in.ID(), "explicit-id")
	}
}

func TestExecContextSessionIDAndNameInsideActionAndGuard(t *testing.T) {
	var gotSessionID string
	var gotName string
	var guardSessionID string
	var guardName string

	recordAndOpen := Action(func(d *Door, ec ExecContext) error {
		gotSessionID = ec.SessionID()
		gotName = ec.Name()
		d.OpenCount++
		return nil
	})
	guardSeesIdentity := Cond(func(d *Door, ec ExecContext) bool {
		guardSessionID = ec.SessionID()
		guardName = ec.Name()
		return true
	})

	chart, err := Build(
		Compound("door", "closed",
			Children(
				Atomic("closed", On("open.request", Target("open"), If(guardSeesIdentity), Then(recordAndOpen))),
				Atomic("open"),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	d := &Door{}
	in := New(chart, d, WithSessionID("sess-xyz"))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := in.Send(ctx, Event{Name: "open.request", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if gotSessionID != "sess-xyz" {
		t.Fatalf("ExecContext.SessionID() inside action = %q, want %q", gotSessionID, "sess-xyz")
	}
	if gotName != string(chart.ID()) {
		t.Fatalf("ExecContext.Name() inside action = %q, want %q", gotName, chart.ID())
	}
	if guardSessionID != "sess-xyz" {
		t.Fatalf("ExecContext.SessionID() inside guard = %q, want %q", guardSessionID, "sess-xyz")
	}
	if guardName != string(chart.ID()) {
		t.Fatalf("ExecContext.Name() inside guard = %q, want %q", guardName, chart.ID())
	}

	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// describingIOProcessor is a minimal IOProcessor that also implements
// IOProcessorDescriber, for exercising ExecContext.IOProcessors() /
// IOProcessorLocation() end to end.
type describingIOProcessor struct {
	infos []IOProcessorInfo
}

func (p *describingIOProcessor) Attach(Dispatcher) {}

func (p *describingIOProcessor) Send(context.Context, SendRequest) error { return nil }

func (p *describingIOProcessor) IOProcessors() []IOProcessorInfo { return p.infos }

// mustLocation parses s as a Location, failing the test immediately if s is
// malformed -- every literal this helper is called with in this test suite
// is a well-formed URL, so a parse failure here means the test fixture
// itself is broken.
func mustLocation(t *testing.T, s string) Location {
	t.Helper()
	loc, err := NewLocation(s)
	if err != nil {
		t.Fatalf("NewLocation(%q): %v", s, err)
	}
	return loc
}

func TestExecContextIOProcessorsSurfacesDescriberEntries(t *testing.T) {
	var gotList []IOProcessorInfo
	var gotLocation Location
	var gotOK bool

	recordAndOpen := Action(func(d *Door, ec ExecContext) error {
		gotList = ec.IOProcessors()
		gotLocation, gotOK = ec.IOProcessorLocation("mock")
		d.OpenCount++
		return nil
	})

	chart, err := Build(
		Compound("door", "closed",
			Children(
				Atomic("closed", On("open.request", Target("open"), Then(recordAndOpen))),
				Atomic("open"),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	io := &describingIOProcessor{infos: []IOProcessorInfo{{Type: "mock", Location: mustLocation(t, "mock://door-1")}}}
	d := &Door{}
	in := New(chart, d, WithIOProcessor(SCXMLEventProcessor, io))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := in.Send(ctx, Event{Name: "open.request", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if len(gotList) != 1 || gotList[0].Type != "mock" || gotList[0].Location.String() != "mock://door-1" {
		t.Fatalf("ExecContext.IOProcessors() = %v, want [{mock mock://door-1}]", gotList)
	}
	if !gotOK || gotLocation.String() != "mock://door-1" {
		t.Fatalf("ExecContext.IOProcessorLocation(%q) = (%q, %v), want (%q, true)", "mock", gotLocation, gotOK, "mock://door-1")
	}

	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestExecContextIOProcessorsIncludesDefaultSCXMLProcessor(t *testing.T) {
	var gotList []IOProcessorInfo
	var gotLocation Location
	var gotOK bool

	record := Action(func(d *Door, ec ExecContext) error {
		gotList = ec.IOProcessors()
		gotLocation, gotOK = ec.IOProcessorLocation(SCXMLEventProcessor)
		d.OpenCount++
		return nil
	})

	chart, err := Build(
		Compound("door", "closed",
			Children(
				Atomic("closed", On("open.request", Target("open"), Then(record))),
				Atomic("open"),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	d := &Door{}
	in := New(chart, d, WithSessionID("door-1"))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := in.Send(ctx, Event{Name: "open.request", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if len(gotList) != 1 || gotList[0].Type != SCXMLEventProcessor || gotList[0].Location.String() != "#_scxml_door-1" {
		t.Fatalf("ExecContext.IOProcessors() = %v, want the default SCXML processor at #_scxml_door-1", gotList)
	}
	if !gotOK || gotLocation.String() != "#_scxml_door-1" {
		t.Fatalf("ExecContext.IOProcessorLocation(default) = (%q, %v), want (#_scxml_door-1, true)", gotLocation, gotOK)
	}

	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestExecContextIOProcessorLocationUnknownTypeReturnsNotOK(t *testing.T) {
	var gotOK bool

	record := Action(func(d *Door, ec ExecContext) error {
		_, gotOK = ec.IOProcessorLocation("does-not-exist")
		d.OpenCount++
		return nil
	})

	chart, err := Build(
		Compound("door", "closed",
			Children(
				Atomic("closed", On("open.request", Target("open"), Then(record))),
				Atomic("open"),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	io := &describingIOProcessor{infos: []IOProcessorInfo{{Type: "mock", Location: mustLocation(t, "mock://door-1")}}}
	d := &Door{}
	in := New(chart, d, WithIOProcessor(SCXMLEventProcessor, io))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := in.Send(ctx, Event{Name: "open.request", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if gotOK {
		t.Fatalf("ExecContext.IOProcessorLocation(%q) ok = true, want false (not among the advertised entries)", "does-not-exist")
	}

	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestInstanceCancelledSendNeverFires(t *testing.T) {
	scheduleAndCancel := Action(func(d *struct{}, ec ExecContext) error {
		ec.Send("timeout", SendOptions{SendID: "t1", Delay: 5 * time.Second})
		ec.Cancel("t1")
		return nil
	})

	chart, err := Build(
		Compound("m", "waiting",
			Children(
				Atomic("waiting", OnEntry(scheduleAndCancel), On("timeout", Target("done"))),
				Atomic("done"),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	clock := NewManualClock(time.Unix(0, 0))
	in := New(chart, &struct{}{}, WithClock(clock))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	clock.Advance(5 * time.Second)
	if err := in.Send(ctx, Event{Name: "noop", Type: EventExternal}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if !hasState(in.Configuration(), "waiting") {
		t.Fatalf("configuration = %v, want still 'waiting' (send was cancelled)", in.Configuration())
	}
}

func TestInstanceStopCancelsAndForgetsPendingSends(t *testing.T) {
	clock := NewManualClock(time.Unix(0, 0))
	chart, err := Build(
		Atomic("waiting", OnEntry(SendEvent("timeout", SendOptions{Delay: 24 * time.Hour}))),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	in := New(chart, nil, WithClock(clock))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := in.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if len(in.ip.pending) != 0 {
		t.Fatalf("pending sends after Stop = %d, want 0", len(in.ip.pending))
	}
	clock.mu.Lock()
	defer clock.mu.Unlock()
	if len(clock.timers) != 1 || !clock.timers[0].stopped {
		t.Fatalf("manual timers after Stop = %+v, want the pending timer stopped", clock.timers)
	}
}

type captureIOProcessor struct {
	requests []SendRequest
	err      error
}

func (p *captureIOProcessor) Attach(Dispatcher) {}

func (p *captureIOProcessor) Send(_ context.Context, req SendRequest) error {
	p.requests = append(p.requests, req)
	return p.err
}

type invalidSendTestError struct{}

func (invalidSendTestError) Error() string       { return "unsupported send target" }
func (invalidSendTestError) SendExecutionError() {}

func TestInstanceDefaultIOProcessorReportsUndeliverableSend(t *testing.T) {
	chart, err := Build(
		Compound("root", "active", Children(
			Atomic("active",
				OnEntry(SendEvent("outbound", SendOptions{Target: "missing"})),
				On("error", Target("failed")),
			),
			Atomic("failed"),
		)),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, nil)
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer in.Stop(ctx)
	if !hasState(in.Configuration(), "failed") {
		t.Fatalf("configuration = %v, want failed after undeliverable send", in.Configuration())
	}
}

func TestInstanceSendAlwaysEntersThroughExternalQueue(t *testing.T) {
	var got EventType
	chart, err := Build(Atomic("root", On("go", Then(func(ec ExecContext) error {
		ev, _ := ec.Event()
		got = ev.Type
		return nil
	}))))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, nil)
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer in.Stop(ctx)
	if err := in.Send(ctx, Event{Name: "go", Type: EventPlatform}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got != EventExternal {
		t.Fatalf("event type observed through Instance.Send = %v, want external", got)
	}
}

func TestInstanceSelfSendPopulatesSCXMLOriginMetadata(t *testing.T) {
	var got Event
	chart, err := Build(Atomic("root",
		OnEntry(SendEvent("self", SendOptions{})),
		On("self", Then(func(ec ExecContext) error {
			got, _ = ec.Event()
			return nil
		})),
	))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, nil, WithSessionID("session-1"))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer in.Stop(ctx)
	if got.Origin != "#_scxml_session-1" || got.OriginType != SCXMLEventProcessor {
		t.Fatalf("self-send origin = %q/%q, want standard SCXML metadata", got.Origin, got.OriginType)
	}
}

func TestInstanceExplicitOwnSCXMLTargetRoutesToExternalQueue(t *testing.T) {
	var received bool
	chart, err := Build(Atomic("root",
		OnEntry(SendEvent("self", SendOptions{Target: "#_scxml_session-1"})),
		On("self", Then(func(ExecContext) error {
			received = true
			return nil
		})),
	))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, nil, WithSessionID("session-1"))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer in.Stop(ctx)
	if !received {
		t.Fatal("send to this session's #_scxml_<sessionid> target was not delivered")
	}
}

func TestInstanceFailedSendErrorCarriesGeneratedSendID(t *testing.T) {
	processor := &captureIOProcessor{err: errors.New("offline")}
	var got Identifier
	chart, err := Build(
		Compound("root", "active", Children(
			Atomic("active",
				OnEntry(SendEvent("outbound", SendOptions{Target: "service"})),
				On(string(ErrEventCommunication), Then(func(ec ExecContext) error {
					ev, _ := ec.Event()
					got = ev.SendID
					return nil
				})),
			),
		)),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, nil, WithIOProcessor(SCXMLEventProcessor, processor))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer in.Stop(ctx)
	if got != "send.1" {
		t.Fatalf("failed-send error SendID = %q, want generated execution ID send.1", got)
	}
}

func TestInstanceSendUsesDefaultSCXMLEventProcessorType(t *testing.T) {
	processor := &captureIOProcessor{}
	chart, err := Build(Atomic("root", OnEntry(SendEvent("outbound", SendOptions{Target: "service"}))))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, nil, WithIOProcessor(SCXMLEventProcessor, processor))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer in.Stop(ctx)
	if len(processor.requests) != 1 {
		t.Fatalf("processor requests = %d, want 1", len(processor.requests))
	}
	if got, want := processor.requests[0].Type, Identifier("http://www.w3.org/TR/scxml/#SCXMLEventProcessor"); got != want {
		t.Fatalf("default send type = %q, want %q", got, want)
	}
}

func TestInstanceUnsupportedSendProducesExecutionError(t *testing.T) {
	processor := &captureIOProcessor{err: invalidSendTestError{}}
	chart, err := Build(
		Compound("root", "active", Children(
			Atomic("active",
				OnEntry(SendEvent("outbound", SendOptions{Target: "unsupported"})),
				On(string(ErrEventExecution), Target("execution-error")),
				On(string(ErrEventCommunication), Target("communication-error")),
			),
			Atomic("execution-error"),
			Atomic("communication-error"),
		)),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, nil, WithIOProcessor(SCXMLEventProcessor, processor))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer in.Stop(ctx)
	if !hasState(in.Configuration(), "execution-error") {
		t.Fatalf("configuration = %v, want execution-error", in.Configuration())
	}
}

func TestInstanceDefaultProcessorReportsUnsupportedTypeAsExecutionError(t *testing.T) {
	chart, err := Build(
		Compound("root", "active", Children(
			Atomic("active",
				OnEntry(SendEvent("outbound", SendOptions{Target: "service", Type: "unsupported"})),
				On(string(ErrEventExecution), Target("execution-error")),
				On(string(ErrEventCommunication), Target("communication-error")),
			),
			Atomic("execution-error"),
			Atomic("communication-error"),
		)),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, nil)
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer in.Stop(ctx)
	if !hasState(in.Configuration(), "execution-error") {
		t.Fatalf("configuration = %v, want execution-error for unsupported send type", in.Configuration())
	}
}

func TestInstanceDelayedInternalSendRetainsInternalEventType(t *testing.T) {
	clock := NewManualClock(time.Unix(0, 0))
	var got EventType
	chart, err := Build(
		Compound("root", "active", Children(
			Atomic("active",
				OnEntry(SendEvent("later", SendOptions{Target: "#_internal", Delay: time.Second})),
				On("later", Target("done"), Then(func(ec ExecContext) error {
					ev, _ := ec.Event()
					got = ev.Type
					return nil
				})),
			),
			Atomic("done"),
		)),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, nil, WithClock(clock))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer in.Stop(ctx)
	clock.Advance(time.Second)
	if err := in.Send(ctx, Event{Name: "drain"}); err != nil {
		t.Fatalf("drain timer: %v", err)
	}
	if !hasState(in.Configuration(), "done") || got != EventInternal {
		t.Fatalf("configuration/type = %v/%v, want done/internal", in.Configuration(), got)
	}
}

func TestInstanceDelayedSendsWithSameIDKeepTheirOwnDeadlines(t *testing.T) {
	clock := NewManualClock(time.Unix(0, 0))
	var received []Identifier
	chart, err := Build(Atomic("root",
		OnEntry(func(ec ExecContext) error {
			ec.Send("first", SendOptions{SendID: "shared", Delay: time.Hour})
			ec.Send("second", SendOptions{SendID: "shared", Delay: 2 * time.Hour})
			return nil
		}),
		On("first second", Then(func(ec ExecContext) error {
			ev, _ := ec.Event()
			received = append(received, ev.Name)
			return nil
		})),
	))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, nil, WithClock(clock))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer in.Stop(ctx)
	snap, err := in.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap.PendingSends) != 2 || !snap.PendingSends[0].FireAt.Equal(time.Unix(0, 0).Add(time.Hour)) || !snap.PendingSends[1].FireAt.Equal(time.Unix(0, 0).Add(2*time.Hour)) {
		t.Fatalf("pending sends = %+v, want both sends sharing an ID at their own deadlines", snap.PendingSends)
	}

	clock.Advance(time.Hour)
	if err := in.Send(ctx, Event{Name: "drain"}); err != nil {
		t.Fatalf("drain first timer: %v", err)
	}
	if fmt.Sprint(received) != "[first]" {
		t.Fatalf("events after first deadline = %v, want [first]", received)
	}

	clock.Advance(time.Hour)
	if err := in.Send(ctx, Event{Name: "drain"}); err != nil {
		t.Fatalf("drain second timer: %v", err)
	}
	if fmt.Sprint(received) != "[first second]" {
		t.Fatalf("events after second deadline = %v, want [first second]", received)
	}
}

func TestInstanceCancelStopsAllDelayedSendsSharingAnID(t *testing.T) {
	clock := NewManualClock(time.Unix(0, 0))
	var received []Identifier
	chart, err := Build(Atomic("root",
		OnEntry(
			SendEvent("first", SendOptions{SendID: "shared", Delay: time.Hour}),
			SendEvent("second", SendOptions{SendID: "shared", Delay: 2 * time.Hour}),
			CancelSend("shared"),
		),
		On("first second", Then(func(ec ExecContext) error {
			ev, _ := ec.Event()
			received = append(received, ev.Name)
			return nil
		})),
	))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, nil, WithClock(clock))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer in.Stop(ctx)
	clock.Advance(3 * time.Hour)
	if err := in.Send(ctx, Event{Name: "drain"}); err != nil {
		t.Fatalf("drain timers: %v", err)
	}
	if len(received) != 0 {
		t.Fatalf("events after cancelling shared send ID = %v, want none", received)
	}
}

func TestInstanceCancelDoesNotMatchUnexposedGeneratedSendID(t *testing.T) {
	clock := NewManualClock(time.Unix(0, 0))
	received := false
	chart, err := Build(Atomic("root",
		OnEntry(
			SendEvent("explicit", SendOptions{SendID: "send.1", Delay: time.Hour}),
			SendEvent("generated", SendOptions{Delay: time.Hour}),
			CancelSend("send.1"),
		),
		On("generated", Then(func(ExecContext) error {
			received = true
			return nil
		})),
	))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, nil, WithClock(clock))
	ctx := context.Background()
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer in.Stop(ctx)
	clock.Advance(time.Hour)
	if err := in.Send(ctx, Event{Name: "drain"}); err != nil {
		t.Fatalf("drain timers: %v", err)
	}
	if !received {
		t.Fatal("cancel matched an implementation-generated send ID that was not exposed to the author")
	}
}

func TestInstanceRestorePreservesEqualDeadlineSendOrder(t *testing.T) {
	clock := NewManualClock(time.Unix(0, 0))
	var received []Identifier
	chart, err := Build(Atomic("root",
		OnEntry(
			SendEvent("z-first", SendOptions{SendID: "z", Delay: time.Hour}),
			SendEvent("a-second", SendOptions{SendID: "a", Delay: time.Hour}),
		),
		On("z-first a-second", Then(func(ec ExecContext) error {
			ev, _ := ec.Event()
			received = append(received, ev.Name)
			return nil
		})),
	))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	ctx := context.Background()
	original := New(chart, nil, WithClock(clock))
	if err := original.Start(ctx); err != nil {
		t.Fatalf("Start original: %v", err)
	}
	snap, err := original.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := original.Stop(ctx); err != nil {
		t.Fatalf("Stop original: %v", err)
	}

	restored, err := Restore(chart, nil, snap, WithClock(clock))
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if err := restored.Start(ctx); err != nil {
		t.Fatalf("Start restored: %v", err)
	}
	defer restored.Stop(ctx)
	clock.Advance(time.Hour)
	if err := restored.Send(ctx, Event{Name: "drain"}); err != nil {
		t.Fatalf("drain timers: %v", err)
	}
	if fmt.Sprint(received) != "[z-first a-second]" {
		t.Fatalf("equal-deadline events after restore = %v, want [z-first a-second]", received)
	}
}

func TestInstanceResultUsesTopLevelFinalDoneData(t *testing.T) {
	chart, err := Build(
		Compound("root", "done", Children(
			Final("done", WithDone(func(ExecContext) any { return "root result" })),
		)),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in := New(chart, nil)
	if err := in.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	got, err := in.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if got != "root result" {
		t.Fatalf("Result = %#v, want root result", got)
	}
}
