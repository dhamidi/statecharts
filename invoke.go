package statecharts

import (
	"context"
	"fmt"
)

// InvokeIO is an invoked service's only channel back into (and, for
// services that accept forwarded events, in from) the chart that invoked
// it -- the <invoke> analogue of IOProcessor for the rest of the world.
type InvokeIO struct {
	// Deliver posts ev to the invoking chart's external event queue,
	// tagged with this invocation's InvokeID, exactly as SCXML 6.4
	// requires of events an invoked service returns. Safe to call from any
	// goroutine at any time, including after the invocation has been
	// cancelled (a no-op once cancelled, per SCXML 6.4.3: a cancelled
	// invocation's events "MUST NOT" reach the invoking session).
	Deliver func(Event)

	// Incoming delivers events sent to this invocation from the chart --
	// via Send(name, SendOptions{Target: "#_<invokeid>"}), SCXML 6.4.4's
	// addressing form for talking back to a running invocation. Always
	// non-nil; a service that never expects inbound traffic simply never
	// reads from it.
	Incoming <-chan Event
}

// InvokeFunc is the body of one <invoke> instance (SCXML 6.4): it starts
// when its containing state is entered (deferred to the point the
// containing macrostep settles, so a state entered and exited again within
// the same macrostep is never invoked at all) and runs for as long as the
// external service it represents is alive, in its own goroutine. ctx is
// cancelled when the containing state is exited before the service
// finishes -- SCXML 6.4.2's "cancel operation MUST act as if it were the
// final onexit handler in the invoking state" -- or when the Instance
// stops; InvokeFunc should return promptly once ctx is done. A nil error
// return is the service's own normal completion, on which the interpreter
// synthesizes done.invoke.<id> (carrying the returned data) onto the
// invoking chart's external queue, mirroring SCXML 6.4.3; a non-nil error
// instead reports error.communication on the internal queue. Neither event
// is generated if ctx was already cancelled by the time InvokeFunc returns.
type InvokeFunc func(ctx context.Context, params any, io InvokeIO) (data any, err error)

// InvokeResumeFunc reattaches to a possibly-still-running invocation after
// Rehydrate, instead of starting a fresh one via Start. id is the
// invocation's own id, preserved exactly as it was before the restart --
// whatever identity a resumable invocation uses to find the real-world
// resource it was talking to (a subprocess's PID, a job id in an external
// queue, a container name) has to be either id itself or something params
// encodes, since nothing else about the pre-restart invocation survives.
// params is recomputed fresh, by calling Params again against the
// fully-restored datamodel; there is no separate "original params"
// preserved anywhere. Unlike a live invocation's Params call, _event is
// unbound during this recomputation (SCXML 5.10.1's rule for before the
// first event is processed): there is no single well-defined triggering
// event left once replay has caught up. Write a Params callback meant to
// run again during Resume so its result depends on datamodel state, not on
// the current event.
//
// Resume's return is treated exactly like Start's: a non-nil error becomes
// error.communication, a nil error with data becomes done.invoke.<id>
// immediately (the work finished while nothing was watching it), and
// blocking on ctx or io.Incoming continues the invocation exactly as if it
// had never stopped.
type InvokeResumeFunc func(ctx context.Context, id Identifier, params any, io InvokeIO) (data any, err error)

// InvokeSpec is the uncompiled description of one <invoke> attached to a
// state, built via Invoke and InvokeOptions.
type InvokeSpec struct {
	ID             Identifier // empty = auto-generated ("<stateid>.invoke<n>") at invoke time
	Start          InvokeFunc
	Params         func(ExecContext) any // evaluated once, synchronously, when the invocation starts; nil => nil params
	Finalize       []ActionFunc
	AutoForward    bool
	Resume         InvokeResumeFunc
	IDLocation     IDLocationFunc
	finalizeBlocks []actionBlock
}

// InvokeOption configures an InvokeSpec being built by Invoke.
type InvokeOption func(*InvokeSpec)

// WithInvokeID sets an explicit invoke ID, e.g. for targeting it later via
// SendOptions{Target: "#_" + id}. Left unset, an ID unique within the
// session is generated when the invocation actually starts.
func WithInvokeID(id Identifier) InvokeOption {
	return func(s *InvokeSpec) { s.ID = id }
}

// WithInvokeIDLocation assigns the generated invoke ID synchronously when the
// invocation is evaluated, before params and Start. It is mutually exclusive
// with WithInvokeID; an assignment error or panic produces error.execution
// and aborts the invocation.
func WithInvokeIDLocation(fn IDLocationFunc) InvokeOption {
	return func(s *InvokeSpec) { s.IDLocation = fn }
}

// WithInvokeParams sets the callback that computes the data passed to
// Start (SCXML's <param>/namelist equivalent), evaluated synchronously
// against the state being entered, before Start's goroutine is spawned.
func WithInvokeParams(fn func(ExecContext) any) InvokeOption {
	return func(s *InvokeSpec) { s.Params = fn }
}

// WithFinalize attaches executable content run whenever an event carrying
// this invocation's InvokeID is processed, immediately before transitions
// are selected for it (SCXML 6.5): the mechanism for normalizing data an
// invoked service returns before any transition's guard inspects it. Send,
// Raise, and Cancel are rejected with error.execution in this content.
// Since actions are arbitrary Go, applications are responsible for not doing
// direct external I/O from a finalize callback.
func WithFinalize(actions ...ActionFunc) InvokeOption {
	return func(s *InvokeSpec) {
		s.Finalize = append(s.Finalize, actions...)
		s.finalizeBlocks = append(s.finalizeBlocks, append(actionBlock(nil), actions...))
	}
}

// WithAutoForward makes the chart forward an exact copy of every external
// event it processes to this invocation's InvokeIO.Incoming, for as long
// as it's active -- SCXML 6.4.1's 'autoforward' attribute. The copy is
// unconditional: unlike <finalize>, it happens whether or not the event's
// own InvokeID matches this invocation.
func WithAutoForward() InvokeOption {
	return func(s *InvokeSpec) { s.AutoForward = true }
}

// WithInvokeResume sets the callback Rehydrate calls, once replay catches
// up, to reattach to this invocation instead of assuming it is gone. Left
// unset, Rehydrate reports error.communication for this invocation
// unconditionally, the only honest default for one with no way to check.
func WithInvokeResume(fn InvokeResumeFunc) InvokeOption {
	return func(s *InvokeSpec) { s.Resume = fn }
}

// Invoke attaches an external service instance to a state (SCXML's
// <invoke>) -- fn is any Go function willing to run in its own goroutine
// and talk back through InvokeIO. Use InvokeChart to run another *Chart as
// a full child SCXML session (SCXML 6.4's
// type="http://www.w3.org/TR/scxml/" case) rather than writing fn by hand.
func Invoke(fn InvokeFunc, opts ...InvokeOption) StateOption {
	return func(s *StateSpec) {
		spec := InvokeSpec{Start: fn}
		for _, opt := range opts {
			opt(&spec)
		}
		s.Invokes = append(s.Invokes, spec)
	}
}

// compiledInvoke is the compiled form of one InvokeSpec, owned by the
// compiledState it was declared on.
type compiledInvoke struct {
	id          Identifier
	start       InvokeFunc
	params      func(ExecContext) any
	finalize    []actionBlock
	autoForward bool
	resume      InvokeResumeFunc
	idLocation  IDLocationFunc
}

// runningInvoke is the interpreter-core bookkeeping for one active
// invocation -- enough to cancel it (SCXML 6.4.2), to route a matching
// finalize handler when a reply carrying its InvokeID arrives (SCXML 6.5),
// and to forward it a copy of every external event if it autoforwards
// (SCXML 6.4.1). It is deliberately independent of however the invoked
// service was actually started (see invokeRunnerFunc): cancel and
// incoming are plain callbacks/channels, not a reference back to whatever
// goroutine or Instance is on the other end.
type runningInvoke struct {
	id          Identifier
	state       *compiledState
	specIndex   int // this invocation's position among state's <invoke> elements, in document order
	finalize    []actionBlock
	autoForward bool
	cancel      func()
	incoming    chan<- Event
}

// invokeRunnerFunc starts one instance of spec's external service and
// returns a way to cancel it and a channel to forward events sent to it.
// Supplied by Instance (see newInstance) because spawning goroutines and
// delivering their results back through the actor's own inbox are actor
// concerns, not core-interpreter ones -- the same seam actorClock already
// uses for <send delay="...">. The default, used by a bare interpretation
// with no owning Instance (e.g. under test), starts nothing.
type invokeRunnerFunc func(id Identifier, spec *compiledInvoke, params any) (cancel func(), incoming chan<- Event)

func noopInvokeRunner(Identifier, *compiledInvoke, any) (func(), chan<- Event) {
	return func() {}, nil
}

// parentIOProcessor is the IOProcessor InvokeChart gives a child session:
// it recognizes SCXML Appendix C.1's "#_parent" special send target,
// routing it to this invocation's own InvokeIO.Deliver (exactly SCXML
// 6.4.4's "it can use <send> with target '_parent' ... to send events ...
// to the invoking session"), and defers every other target to next --
// nil reports an honest "no transport" error for those, the same posture
// as LocalIOProcessor.
type parentIOProcessor struct {
	deliver    func(Event)
	next       IOProcessor
	dispatcher Dispatcher
}

func (p *parentIOProcessor) Attach(d Dispatcher) {
	p.dispatcher = d
	if p.next != nil {
		p.next.Attach(d)
	}
}

func (p *parentIOProcessor) Send(ctx context.Context, req SendRequest) error {
	if req.Target == "#_parent" {
		if req.Type != SCXMLEventProcessor {
			return localUnsupportedSendError{typ: req.Type}
		}
		origin := Identifier("")
		for _, info := range p.IOProcessors() {
			if info.Type == SCXMLEventProcessor {
				origin = Identifier(info.Location.String())
				break
			}
		}
		p.deliver(Event{Name: req.Event, Data: req.Data, SendID: req.EventSendID, Origin: origin, OriginType: SCXMLEventProcessorAlias})
		return nil
	}
	if p.next == nil {
		if req.Type != SCXMLEventProcessor {
			return localUnsupportedSendError{typ: req.Type}
		}
		return fmt.Errorf("statecharts: no IOProcessor configured for send target %q", req.Target)
	}
	return p.next.Send(ctx, req)
}

// IOProcessors includes the child's mandatory SCXML session address and any
// entries advertised by next. It deliberately does not synthesize a
// "#_parent" entry: _ioprocessors describes how *other* sessions reach this
// one, while "#_parent" is the reverse direction.
func (p *parentIOProcessor) IOProcessors() []IOProcessorInfo {
	var infos []IOProcessorInfo
	if d, ok := p.next.(IOProcessorDescriber); ok {
		infos = append(infos, d.IOProcessors()...)
	}
	for _, info := range infos {
		if info.Type == SCXMLEventProcessor {
			return infos
		}
	}
	identified, ok := p.dispatcher.(interface{ ID() SessionID })
	if !ok {
		return infos
	}
	self := IOProcessorInfo{
		Type:     SCXMLEventProcessor,
		Location: LocationFromIdentifier(Identifier("#_scxml_" + string(identified.ID()))),
	}
	return append([]IOProcessorInfo{self}, infos...)
}

// InvokeChart returns an InvokeFunc that runs chart as a full child SCXML
// session -- SCXML 6.4's type="http://www.w3.org/TR/scxml/" case -- rather
// than an arbitrary external service. newDatamodel builds the child's
// datamodel from whatever Params produced (SCXML's <param>/namelist
// equivalent); baseIO is used for any send target other than "#_parent"
// (nil means no fallback transport for those targets).
//
// The child's own Send(name, SendOptions{Target: "#_parent"}) reaches
// back into this invocation's InvokeIO.Deliver, tagged with this
// invocation's InvokeID by the interpreter as usual (Appendix C.1: "the
// Processor MUST add the event to the external event queue of the SCXML
// session that invoked the sending session"). Every event the parent
// forwards to this invocation -- an explicit SendOptions{Target:
// "#_<invokeid>"}, or, with WithAutoForward, a copy of every external
// event the parent processes -- is delivered to the child exactly as an
// application calling Send on it directly would (SCXML 6.4's
// autoforwarding). The child is stopped, and its own onexit handlers run
// via exitInterpreter, when this invocation is cancelled; reaching its
// own top-level final state is this invocation's own natural completion,
// generating done.invoke.<id> on the parent exactly as for any other
// InvokeFunc.
func InvokeChart(chart *Chart, newDatamodel func(params any) any, baseIO IOProcessor) InvokeFunc {
	return func(ctx context.Context, params any, io InvokeIO) (any, error) {
		datamodel := newDatamodel(params)
		child := New(chart, datamodel, WithIOProcessor(SCXMLEventProcessor, &parentIOProcessor{deliver: io.Deliver, next: baseIO}))

		// Start's own actor goroutine runs regardless of whether Start
		// itself returns early because ctx raced its way to already
		// being cancelled (e.g. the invoking state was exited again
		// immediately) -- so the child must always be stopped, even when
		// Start reports an error, or it would keep running orphaned.
		defer child.Stop(context.Background())

		if err := child.Start(ctx); err != nil {
			return nil, err
		}

		go func() {
			for {
				select {
				case ev, ok := <-io.Incoming:
					if !ok {
						return
					}
					if child.Send(ctx, ev) != nil {
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()

		if err := child.Wait(ctx); err != nil {
			return nil, err
		}
		return child.Result()
	}
}
