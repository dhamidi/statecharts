package statecharts

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Instance is one running chart: a single goroutine driving the interpreter
// algorithm, exposed through plain method calls. No channel type ever
// appears in this public surface -- Send and Stop internally hand a
// request to the actor's own goroutine and wait for it to be accepted, but
// that plumbing is entirely unexported.
type Instance struct {
	chart            *Chart
	ip               *interpretation
	session          DatamodelSession
	closeSessionOnce sync.Once
	clock            Clock
	invokeHandlers   map[Identifier]InvokeHandlerFactory
	// ingressHook runs on the actor goroutine immediately before an event
	// delivered through Send/Deliver is applied. Durable runtimes use it for
	// write-ahead logging so invocation results and IOProcessor callbacks pass
	// through the same persistence boundary as direct application messages.
	ingressHook func(Event) error

	inbox   chan actorRequest
	doneCh  chan struct{}
	readyCh chan struct{}

	started     atomic.Bool
	config      atomic.Pointer[[]Identifier]
	terminalErr atomic.Pointer[error]

	// suppressInvoke, while true, makes startInvoke record an invocation's
	// bookkeeping (activeInvokes/invokesByID, via the interpreter core)
	// without actually starting its real goroutine -- Rehydrate sets this
	// for the whole bootstrap-plus-replay pass, so reconstructing history
	// never re-triggers a real-world <invoke> side effect a second time
	// (see replay.go). Untouched (always false) outside Rehydrate.
	suppressInvoke atomic.Bool

	// deferTimerActivation keeps restored pending sends inert while
	// Rehydrate replays the log after a checkpoint. Rehydrate activates
	// whatever remains in one actor request after replay catches up; direct
	// Restore users leave this false and Start activates timers normally.
	deferTimerActivation atomic.Bool
}

type actorReqKind uint8

const (
	reqSend actorReqKind = iota
	reqStop
	reqTimerFired
	reqSnapshot
	reqCheckpoint
	reqActiveInvokes
	reqReplayTimerFired
	reqFinishReplay
)

type actorRequest struct {
	kind       actorReqKind
	event      Event
	fn         func() // reqTimerFired only
	checkpoint func(Snapshot) error
	entry      LogEntry            // reqReplayTimerFired only
	clock      Clock               // reqFinishReplay only
	reply      chan error          // reqSend/reqStop/reqCheckpoint/reqReplayTimerFired/reqFinishReplay
	snapOut    chan snapshotResult // reqSnapshot only
	invokesOut chan bool           // reqActiveInvokes only
}

type snapshotResult struct {
	snapshot Snapshot
	err      error
}

// Option configures an Instance built by New or Restore.
type Option func(*instanceConfig)

type instanceConfig struct {
	processors               []processorRegistration
	configuredProcessorTypes map[Identifier]bool
	invokeHandlers           map[Identifier]InvokeHandlerFactory
	clock                    Clock
	logger                   Logger
	inboxSize                int
	ingressHook              func(Event) error
	timerFiredHook           func(Identifier, Identifier, Identifier, Event) error
	idGen                    IDGenerator
	sessionID                SessionID
	deliveryNamespace        string
	platformVariables        map[string]any
}

// WithInvokeHandler binds one declarative invocation type in this Instance's
// environment. The factory is called once per live start or resume; static
// requirements are validated before a datamodel session is created.
func WithInvokeHandler(typ Identifier, factory InvokeHandlerFactory) Option {
	return func(c *instanceConfig) {
		typ = canonicalInvokeType(typ)
		if typ == "" || factory == nil {
			panic("statecharts: invoke handler type and factory must be non-empty")
		}
		if c.invokeHandlers[typ] != nil {
			panic(fmt.Sprintf("statecharts: duplicate invoke handler type %q", typ))
		}
		c.invokeHandlers[typ] = factory
	}
}

// WithPlatformVariables supplies the opaque capabilities exposed through
// SCXML's protected _x system-variable root. The binding map is copied;
// callers must provide the option again to Restore or Rehydrate because
// platform capabilities are runtime configuration, not snapshot data.
func WithPlatformVariables(values map[string]any) Option {
	return func(c *instanceConfig) {
		c.platformVariables = make(map[string]any, len(values))
		for k, v := range values {
			c.platformVariables[k] = v
		}
	}
}

var incarnationSeq atomic.Uint64

// WithDeliveryNamespace sets a stable external-delivery namespace. It is
// primarily intended for durable runtimes; ordinary instances receive a
// unique incarnation namespace automatically.
func WithDeliveryNamespace(namespace string) Option {
	return func(c *instanceConfig) { c.deliveryNamespace = namespace }
}

type processorRegistration struct {
	typ Identifier
	io  IOProcessor
}

// WithIOProcessor sets the IOProcessor used for genuinely external dispatch.
// Defaults to LocalIOProcessor, which reports unreachable targets instead of
// silently discarding them.
func WithIOProcessor(typ Identifier, p IOProcessor) Option {
	return func(c *instanceConfig) {
		if typ == "" || p == nil {
			panic("statecharts: IOProcessor type and processor must be non-empty")
		}
		typ = canonicalIOProcessorType(typ)
		if c.configuredProcessorTypes[typ] {
			panic(fmt.Sprintf("statecharts: duplicate IOProcessor type %q", typ))
		}
		c.configuredProcessorTypes[typ] = true
		if typ == SCXMLEventProcessor {
			c.processors[0].io = p
			return
		}
		c.processors = append(c.processors, processorRegistration{typ, p})
	}
}

// withIOProcessorReplacement is the replay bootstrap counterpart to
// WithIOProcessor. It replaces a registration without treating the replay
// gate as a duplicate user registration.
func withIOProcessorReplacement(typ Identifier, p IOProcessor) Option {
	return func(c *instanceConfig) {
		for i := range c.processors {
			if c.processors[i].typ == typ {
				c.processors[i].io = p
				return
			}
		}
		c.processors = append(c.processors, processorRegistration{typ, p})
	}
}

// WithClock sets the Clock used for delayed-send timers. Defaults to the
// real wall clock.
func WithClock(clk Clock) Option {
	return func(c *instanceConfig) { c.clock = clk }
}

// WithLogger sets the Logger that ExecContext.Log calls are routed to.
// Defaults to NoopLogger.
func WithLogger(l Logger) Option {
	return func(c *instanceConfig) { c.logger = l }
}

// WithInboxSize sets the buffer size of the actor's ingress channel, i.e.
// how many in-flight Send/Stop calls can be accepted before callers start
// experiencing backpressure. Defaults to 1.
func WithInboxSize(n int) Option {
	return func(c *instanceConfig) { c.inboxSize = n }
}

// WithIngressHook registers a callback run on the Instance's goroutine
// immediately before every event accepted through Send or Deliver is
// applied. Returning an error rejects the event and terminates the Instance:
// a durable runtime cannot safely continue after its write-ahead record
// failed. Rehydrate suppresses this hook while replaying entries already in
// the Log, then enables it for new live arrivals.
func WithIngressHook(fn func(Event) error) Option {
	return func(c *instanceConfig) { c.ingressHook = fn }
}

// WithTimerFiredHook registers a callback invoked synchronously, on the
// interpreter's own goroutine, immediately before a fired delayed-send's
// event is applied. It exists so a Log implementation can satisfy the
// write-ahead requirement (record the message, then let it be applied) for
// the one kind of inbound message that has no explicit Instance.Send call
// site for an application to hook itself -- see log.go,
// LoggingTimerFiredHook. A non-nil returned error is treated as this
// Instance's fatal terminal error (surfaced via Err()/Wait()) rather than
// silently letting the event through.
func WithTimerFiredHook(fn func(sendID Identifier, ev Event) error) Option {
	return func(c *instanceConfig) {
		c.timerFiredHook = func(sendID, target, typ Identifier, ev Event) error {
			return fn(sendID, ev)
		}
	}
}

// WithTimerFiredDetailsHook is WithTimerFiredHook's metadata-preserving
// counterpart. In addition to the generated send ID and event, fn receives
// the original send target and I/O processor type so a durable log can
// reconstruct the dispatch even if its pending-send record is unavailable.
func WithTimerFiredDetailsHook(fn func(sendID, target, typ Identifier, ev Event) error) Option {
	return func(c *instanceConfig) { c.timerFiredHook = fn }
}

// WithIDGenerator sets the IDGenerator used to mint this Instance's session
// id (SCXML 5.10's _sessionid) when no explicit WithSessionID is given.
// Defaults to IDGeneratorFunc(rand.Text) (crypto/rand.Text). Tests that need
// a reproducible id instead of random text can pass a ManualIDGenerator.
func WithIDGenerator(g IDGenerator) Option {
	return func(c *instanceConfig) { c.idGen = g }
}

// WithSessionID pins this Instance's session id (SCXML 5.10's _sessionid) to
// an id the caller already has -- e.g. from a Log -- instead of minting a
// fresh one. It takes priority over both the configured IDGenerator and,
// for Restore, any id recorded in the Snapshot being restored from.
func WithSessionID(id SessionID) Option {
	return func(c *instanceConfig) { c.sessionID = id }
}

func defaultInstanceConfig() instanceConfig {
	return instanceConfig{
		processors:               []processorRegistration{{SCXMLEventProcessor, NewLocalIOProcessor()}},
		configuredProcessorTypes: make(map[Identifier]bool),
		invokeHandlers:           make(map[Identifier]InvokeHandlerFactory),
		clock:                    NewRealClock(),
		logger:                   NoopLogger,
		inboxSize:                1,
		idGen:                    IDGeneratorFunc(func() SessionID { return SessionID(rand.Text()) }),
	}
}

// Prepare validates this chart's statically declared runtime requirements
// against opts without creating a datamodel session or running behavior.
// Dynamic invoke type expressions are deliberately resolved at runtime.
func (c *Chart) Prepare(opts ...Option) error {
	cfg := defaultInstanceConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	for _, state := range c.order {
		for _, invoke := range state.invokes {
			if !invoke.declarative || invoke.hasTypeExpr {
				continue
			}
			if cfg.invokeHandlers[canonicalInvokeType(invoke.staticType)] == nil {
				return fmt.Errorf("statecharts: invoke definition %q requires handler type %q", invoke.definitionID, canonicalInvokeType(invoke.staticType))
			}
		}
	}
	return nil
}

// New constructs an Instance for chart, bound to the given datamodel value
// (typically a pointer to the caller's own struct). It assigns the
// Instance's session id: an explicit WithSessionID if given, otherwise one
// minted by the configured IDGenerator (see Instance.ID). The interpreter
// goroutine is not started until Start is called.
func New(chart *Chart, datamodel any, opts ...Option) *Instance {
	if err := chart.Prepare(opts...); err != nil {
		panic(err)
	}
	session := newLegacyDatamodelSession(datamodel)
	in, err := newInstanceForSession(chart, session, opts...)
	if err != nil {
		panic(err)
	}
	return in
}

type datamodelSessionFactory func() (DatamodelSession, error)

// NewInstance constructs an Instance with a fresh session from the chart's
// DatamodelProgram. The interpreter goroutine is not started until Start is
// called.
func (c *Chart) NewInstance(opts ...Option) (*Instance, error) {
	if c.program == nil {
		return nil, fmt.Errorf("statecharts: chart has no datamodel program")
	}
	if err := c.Prepare(opts...); err != nil {
		return nil, err
	}
	return newInstanceFromFactory(c, func() (DatamodelSession, error) {
		return c.program.NewSession(SessionOptions{})
	}, opts...)
}

func newInstanceFromFactory(chart *Chart, factory datamodelSessionFactory, opts ...Option) (*Instance, error) {
	session, err := factory()
	if err != nil {
		return nil, fmt.Errorf("statecharts: create datamodel session: %w", err)
	}
	if session == nil {
		return nil, fmt.Errorf("statecharts: create datamodel session: program returned nil session")
	}
	in, err := newInstanceForSession(chart, session, opts...)
	if err != nil {
		_ = closeSession(session)
		return nil, err
	}
	return in, nil
}

func newInstanceForSession(chart *Chart, session DatamodelSession, opts ...Option) (*Instance, error) {
	cfg := defaultInstanceConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	id := cfg.sessionID
	if id == "" {
		id = cfg.idGen.NewID()
	}
	ip := newInterpretation(chart, legacySessionValue(session))
	ip.session = session
	if cfg.deliveryNamespace != "" {
		ip.deliveryNamespace = cfg.deliveryNamespace
	} else {
		ip.deliveryNamespace = fmt.Sprintf("incarnation-%d", incarnationSeq.Add(1))
	}
	return newInstance(chart, ip, session, cfg, id), nil
}

func legacySessionValue(session DatamodelSession) any {
	legacy, _ := session.(interface{ legacyValue() any })
	if legacy == nil {
		return nil
	}
	return legacy.legacyValue()
}

func newInstance(chart *Chart, ip *interpretation, session DatamodelSession, cfg instanceConfig, id SessionID) *Instance {
	in := &Instance{
		chart:       chart,
		session:     session,
		clock:       cfg.clock,
		ingressHook: cfg.ingressHook,
		inbox:       make(chan actorRequest, cfg.inboxSize),
		doneCh:      make(chan struct{}),
		readyCh:     make(chan struct{}),
	}

	ip.ioProcessorsByType = make(map[Identifier]IOProcessor, len(cfg.processors))
	ip.ioProcessorOrder = ip.ioProcessorOrder[:0]
	for _, registered := range cfg.processors {
		ip.ioProcessorsByType[registered.typ] = registered.io
		ip.ioProcessorOrder = append(ip.ioProcessorOrder, registered.typ)
	}
	ip.clock = &actorClock{real: cfg.clock, inbox: in.inbox, done: in.doneCh}
	ip.logger = cfg.logger
	ip.timerFiredHook = cfg.timerFiredHook
	ip.startInvoke = in.startInvoke
	ip.sessionID = id
	ip.name = chart.Name()
	ip.platformVariables = make(map[string]any, len(cfg.platformVariables))
	for k, v := range cfg.platformVariables {
		ip.platformVariables[k] = v
	}
	in.invokeHandlers = make(map[Identifier]InvokeHandlerFactory, len(cfg.invokeHandlers))
	for typ, factory := range cfg.invokeHandlers {
		in.invokeHandlers[typ] = factory
	}
	in.ip = ip

	for _, registered := range cfg.processors {
		registered.io.Attach(in)
	}
	return in
}

// ID returns this Instance's session id (SCXML 5.10's _sessionid). In order
// of precedence, it is an explicit WithSessionID, an id recorded in the
// Snapshot Restore was called with, or one minted by the configured
// IDGenerator. It is stable for the lifetime of the Instance.
func (in *Instance) ID() SessionID {
	return in.ip.sessionID
}

// invokeIncomingBuffer bounds how many "#_<invokeid>"-addressed events an
// invocation can have queued before dispatchNow reports error.communication
// (never blocking the interpreter goroutine on a slow or absent reader).
const invokeIncomingBuffer = 16

// startInvoke is interpretation's invokeRunnerFunc: it launches spec.start
// in its own goroutine -- not the actor's -- and wires its completion (or
// failure, or panic) back through Deliver, exactly like any other external
// arrival. ctx is cancelled by the returned cancel func, which
// interpretation.cancelInvokes calls as part of exiting the invoking state
// (SCXML 6.4.2); a goroutine that observes ctx already done by the time it
// returns generates neither done.invoke nor error.communication, per SCXML
// 6.4.3.
func (in *Instance) startInvoke(request InvokeRequest, spec *compiledInvoke) (cancel func(), incoming chan<- Event, err error) {
	// Rehydrate flips suppressInvoke for its whole bootstrap-plus-replay
	// pass: entering an invoking state is deterministic replay of history
	// (see enterState/processInvokes), so it must still happen -- and
	// beginInvoke's caller still records the resulting runningInvoke in
	// activeInvokes/invokesByID exactly as a live run would -- but actually
	// starting spec.start's goroutine would repeat a real-world side effect
	// that already happened once, live (ADR 0010). A no-op cancel and nil
	// incoming are indistinguishable, from the interpreter core's side, from
	// an invocation that simply never receives any "#_<invokeid>" traffic.
	if spec.declarative {
		factory := in.invokeHandlers[canonicalInvokeType(request.Type)]
		if factory == nil {
			return nil, nil, invokeHandlerUnavailableError{request.Type}
		}
		if in.suppressInvoke.Load() {
			return func() {}, nil, nil
		}
		handler, err := makeInvokeHandler(factory, request.Type)
		if err != nil {
			return nil, nil, err
		}
		request = cloneInvokeRequest(request)
		cancel, incoming = in.runInvokeGoroutine(request.ID, func(ctx context.Context, io InvokeIO) (Value, error) {
			return handler.Start(ctx, request, io)
		})
		return cancel, incoming, nil
	}
	if in.suppressInvoke.Load() {
		return func() {}, nil, nil
	}
	cancel, incoming = in.runInvokeGoroutine(request.ID, func(ctx context.Context, io InvokeIO) (Value, error) {
		return spec.start(ctx, request.Data, io)
	})
	return cancel, incoming, nil
}

type invokeHandlerUnavailableError struct{ typ Identifier }

func (e invokeHandlerUnavailableError) Error() string {
	return fmt.Sprintf("statecharts: no invoke handler registered for type %q", e.typ)
}

func cloneInvokeRequest(request InvokeRequest) InvokeRequest {
	request.Data = request.Data.Clone()
	return request
}

func makeInvokeHandler(factory InvokeHandlerFactory, typ Identifier) (handler InvokeHandler, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			handler = nil
			err = fmt.Errorf("statecharts: invoke handler factory for %q panicked: %v", typ, recovered)
		}
	}()
	handler = factory()
	if handler == nil {
		return nil, fmt.Errorf("statecharts: invoke handler factory for %q returned nil", typ)
	}
	return handler, nil
}

// resumeInvokesAfterReplay runs on the actor's own goroutine as the second
// half of reqFinishReplay once replay has caught up (see replay.go): for
// every invocation ip.invokesByID still holds, in the same deterministic
// order applyInvokeSideEffects already uses, it either reports
// error.communication (no Resume configured) or calls resumeInvoke and
// records the real cancel/incoming it returns onto the runningInvoke in
// place.
//
// This must run here, not on Rehydrate's own calling goroutine: a
// runningInvoke's fields are exactly as single-goroutine-owned as the rest
// of interpretation's state, and resumeInvoke's spawned goroutine can call
// Deliver -- reaching this Instance's inbox, and from there this same actor
// goroutine, via a completely ordinary Send -- the moment Resume returns,
// which for an error or already-finished outcome can be almost immediately.
// Reconciling from any other goroutine would race that delivery against
// whichever runningInvoke it still needed to finish updating.
func (in *Instance) resumeInvokesAfterReplay() {
	for _, id := range sortedInvokeIDs(in.ip.invokesByID) {
		ri, ok := in.ip.invokesByID[id]
		if !ok {
			// Resolving an earlier id in this same pass already cancelled
			// this one, e.g. by exiting a parallel region both belonged to.
			continue
		}
		spec := ri.spec
		if !spec.declarative && spec.resume == nil {
			in.ip.enqueueInternal(Event{
				Name:     ErrEventCommunication,
				Type:     EventPlatform,
				InvokeID: id,
				Data:     PlatformErrorValue(ErrEventCommunication, fmt.Errorf("statecharts: Rehydrate: invoke %q was active before restart; its continuation cannot be guaranteed", id)),
			})
			in.ip.runToStable()
			continue
		}
		// _event is unbound here (SCXML 5.10.1), not the tail of replay.
		ec := in.ip.execContext()
		ec.event, ec.hasEvent = Event{}, false
		request, requestOK := in.ip.evaluateInvokeResumeRequest(ri, ec)
		if !requestOK {
			in.ip.runToStable()
			continue
		}
		var run func(context.Context, InvokeIO) (Value, error)
		if spec.declarative {
			factory := in.invokeHandlers[canonicalInvokeType(request.Type)]
			if factory == nil {
				in.ip.reportError(invokeHandlerUnavailableError{request.Type})
				in.ip.runToStable()
				continue
			}
			handler, err := makeInvokeHandler(factory, request.Type)
			if err != nil {
				in.ip.enqueueInternal(Event{Name: ErrEventCommunication, Type: EventPlatform, InvokeID: id, Data: PlatformErrorValue(ErrEventCommunication, err)})
				in.ip.runToStable()
				continue
			}
			resumable, ok := handler.(ResumableInvokeHandler)
			if !ok {
				in.ip.enqueueInternal(Event{
					Name: ErrEventCommunication, Type: EventPlatform, InvokeID: id,
					Data: PlatformErrorValue(ErrEventCommunication, fmt.Errorf("statecharts: Rehydrate: invoke %q handler type %q cannot resume", id, request.Type)),
				})
				in.ip.runToStable()
				continue
			}
			request = cloneInvokeRequest(request)
			run = func(ctx context.Context, io InvokeIO) (Value, error) { return resumable.Resume(ctx, request, io) }
		} else {
			run = func(ctx context.Context, io InvokeIO) (Value, error) { return spec.resume(ctx, id, request.Data, io) }
		}
		cancel, incoming := in.runInvokeGoroutine(id, run)
		ri.cancel = cancel
		ri.incoming = incoming
	}
}

// runInvokeGoroutine is the machinery startInvoke and resumeInvoke share:
// building ctx/InvokeIO, spawning the goroutine that runs run, recovering a
// panic, and turning run's return into done.invoke.<id> or
// error.communication via Deliver, exactly as SCXML 6.4.3 requires of an
// InvokeFunc's own return. It is written once so a resumed invocation is
// genuinely "the same invocation, still going" rather than a parallel
// mechanism with its own edge cases to keep in sync by hand. ctx is
// cancelled by the returned cancel func, which interpretation.cancelInvokes
// calls as part of exiting the invoking state (SCXML 6.4.2); run observing
// ctx already done by the time it returns generates neither event.
func (in *Instance) runInvokeGoroutine(id Identifier, run func(context.Context, InvokeIO) (Value, error)) (cancel func(), incoming chan<- Event) {
	ctx, cancelFn := context.WithCancel(context.Background())
	inbound := make(chan Event, invokeIncomingBuffer)
	io := InvokeIO{
		Deliver: func(ev Event) {
			if ctx.Err() != nil {
				return
			}
			ev = cloneEvent(ev)
			ev.InvokeID = id
			ev.Type = EventExternal
			_ = in.Deliver(ctx, ev)
		},
		Incoming: inbound,
	}

	go func() {
		defer func() {
			if r := recover(); r != nil && ctx.Err() == nil {
				_ = in.Deliver(context.Background(), Event{
					Name: ErrEventCommunication, Type: EventPlatform, InvokeID: id,
					Data: PlatformErrorValue(ErrEventCommunication, fmt.Errorf("statecharts: invoke %q panicked: %v", id, r)),
				})
			}
		}()

		data, err := run(ctx, io)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			_ = in.Deliver(context.Background(), Event{
				Name: ErrEventCommunication, Type: EventPlatform, InvokeID: id, Data: PlatformErrorValue(ErrEventCommunication, err),
			})
			return
		}
		_ = in.Deliver(context.Background(), Event{Name: Identifier("done.invoke." + string(id)), Type: EventExternal, InvokeID: id, Data: data.Clone()})
	}()

	return cancelFn, inbound
}

// actorClock wraps a real Clock so that AfterFunc callbacks -- which fire on
// a background timer goroutine -- are handed off to the actor's own
// goroutine instead of mutating interpreter state directly. This is what
// keeps interpretation's single-mutator invariant true even though real
// timers run on their own goroutine.
type actorClock struct {
	real  Clock
	inbox chan actorRequest
	done  chan struct{}
}

func (c *actorClock) Now() time.Time { return c.real.Now() }

func (c *actorClock) AfterFunc(d time.Duration, f func()) func() bool {
	return c.real.AfterFunc(d, func() {
		req := actorRequest{kind: reqTimerFired, fn: f}
		select {
		case c.inbox <- req:
		case <-c.done:
		}
	})
}

// activatePendingTimers switches the interpreter to clock, then either fires
// each overdue pending send or arms it for its remaining delay. It runs only
// on the actor goroutine. Processing each overdue send to stability before
// considering the next preserves normal timer ordering: an earlier timer's
// transition can still cancel a later one before that later send fires.
func (in *Instance) activatePendingTimers(clock Clock) error {
	in.clock = clock
	in.ip.clock = &actorClock{real: clock, inbox: in.inbox, done: in.doneCh}

	records := make([]*pendingSendRecord, 0, len(in.ip.pending))
	for rec := range in.ip.pending {
		records = append(records, rec)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].fireAt.Equal(records[j].fireAt) {
			return records[i].order < records[j].order
		}
		return records[i].fireAt.Before(records[j].fireAt)
	})

	for _, rec := range records {
		if !in.ip.pending[rec] {
			continue // an earlier overdue send cancelled or replaced it
		}
		if rec.stop != nil {
			rec.stop()
			rec.stop = nil
		}

		delay := rec.fireAt.Sub(clock.Now())
		if delay > 0 {
			rec.stop = in.ip.clock.AfterFunc(delay, func() { in.ip.handleTimerFire(rec) })
			continue
		}

		in.ip.handleTimerFire(rec)
		if err := in.takeTimerHookError(); err != nil {
			return err
		}
		in.drainQueuedEvents()
		if !in.ip.running {
			break
		}
	}
	return nil
}

// Checkpoint calls save with a stable Snapshot on the Instance's own
// goroutine. No event or timer can be applied until save returns, allowing a
// durable runtime to pair the Snapshot atomically with its current Log
// sequence. A successful save stops the Instance at that exact boundary; a
// failed save leaves it running so checkpointing can be retried. Because save
// runs on the actor goroutine, it must not call back into this Instance.
func (in *Instance) Checkpoint(ctx context.Context, save func(Snapshot) error) error {
	req := actorRequest{kind: reqCheckpoint, checkpoint: save, reply: make(chan error, 1)}
	if err := in.submit(ctx, req); err != nil {
		return err
	}
	return in.awaitReply(ctx, req.reply)
}

func (in *Instance) takeTimerHookError() error {
	if in.ip.hookErr == nil {
		return nil
	}
	err := in.ip.hookErr
	in.ip.hookErr = nil
	in.setTerminalErr(err)
	in.ip.running = false
	return err
}

func (in *Instance) drainQueuedEvents() {
	for in.ip.running && in.ip.processNextExternal() {
	}
	if in.ip.running {
		in.ip.runToStable()
	}
}

// finishReplay atomically makes delayed sends live and reconciles invocations.
// An overdue timer may exit an invoking state or finish the whole chart, so
// both steps belong to one actor request and invocation resumption runs only
// if the chart remains active afterward.
func (in *Instance) finishReplay(clock Clock) error {
	if err := in.activatePendingTimers(clock); err != nil {
		return err
	}
	if in.ip.running {
		in.resumeInvokesAfterReplay()
	}
	return nil
}

// Start spawns the interpreter goroutine, enters the chart's initial
// configuration, and drains to the first stable point before returning.
// ctx bounds only this handshake, not the goroutine's subsequent lifetime.
func (in *Instance) Start(ctx context.Context) error {
	if !in.started.CompareAndSwap(false, true) {
		return fmt.Errorf("statecharts: instance already started")
	}
	go in.run()
	select {
	case <-in.readyCh:
		return nil
	case <-in.doneCh:
		return in.Err()
	case <-ctx.Done():
		// Start returning an error must not leave a session running behind
		// the caller's back. The actor goroutine remains the sole owner of
		// model execution and closes the session after it can process Stop.
		go func() { _ = in.Stop(context.Background()) }()
		return ctx.Err()
	}
}

func (in *Instance) run() {
	defer close(in.doneCh)
	defer in.closeDatamodelSession()
	defer func() {
		if r := recover(); r != nil {
			in.setTerminalErr(fmt.Errorf("statecharts: instance panic: %v", r))
		}
	}()

	if !in.ip.restored {
		in.ip.start()
	} else if !in.deferTimerActivation.Load() {
		if err := in.activatePendingTimers(in.clock); err != nil {
			in.ip.exitInterpreter()
			in.publishConfig()
			return
		}
	}
	in.drainQueuedEvents()
	if !in.ip.running {
		in.ip.exitInterpreter()
	}
	in.publishConfig()
	close(in.readyCh)

	for in.ip.running {
		req, ok := <-in.inbox
		if !ok {
			in.ip.running = false
			break
		}

		var snapOut chan snapshotResult
		var invokesOut chan bool
		var reqErr error
		switch req.kind {
		case reqStop:
			in.ip.running = false
		case reqSend:
			// Revalidate invocation ownership on the actor. Deliver may have
			// passed its optimistic ctx check and then waited here while the
			// invoking state's onexit cancelled and removed the invocation.
			if req.event.InvokeID != "" && in.ip.invokesByID[req.event.InvokeID] == nil {
				break
			}
			if in.ingressHook != nil {
				reqErr = in.ingressHook(req.event)
			}
			if reqErr == nil {
				in.ip.enqueue(req.event)
			} else {
				reqErr = fmt.Errorf("statecharts: ingress hook: %w", reqErr)
				in.setTerminalErr(reqErr)
				in.ip.running = false
			}
		case reqTimerFired:
			req.fn()
			reqErr = in.takeTimerHookError()
		case reqSnapshot:
			snapOut = req.snapOut
		case reqCheckpoint:
			var snap Snapshot
			snap, reqErr = in.buildSnapshot()
			if reqErr == nil {
				reqErr = req.checkpoint(snap)
			}
			if reqErr == nil {
				in.ip.running = false
			}
		case reqActiveInvokes:
			invokesOut = req.invokesOut
		case reqReplayTimerFired:
			if in.ip.replayTimerFire(req.entry.SendID, req.entry.Target, req.entry.Type, req.entry.Event) {
				// This fire is already in the log, so bypass the live hook:
				// replay must not append the same timer_fired entry again.
			} else {
				// The log is authoritative even if a checkpoint or chart drift
				// left no matching pending record to recompute the dispatch.
				in.ip.dispatchNow(req.entry.SendID, req.entry.Target, req.entry.Type, req.entry.Event)
			}
		case reqFinishReplay:
			reqErr = in.finishReplay(req.clock)
		}
		in.drainQueuedEvents()
		if !in.ip.running {
			in.ip.exitInterpreter()
		}
		in.publishConfig()

		if snapOut != nil {
			snap, err := in.buildSnapshot()
			snapOut <- snapshotResult{snap, err}
		}
		if invokesOut != nil {
			invokesOut <- len(in.ip.invokesByID) > 0
		}

		// Reply only after the resulting macrostep(s) are fully processed
		// and Configuration() published, not merely after acceptance into
		// the queue -- for a single, synchronous, in-process actor like
		// this one, that stronger guarantee is free to provide and removes
		// an entire class of "did my Send take effect yet" races for
		// callers, without contradicting SCXML (which leaves the timing of
		// local delivery confirmation implementation-defined; only
		// cross-session IOProcessor dispatch is genuinely asynchronous
		// here).
		if req.reply != nil {
			req.reply <- reqErr
		}
	}
}

func (in *Instance) closeDatamodelSession() {
	in.closeSessionOnce.Do(func() {
		if in.session == nil {
			return
		}
		if err := closeSession(in.session); err != nil && in.Err() == nil {
			in.setTerminalErr(fmt.Errorf("statecharts: close datamodel session: %w", err))
		}
	})
}

func closeSession(session DatamodelSession) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("datamodel session close panicked: %v", recovered)
		}
	}()
	return session.Close()
}

// Send places ev on the external queue, regardless of its incoming Type, and
// blocks until the resulting macrostep(s) have been fully processed and
// Configuration() reflects them, honoring ctx.
func (in *Instance) Send(ctx context.Context, ev Event) error {
	ev.Type = EventExternal
	return in.send(ctx, ev)
}

func (in *Instance) send(ctx context.Context, ev Event) error {
	req := actorRequest{kind: reqSend, event: cloneEvent(ev), reply: make(chan error, 1)}
	if err := in.submit(ctx, req); err != nil {
		return err
	}
	return in.awaitReply(ctx, req.reply)
}

// Stop is modeled as SCXML's own cancellation: a message on the same
// ingress path as any Send, giving it the same FIFO ordering guarantee
// relative to in-flight Sends that a second, separate channel could not.
// Stopping an already-stopped Instance is not an error.
func (in *Instance) Stop(ctx context.Context) error {
	if in.started.CompareAndSwap(false, true) {
		in.closeDatamodelSession()
		close(in.readyCh)
		close(in.doneCh)
		return in.Err()
	}
	req := actorRequest{kind: reqStop, reply: make(chan error, 1)}
	err := in.submit(ctx, req)
	if err == nil {
		err = in.awaitReply(ctx, req.reply)
	}
	if errors.Is(err, ErrInstanceStopped) {
		return nil
	}
	return err
}

// ErrInstanceStopped is returned by Send (and, internally, treated as
// success by Stop) when a message could not be confirmed as accepted by a
// live interpreter goroutine -- either the goroutine had already exited, or
// it exited in the narrow window between the message being buffered and
// the goroutine actually dequeuing it. It is distinct from Err(): the
// instance's terminal error may well be nil (a clean stop), but that must
// never be confused with "this particular message was processed".
var ErrInstanceStopped = errors.New("statecharts: instance stopped")

func (in *Instance) submit(ctx context.Context, req actorRequest) error {
	select {
	case in.inbox <- req:
		return nil
	case <-in.doneCh:
		return ErrInstanceStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

// awaitReply waits for the actor to confirm processing of a request already
// accepted into the inbox. Reaching doneCh does not by itself mean the
// request was dropped: the actor always writes to reply (a buffer-1
// channel) strictly before its final close(doneCh), so a non-blocking
// re-check of reply after doneCh fires reliably distinguishes "processed,
// just lost the select race against doneCh" from "genuinely never
// dequeued".
func (in *Instance) awaitReply(ctx context.Context, reply chan error) error {
	select {
	case err := <-reply:
		return err
	case <-in.doneCh:
		select {
		case err := <-reply:
			return err
		default:
			return ErrInstanceStopped
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Wait blocks until the interpreter goroutine has exited (a top-level final
// state was reached, Stop was called, or a fatal error occurred), honoring
// ctx, and returns the terminal error (nil on clean exit).
func (in *Instance) Wait(ctx context.Context) error {
	select {
	case <-in.doneCh:
		return in.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Result returns the data produced by the top-level final state. It is
// available after the Instance has completed naturally; a stopped or still
// running Instance has no final result.
func (in *Instance) Result() (Value, error) {
	select {
	case <-in.doneCh:
		if err := in.Err(); err != nil {
			return Value{}, err
		}
		if !in.ip.completed {
			return Value{}, fmt.Errorf("statecharts: instance stopped without reaching a top-level final state")
		}
		return in.ip.result.Clone(), nil
	default:
		return Value{}, fmt.Errorf("statecharts: instance is still running")
	}
}

// Done returns a channel that's closed once the interpreter goroutine has
// exited -- a top-level final state was reached, Stop was called, or a
// fatal error occurred -- the same condition Wait blocks on. Unlike Wait,
// Done never blocks by itself: it's meant for a non-blocking check (a
// select against it with a default case) or as one case among several in a
// larger select, e.g. an embedder deciding whether an Instance that has
// stopped on its own is now safe to discard.
func (in *Instance) Done() <-chan struct{} {
	return in.doneCh
}

// Configuration returns the active state IDs as of the last completed
// macrostep.
func (in *Instance) Configuration() []Identifier {
	if p := in.config.Load(); p != nil {
		return *p
	}
	return nil
}

// HasActiveInvokes reports whether at least one <invoke> currently belongs
// to this Instance's active configuration -- true for as long as its
// invoking state remains entered, regardless of whether the invoked service
// itself has already finished (SCXML 6.4.2: cancellation on state exit, not
// on the service's own completion, is what ends an invocation). Snapshot
// and Rehydrate cannot capture or restart a real invocation (ADR 0010), so
// the actors package uses this to avoid paging out (and later Rehydrating)
// an actor while one is running -- see System's eviction paths.
func (in *Instance) HasActiveInvokes(ctx context.Context) (bool, error) {
	req := actorRequest{kind: reqActiveInvokes, invokesOut: make(chan bool, 1)}
	if err := in.submit(ctx, req); err != nil {
		return false, err
	}
	select {
	case v := <-req.invokesOut:
		return v, nil
	case <-in.doneCh:
		select {
		case v := <-req.invokesOut:
			return v, nil
		default:
			return false, ErrInstanceStopped
		}
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// Err returns the sticky terminal error, or nil while the instance is
// healthy (including while it is still running).
func (in *Instance) Err() error {
	if p := in.terminalErr.Load(); p != nil {
		return *p
	}
	return nil
}

func (in *Instance) setTerminalErr(err error) {
	in.terminalErr.Store(&err)
}

func (in *Instance) publishConfig() {
	cfg := in.ip.activeStates()
	in.config.Store(&cfg)
}

// Deliver implements Dispatcher, letting an attached IOProcessor feed
// events back into this Instance exactly as any other caller would via
// Send.
func (in *Instance) Deliver(ctx context.Context, ev Event) error {
	return in.send(ctx, ev)
}
