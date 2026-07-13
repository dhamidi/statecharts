package statecharts

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// Instance is one running chart: a single goroutine driving the interpreter
// algorithm, exposed through plain method calls. No channel type ever
// appears in this public surface -- Send and Stop internally hand a
// request to the actor's own goroutine and wait for it to be accepted, but
// that plumbing is entirely unexported.
type Instance struct {
	chart *Chart
	ip    *interpretation

	inbox   chan actorRequest
	doneCh  chan struct{}
	readyCh chan struct{}

	started     atomic.Bool
	config      atomic.Pointer[[]Identifier]
	terminalErr atomic.Pointer[error]
}

type actorReqKind uint8

const (
	reqSend actorReqKind = iota
	reqStop
	reqTimerFired
	reqSnapshot
)

type actorRequest struct {
	kind    actorReqKind
	event   Event
	fn      func()        // reqTimerFired only
	reply   chan error    // reqSend/reqStop
	snapOut chan Snapshot // reqSnapshot only
}

// Option configures an Instance built by New or Restore.
type Option func(*instanceConfig)

type instanceConfig struct {
	io             IOProcessor
	clock          Clock
	logger         Logger
	inboxSize      int
	timerFiredHook func(Identifier, Event) error
	idGen          IDGenerator
	sessionID      string
}

// WithIOProcessor sets the IOProcessor used for genuinely external dispatch.
// Defaults to NoopIOProcessor.
func WithIOProcessor(p IOProcessor) Option {
	return func(c *instanceConfig) { c.io = p }
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
func WithSessionID(id string) Option {
	return func(c *instanceConfig) { c.sessionID = id }
}

func defaultInstanceConfig() instanceConfig {
	return instanceConfig{
		io:        NoopIOProcessor,
		clock:     NewRealClock(),
		logger:    NoopLogger,
		inboxSize: 1,
		idGen:     IDGeneratorFunc(rand.Text),
	}
}

// New constructs an Instance for chart, bound to the given datamodel value
// (typically a pointer to the caller's own struct). It assigns the
// Instance's session id: an explicit WithSessionID if given, otherwise one
// minted by the configured IDGenerator (see Instance.ID). The interpreter
// goroutine is not started until Start is called.
func New(chart *Chart, datamodel any, opts ...Option) *Instance {
	cfg := defaultInstanceConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	id := cfg.sessionID
	if id == "" {
		id = cfg.idGen.NewID()
	}
	return newInstance(chart, newInterpretation(chart, datamodel), cfg, id)
}

func newInstance(chart *Chart, ip *interpretation, cfg instanceConfig, id string) *Instance {
	in := &Instance{
		chart:   chart,
		inbox:   make(chan actorRequest, cfg.inboxSize),
		doneCh:  make(chan struct{}),
		readyCh: make(chan struct{}),
	}

	ip.io = cfg.io
	ip.clock = &actorClock{real: cfg.clock, inbox: in.inbox, done: in.doneCh}
	ip.logger = cfg.logger
	ip.timerFiredHook = cfg.timerFiredHook
	ip.startInvoke = in.startInvoke
	ip.sessionID = id
	ip.name = chart.ID()
	in.ip = ip

	cfg.io.Attach(in)
	return in
}

// ID returns this Instance's session id (SCXML 5.10's _sessionid). In order
// of precedence, it is an explicit WithSessionID, an id recorded in the
// Snapshot Restore was called with, or one minted by the configured
// IDGenerator. It is stable for the lifetime of the Instance.
func (in *Instance) ID() string {
	return in.ip.sessionID
}

// invokeIncomingBuffer bounds how many "#_<invokeid>"-addressed events an
// invocation can have queued before dispatchNow starts dropping them
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
func (in *Instance) startInvoke(id Identifier, spec *compiledInvoke, params any) (cancel func(), incoming chan<- Event) {
	ctx, cancelFn := context.WithCancel(context.Background())
	inbound := make(chan Event, invokeIncomingBuffer)
	io := InvokeIO{
		Deliver: func(ev Event) {
			if ctx.Err() != nil {
				return
			}
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
					Data: fmt.Errorf("statecharts: invoke %q panicked: %v", id, r),
				})
			}
		}()

		data, err := spec.start(ctx, params, io)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			_ = in.Deliver(context.Background(), Event{
				Name: ErrEventCommunication, Type: EventPlatform, InvokeID: id, Data: err,
			})
			return
		}
		_ = in.Deliver(context.Background(), Event{
			Name: Identifier("done.invoke." + string(id)), Type: EventExternal, InvokeID: id, Data: data,
		})
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
		return ctx.Err()
	}
}

func (in *Instance) run() {
	defer close(in.doneCh)
	defer func() {
		if r := recover(); r != nil {
			in.setTerminalErr(fmt.Errorf("statecharts: instance panic: %v", r))
		}
	}()

	if !in.ip.restored {
		in.ip.start()
	}
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

		var snapOut chan Snapshot
		switch req.kind {
		case reqStop:
			in.ip.running = false
		case reqSend:
			in.ip.enqueue(req.event)
		case reqTimerFired:
			req.fn()
			if in.ip.hookErr != nil {
				in.setTerminalErr(in.ip.hookErr)
				in.ip.hookErr = nil
				in.ip.running = false
			}
		case reqSnapshot:
			snapOut = req.snapOut
		}
		for in.ip.running && in.ip.processNextExternal() {
		}
		if in.ip.running {
			in.ip.runToStable()
		}
		if !in.ip.running {
			in.ip.exitInterpreter()
		}
		in.publishConfig()

		if snapOut != nil {
			snapOut <- in.buildSnapshot()
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
			req.reply <- nil
		}
	}
}

// Send enqueues ev (routed onto the internal or external queue based on
// ev.Type) and blocks until the resulting macrostep(s) have been fully
// processed and Configuration() reflects them, honoring ctx.
func (in *Instance) Send(ctx context.Context, ev Event) error {
	req := actorRequest{kind: reqSend, event: ev, reply: make(chan error, 1)}
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

// Configuration returns the active state IDs as of the last completed
// macrostep.
func (in *Instance) Configuration() []Identifier {
	if p := in.config.Load(); p != nil {
		return *p
	}
	return nil
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
	return in.Send(ctx, ev)
}
