package statecharts

import "context"

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

// InvokeSpec is the uncompiled description of one <invoke> attached to a
// state, built via Invoke and InvokeOptions.
type InvokeSpec struct {
	ID       Identifier // empty = auto-generated ("<stateid>.invoke<n>") at invoke time
	Start    InvokeFunc
	Params   func(ExecContext) any // evaluated once, synchronously, when the invocation starts; nil => nil params
	Finalize []ActionFunc
}

// InvokeOption configures an InvokeSpec being built by Invoke.
type InvokeOption func(*InvokeSpec)

// WithInvokeID sets an explicit invoke ID, e.g. for targeting it later via
// SendOptions{Target: "#_" + id}. Left unset, an ID unique within the
// session is generated when the invocation actually starts.
func WithInvokeID(id Identifier) InvokeOption {
	return func(s *InvokeSpec) { s.ID = id }
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
// invoked service returns before any transition's guard inspects it.
func WithFinalize(actions ...ActionFunc) InvokeOption {
	return func(s *InvokeSpec) { s.Finalize = append(s.Finalize, actions...) }
}

// Invoke attaches an external service instance to a state: SCXML's
// <invoke>, minus the child-SCXML-session-specific machinery (ADR 0005)
// that's out of scope here -- fn is any Go function willing to run in its
// own goroutine and talk back through InvokeIO.
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
	id       Identifier
	start    InvokeFunc
	params   func(ExecContext) any
	finalize []ActionFunc
}

// runningInvoke is the interpreter-core bookkeeping for one active
// invocation -- enough to cancel it (SCXML 6.4.2) and to route a matching
// finalize handler when a reply carrying its InvokeID arrives (SCXML 6.5).
// It is deliberately independent of however the invoked service was
// actually started (see invokeRunnerFunc): cancel and incoming are plain
// callbacks/channels, not a reference back to whatever goroutine or
// Instance is on the other end.
type runningInvoke struct {
	id       Identifier
	state    *compiledState
	finalize []ActionFunc
	cancel   func()
	incoming chan<- Event
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
