package statecharts

import "fmt"

// ExecContext is passed to every ActionFunc, CondFunc, and DoneDataFunc. It
// exposes the SCXML datamodel touchpoints (_event, the In() predicate) that
// would otherwise be evaluated against an expression language -- since the
// only supported datamodel is Go itself, these become plain method calls
// instead.
type ExecContext struct {
	event             Event
	hasEvent          bool
	datamodel         any
	sessionID         string
	name              string
	platformVariables map[string]any
	active            func(Identifier) bool
	raise             func(Event)
	send              func(name Identifier, opts SendOptions)
	cancel            func(sendID Identifier)
	log               func(label string, data Value)
	reportError       func(error)
	ioProcessors      func() []IOProcessorInfo
}

// Event returns the event currently being processed, if any. Per SCXML
// 5.10.1, _event is unbound before the first event is processed.
func (ec ExecContext) Event() (Event, bool) {
	return ec.event, ec.hasEvent
}

// In implements the SCXML In() predicate: reports whether id is a member of
// the current configuration.
func (ec ExecContext) In(id Identifier) bool {
	if ec.active == nil {
		return false
	}
	return ec.active(id)
}

// Raise enqueues ev on the internal queue, as <raise> would. ev.Type is
// always overwritten to EventInternal.
func (ec ExecContext) Raise(ev Event) {
	if ec.raise == nil {
		return
	}
	ev.Type = EventInternal
	ev.SendID = ""
	ev.Origin = ""
	ev.OriginType = ""
	ev.InvokeID = ""
	ec.raise(ev)
}

// Datamodel returns the raw datamodel value passed to New/Restore, escaping
// the Action[D]/Cond[D] adapters for callers that need untyped access.
func (ec ExecContext) Datamodel() any {
	return ec.datamodel
}

// SessionID returns this session's id, bound for its entire lifetime, per
// SCXML 5.10's _sessionid. See WithSessionID and WithIDGenerator
// (instance.go) for how it is minted or supplied.
func (ec ExecContext) SessionID() string {
	return ec.sessionID
}

// Name returns the SCXML document name, bound for this session's
// entire lifetime, per SCXML 5.10's _name.
func (ec ExecContext) Name() string {
	return ec.name
}

// PlatformVariables returns a fresh binding map for SCXML's protected _x
// system variable. Opaque values are shared, but the map itself is not.
func (ec ExecContext) PlatformVariables() map[string]any {
	result := make(map[string]any, len(ec.platformVariables))
	for k, v := range ec.platformVariables {
		result[k] = v
	}
	return result
}

// IDLocationFunc is the Go datamodel profile's equivalent of SCXML's
// idlocation expression. It synchronously assigns a generated send or invoke
// ID while that element is being evaluated. Returning an error or panicking
// produces error.execution and aborts the element.
type IDLocationFunc func(ExecContext, Identifier) error

// IOProcessors reports every Event I/O Processor available to this session,
// per SCXML 5.10's _ioprocessors -- specifically, the ones with an address
// another session could use to reach this one (see IOProcessorDescriber).
// It is empty/nil if the configured IOProcessor has nothing to advertise.
func (ec ExecContext) IOProcessors() []IOProcessorInfo {
	if ec.ioProcessors == nil {
		return nil
	}
	return ec.ioProcessors()
}

// IOProcessorLocation returns the Location advertised for typ among
// IOProcessors(), the common case of wanting one specific processor's own
// address (e.g. to embed in outgoing event data so a peer can reply) --
// callers don't need to write that lookup loop themselves. ok is false if no
// processor of that Type is advertised, in which case location is the zero
// Location.
func (ec ExecContext) IOProcessorLocation(typ Identifier) (location Location, ok bool) {
	for _, info := range ec.IOProcessors() {
		if info.Type == typ {
			return info.Location, true
		}
	}
	return Location{}, false
}

// Send schedules delivery of an event, mirroring <send>: immediately (if
// opts.Delay is zero) or after opts.Delay elapses. Dispatch failures are
// never returned here -- per SCXML, invalid/unsupported requests become
// error.execution and delivery failures become error.communication on the
// internal queue, rather than Go errors propagated to the caller.
func (ec ExecContext) Send(name Identifier, opts SendOptions) {
	if ec.send != nil {
		ec.send(name, opts)
	}
}

// Cancel best-effort cancels every pending delayed Send with the author-given
// sendID, mirroring <cancel>. An unknown or already-fired sendID is a no-op.
func (ec ExecContext) Cancel(sendID Identifier) {
	if ec.cancel != nil {
		ec.cancel(sendID)
	}
}

// Log records one diagnostic entry, mirroring <log>: label names it, data
// carries canonical data. It is routed to
// whichever Logger the owning Instance was configured with (see
// WithLogger), and is a silent no-op if none was. Unlike Send, Log never
// produces an event another transition can match against, and unlike
// Raise, it never touches either event queue -- it exists purely for a
// human or a log aggregator to read.
func (ec ExecContext) Log(label string, data Value) {
	if ec.log != nil {
		ec.log(label, data)
	}
}

// ActionFunc is executable content: <onentry>/<onexit>/transition content.
// A non-nil error is reported into the chart as an error.execution event
// (SCXML's own error model), not returned to the caller as a Go error.
type ActionFunc func(ExecContext) error

// CondFunc is a transition guard (the "cond" attribute). A panic is treated
// as false and reported to the chart as error.execution, per SCXML 5.9.1.
// Conditions that need ordinary, expected failure handling should still
// express it as state plus a preceding action instead.
type CondFunc func(ExecContext) bool

// DoneDataFunc produces the canonical payload for a final state's done event.
type DoneDataFunc func(ExecContext) Value

// actionBlock is one SCXML block of executable content. An error skips the
// rest of this block without affecting later blocks (for example, a second
// <onentry> handler on the same state).
type actionBlock []ActionFunc

// Action adapts a callback operating on the chart's concrete datamodel type
// D into an ActionFunc. A chart is bound to exactly one D for its entire
// life; a mismatch (only reachable via programmer error pairing the wrong
// Instance datamodel with this chart) is reported as an error rather than a
// panic.
func Action[D any](fn func(*D, ExecContext) error) ActionFunc {
	return func(ec ExecContext) error {
		d, ok := ec.datamodel.(*D)
		if !ok {
			var zero D
			return fmt.Errorf("statecharts: Action[%T]: datamodel is %T, not *%T", zero, ec.datamodel, zero)
		}
		return fn(d, ec)
	}
}

// Cond adapts a typed guard callback into a CondFunc. A datamodel type
// mismatch is an expression-evaluation error: it evaluates to false and
// reports error.execution when run by an interpreter.
func Cond[D any](fn func(*D, ExecContext) bool) CondFunc {
	return func(ec ExecContext) bool {
		d, ok := ec.datamodel.(*D)
		if !ok {
			if ec.reportError != nil {
				var zero D
				ec.reportError(fmt.Errorf("statecharts: Cond[%T]: datamodel is %T, not *%T", zero, ec.datamodel, zero))
			}
			return false
		}
		return fn(d, ec)
	}
}
