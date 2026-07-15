package statecharts

import (
	"time"
)

// ExecContext exposes the runtime context supplied to datamodel operations.
type ExecContext struct {
	event             Event
	hasEvent          bool
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

type compiledAction struct {
	op *compiledOperation
}

type compiledOperation struct {
	kind        ExecutableKind
	expressions []CompiledExpression
	static      []string
	blocks      [][]actionBlock
	bindings    IterationBindings
	payload     *compiledPayload
	delay       time.Duration
}

type compiledPayload struct {
	params     []compiledParam
	content    CompiledExpression
	hasContent bool
}
type compiledParam struct {
	name       Identifier
	expression CompiledExpression
}
type compiledData struct {
	location       CompiledExpression
	initializer    CompiledExpression
	hasInitializer bool
}

// actionBlock is one statechart block of executable content. An error skips
// the rest of this block without affecting later blocks.
type actionBlock []compiledAction
