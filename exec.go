package statecharts

import "fmt"

// ExecContext is passed to every ActionFunc, CondFunc, and DoneDataFunc. It
// exposes the SCXML datamodel touchpoints (_event, the In() predicate) that
// would otherwise be evaluated against an expression language -- since the
// only supported datamodel is Go itself, these become plain method calls
// instead.
type ExecContext struct {
	event     Event
	hasEvent  bool
	datamodel any
	active    func(Identifier) bool
	raise     func(Event)
	send      func(name Identifier, opts SendOptions)
	cancel    func(sendID Identifier)
	log       func(label string, data any)
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
	ec.raise(ev)
}

// Datamodel returns the raw datamodel value passed to New/Restore, escaping
// the Action[D]/Cond[D] adapters for callers that need untyped access.
func (ec ExecContext) Datamodel() any {
	return ec.datamodel
}

// Send schedules delivery of an event, mirroring <send>: immediately (if
// opts.Delay is zero) or after opts.Delay elapses. Dispatch failures are
// never returned here -- per SCXML, they are surfaced as an
// error.communication event on the internal queue, exactly like any other
// platform event, not as a Go error propagated to the caller.
func (ec ExecContext) Send(name Identifier, opts SendOptions) {
	if ec.send != nil {
		ec.send(name, opts)
	}
}

// Cancel best-effort cancels a previously scheduled delayed Send, mirroring
// <cancel>. An unknown or already-fired sendID is silently a no-op.
func (ec ExecContext) Cancel(sendID Identifier) {
	if ec.cancel != nil {
		ec.cancel(sendID)
	}
}

// Log records one diagnostic entry, mirroring <log>: label names it, data
// carries whatever value the call site wants recorded. It is routed to
// whichever Logger the owning Instance was configured with (see
// WithLogger), and is a silent no-op if none was. Unlike Send, Log never
// produces an event another transition can match against, and unlike
// Raise, it never touches either event queue -- it exists purely for a
// human or a log aggregator to read.
func (ec ExecContext) Log(label string, data any) {
	if ec.log != nil {
		ec.log(label, data)
	}
}

// ActionFunc is executable content: <onentry>/<onexit>/transition content.
// A non-nil error is reported into the chart as an error.execution event
// (SCXML's own error model), not returned to the caller as a Go error.
type ActionFunc func(ExecContext) error

// CondFunc is a transition guard (the "cond" attribute). Evaluation errors
// have no Go representation here -- write the guard so it cannot panic or
// signal failure; a guard that must be able to fail should be expressed as
// state plus a preceding action instead.
type CondFunc func(ExecContext) bool

// DoneDataFunc produces the payload for a final state's done event.
type DoneDataFunc func(ExecContext) any

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
// mismatch evaluates to false rather than panicking, consistent with
// CondFunc's "cannot signal failure" contract.
func Cond[D any](fn func(*D, ExecContext) bool) CondFunc {
	return func(ec ExecContext) bool {
		d, ok := ec.datamodel.(*D)
		if !ok {
			return false
		}
		return fn(d, ec)
	}
}
