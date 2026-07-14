package statecharts

import (
	"context"
	"fmt"
)

// SendRequest is a request to dispatch an event to a genuinely external
// target (Target is neither "" nor "#_internal" -- those two are handled
// directly by the interpreter core, never reaching an IOProcessor, since
// they aren't real I/O). By the time an IOProcessor sees a SendRequest, any
// <send delay="..."> has already elapsed: delay-timer bookkeeping belongs
// to the interpreter core, so Send is always an immediate dispatch request.
type SendRequest struct {
	DeliveryID  DeliveryID
	SendID      Identifier // execution ID, including an implementation-generated ID
	EventSendID Identifier // author-specified ID exposed as _event.sendid; empty if none
	Target      Identifier
	Type        Identifier
	Event       Identifier
	Data        any
}

// AcknowledgingIOProcessor reports stable asynchronous acceptance. complete
// must be called exactly once after the destination has accepted (or rejected)
// the request; returning an error reports a synchronous failure.
type AcknowledgingIOProcessor interface {
	IOProcessor
	SendWithAck(ctx context.Context, req SendRequest, complete func(error)) error
}

// ReplayAwareIOProcessor lets a durable processor reproduce the recorded
// synchronous result of a send while deterministic replay is in progress,
// and hand persisted work back to its transport once replay has caught up.
// Implementations must not perform external I/O from ReplaySend.
type ReplayAwareIOProcessor interface {
	IOProcessor
	ReplaySend(ctx context.Context, req SendRequest) error
	Recover(ctx context.Context) error
}

// SCXMLEventProcessor is the mandatory SCXML Event I/O Processor type and
// the default for a send that does not specify a type.
const SCXMLEventProcessor Identifier = "http://www.w3.org/TR/scxml/#SCXMLEventProcessor"

// Dispatcher lets an IOProcessor deliver events back into whichever
// Instance attached it -- fired invoke results, responses, or messages from
// other sessions. Instance implements Dispatcher.
type Dispatcher interface {
	Deliver(ctx context.Context, ev Event) error
}

// IOProcessor is the seam every real side effect goes through: anything
// that reaches outside the process, or otherwise isn't pure and
// deterministic, is routed through here, so the interpreter core stays
// pure everywhere else. A replay-safe caller can swap in a suppressing
// implementation (see NoopIOProcessor and Rehydrate) without the
// interpreter noticing any difference.
type IOProcessor interface {
	// Attach is called once, before Start, giving the processor a way to
	// deliver events back in later via Dispatcher.
	Attach(d Dispatcher)

	// Send dispatches req immediately. It is called synchronously, in-line,
	// while executing a microstep's action list, to preserve SCXML's
	// lock-step ordering of executable content -- implementations MUST
	// return quickly (hand off to their own goroutine and return) rather
	// than blocking on real delivery.
	Send(ctx context.Context, req SendRequest) error
}

// SendExecutionError marks an IOProcessor.Send failure as an invalid or
// unsupported target/type rather than a delivery failure. The interpreter
// turns marked errors into error.execution; all other Send errors become
// error.communication.
type SendExecutionError interface {
	error
	SendExecutionError()
}

// IOProcessorInfo describes one Event I/O Processor available to the
// current session, per SCXML 5.10's _ioprocessors: Type is the same value
// an action would put in SendOptions.Type / that arrives as SendRequest.Type
// to select this processor, and Location is the address a *different*
// session must set as its own SendOptions.Target in order to reach this
// session through that processor.
type IOProcessorInfo struct {
	Type     Identifier
	Location Location
}

// IOProcessorDescriber is implemented by an IOProcessor that has an address
// to advertise for the current session -- one another session could use to
// reach it. An IOProcessor with no transport of its own (NoopIOProcessor,
// LocalIOProcessor) simply doesn't implement this: ExecContext.IOProcessors
// reports no entries for it rather than guessing at an address.
type IOProcessorDescriber interface {
	IOProcessors() []IOProcessorInfo
}

type noopIOProcessor struct{}

func (noopIOProcessor) Attach(Dispatcher) {}

func (noopIOProcessor) Send(context.Context, SendRequest) error { return nil }

// NoopIOProcessor suppresses all outbound dispatch while reporting success.
// Replaying Instances use it so real-world effects are never repeated when
// reconstructing state from a Log (see replay.go).
var NoopIOProcessor IOProcessor = noopIOProcessor{}

// LocalIOProcessor is the default IOProcessor for a single, non-distributed
// Instance: it has no transport for genuinely external targets and reports
// that honestly, rather than silently discarding. Internal/self dispatch
// never reaches an IOProcessor at all (see SendRequest), so this type has
// nothing to do for that traffic either.
type LocalIOProcessor struct {
	dispatcher Dispatcher
}

// NewLocalIOProcessor returns the default IOProcessor.
func NewLocalIOProcessor() *LocalIOProcessor { return &LocalIOProcessor{} }

func (p *LocalIOProcessor) Attach(d Dispatcher) { p.dispatcher = d }

func (p *LocalIOProcessor) Send(ctx context.Context, req SendRequest) error {
	if req.Type != SCXMLEventProcessor {
		return localUnsupportedSendError{typ: req.Type}
	}
	return fmt.Errorf("statecharts: LocalIOProcessor has no transport for target %q", req.Target)
}

type localUnsupportedSendError struct{ typ Identifier }

func (e localUnsupportedSendError) Error() string {
	return fmt.Sprintf("statecharts: LocalIOProcessor does not support send type %q", e.typ)
}

func (localUnsupportedSendError) SendExecutionError() {}

// IOProcessors advertises this session's mandatory SCXML processor address.
func (p *LocalIOProcessor) IOProcessors() []IOProcessorInfo {
	identified, ok := p.dispatcher.(interface{ ID() SessionID })
	if !ok {
		return nil
	}
	return []IOProcessorInfo{{
		Type:     SCXMLEventProcessor,
		Location: LocationFromIdentifier(Identifier("#_scxml_" + string(identified.ID()))),
	}}
}

// SendEvent returns executable content that schedules delivery of an event
// named name, per opts -- the Go-API equivalent of <send>, for use in
// Then(...)/OnEntry(...)/OnExit(...).
func SendEvent(name Identifier, opts SendOptions) ActionFunc {
	return func(ec ExecContext) error {
		ec.Send(name, opts)
		return nil
	}
}

// CancelSend returns executable content that best-effort cancels a pending
// delayed send by ID -- the Go-API equivalent of <cancel>.
func CancelSend(sendID Identifier) ActionFunc {
	return func(ec ExecContext) error {
		ec.Cancel(sendID)
		return nil
	}
}
