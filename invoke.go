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

// SCXMLInvokeType is the standard handler type used when an invocation does
// not declare a type. Environments may bind it to a child-chart resolver.
const SCXMLInvokeType Identifier = "http://www.w3.org/TR/scxml/"

// InvokeRequest is the fully evaluated, immutable input to one invocation.
// DefinitionID identifies the declaration across definition edits; ID is the
// runtime address visible to events and #_<invokeid> sends.
type InvokeRequest struct {
	DefinitionID Identifier
	ID           Identifier
	Type         Identifier
	Source       string
	Data         Value
}

// InvokeHandler implements one environment-provided invocation type. The
// runtime cancels ctx when the owning state exits and owns event delivery,
// completion, and error classification around the handler call.
type InvokeHandler interface {
	Start(context.Context, InvokeRequest, InvokeIO) (Value, error)
}

// InvokeHandlerFunc adapts a function to InvokeHandler.
type InvokeHandlerFunc func(context.Context, InvokeRequest, InvokeIO) (Value, error)

func (fn InvokeHandlerFunc) Start(ctx context.Context, request InvokeRequest, io InvokeIO) (Value, error) {
	return fn(ctx, request, io)
}

// ResumableInvokeHandler can reattach to an invocation that was active when
// a durable process stopped. A fresh handler is created for every resume.
type ResumableInvokeHandler interface {
	InvokeHandler
	Resume(context.Context, InvokeRequest, InvokeIO) (Value, error)
}

// InvokeHandlerFactory creates isolated mutable handler state for one start
// or resume. Registrations are Instance- or System-scoped, never global.
type InvokeHandlerFactory func() InvokeHandler

func canonicalInvokeType(typ Identifier) Identifier {
	if typ == "" || typ == "scxml" {
		return SCXMLInvokeType
	}
	return typ
}

// InvokeOption appends serializable authoring data to an invocation.
type InvokeOption func(*InvokeDefinition)

// WithInvokeDefinitionID sets the stable declaration identity used by
// snapshots and hot-deployment revision pinning.
func WithInvokeDefinitionID(id Identifier) InvokeOption {
	return func(invoke *InvokeDefinition) { invoke.DefinitionID = id }
}

// WithInvokeID sets an explicit invoke ID, e.g. for targeting it later via
// SendOptions{Target: "#_" + id}. Left unset, an ID unique within the
// session is generated when the invocation actually starts.
func WithInvokeID(id Identifier) InvokeOption {
	return func(invoke *InvokeDefinition) { invoke.ID = id }
}

// WithInvokeIDLocation stores a generated runtime ID in a model location.
func WithInvokeIDLocation(location Expression) InvokeOption {
	return func(invoke *InvokeDefinition) {
		value := location.Clone()
		invoke.IDLocation = &value
	}
}

// WithInvokeTypeExpression selects a handler type from the datamodel.
func WithInvokeTypeExpression(expression Expression) InvokeOption {
	return func(invoke *InvokeDefinition) {
		value := expression.Clone()
		invoke.Type = ""
		invoke.TypeExpr = &value
	}
}

// WithInvokeSourceExpression selects a handler source from the datamodel.
func WithInvokeSourceExpression(expression Expression) InvokeOption {
	return func(invoke *InvokeDefinition) {
		value := expression.Clone()
		invoke.Src = ""
		invoke.SrcExpr = &value
	}
}

// WithInvokeParams sets named invocation input expressions.
func WithInvokeParams(params ...ParamDefinition) InvokeOption {
	return func(invoke *InvokeDefinition) { invoke.Params = cloneParams(params) }
}

// WithInvokeContent sets one whole invocation input expression.
func WithInvokeContent(content Expression) InvokeOption {
	return func(invoke *InvokeDefinition) {
		value := content.Clone()
		invoke.Content = &value
	}
}

// WithFinalize attaches executable blocks run before processing an event
// returned by this invocation. Each call creates one independent block.
func WithFinalize(actions ...Executable) InvokeOption {
	return func(invoke *InvokeDefinition) {
		invoke.Finalize = append(invoke.Finalize, cloneExecutableBlock(actions))
	}
}

// WithAutoForward forwards each external event to the active invocation.
func WithAutoForward() InvokeOption {
	return func(invoke *InvokeDefinition) { invoke.AutoForward = true }
}

// Invoke attaches a declarative external service to a state. Runtime
// behavior is supplied separately through WithInvokeHandler.
func Invoke(typ, source string, opts ...InvokeOption) StateOption {
	return func(state *StateDefinition) {
		definition := InvokeDefinition{Type: typ, Src: source}
		for _, opt := range opts {
			opt(&definition)
		}
		state.Invokes = append(state.Invokes, definition.clone())
	}
}

// compiledInvoke is the immutable runtime form of one InvokeDefinition.
type compiledInvoke struct {
	definitionID       Identifier
	owner              *compiledState
	id                 Identifier
	staticType         Identifier
	typeExpr           CompiledExpression
	hasTypeExpr        bool
	staticSource       string
	sourceExpr         CompiledExpression
	hasSourceExpr      bool
	payload            *compiledPayload
	modelIDLocation    CompiledExpression
	hasModelIDLocation bool
	finalize           []actionBlock
	autoForward        bool
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
	spec        *compiledInvoke
	typ         Identifier
	source      string
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
type invokeRunnerFunc func(request InvokeRequest, spec *compiledInvoke) (cancel func(), incoming chan<- Event, err error)

func noopInvokeRunner(InvokeRequest, *compiledInvoke) (func(), chan<- Event, error) {
	return func() {}, nil, nil
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

// InvokeChartHandler returns an environment-scoped handler factory for the
// standard child-chart invocation type. The child chart and fallback
// transport remain runtime capabilities; only the invocation source key
// belongs in a serializable InvokeDefinition. Child input should be modeled
// explicitly by the child definition rather than by replacing its model.
func InvokeChartHandler(chart *Chart, baseIO IOProcessor) InvokeHandlerFactory {
	return func() InvokeHandler {
		return InvokeHandlerFunc(func(ctx context.Context, _ InvokeRequest, io InvokeIO) (Value, error) {
			child, err := chart.NewInstance(WithIOProcessor(SCXMLEventProcessor, &parentIOProcessor{deliver: io.Deliver, next: baseIO}))
			if err != nil {
				return Value{}, err
			}

			// Start's own actor goroutine runs regardless of whether Start
			// itself returns early because ctx raced its way to already
			// being cancelled (e.g. the invoking state was exited again
			// immediately) -- so the child must always be stopped, even when
			// Start reports an error, or it would keep running orphaned.
			defer child.Stop(context.Background())

			if err := child.Start(ctx); err != nil {
				return Value{}, err
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
				return Value{}, err
			}
			return child.Result()
		})
	}
}
