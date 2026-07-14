package actors

import (
	"context"
	"fmt"
	"strings"

	"github.com/dhamidi/statecharts"
)

func routingKey(actorID statecharts.Identifier, node string) statecharts.Identifier {
	if node == "" {
		return actorID
	}
	return statecharts.Identifier(string(actorID) + "@" + node)
}

func splitRoutingKey(key statecharts.Identifier) (actorID statecharts.Identifier, node string, ok bool) {
	value := string(key)
	separator := strings.IndexByte(value, '@')
	if separator <= 0 || separator == len(value)-1 || strings.Contains(value[separator+1:], "@") {
		return "", "", false
	}
	return statecharts.Identifier(value[:separator]), value[separator+1:], true
}

// actorOriginContextKey carries the source actor's routing state across the
// fallback IOProcessor seam. SendRequest has no sender/Dispatcher fields,
// but an asynchronous Bridge needs both to preserve Origin, report a late
// failure, and register its work with the source System's shutdown.
type actorOriginContextKey struct{}

type actorRouteContext struct {
	address    statecharts.Identifier
	system     *System
	dispatcher statecharts.Dispatcher
	sendID     statecharts.Identifier
}

func withActorOrigin(ctx context.Context, route actorRouteContext) context.Context {
	return context.WithValue(ctx, actorOriginContextKey{}, route)
}

func actorRouteFrom(ctx context.Context) (actorRouteContext, bool) {
	route, ok := ctx.Value(actorOriginContextKey{}).(actorRouteContext)
	return route, ok
}

// routingProcessor is the statecharts.IOProcessor every actor a System
// spawns is constructed with. It is closed over the actor's own name
// (self) and the System, and is the only mechanism through which a chart
// running inside a System can address another actor by name.
//
// Send routes the SCXML processor type by actor name. Custom processor types
// are selected directly by the interpreter and never reach this router; an
// unknown SCXML location is offered to the configured SCXML peer.
// Actually acquiring a local target -- paging it in if necessary, possibly
// evicting another resident actor first to stay within the residency limit --
// and delivering the event is handed off to the System's ordered dispatcher,
// so Send never blocks on the target's own processing. Two actors sending to
// each other at the same instant would otherwise be able to deadlock each
// other's Send call.
type routingProcessor struct {
	sys  *System
	self statecharts.Identifier

	disp statecharts.Dispatcher
}

func newRoutingProcessor(sys *System, self statecharts.Identifier) *routingProcessor {
	return &routingProcessor{sys: sys, self: self}
}

// Attach implements statecharts.IOProcessor, capturing self's own Instance
// as a Dispatcher. It is what lets an asynchronous delivery failure --
// discovered after Send has already returned -- find its way back into
// this actor's own queue: there is no other route back into a session once
// its own dispatchNow call has returned.
func (p *routingProcessor) Attach(d statecharts.Dispatcher) {
	p.disp = d
}

// Send implements statecharts.IOProcessor. See routingProcessor's own doc
// comment for the synchronous/asynchronous split this relies on.
func (p *routingProcessor) Send(ctx context.Context, req statecharts.SendRequest) error {
	localType := req.Type == statecharts.SCXMLEventProcessor
	_, target, ok := p.sys.resolveTarget(req.Target)
	if !localType {
		return unsupportedPeerTypeError{typ: req.Type}
	}
	if !ok {
		if p.sys.cfg.scxmlPeer != nil {
			// self rides along on ctx, not on req: SendRequest has no field for
			// who is calling (an IOProcessor is shared machinery with no notion
			// of "which actor" built into its own signature), and a fallback is
			// one value shared by every actor in sys, so there is nowhere else
			// to attach it without racing concurrent Send calls from different
			// actors against each other. actorRouteFrom recovers it.
			return p.sys.cfg.scxmlPeer.Send(withActorOrigin(ctx, actorRouteContext{
				address: p.self, system: p.sys, dispatcher: p.disp, sendID: req.SendID,
			}), req)
		}
		return fmt.Errorf("actors: unknown actor %q", req.Target)
	}

	ev := statecharts.Event{
		Name:       req.Event,
		Type:       statecharts.EventExternal,
		Data:       req.Data,
		SendID:     req.EventSendID,
		Origin:     p.self,
		OriginType: statecharts.SCXMLEventProcessorAlias,
		DeliveryID: req.DeliveryID,
	}

	origin := p.disp
	return p.sys.enqueueDispatch(func() {
		p.sys.deliverAsync(context.Background(), target, ev, origin, req.SendID)
	})
}

// SendWithAck acknowledges only after target ingress has reached its durable
// WAL/dedup boundary (or ordinary acceptance for an ephemeral target).
func (p *routingProcessor) SendWithAck(ctx context.Context, req statecharts.SendRequest, complete func(error)) error {
	localType := req.Type == statecharts.SCXMLEventProcessor
	_, target, ok := p.sys.resolveTarget(req.Target)
	if localType && !ok && p.sys.cfg.scxmlPeer != nil {
		ctx = withActorOrigin(ctx, actorRouteContext{address: p.self, system: p.sys, dispatcher: p.disp, sendID: req.SendID})
		if ack, supportsAck := p.sys.cfg.scxmlPeer.(statecharts.AcknowledgingIOProcessor); supportsAck {
			return ack.SendWithAck(ctx, req, complete)
		}
	}
	if !localType || !ok {
		err := p.Send(ctx, req)
		if err != nil {
			return err
		}
		complete(nil)
		return nil
	}
	ev := statecharts.Event{Name: req.Event, Type: statecharts.EventExternal, Data: req.Data, SendID: req.EventSendID, Origin: p.self, OriginType: statecharts.SCXMLEventProcessorAlias, DeliveryID: req.DeliveryID}
	return p.sys.enqueueDispatch(func() { complete(p.sys.deliver(context.Background(), target, ev)) })
}

type unsupportedPeerTypeError struct{ typ statecharts.Identifier }

func (e unsupportedPeerTypeError) Error() string {
	return fmt.Sprintf("actors: routing IOProcessor does not support send type %q", e.typ)
}

func (unsupportedPeerTypeError) SendExecutionError() {}

// Cancel implements statecharts.IOProcessor. Delayed-send bookkeeping lives
// in the interpreter core (see clock.go, interpreter.go), never inside an
// IOProcessor, so by the time Cancel could matter for a cross-actor send,
// the sender's own pending-send record is already gone; there is nothing
// left here for the routing processor itself to cancel.
// IOProcessors implements statecharts.IOProcessorDescriber. self is the
// actor's routable ID@node key when the System has a node name, or its local
// actor ID otherwise. Any processors advertised by the fallback remain
// visible through the System's routing composite.
func (p *routingProcessor) IOProcessors() []statecharts.IOProcessorInfo {
	infos := []statecharts.IOProcessorInfo{
		{Type: statecharts.SCXMLEventProcessor, Location: statecharts.LocationFromIdentifier(p.self)},
	}
	if describer, ok := p.sys.cfg.scxmlPeer.(statecharts.IOProcessorDescriber); ok {
		infos = append(infos, describer.IOProcessors()...)
	}
	return infos
}
