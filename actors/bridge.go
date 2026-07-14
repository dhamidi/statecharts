package actors

import (
	"context"
	"fmt"
	"sync"

	"github.com/dhamidi/statecharts"
)

// Bridge is an in-process SCXML peer (see WithSCXMLPeer) that connects two
// Systems. Send accepts only targets qualified by the target System's node
// name, parses the actor ID before "@", and delivers it to target via Tell.
// For a target System without a node name, Bridge uses its configured target
// node.
// An actor in system A reaches an actor named "billing" in system B by
// addressing "billing@warehouse-b", once A is built with:
//
//	sysA := actors.NewSystem(actors.WithSCXMLPeer(
//		actors.NewBridge("warehouse-b", sysB, "warehouse-a"),
//	))
//
// Events from a node-named source System use the sender's already-qualified
// routing key as Origin. For a source System without a node name, Bridge
// appends its configured source node with "@". A reply from inside
// the target System is then another Send targeting ev.Origin. Two Systems
// bridged this way need one Bridge each, one per direction.
//
// Wiring two Systems together this way is inherently circular -- each
// Bridge's target is the other System, but neither System can finish being
// built (WithSCXMLPeer wants a complete IOProcessor) before the other one
// already exists. NewBridge accepts a nil target to break the cycle: build
// both Systems with a Bridge apiece, target nil, then SetTarget each Bridge
// once both Systems exist, before either receives any traffic.
type Bridge struct {
	targetNode statecharts.Identifier
	sourceNode statecharts.Identifier

	mu     sync.RWMutex
	target *System
}

// NewBridge returns a Bridge that forwards "<actor-id>@<target-node>" to
// target's actor ID and stamps Origin as "<sender-id>@<source-node>" when
// the source or target System has no WithNodeName configuration of its own.
// target may be nil and filled in later with SetTarget.
func NewBridge(targetNode statecharts.Identifier, target *System, sourceNode statecharts.Identifier) *Bridge {
	return &Bridge{targetNode: targetNode, target: target, sourceNode: sourceNode}
}

// SetTarget replaces the System b forwards into. It exists to complete a
// Bridge built with a nil target (see Bridge's own doc comment) once that
// System exists. Safe to call concurrently with Send -- both take the same
// lock -- but a Send already in flight against a nil target still fails
// with "unknown actor", so call SetTarget before b starts receiving
// traffic rather than racing the two.
func (b *Bridge) SetTarget(target *System) {
	b.mu.Lock()
	b.target = target
	b.mu.Unlock()
}

func (b *Bridge) targetSystem() *System {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.target
}

// Attach implements statecharts.IOProcessor. A Bridge is never itself the
// IOProcessor an Instance is constructed with -- it only ever receives a
// Send handed to it by a System's own routing IOProcessor (see
// WithSCXMLPeer) -- so there is no Dispatcher for it to capture here.
func (b *Bridge) Attach(statecharts.Dispatcher) {}

// Send implements statecharts.IOProcessor. It synchronously validates the
// target node and actor, then queues delivery on the source System's ordered
// dispatcher. This keeps Send non-blocking, preserves sender order, makes
// source shutdown wait for accepted work, and lets a late delivery failure
// return to the sending actor. A direct call without source routing context
// instead uses the target System's dispatcher and cannot report late errors.
func (b *Bridge) Send(ctx context.Context, req statecharts.SendRequest) error {
	return b.send(ctx, req, nil)
}

// SendWithAck acknowledges after the target System has accepted the event
// through its ingress WAL/dedup boundary. The work uses the same source FIFO
// as Send, preserving ordering between acknowledged and ordinary traffic.
func (b *Bridge) SendWithAck(ctx context.Context, req statecharts.SendRequest, complete func(error)) error {
	return b.send(ctx, req, complete)
}

func (b *Bridge) send(ctx context.Context, req statecharts.SendRequest, complete func(error)) error {
	if req.Type != statecharts.SCXMLEventProcessor {
		return unsupportedPeerTypeError{typ: req.Type}
	}
	target := b.targetSystem()
	if target == nil {
		return fmt.Errorf("actors: Bridge: unknown actor %q (no target system configured)", req.Target)
	}
	node := string(b.targetNode)
	if target.cfg.nodeName != "" {
		node = target.cfg.nodeName
	}
	name, targetNode, ok := splitRoutingKey(req.Target)
	if !ok || targetNode != node {
		return fmt.Errorf("actors: Bridge: %q is not addressed to target node %q", req.Target, node)
	}
	if _, _, ok := target.resolveTarget(name); !ok {
		return fmt.Errorf("actors: Bridge: unknown actor %q in target system", name)
	}

	route, routed := actorRouteFrom(ctx)
	sender := route.address
	origin := b.sourceNode
	if routed && route.system != nil && route.system.cfg.nodeName != "" {
		origin = sender
	} else if sender != "" {
		origin = routingKey(sender, string(b.sourceNode))
	}
	ev := statecharts.Event{
		Name:       req.Event,
		Type:       statecharts.EventExternal,
		Data:       req.Data,
		SendID:     req.EventSendID,
		Origin:     origin,
		OriginType: statecharts.SCXMLEventProcessor,
		DeliveryID: req.DeliveryID,
	}

	job := func() {
		err := target.deliver(context.Background(), name, ev)
		if complete != nil {
			complete(err)
		} else if err != nil && routed && route.system != nil {
			route.system.reportDeliveryFailure(context.Background(), route.dispatcher, route.sendID, req.Target, err)
		}
	}
	if routed && route.system != nil {
		return route.system.enqueueDispatch(job)
	}
	return target.enqueueDispatch(job)
}

// Cancel implements statecharts.IOProcessor. A Bridge keeps no bookkeeping
// of its own about a Send once it has handed the event's delivery off to
// target (see Send), so there is nothing here for it to cancel.
