package actors

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/dhamidi/statecharts"
)

// Bridge is a fallback IOProcessor (see WithFallback) that connects two
// Systems. Send accepts only targets qualified by the target System's node
// name, strips that prefix, and delivers the rest to target via Tell. For a
// target System without a node name, Bridge uses its configured namespace.
// An actor in system A reaches an actor named "billing" in system B by
// addressing "warehouse-b.billing", once A is built with:
//
//	sysA := actors.NewSystem(actors.WithFallback(
//		actors.NewBridge("warehouse-b", sysB, "warehouse-a"),
//	))
//
// Events from a node-named source System use the sender's already-qualified
// address as Origin. For a source System without a node name, origin is the
// namespace Bridge prepends to the local sender name. A reply from inside
// the target System is then another Send targeting ev.Origin. Two Systems
// bridged this way need one Bridge each, one per direction.
//
// Wiring two Systems together this way is inherently circular -- each
// Bridge's target is the other System, but neither System can finish being
// built (WithFallback wants a complete IOProcessor) before the other one
// already exists. NewBridge accepts a nil target to break the cycle: build
// both Systems with a Bridge apiece, target nil, then SetTarget each Bridge
// once both Systems exist, before either receives any traffic.
type Bridge struct {
	namespace statecharts.Identifier
	origin    statecharts.Identifier

	mu     sync.RWMutex
	target *System
}

// NewBridge returns a Bridge that forwards a Send targeting
// "<namespace>.<rest>" to target's Tell("<rest>", ...), stamping Origin as
// "<origin>.<sender>" on the event target receives. target may be nil,
// filled in later with SetTarget -- see Bridge's own doc comment for why.
func NewBridge(namespace statecharts.Identifier, target *System, origin statecharts.Identifier) *Bridge {
	return &Bridge{namespace: namespace, target: target, origin: origin}
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
// WithFallback) -- so there is no Dispatcher for it to capture here.
func (b *Bridge) Attach(statecharts.Dispatcher) {}

// Send implements statecharts.IOProcessor. It synchronously validates the
// target node and actor, then queues delivery on the source System's ordered
// dispatcher. This keeps Send non-blocking, preserves sender order, makes
// source shutdown wait for accepted work, and lets a late delivery failure
// return to the sending actor. A direct call without source routing context
// instead uses the target System's dispatcher and cannot report late errors.
func (b *Bridge) Send(ctx context.Context, req statecharts.SendRequest) error {
	target := b.targetSystem()
	if target == nil {
		return fmt.Errorf("actors: Bridge: unknown actor %q (no target system configured)", req.Target)
	}
	namespace := b.namespace
	if target.cfg.nodeName != "" {
		namespace = statecharts.Identifier(target.cfg.nodeName)
	}
	name, ok := stripNamespace(req.Target, namespace)
	if !ok {
		return fmt.Errorf("actors: Bridge: %q is not addressed to target node %q", req.Target, namespace)
	}
	if _, _, ok := target.resolveTarget(name); !ok {
		return fmt.Errorf("actors: Bridge: unknown actor %q in target system", name)
	}

	route, routed := actorRouteFrom(ctx)
	sender := route.address
	origin := b.origin
	if routed && route.system != nil && route.system.cfg.nodeName != "" {
		origin = sender
	} else if sender != "" {
		origin = statecharts.Identifier(string(b.origin) + "." + string(sender))
	}
	ev := statecharts.Event{
		Name:       req.Event,
		Type:       statecharts.EventExternal,
		Data:       req.Data,
		SendID:     req.EventSendID,
		Origin:     origin,
		OriginType: originTypeActors,
	}

	job := func() {
		if err := target.Tell(context.Background(), name, ev); err != nil && routed && route.system != nil {
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
func (b *Bridge) Cancel(context.Context, statecharts.Identifier) error {
	return nil
}

// stripNamespace reports the Identifier remaining after removing ns and its
// following dot. Both namespace and remainder may contain multiple segments.
func stripNamespace(id, ns statecharts.Identifier) (statecharts.Identifier, bool) {
	prefix := string(ns) + "."
	value := string(id)
	if !strings.HasPrefix(value, prefix) || len(value) == len(prefix) {
		return "", false
	}
	return statecharts.Identifier(strings.TrimPrefix(value, prefix)), true
}
