package actors

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/dhamidi/statecharts"
)

// Bridge is a fallback IOProcessor (see WithFallback) that connects two
// Systems. A Bridge is configured with a namespace and a target System:
// Send accepts only targets whose first Identifier segment is that
// namespace, strips it, and delivers the rest to target via Tell. An actor
// in system A reaches an actor named "billing" in system B by addressing
// "warehouse-b.billing", once A is built with:
//
//	sysA := actors.NewSystem(actors.WithFallback(
//		actors.NewBridge("warehouse-b", sysB, "warehouse-a"),
//	))
//
// origin is the namespace this Bridge stamps onto Origin for every event it
// forwards, e.g. "warehouse-a.caller-1" for an event originally sent by
// "caller-1". It exists so a reply from inside the target System is just
// another Send targeting ev.Origin: system B's own routing doesn't
// recognize "warehouse-a.caller-1" as one of its own actors either, falls
// through to whatever fallback B was built with, and a Bridge configured the
// other way around (namespace "warehouse-a", target sysA, origin
// "warehouse-b") strips the prefix and delivers the reply back to
// "caller-1". Two Systems bridged this way need one Bridge each, one per
// direction.
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

// Send implements statecharts.IOProcessor. It resolves only whether
// req.Target carries b's own namespace and whether the name that remains is
// known to b's target System, both cheap and synchronous; either check
// failing is an ordinary returned error, exactly as an unrecognized name is
// for a System's own routing IOProcessor. Once both checks pass, delivering
// the event -- which may page an actor into the target System, replay its
// log, or block on its own bookkeeping -- happens on a new goroutine, so
// Send itself always returns immediately. A failure discovered only during
// that later delivery (paging the actor back in, say) is not reported back
// to the sender the way a same-System peer Send's failure is -- there is no
// Dispatcher here to report it to (see Attach) -- so it is silently
// dropped.
func (b *Bridge) Send(ctx context.Context, req statecharts.SendRequest) error {
	name, ok := stripNamespace(req.Target, b.namespace)
	if !ok {
		return fmt.Errorf("actors: Bridge: %q is not addressed to namespace %q", req.Target, b.namespace)
	}
	target := b.targetSystem()
	if target == nil {
		return fmt.Errorf("actors: Bridge: unknown actor %q (no target system configured)", req.Target)
	}
	if _, ok := target.resolve(name); !ok {
		return fmt.Errorf("actors: Bridge: unknown actor %q in target system", name)
	}

	sender, _ := actorOriginFrom(ctx)
	origin := b.origin
	if sender != "" {
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

	go func() {
		_ = target.Tell(context.Background(), name, ev)
	}()
	return nil
}

// Cancel implements statecharts.IOProcessor. A Bridge keeps no bookkeeping
// of its own about a Send once it has handed the event's delivery off to
// target (see Send), so there is nothing here for it to cancel.
func (b *Bridge) Cancel(context.Context, statecharts.Identifier) error {
	return nil
}

// stripNamespace reports the Identifier remaining after removing ns as
// id's first dot-separated segment, and whether id had that segment at all.
func stripNamespace(id, ns statecharts.Identifier) (statecharts.Identifier, bool) {
	segments := id.Segments()
	if len(segments) < 2 || segments[0] != string(ns) {
		return "", false
	}
	return statecharts.Identifier(strings.Join(segments[1:], ".")), true
}
