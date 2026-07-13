package actors

import (
	"context"
	"fmt"

	"github.com/dhamidi/statecharts"
)

// originTypeActors is the OriginType stamped on every event a System
// delivers, identifying the routing mechanism that populated Origin.
const originTypeActors statecharts.Identifier = "actors"

// actorOriginContextKey is the context key withActorOrigin/actorOriginFrom
// use to carry a Send call's sending actor across the one seam that has no
// field for it: a fallback IOProcessor's Send(ctx, req) (see WithFallback).
type actorOriginContextKey struct{}

// withActorOrigin returns a copy of ctx carrying self as the acting actor,
// for a fallback IOProcessor's own Send to recover with actorOriginFrom.
func withActorOrigin(ctx context.Context, self statecharts.Identifier) context.Context {
	return context.WithValue(ctx, actorOriginContextKey{}, self)
}

// actorOriginFrom reports the actor that made the Send call ctx was passed
// to, if ctx reached a fallback IOProcessor through a System's own routing
// (see withActorOrigin). ok is false for any other ctx, e.g. one an
// IOProcessor receives directly from the interpreter for a non-fallback
// Send.
func actorOriginFrom(ctx context.Context) (statecharts.Identifier, bool) {
	self, ok := ctx.Value(actorOriginContextKey{}).(statecharts.Identifier)
	return self, ok
}

// routingProcessor is the statecharts.IOProcessor every actor a System
// spawns is constructed with. It is closed over the actor's own name
// (self) and the System, and is the only mechanism through which a chart
// running inside a System can address another actor by name.
//
// Send resolves only whether the target name is known to sys, which is
// cheap and synchronous; an unknown name is returned as an ordinary error,
// which the interpreter core turns into an error.communication event on
// the sender's own queue automatically -- unless sys has a fallback
// configured (see WithFallback), in which case the request is handed to it
// instead of failing outright. Actually acquiring the target -- paging it
// in if necessary, possibly evicting another resident actor first to stay
// within the residency limit -- and delivering the event is handed off to a
// goroutine, so Send never blocks on the target's own processing. Two
// actors sending to each other at the same instant would otherwise be able
// to deadlock each other's Send call.
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
	if _, ok := p.sys.resolve(req.Target); !ok {
		if p.sys.cfg.fallback == nil {
			return fmt.Errorf("actors: unknown actor %q", req.Target)
		}
		// self rides along on ctx, not on req: SendRequest has no field for
		// who is calling (an IOProcessor is shared machinery with no notion
		// of "which actor" built into its own signature), and a fallback is
		// one value shared by every actor in sys, so there is nowhere else
		// to attach it without racing concurrent Send calls from different
		// actors against each other. actorOriginFrom recovers it.
		return p.sys.cfg.fallback.Send(withActorOrigin(ctx, p.self), req)
	}

	ev := statecharts.Event{
		Name:       req.Event,
		Type:       statecharts.EventExternal,
		Data:       req.Data,
		SendID:     req.SendID,
		Origin:     p.self,
		OriginType: originTypeActors,
	}

	origin := p.disp
	target := req.Target
	p.sys.asyncWG.Add(1)
	go func() {
		defer p.sys.asyncWG.Done()
		p.sys.deliverAsync(context.Background(), target, ev, origin)
	}()
	return nil
}

// Cancel implements statecharts.IOProcessor. Delayed-send bookkeeping lives
// in the interpreter core (see clock.go, interpreter.go), never inside an
// IOProcessor, so by the time Cancel could matter for a cross-actor send,
// the sender's own pending-send record is already gone; there is nothing
// left here for the routing processor itself to cancel.
func (p *routingProcessor) Cancel(ctx context.Context, sendID statecharts.Identifier) error {
	return nil
}

// IOProcessors implements statecharts.IOProcessorDescriber, advertising
// self's own name as the address any other actor in sys can already reach
// it at -- see Send's own resolution of req.Target against sys, which
// treats an actor's name as its address directly. This does not attempt to
// account for a Bridge-qualified cross-system address (see WithFallback):
// which Bridge, if any, exposes this actor to which peer under what
// namespace is not something routingProcessor itself knows.
func (p *routingProcessor) IOProcessors() []statecharts.IOProcessorInfo {
	return []statecharts.IOProcessorInfo{
		{Type: originTypeActors, Location: statecharts.LocationFromIdentifier(p.self)},
	}
}
