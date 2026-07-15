package server

import (
	"time"

	"github.com/dhamidi/statecharts"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

// leaseTTL is how long a tool's executor lease survives with no renewal --
// a hard client drop (kill -9, network partition) simply stops renewals and
// the lease lapses on its own, no explicit cleanup required.
const leaseTTL = 30 * time.Second

// sweepInterval is how often expired leases are actually removed from the
// map, bounding how late a lapsed lease can still block a fresh claim.
const sweepInterval = 10 * time.Second

// toolClaim is "claim"/"release"'s payload -> toolregistry. A connection's
// own ConnectionActor sends "claim" once up front and again on every
// renewal tick for as long as it's alive (see connection.go), and "release"
// once, immediately, on a graceful disconnect for a fast handoff.
type toolClaim struct {
	Tool  protocol.ToolName
	Owner protocol.ConnectionID
}

// toolOffer is "offer"'s payload -> toolregistry, from ConversationActor.
type toolOffer struct {
	ConversationID protocol.ConversationID
	Tool           protocol.ToolName
	CallID         protocol.CallID
	Args           protocol.ToolArgs
}

type toolLease struct {
	Owner     protocol.ConnectionID
	ExpiresAt time.Time

	// DeliveredCallID is the CallID of the last offer actually handed to
	// Owner via "tool_call", or "" if none yet. offerToRegistry consults
	// this to avoid re-delivering the same call to a still-current owner
	// while it may still be running it -- see the doc comment there. It is
	// carried forward across a renewal claim from the same Owner (so an
	// in-flight call's delivery record survives leaseRenewInterval ticks)
	// but reset to "" whenever Owner changes, since a fresh executor has
	// never been handed anything.
	DeliveredCallID protocol.CallID
}

// toolRegistryModel is ToolRegistryActor's (non-durable) datamodel: one
// lease per tool name, global across the server -- not per-conversation.
type toolRegistryModel struct {
	Leases map[protocol.ToolName]toolLease
}

func claimLease(clock statecharts.Clock) statecharts.ActionFunc {
	return statecharts.Action(func(d *toolRegistryModel, ec statecharts.ExecContext) error {
		ev, _ := ec.Event()
		c, ok := decodeToolClaim(ev.Data)
		if !ok {
			return nil
		}
		next := toolLease{Owner: c.Owner, ExpiresAt: clock.Now().Add(leaseTTL)}
		if existing, ok := d.Leases[c.Tool]; ok && existing.Owner == c.Owner {
			// A renewal claim from the same owner (see connection.go's
			// renewLeases, on leaseRenewInterval) -- not a new executor, so
			// carry the delivery record forward rather than letting a
			// renewal that lands mid-execution make offerToRegistry think
			// this owner has never seen the pending call.
			next.DeliveredCallID = existing.DeliveredCallID
		}
		d.Leases[c.Tool] = next
		return nil
	})
}

var releaseLease = statecharts.Action(func(d *toolRegistryModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	c, ok := decodeToolClaim(ev.Data)
	if !ok {
		return nil
	}
	if lease, ok := d.Leases[c.Tool]; ok && lease.Owner == c.Owner {
		delete(d.Leases, c.Tool)
	}
	return nil
})

// offerToRegistry hands a pending call to whichever connection currently
// holds the lease for its tool -- but only once per (tool, owner, CallID):
// ConversationActor's awaiting_tool retries this same offer every
// toolOfferRetryInterval for as long as it's waiting on a "tool_result", with
// no idea whether an executor has already started running it (that
// knowledge lives here, not there). Re-sending "tool_call" for a CallID
// already delivered to its still-current owner would make ToolActor queue
// and later re-run it a second time once the first run finishes (see
// tool.go's dequeueNext) -- a real duplicated side effect for anything that
// takes longer than toolOfferRetryInterval to run. Comparing against
// lease.DeliveredCallID distinguishes that ("already handed to the owner
// that may be running it, don't resend") from "nobody has the lease yet, or
// the owner changed" (worth (re)delivering).
func offerToRegistry(clock statecharts.Clock) statecharts.ActionFunc {
	return statecharts.Action(func(d *toolRegistryModel, ec statecharts.ExecContext) error {
		ev, _ := ec.Event()
		offer, ok := decodeToolOffer(ev.Data)
		if !ok {
			return nil
		}
		lease, ok := d.Leases[offer.Tool]
		if !ok || clock.Now().After(lease.ExpiresAt) {
			return nil // nobody currently holds the lease; ConversationActor's own retry loop tries again later
		}
		if lease.DeliveredCallID == offer.CallID {
			return nil // already delivered to this exact, still-current owner; it may still be running -- resending would risk running it twice
		}
		lease.DeliveredCallID = offer.CallID
		d.Leases[offer.Tool] = lease
		ec.Send("tool_call", statecharts.SendOptions{
			Target: statecharts.Identifier(lease.Owner),
			Data: encodeToolCall(toolCallDelivery{
				ConversationID: offer.ConversationID, CallID: offer.CallID, Name: offer.Tool, Args: offer.Args,
			}),
		})
		return nil
	})
}

func sweepExpiredLeases(clock statecharts.Clock) statecharts.ActionFunc {
	return statecharts.Action(func(d *toolRegistryModel, ec statecharts.ExecContext) error {
		now := clock.Now()
		for tool, lease := range d.Leases {
			if now.After(lease.ExpiresAt) {
				delete(d.Leases, tool)
			}
		}
		ec.Send("sweep", statecharts.SendOptions{Delay: sweepInterval})
		return nil
	})
}

// toolCallDelivery is "tool_call"'s payload -> a specific connection,
// naming exactly which offer it is (mirrors protocol.ToolCallFrame, as its
// own type at this internal actor-to-actor hop).
type toolCallDelivery struct {
	ConversationID protocol.ConversationID
	CallID         protocol.CallID
	Name           protocol.ToolName
	Args           protocol.ToolArgs
}

// ToolRegistryKind is the chart kind name the singleton "toolregistry"
// actor is Registered and Spawned under.
const ToolRegistryKind statecharts.Identifier = "toolregistry"

// BuildToolRegistryChart returns the non-durable "toolregistry" singleton:
// a name-keyed, leased, single-owner executor assignment per tool, with
// TTL-based expiry so a hard client drop hands off automatically.
func BuildToolRegistryChart(clock statecharts.Clock) (*statecharts.Chart, error) {
	return statecharts.Build(
		statecharts.Atomic("toolregistry",
			statecharts.OnEntry(sweepExpiredLeases(clock)),
			statecharts.On("claim", statecharts.Then(claimLease(clock))),
			statecharts.On("release", statecharts.Then(releaseLease)),
			statecharts.On("offer", statecharts.Then(offerToRegistry(clock))),
			statecharts.On("sweep", statecharts.Then(sweepExpiredLeases(clock))),
		),
		statecharts.WithNewDatamodel(func() any {
			return &toolRegistryModel{Leases: map[protocol.ToolName]toolLease{}}
		}), statecharts.WithVersion("v1"))
}
