package statecharts

import (
	"context"
	"errors"
	"iter"
	"time"
)

// ErrInvalidSnapshot marks malformed or incompatible snapshot cache data.
var ErrInvalidSnapshot = errors.New("statecharts: invalid snapshot")

// ErrOutboundCollision marks two different durable send intents carrying
// the same DeliveryID. A durable store must reject the second intent rather
// than silently treating it as an idempotent retry of the first.
var ErrOutboundCollision = errors.New("statecharts: outbound delivery ID collision")

// EntryKind distinguishes durable inputs and the session-start boundary.
// Everything else a chart does (<raise>, <cancel>, history recording, new
// transitions) is pure, deterministic recomputation given the initial
// configuration and these entries (see Rehydrate in replay.go).
type EntryKind string

const (
	// KindSessionStarted is written before a durable actor is started for the
	// first time. It is a no-op during replay, but proves initial executable
	// content may already have run -- particularly an initial <invoke> -- even
	// when the process crashed before the actor received its first message.
	// Its Timestamp also anchors initial delayed sends to the original start
	// time instead of the recovery time.
	KindSessionStarted EntryKind = "session_started"

	// KindExternalEvent records an explicit application call, Instance.Send.
	KindExternalEvent EntryKind = "external_event"

	// KindTimerFired records a delayed <send>'s timer elapsing -- this
	// originates outside the deterministic microstep computation (a real
	// wall-clock timer) and must be replayed as data, never re-derived by
	// letting a real timer re-elapse.
	KindTimerFired EntryKind = "timer_fired"
)

// LogEntry is one recorded message. SessionID+Seq is monotonically
// increasing and gapless, assigned by Log.Append.
type LogEntry struct {
	SessionID SessionID
	Seq       uint64
	Kind      EntryKind

	// Timestamp is the entry's logical append time (UTC), obtained from the
	// same configured Clock that drives the live Instance. It is used as
	// "now" during replay when recomputing a new pending send's FireAt
	// (FireAt = Timestamp + delay) -- replay never consults the real clock.
	Timestamp time.Time

	Event Event // the inbound event (for KindTimerFired, the event that fired)

	SendID Identifier // KindTimerFired only
	Target Identifier // KindTimerFired only
	Type   Identifier // KindTimerFired only; original I/O processor selector
}

// Log is the primary persistence mechanism for a chart: recording every
// inbound message lets a session's state be reconstructed exactly by
// replaying them, rather than by directly serializing the datamodel.
type Log interface {
	// Append durably records entry, assigning the next Seq for
	// entry.SessionID (entry.Seq is ignored on input), and returns the
	// assigned Seq. Callers must Append before applying entry's effects
	// (write-ahead): a crash between Append and full processing just means
	// replay reprocesses that entry from the last stable configuration,
	// which is safe since processing is deterministic and replay's
	// IOProcessor is suppressed.
	Append(ctx context.Context, entry LogEntry) (seq uint64, err error)

	// Read streams entries for sessionID in ascending Seq order, starting
	// at from (inclusive). Iteration stops as soon as the consumer stops
	// ranging, or on the first yielded error.
	Read(ctx context.Context, sessionID SessionID, from uint64) iter.Seq2[LogEntry, error]

	// LastSeq returns the highest Seq recorded for sessionID, or 0 if none.
	LastSeq(ctx context.Context, sessionID SessionID) (uint64, error)
}

// OutboundStatus is the durable lifecycle of an external send intent.
type OutboundStatus string

const (
	OutboundPending  OutboundStatus = "pending"
	OutboundResolved OutboundStatus = "resolved"
)

// OutboundResult is the recorded synchronous/asynchronous outcome.
type OutboundResult struct {
	Error     string
	Execution bool
	// Synchronous distinguishes an outcome returned by Send (and therefore
	// reproduced during replay) from a callback/recovery failure, which is
	// reported as a separate inbound platform event.
	Synchronous bool
}

// OutboundMessage is one stable external send intent.
type OutboundMessage struct {
	SessionID  SessionID
	DeliveryID DeliveryID
	Request    SendRequest
	Status     OutboundStatus
	Result     OutboundResult
}

// DurableLog combines the WAL inbox/dedup and durable outbox boundary.
type DurableLog interface {
	Log
	// AppendIngress atomically deduplicates deliveryID and appends entry. When
	// the implementation also stores ActorMetadata for entry.SessionID, it
	// must linearize with MarkActorTerminal and return ErrActorTerminal instead
	// of appending after that actor is terminal.
	AppendIngress(ctx context.Context, entry LogEntry, deliveryID DeliveryID) (seq uint64, appended bool, err error)
	StoreOutbound(ctx context.Context, message OutboundMessage) error
	ResolveOutbound(ctx context.Context, sessionID SessionID, deliveryID DeliveryID, result OutboundResult) error
	Outbounds(ctx context.Context, sessionID SessionID) ([]OutboundMessage, error)
}

// SnapshotStore persists Checkpoints, letting Rehydrate skip replaying a
// Log from the very beginning.
type SnapshotStore interface {
	Save(ctx context.Context, sessionID SessionID, cp Checkpoint) error
	Load(ctx context.Context, sessionID SessionID) (Checkpoint, bool, error)
}

// LoggingTimerFiredHook returns a callback for WithTimerFiredHook that
// appends a KindTimerFired entry to log before the event is allowed to
// apply, satisfying the write-ahead requirement for timer-fired events --
// the one kind of inbound message with no explicit Instance.Send call site
// for an application to log itself. Explicit application Sends must
// separately call log.Append(ctx, LogEntry{Kind: KindExternalEvent, ...})
// before calling Instance.Send; that ordinary call site needs no hook.
func LoggingTimerFiredHook(log Log, sessionID SessionID, clocks ...Clock) func(Identifier, Event) error {
	clock := Clock(NewRealClock())
	if len(clocks) > 0 && clocks[0] != nil {
		clock = clocks[0]
	}
	return func(sendID Identifier, ev Event) error {
		_, err := log.Append(context.Background(), LogEntry{
			SessionID: sessionID,
			Kind:      KindTimerFired,
			Timestamp: clock.Now().UTC(),
			Event:     ev,
			SendID:    sendID,
		})
		return err
	}
}

// LoggingTimerFiredDetailsHook returns a callback for
// WithTimerFiredDetailsHook. It persists the original target and I/O
// processor type as well as the event, and uses clock for the entry's logical
// timestamp.
func LoggingTimerFiredDetailsHook(log Log, sessionID SessionID, clock Clock) func(Identifier, Identifier, Identifier, Event) error {
	if clock == nil {
		clock = NewRealClock()
	}
	return func(sendID, target, typ Identifier, ev Event) error {
		_, err := log.Append(context.Background(), LogEntry{
			SessionID: sessionID,
			Kind:      KindTimerFired,
			Timestamp: clock.Now().UTC(),
			Event:     ev,
			SendID:    sendID,
			Target:    target,
			Type:      typ,
		})
		return err
	}
}
