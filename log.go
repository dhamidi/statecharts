package statecharts

import (
	"context"
	"iter"
	"time"
)

// EntryKind distinguishes the two -- and only two -- kinds of message that
// ever cross an Instance's boundary from outside. Everything else a chart
// does (<raise>, <cancel>, history recording, new transitions) is pure,
// deterministic recomputation given the current configuration and the
// inbound event, so logging only these two kinds is sufficient for exact
// replay (see Rehydrate in replay.go).
type EntryKind string

const (
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

	// Timestamp is when this entry was durably appended (UTC). It is used
	// as "now" during replay when recomputing a new pending send's FireAt
	// (FireAt = Timestamp + delay) -- replay never consults the real clock.
	Timestamp time.Time

	Event Event // the inbound event (for KindTimerFired, the event that fired)

	SendID Identifier // KindTimerFired only
	Target Identifier // KindTimerFired only
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
func LoggingTimerFiredHook(log Log, sessionID SessionID) func(Identifier, Event) error {
	return func(sendID Identifier, ev Event) error {
		_, err := log.Append(context.Background(), LogEntry{
			SessionID: sessionID,
			Kind:      KindTimerFired,
			Timestamp: time.Now().UTC(),
			Event:     ev,
			SendID:    sendID,
		})
		return err
	}
}
