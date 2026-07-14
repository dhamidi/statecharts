# 6. Log as primary persistence mechanism; Snapshot as a derivable checkpoint

Date: 2026-07-13

## Status

Accepted

## Context

Per the user's constraints: (5) a chart's state (active state set, timers,
queues) can be snapshotted/serialized/restored, and (6) a log interface
recording all incoming messages to the statechart actor, replayed, is the
main persistence mechanism.

The key question is what must actually be logged for exact replay to work.
Inspecting the interpreter algorithm (interpreter.go) shows that only two
kinds of message ever cross the actor boundary from outside:

1. An explicit application call to `Instance.Send`.
2. A delayed `<send>` timer elapsing (a real wall-clock event).

Everything else a chart does in response — `<raise>`, `<cancel>`, history
recording, transition selection, new transitions, new delayed sends — is
pure, deterministic recomputation by the interpreter given the current
configuration and the inbound event. Replaying only those two kinds of
entry, against the same deterministic algorithm, is therefore sufficient to
reconstruct exact state, provided real-world side effects are suppressed
during replay (see the `IOProcessor`/timer-ownership split in ADR 0007) and
delay computations use the log entry's own recorded `Timestamp` as "now"
rather than the real clock.

## Decision

`Log` (log.go) has exactly two `EntryKind`s: `KindExternalEvent` and
`KindTimerFired`. `Snapshot` (snapshot.go) is treated as a derivable
checkpoint/cache — "the state you'd get by replaying the log up to some
sequence number" — not an independent source of truth; `Checkpoint{Snapshot,
Seq}` is the pairing type used only by the log-integration flow
(`Rehydrate`, `SnapshotStore`), keeping bare `Snapshot` reusable for
log-free backup/restore use cases that don't need a meaningless
always-zero `Seq` field.

`Rehydrate` (replay.go) drives cold-start recovery by loading the latest
`Checkpoint` if one exists (to avoid replaying from sequence 0 every time),
`Restore`ing from it, then applying each subsequent entry on the Instance's
own actor goroutine. External entries use the same `Instance.Send` path as a
live caller. A timer-fire entry instead consumes the recomputed pending send
by `SendID` and dispatches it without invoking the live timer hook again —
the fire is already durably represented by that log entry.

Explicit application `Send`s are the application's own responsibility to
log (`log.Append` before `Instance.Send`, satisfying the write-ahead
ordering) — there is no wrapper needed since the application already
controls that call site. Timer-fired events originate inside `Instance`
itself with no such call site, so `WithTimerFiredDetailsHook` (instance.go)
gives a `Log` implementation a synchronous, on-the-actor's-own-goroutine
seam to append the send ID, target, I/O processor type, and event before the
event is allowed to apply (`LoggingTimerFiredDetailsHook` in log.go is the
ready-made adapter).
A non-nil error from this hook is treated as the `Instance`'s fatal terminal
error, since a failed log append must not silently let the event through.

An actor-system checkpoint uses `Instance.Checkpoint`, whose persistence
callback runs while the actor goroutine is paused. Reading `Log.LastSeq` and
saving `Checkpoint{Snapshot, Seq}` inside that callback makes the pair one
logical boundary: a timer cannot append after the snapshot was captured but
before `Seq` was read. A failed callback leaves the same instance running so
checkpointing remains retryable.

## Consequences

- `Log.Append` must be called before applying an entry's effects
  (write-ahead); a crash between append and full processing is safe to
  reprocess on restart, since processing is deterministic and replay's
  `IOProcessor` is suppressed (`replayGate` in replay.go).
- Datamodel state is never part of `Snapshot` or `Log` — it is the caller's
  own Go value(s), reconstructed by the caller (often trivially, by passing
  a fresh zero value to `Restore`/`Rehydrate` and letting replay's action
  callbacks rebuild it) or serialized separately if needed.
- `LogEntry.Timestamp` doubles as the "logical now" used during replay to
  recompute `FireAt` for any newly-scheduled delayed send discovered while
  replaying — a non-firing replay clock prevents those timers from elapsing
  again. Once replay catches up, remaining timers are armed against the live
  configured `Clock`, and overdue sends fire before `Rehydrate` returns. Live
  log entries use that same configured clock, not an independent call to the
  process wall clock.
