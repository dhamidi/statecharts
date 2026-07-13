# 3. Actor concurrency model

Date: 2026-07-13

## Status

Accepted

## Context

Per the user's constraint, one running statechart is a running goroutine,
and channels are never part of the public API. `Instance` (instance.go)
needs to expose plain method calls (`Send`, `Stop`, `Wait`, `Configuration`,
`Err`, `Snapshot`) while internally driving `interpretation` (interpreter.go)
from a single goroutine, since the interpreter algorithm itself has no
internal locking and assumes single-threaded mutation.

Two design questions needed resolving:

1. How does the actor's ingress path preserve SCXML's own ordering
   guarantees, in particular that a `Stop` (SCXML's "cancel event") must not
   be reordered relative to already-queued `Send` calls?
2. When does `Send` return — as soon as the event is merely accepted into a
   queue, or once its resulting macrostep has fully processed?

## Decision

**Single request channel, not per-operation channels.** `Send`, `Stop`, a
fired delayed-send timer, and `Snapshot` all submit one `actorRequest` onto
a single unexported `inbox chan actorRequest`, consumed by one `select` arm
in the actor's run loop. SCXML models cancellation as just another dequeue
from the same external queue (`externalEvent = externalQueue.dequeue(); if
isCancelEvent(...): running = false`); a single physical channel gives that
FIFO ordering for free. Per-operation channels were rejected: Go's `select`
over multiple ready channels does not guarantee arrival-order fairness, so
a `Stop` could in principle be serviced ahead of an earlier `Send` purely
from `select`'s pseudo-random tie-breaking.

**`Send` waits for the resulting macrostep(s) to fully process, not just
for acceptance into the queue.** This is a deliberate refinement discovered
during implementation: an initial design (matching the original plan text)
had the actor reply as soon as the event was enqueued, before running
`processNextExternal`/`runToStable`/`publishConfig`. Under `go test -race`
this reliably (not just occasionally) surfaced a real bug: `Send` could
return `nil` while `Configuration()` still reflected the pre-transition
state, since the reply and the configuration publish were not ordered
relative to each other. For a single, synchronous, in-process actor like
this one, making `Send` wait for the full macrostep is free to provide and
removes an entire class of "did my Send take effect yet" races for callers.
It does not contradict SCXML, which leaves the timing of local delivery
confirmation implementation-defined; only genuinely cross-session
`IOProcessor` dispatch is asynchronous here.

**`ErrInstanceStopped` distinguishes "not confirmed processed" from "no
terminal error".** A message can be accepted into the inbox's channel
buffer in the narrow window before the actor goroutine exits, without ever
being dequeued. Returning the instance's terminal error (`Err()`, often
`nil` for a clean stop) in that case would misreport a dropped message as a
success. `awaitReply` instead does a non-blocking re-check of the reply
channel after `doneCh` fires — reliable because the actor always writes to
`reply` strictly before its final `close(doneCh)` — and returns
`ErrInstanceStopped` only when that re-check confirms the request was
genuinely never dequeued.

## Consequences

- Callers get a strong, race-free guarantee: after `Send` returns
  successfully, `Configuration()` reflects its effect.
- `Stop` is idempotent (`errors.Is(err, ErrInstanceStopped)` is treated as
  success), while `Send` after a stopped instance surfaces
  `ErrInstanceStopped` distinctly from the instance's own `Err()`.
- User callbacks (`ActionFunc`/`CondFunc`) must never call `Send`/`Stop`/
  `Wait` on their own `Instance` — that goroutine is the one that would have
  to service the request, causing a deadlock. Use `ExecContext.Raise` for
  same-session follow-up events instead.
- `Instance.Snapshot` reuses the same request-channel mechanism (a
  `reqSnapshot` request carrying a `snapOut chan Snapshot`) to safely read
  otherwise-unsynchronized interpreter state from outside its goroutine.
