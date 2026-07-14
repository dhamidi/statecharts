# 7. Pending delayed-send timer bookkeeping lives in the interpreter core

Date: 2026-07-13

## Status

Accepted

## Context

Two independent design passes were done for this library: one covering the
actor/engine side (constraints 1-4: goroutine model, datamodel access,
`Identifier`, `IOProcessor` isolation), one covering persistence
(constraints 5-7: snapshot, log, sqllog). Synthesizing them surfaced a real
conflict: the persistence design requires `Snapshot.PendingSends` to
capture every outstanding delayed `<send>` (constraint 5 explicitly lists
"timers" as part of what must be snapshottable), but the engine design's
first draft kept that bookkeeping inside the default `IOProcessor`
implementation (`LocalIOProcessor`), which would make it invisible to
`Snapshot` and dependent on which `IOProcessor` happened to be plugged in.

## Decision

Pending-send bookkeeping (`SendID`, `Target`, `Type`, the event to raise,
and the absolute `FireAt` time) is tracked by `interpretation` itself
(interpreter.go's `pending map[Identifier]*pendingSendRecord`), driven by an
injected `Clock` seam (clock.go), not by whatever `IOProcessor` is plugged
in. `IOProcessor.Send` (ioprocessor.go) is only ever invoked for immediate
dispatch to a genuinely external target (`Target` neither empty nor
`"#_internal"`), and always with the delay already resolved to zero by the
time it's called — delay timing is core-interpreter logic, not an
`IOProcessor` concern.

This keeps both constraints true simultaneously: `IOProcessor` still
isolates all genuine external I/O (constraint 4), and `Snapshot` stays
complete and correct regardless of which `IOProcessor` implementation is in
use (constraint 5), since the pending-send table it reads from is always
the interpreter's own.

A useful consequence discovered later (see ADR 0006): real-world dispatch is
gated at the `IOProcessor.Send`/`Cancel` boundary (`replayGate` in replay.go),
while delay timing is independently gated by a non-firing replay `Clock`.
Keeping both gates active until log replay catches up prevents either external
dispatch or a recomputed historical timer from repeating real-world work.

## Consequences

- `ExecContext.Send`/`Cancel` (exec.go) call into `interpretation.doSend`/
  `doCancel` directly, not through `IOProcessor`, for the
  scheduling/cancellation bookkeeping itself.
- `IOProcessor.Cancel` is reserved for the rarer case of cancelling an
  already-dispatched, still-in-flight external request (something a real
  HTTP-backed processor might support); it is not involved in cancelling a
  still-pending local timer, which the interpreter core handles by calling
  the `Clock.AfterFunc` stop-function directly.
- `Restore` (snapshot.go) reconstructs every `PendingSend` as an inert record.
  A direct `Start` arms those records relative to the configured `Clock.Now`
  and synchronously applies overdue sends before returning. `Rehydrate`
  defers the same activation until all post-checkpoint log entries have
  replayed, since one of those entries may fire or cancel a checkpointed
  timer.
- `Snapshot` also preserves the high-water marks used for anonymous send and
  invoke IDs. Restoring the visible pending/active records without those
  counters could reuse an old `send.<n>` or `<state>.invoke<n>` identity and
  misattribute a later callback.
- Interpreter exit cancels and forgets every pending timer. A stopped or
  terminal instance must not retain callbacks that can outlive it and race a
  separately restored copy.
