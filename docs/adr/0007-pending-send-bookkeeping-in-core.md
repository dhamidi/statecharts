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

A useful consequence discovered later (see ADR 0006): because real-world
dispatch is entirely gated at the `IOProcessor.Send`/`Cancel` boundary
(`replayGate` in replay.go), and delay timing never touches `IOProcessor` at
all, suppressing replay side effects requires gating only `IOProcessor` —
no separate `Clock` swap is needed on a replaying `Instance`.

## Consequences

- `ExecContext.Send`/`Cancel` (exec.go) call into `interpretation.doSend`/
  `doCancel` directly, not through `IOProcessor`, for the
  scheduling/cancellation bookkeeping itself.
- `IOProcessor.Cancel` is reserved for the rarer case of cancelling an
  already-dispatched, still-in-flight external request (something a real
  HTTP-backed processor might support); it is not involved in cancelling a
  still-pending local timer, which the interpreter core handles by calling
  the `Clock.AfterFunc` stop-function directly.
- `Restore` (snapshot.go) re-arms real timers for every `PendingSend` still
  outstanding at snapshot time, relative to `time.Until(FireAt)` (firing
  immediately if already overdue) — the same mechanism regardless of
  whether the `Instance` arrived there via `Restore` alone or via
  `Rehydrate`'s log replay.
