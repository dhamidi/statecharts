# 10. `<invoke>` as a plain Go goroutine, not a child session

Date: 2026-07-13

## Status

Accepted

## Context

ADR 0005 deferred `<invoke>` out of v1 specifically because the case it had
in mind -- an invoked service that is *itself* an SCXML session -- drags in
session-id namespacing and parent/child `Dispatcher` wiring that have
nothing to do with getting the microstep/macrostep algorithm right. That
reasoning doesn't apply to `<invoke>` in general: SCXML 6.4's `type`
attribute is explicitly open-ended ("plus other platform-specific values"),
and the spec's own running example of it (6.5, the `x-clock`/`clock.pl`
service in the `<finalize>` walkthrough) invokes something that is not an
SCXML session at all -- an external process that periodically reports the
time. What actually belongs in the interpreter core, independent of what
kind of thing is on the other end, is: deferring invocation to the point a
macrostep settles (so a state entered and exited within one macrostep is
never invoked), cancelling on state exit as if it were onexit's last
handler (6.4.2), and routing `<finalize>` and `"#_<invokeid>"` sends by
matching `InvokeID` (6.4.4, 6.5). None of that requires a session registry.

## Decision

`InvokeFunc` (invoke.go) is a plain Go function run in its own
goroutine -- not a child `Instance`, not a nested interpreter. It receives
an `InvokeIO` with a `Deliver` callback (events back to the invoking
chart, tagged with this invocation's `InvokeID`) and an `Incoming` channel
(events forwarded to it via `SendOptions{Target: "#_<invokeid>"}`), and a
`context.Context` the interpreter cancels on state exit. This is
additive exactly as ADR 0005 anticipated: a new `StateSpec.Invokes` field,
no changes to `Identifier`, `Event`, or `IOProcessor`.

The bookkeeping that *is* core-interpreter algorithm now lives in
interpreter.go, alongside the rest of the SCXML D algorithm it mirrors:

- `statesToInvoke` accumulates states entered since the last macrostep
  settled; `processInvokes` starts their invocations once
  `runToStable` finds no more eventless transitions or internal events
  (SCXML mainEventLoop's `for state in statesToInvoke...`), and
  `exitState` removes a state from it again before that point if the
  state is exited first -- so a transient state is never invoked.
- `cancelInvokes` runs immediately after `s.onExit` inside `exitState`,
  for both an ordinary transition exiting `s` and `exitInterpreter`'s
  cleanup (ADR/fix for `exitInterpreter`, same commit series) -- matching
  6.4.2's "cancel operation MUST act as if it were the final onexit
  handler."
- `applyFinalize` matches an arriving event's `InvokeID` against
  `invokesByID` and runs that invocation's finalize content before
  `selectTransitions` ever sees the event (6.5) -- with no special
  exemption for the synthesized `done.invoke.<id>`/`error.communication`
  events, since 6.5 matches by invokeid alone; a finalize handler that
  cares about payload shape must check it itself; the built-in clock
  test in invoke_test.go does exactly this.

What's genuinely an actor concern, not a core-interpreter one, is spawning
the goroutine and delivering its result back through the actor's own
inbox -- the same seam `actorClock` already uses for `<send delay="...">`.
`interpretation.startInvoke` (an `invokeRunnerFunc`) is supplied by
`Instance` (`instance.go`), exactly parallel to `ip.clock`; a bare
`interpretation` with no owning `Instance` gets a no-op runner, the same
default posture as `NoopIOProcessor`.

## Consequences

- Only the invoking-chart-is-the-parent half of `<invoke>` exists.
  There is still no child-SCXML-session registry, `#_parent` addressing,
  or autoforwarding of every external event (SCXML 6.4's `autoforward`
  attribute) -- those remain the ADR 0005 boundary, now narrower: they're
  about *spawning and naming other chart instances*, which is a
  deployment/actors-layer concern (see ADR 0009), not about the
  invoke/finalize/cancel algorithm itself.
- `done.invoke.<id>` is synthesized by the interpreter on `InvokeFunc`'s
  own (non-cancelled) return, carrying whatever data it returns; a
  service that wants to signal completion with specific donedata just
  returns it, rather than constructing the event itself.
- `Snapshot`/`Restore`/`Rehydrate` do not capture in-flight invocations.
  Restoring an `Instance` does not resume, re-cancel, or otherwise know
  about invocations that were running when a snapshot was taken -- the
  same way `Rehydrate` already doesn't repeat real `IOProcessor` dispatch
  during replay (ADR 0006), an invocation is a real-world side effect
  outside the interpreter's persisted state. Applications that need an
  invocation to survive a restart must re-`Invoke` it themselves once
  restored, e.g. from an `onentry` guarded by `ExecContext.In`.
