# 10. `<invoke>` as a plain Go goroutine, with child sessions built on top

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
- `applyInvokeSideEffects` -- run for every external event, right after
  it's dequeued and before `selectTransitions` ever sees it -- does both
  halves of mainEventLoop's per-invoke step in one pass over
  `invokesByID` (already scoped to "for state in configuration", since
  `cancelInvokes` removes an entry the instant its state exits): the
  invocation whose `InvokeID` matches the event gets its `<finalize>`
  content run (6.5), and every invocation configured with
  `WithAutoForward` gets an unconditional copy of the event on its
  `Incoming` channel (6.4.1) -- regardless of whether the event's own
  `InvokeID` matches it, since the spec draws no such exception. A
  finalize handler that cares about a specific payload shape must check
  it itself (6.5 matches by invokeid alone, with no exemption for the
  synthesized `done.invoke.<id>`); the built-in clock test in
  invoke_test.go does exactly this.

What's genuinely an actor concern, not a core-interpreter one, is spawning
the goroutine and delivering its result back through the actor's own
inbox -- the same seam `actorClock` already uses for `<send delay="...">`.
`interpretation.startInvoke` (an `invokeRunnerFunc`) is supplied by
`Instance` (`instance.go`), exactly parallel to `ip.clock`; a bare
`interpretation` with no owning `Instance` gets a no-op runner, the same
default posture as `NoopIOProcessor`.

**A child SCXML session is additive on top of this, not a special case
inside it.** `InvokeChart` (invoke.go) is an `InvokeFunc` like any
other -- it just happens to spend its goroutine running a second, real
`Instance` of another `*Chart`. It needs no new interpreter-core
machinery because the pieces already described cover it completely: the
child's own `Send(name, SendOptions{Target: "#_parent"})` reaches back
into the invoking chart through `parentIOProcessor`, a small `IOProcessor`
that recognizes Appendix C.1's `"#_parent"` special target and routes it
to this invocation's own `InvokeIO.Deliver` -- the exact mechanism ADR
0005 predicted ("the existing `IOProcessor`/`SendRequest`/`Dispatcher`
design already represents arbitrary target `Identifier`s including the
`#_parent`/`#_<invokeid>` special forms"). Autoforwarding to the child is
the same `WithAutoForward` path any other invocation uses; `InvokeChart`
just happens to forward what it receives on `Incoming` into `child.Send`
instead of interpreting it itself. Cancelling the child on state exit is
`context.Context` cancellation causing `child.Stop`, which runs the
child's own `exitInterpreter` cleanup exactly as it would for any
directly-driven `Instance`.

## Consequences

- What's still not modeled is a *named, addressable-by-multiple-parents*
  session registry -- an invocation's child, however it's implemented, is
  owned by exactly the one invocation that started it, reachable only
  through that invocation's own `InvokeID`/`"#_parent"` pair, never by a
  session ID meaningful outside that relationship. Naming, paging, and
  durability for a *pool* of independently-addressable chart instances
  remain the actors-layer concern ADR 0009 describes; `InvokeChart`
  composes with that layer (an actor's own goroutine can itself invoke
  further children) rather than duplicating it.
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
