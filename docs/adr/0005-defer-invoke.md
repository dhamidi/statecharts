# 5. Defer &lt;invoke&gt; (child-session spawning) out of v1

Date: 2026-07-13

## Status

Accepted

## Context

SCXML's `<invoke>` spawns a child interpreter session, wires up
`#_parent`/`#_<invokeid>` addressing between parent and child, autoforwards
events, and raises `done.invoke.<id>` on completion. It is a second
actor-lifecycle feature layered on top of the single-instance actor: it
needs session-id namespacing and parent/child `Dispatcher` wiring that add
nothing to validating the core microstep/macrostep algorithm, which is
ADR 0002's stated v1 goal ("faster path to a correct, spec-conformant
interpreter core").

## Decision

`<invoke>` is omitted entirely from `StateSpec` and the interpreter core in
v1, rather than partially supported. A type or field that claims a
capability it silently drops (e.g. a `StateSpec.Invoke` field that isn't
actually wired to a working child-session lifecycle) is worse than no such
field at all.

The existing `IOProcessor`/`SendRequest`/`Dispatcher` design already
represents arbitrary target `Identifier`s (including the
`#_parent`/`#_<invokeid>` special forms SCXML defines), so adding invoke
later is additive: a new `StateSpec.Invoke` field plus a session registry,
with no breaking changes to `Identifier`, `Event`, or `IOProcessor`.

## Consequences

- Charts cannot spawn child sessions in v1. Any chart requiring `<invoke>`
  semantics must be restructured (e.g. by driving a second `Instance`
  manually from application code, wiring events between them through a
  custom `IOProcessor`) until invoke lands in a later milestone.
- `done.invoke.<id>` and autoforwarding are not implemented; only
  `done.state.<id>` (final-state completion) is, per interpreter.go's
  `enterState`.
