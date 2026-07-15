# 4. Datamodel access via type-erased core, not a fully generic Chart[D]

Date: 2026-07-13

## Status

Superseded by [ADR 0015](0015-serializable-definitions-pluggable-datamodels-and-hot-deployment.md)

## Context

Per the user's constraint, the only supported datamodel is Go itself: there
is no expression language, so every place the SCXML spec evaluates an
expression against the datamodel (`cond`, `<assign>`, `<script>`, transition
and `<onentry>`/`<onexit>` executable content) instead becomes a Go closure
operating on the caller's own datamodel value.

Two shapes were considered for giving those closures typed access to the
datamodel without runtime type assertions leaking into user code:

1. A fully generic core: `Chart[D]`, `StateSpec[D]`, `TransitionSpec[D]`,
   `Instance[D]`, with the datamodel's type `D` threaded through every
   builder function and struct field.
2. A type-erased core (`Chart`, `StateSpec`, `TransitionSpec`,
   `ExecContext` all hold `any`) plus two small generic adapter functions,
   `Action[D any](func(*D, ExecContext) error) ActionFunc` and
   `Cond[D any](func(*D, ExecContext) bool) CondFunc`, each doing one type
   assertion at call time.

## Decision

Option 2. A chart is bound to exactly one concrete datamodel type for its
whole life, so the type parameter only needs to live at the `Action`/`Cond`
adapter boundary, not throughout the tree. A fully generic `Chart[D]` would
force that type parameter through every builder function and slice field
for no compile-time-safety benefit beyond what the two adapters already
give, since a chart is erased to `any` at the queue/log/snapshot boundary
regardless (events flow through one heterogeneous queue; `Snapshot`/`Log`
serialize `Event` values without knowing `D`). Keeping `Chart`/`StateSpec`
as ordinary, non-generic, directly-inspectable Go values also matters for
the persistence layer (snapshot.go), which walks the compiled chart
structure and would otherwise have to fight a type parameter it has no use
for.

A datamodel type mismatch inside `Action[D]` (only reachable via programmer
error pairing the wrong `Instance` datamodel with a chart built against a
different `D`) is reported as an `error.execution`-shaped Go error, per
SCXML's own error model, rather than a panic. `Cond[D]` evaluates to
`false` on a mismatch instead, consistent with `CondFunc`'s "cannot signal
failure" contract (`CondFunc` returns `bool`, not `(bool, error)`).

## Consequences

- `ExecContext.Datamodel() any` is available as an escape hatch for callers
  who need untyped access outside the `Action`/`Cond` adapters.
- Building a chart never requires writing out a datamodel type parameter;
  only `Action[D]`/`Cond[D]` call sites do, and Go's type inference usually
  makes even that implicit from the callback's own signature.
- A chart is not statically pinned to one datamodel type at the `Chart`
  level — nothing prevents constructing an `Instance` with a mismatched
  datamodel value at runtime. This is an accepted trade-off for v1; the
  failure mode (an `error.execution` event, not a panic) is caught quickly
  in practice since every `Action`/`Cond` call site would fail identically.
