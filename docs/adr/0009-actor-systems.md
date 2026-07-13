# 9. Virtual actors as a layer above Chart/Instance/IOProcessor/Log

Date: 2026-07-13

## Status

Accepted

## Context

Naming, spawning, durability, and paging are deployment concerns, not
interpreter concerns. None of them change what a single macrostep
computes; they only change how many `Instance`s exist, where they live,
and how a caller finds one by name instead of by Go reference. ADR 0005
already drew this line once, for a narrower case: `<invoke>` was left out
of the interpreter core because session-id namespacing and parent/child
`Dispatcher` wiring add nothing to getting the microstep/macrostep
algorithm right, and the existing `Identifier`/`IOProcessor`/`Dispatcher`
trio was already expressive enough to add that wiring later without
touching the core. A virtual-actor layer is the same shape of feature at
larger scale: addressing a peer by name, spawning it on demand, resuming
it after a restart, and evicting it under memory pressure are all
questions about which `Instance` values exist right now and how a message
finds one, never questions about what happens inside a single instance's
own transition selection. Building `actors` as a package on top of
`Chart`, `Instance`, `IOProcessor`, and `Log` — rather than growing the
interpreter to know about names, residency, or paging — keeps that
boundary where ADR 0005 already put it: the core computes; everything
above `IOProcessor.Send` decides where a message goes and who's currently
resident to receive it.

The risk of a layer like this is that "layer" turns into "parallel
implementation" — a second event-routing mechanism, a second replay path,
a second notion of what identifies a chart — duplicating machinery the
core already has instead of reusing it. The decisions below are all
instances of the same rule: wherever the core already has a seam or a
concept that does the job, `actors` uses it as-is; a new concept is
introduced only where none of the existing ones fit.

## Decision

**`IOProcessor` is the only routing seam; `actors` does not add a second
one.** Every actor a `System` spawns is built with a routing
`IOProcessor` closed over the actor's own name and the `System`. The
interpreter already sends every `<send>` to a genuinely external target
through whichever `IOProcessor` an `Instance` was constructed with
(ADR 0007); a `System` only has to supply one that recognizes the names
it has spawned. `IOProcessor.Send` must never block on delivery, because
it is called synchronously, in-line, from the interpreter's own
microstep action-list execution — the same in-line-but-non-blocking
contract ADR 0007 established for the core's own delay bookkeeping,
extended here to a processor that may need to page an actor in, or evict
another one to make room, before it can deliver anything. `Send` only
needs to resolve whether a name exists, which is cheap and synchronous;
an unknown name becomes an ordinary returned error, which the interpreter
turns into `error.communication` on the sender's own queue exactly as it
would for any other `IOProcessor` failure. Actually acquiring and
delivering to the target is handed off to the system's own goroutine and
`Send` returns immediately. Doing the acquire-and-deliver step in-line
instead would block the sender's goroutine on the target's entire
macrostep, and two actors sending to each other in the same instant could
deadlock each other's sends.

**Peer delivery and reply delivery are both `Deliver`, not two
mechanisms.** `Instance` already implements `Dispatcher` — `Deliver` is
`Send` under another name (instance.go). The routing `IOProcessor`'s
`Attach` call captures the sending actor's own `Dispatcher` the same way
any `IOProcessor` does. Sending to a peer and reporting an asynchronous
delivery failure back to the original sender are therefore the same
operation from two different callers' points of view: `target.Deliver(ctx,
ev)` on someone's `*Instance`. No separate reply channel or callback type
is introduced; there is no other route back into a session once its own
`dispatchNow` call has returned, so reusing `Deliver` for both directions
means a failure discovered after `Send` already returned nil still has
somewhere to go.

**Durable spawn and page-in are `Rehydrate`, unmodified.** An actor's name
is its session ID. `Spawn(..., Durable())` and paging a durable actor back
in both call `Rehydrate` against the system's configured `Log` and
`SnapshotStore` with that name: load the latest checkpoint if one exists,
replay everything after it with real dispatch suppressed, then go live
(ADR 0006). Nothing about `Rehydrate` needed to change for `actors` to use
it — it already does exactly what "automatically resume where it left
off" means, because it was written to reconstruct a session from a `Log`
without knowing or caring what called it. Writing a second, actors-specific
replay path would duplicate the write-ahead and suppressed-dispatch
invariants `Rehydrate` already gets right, for no behavioral difference.

**At most one live `Instance` per name, at any time, within a `System`.**
`Log.Append` promises strictly increasing, gapless `Seq` values per
session (log.go) on the assumption that a session has exactly one writer.
Two live instances for the same durable name — one freshly paged in while
an older one is still mid-eviction, say — would both believe themselves
that sole writer and could interleave `Append` calls into a genuinely
racy `Seq` sequence, which would in turn make `Rehydrate`'s "replay
everything after the checkpoint" assumption unsound for whoever reads
that log next. Paging in and paging out for a given name are therefore
serialized against each other by the system, so this invariant holds
without asking `Log` itself to detect or reject concurrent writers.

**Paging applies to durable actors only.** Paging out means: capture a
`Snapshot`, pair it with the log's current `LastSeq` as a `Checkpoint`,
persist it, then `Stop` the `Instance` and drop it from the resident
table, keeping only the fact that the name exists and which chart it
runs. That sequence only reconstructs anything because a `Log` exists to
replay forward from the checkpoint afterward. A non-durable actor has no
`Log` behind it, so evicting one from memory would not free it for later
reuse — it would destroy it outright, indistinguishable from a crash a
caller never asked for. Keeping non-durable actors resident for the
system's entire lifetime is therefore not a missing feature; it is the
only behavior that matches what "non-durable" means.

**`Chart` grows `ID()` and `NewDatamodel()` instead of `actors` growing an
`ActorDefinition` wrapper type.** Every chart `actors` can spawn needs an
identity to register under and a way to produce a datamodel value without
a caller present to supply one — both properties a chart already has the
raw material for. A chart's root state is already named deliberately by
whoever built it (`Compound("order", "new", ...)`); `ID()` exposes that
existing name as the chart's own instead of asking every caller who needs
a chart-level identity to invent a second, parallel name that has to be
kept in sync with the first. `WithNewDatamodel` is the one genuinely new
capability, and it is a `BuildOption` on `Chart`, not a field bolted onto
some other type, because "can this chart produce a datamodel on its own"
is a property of the chart, independent of whether anything named
`actors` ever exists. A wrapper `ActorDefinition{Chart, ID, NewDatamodel}`
would hold the same three facts spread across two types instead of one,
with nothing to prevent the two from drifting apart (a chart registered
under an `ActorDefinition` ID that doesn't match the chart's own root
state, for instance). Both additions are additive — a bare `Build(root)`
call is unaffected, and a chart nobody ever registers with a `System`
never needs either — which is the same bar ADR 0005 held `<invoke>`'s
future addition to.

**Residency limits are a predicate, not a number.** `WithResidencyLimit`
takes `func(resident int) bool`, consulted before admitting a new
activation, rather than a fixed integer cap. A fixed number answers "how
many actors" when the actual constraint is almost always "how much
memory," and the relationship between the two varies per chart, per
datamodel, and per deployment. A predicate can inspect `runtime.MemStats`,
a cgroup limit, or a counter shared with everything else on the same
host, and can change its answer over the life of the process as that
pressure changes; a fixed number can only ever be a guess fixed at
`NewSystem` time. `WithMaxResident(n)` — sugar for
`WithResidencyLimit(func(resident int) bool { return resident >= n })` —
is kept alongside it for the common case where a plain count really is
the right proxy, so the predicate form is the general mechanism and the
count form is one instance of it, not two independent options to keep
consistent with each other.

## Consequences

- Naming, spawning, durability, and paging are available to any chart
  without changing a single line of that chart's own transition logic —
  the same chart builds identically whether or not it ever runs inside a
  `System`.
- A crash or restart resumes every durable actor exactly where it left
  off, using the same `Rehydrate` guarantee any other caller of the root
  package already relies on; `actors` adds no new correctness surface to
  audit for replay fidelity.
- A `System` can hold far more addressable actors than it has memory to
  keep resident at once, trading latency (one replay's worth, on first
  message after a page-in) for memory, governed by a caller-supplied
  predicate rather than a number picked in advance.
- An actor's durability is fixed at its first `Spawn` and cannot change
  afterward: a `Log` either has a session's history or it doesn't, and no
  operation retroactively gives one to a session that started without it.
- Non-durable actors cannot be paged out. They are exactly as durable
  actors would be with no `Log` behind them — resident until explicitly
  stopped, gone after that — which also means a system entirely occupied
  by pinned, non-durable actors can have a residency-limit activation fail
  outright, since eviction has nothing left to take back.
- Every actor a `System` spawns is constrained to that `System`: its
  routing `IOProcessor` only recognizes names that `System` has spawned,
  so there is no way to address a name in a different `System` from
  inside a chart. Running more than one `System` process against the same
  `Log`, each capable of paging in the same durable name, is not
  coordinated by this design — the single-live-instance invariant above
  is enforced within one `System`, not across a fleet of them sharing
  storage.
