# 13. Preserving invoke identity across Snapshot/Restore, and resuming a real invocation instead of declaring it dead

Date: 2026-07-13

## Status

Accepted

## Context

ADR 0012 closed the two ways `<invoke>` could be silently duplicated across
a restart: `startInvoke` no longer starts a second real invocation during
`Rehydrate`'s replay (the deterministic bookkeeping in `activeInvokes`/
`invokesByID` still runs, gated by `suppressInvoke`, the same way ADR 0006's
"pure, deterministic recomputation" already covers everything else replay
touches), and once replay catches up, `Rehydrate` synthesizes
`error.communication` for every invocation `ip.invokesByID` still holds,
since it has no way to confirm the real invocation survived the restart.
ADR 0012 also gave `actors.System` an eviction guard, `HasActiveInvokes`,
so `pickEvictionVictim` and `runSweep` both leave a mid-invoke actor
resident rather than page it into that situation.

Two gaps remain.

**Gap 1: `Snapshot` never records which invocations were active, so a
checkpoint-derived restore can't reconstruct them at all.** `Rehydrate`'s
post-replay reconciliation loop only ever sees an invocation whose *entry*
into the invoking state was replayed through `Instance.Send` after the
loaded checkpoint — `processInvokes`/`beginInvoke` populate
`activeInvokes`/`invokesByID` as a side effect of that replay, the same as
a live run. But if the checkpoint itself was captured while the invoke was
genuinely running, with nothing left in the `Log` to replay after it,
`restoreFrom` never touches invoke bookkeeping at all: it reconstructs
`ip.configuration`, `ip.historyValue`, both queues, `ip.running`, and
`ip.pending` (from `snap.PendingSends`) directly off the `Snapshot`, with no
equivalent source for `activeInvokes`/`invokesByID`. The reconciliation
loop then iterates an empty `ip.invokesByID` and finds nothing — the
invocation is neither restarted nor signaled as failed. The actor is stuck
silently, waiting on a `done.invoke.<id>` nothing will ever generate,
because as far as the restored interpretation is concerned no invocation
ever started.

This is reachable today, not just hypothetically. The only call site that
writes a checkpoint through `SnapshotStore.Save` is `actors.System.evictLocked`,
called from three places: `pickEvictionVictim`-driven
residency eviction and `runSweep`-driven idle eviction, both of which do
check `HasActiveInvokes` first and skip the actor entirely (ADR 0012); and
`System.Stop`'s teardown loop, which calls `evictLocked` for every durable
entry unconditionally — with no `HasActiveInvokes` check at all. A
process-wide shutdown while a durable actor has an active `<invoke>`
checkpoints it exactly as-is, mid-invoke, and the next `Rehydrate` hits Gap
1 directly. Even setting `System.Stop` aside, the invariant that makes the
gap unreachable — "nothing calls `Save` while `HasActiveInvokes` is true"
— is enforced by convention at specific call sites in one package, not by
anything structural in `Snapshot`/`Restore` themselves; it depends on every
future checkpoint-writing path remembering to check, and does nothing at
all for a caller that calls `Snapshot`/`Restore` directly against this
package without going through `actors`. `PendingSends` is the existing
counterexample for how this should work instead: `Snapshot`/`restoreFrom`
capture and reconstruct it explicitly, rather than relying on replay to
regenerate delayed sends that predate the checkpoint. Invoke state deserves
the same treatment.

**Gap 2: an invocation backed by a real external process has no way to
confirm whether that process is still alive before being declared dead.**
`<invoke>` (SCXML 6.4) can represent any external service — a subprocess, a
job in an external queue, a container, a remote worker. `InvokeFunc`'s Go
shape (ADR 0010: "a plain Go goroutine") is how one session talks to that
service, not necessarily the service's own lifetime: the goroutine dies
with the process that ran it, but the real-world thing it was managing
might not have. ADR 0012's unconditional `error.communication` is the only
*honest* default for an invocation with no way to check — but it is
needlessly pessimistic for one that can: a subprocess with a known PID, a
job with a queryable status endpoint, anything with an independent
identity a resumable invoke could look up and ask.

## Decision

**Gap 1's fix: `Snapshot` gains `ActiveInvokes []ActiveInvoke`, restored by
`restoreFrom` as bookkeeping, never as a running invocation.**

```go
// ActiveInvoke records one <invoke> that was active in Configuration when a
// Snapshot was taken.
type ActiveInvoke struct {
	State     Identifier // the state that owns this invocation
	SpecIndex int        // this invocation's position among State's <invoke> elements, in document order
	ID        Identifier // the invocation's own id, exactly as assigned when it started
}
```

`SpecIndex`, not a recomputed id, is what lets `restoreFrom` relocate the
exact `compiledInvoke` a captured `ActiveInvoke` came from —
`chart.byID[State].invokes[SpecIndex]`, mirroring the compile-time
construction in `compileState`, which appends one `compiledInvoke` per
`InvokeSpec` in the order `spec.Invokes` lists them, i.e. document order.
Document order is a structural property of the compiled chart, stable
regardless of session history, unlike an auto-generated id
(`"stateid.invoke1"`, produced from `ip.invokeSeq`), which depends on how
many invocations happened to start earlier in the *same session* and so
cannot be recomputed from the chart alone. `ID` is captured and restored
verbatim, not regenerated — this is what lets a `<finalize>` or
`"#_<invokeid>"` send still route correctly after a restart (both key off
`runningInvoke.id` via `ip.invokesByID`, per `applyInvokeSideEffects` and
`interpretation`'s own `"#_"`-target lookup) — and it is why nothing about
`ip.invokeSeq` itself needs to be captured or restored: a restored
invocation's id was already decided, once, live; only a *new* invocation
started after recovery ever calls the generator again, and that call has
no dependency on history predating the restart.

`runningInvoke` (invoke.go) gains a `specIndex int` field, set by
`beginInvoke` from the loop index in `processInvokes`
(`for i, spec := range s.invokes`), so `buildSnapshot` can read it straight
off every entry in `ip.activeInvokes` when producing `Snapshot.ActiveInvokes`
— sorted deterministically by `ID`, the same treatment
`buildSnapshot` already gives `PendingSends` (sorted by `SendID`) so two
snapshots of the same state serialize identically regardless of Go's
randomized map order. `restoreFrom` reconstructs one `runningInvoke` per
captured `ActiveInvoke`: `id` and `state` from the snapshot, `specIndex`
from the snapshot, `finalize`/`autoForward` copied from the located
`compiledInvoke`, `cancel`/`incoming` left as the same no-op/nil shape
`startInvoke` already returns while `suppressInvoke` is set — appended to
`ip.activeInvokes[s]` and `ip.invokesByID[id]`, exactly mirroring how
`PendingSends` are already reconstructed as records, not replayed as
historical events. An `ActiveInvoke` naming a state or a `SpecIndex` the
chart no longer has is an error at `Restore` time, the same class and
severity `restoreFrom` already gives an unknown state in `Configuration` or
`HistoryValue`.

This closes Gap 1 for both of `Rehydrate`'s paths into an active invocation
uniformly — ADR 0012's replay-derived case, and this checkpoint-derived
case — because by the time replay catches up and `goLive` runs,
`ip.invokesByID` correctly reflects every invocation that should be
considered active regardless of which of the two ways it got there. The
post-replay reconciliation loop needs no awareness of the distinction at
all: it already just iterates `ip.invokesByID`.

**Gap 2's fix: an optional `InvokeResumeFunc`, invoked by that same
post-replay reconciliation instead of unconditionally sending
`error.communication`.**

```go
// InvokeResumeFunc reattaches to a possibly-still-running invocation after
// Rehydrate, instead of starting a fresh one via Start. id is the
// invocation's own id, preserved exactly as it was before the restart (see
// Snapshot.ActiveInvokes) -- a resumable invoke uses it, or whatever
// identity it encoded into params, to look up whatever real-world resource
// it was talking to: a subprocess's PID, a job id in an external queue, a
// container name. params is recomputed fresh from the fully-restored
// datamodel exactly as a live entry would compute it -- there is no
// separate "original params" preserved anywhere, consistent with how every
// other side of replay recomputes rather than stores.
//
// Resume's return is treated exactly like Start's: a non-nil error becomes
// error.communication -- an informed "confirmed gone" rather than
// Rehydrate's own unconditional assumption -- a nil error with data becomes
// done.invoke.<id> immediately (the work finished while this session was
// down), and blocking on ctx/io.Incoming continues the invocation exactly
// as if it had never stopped.
type InvokeResumeFunc func(ctx context.Context, id Identifier, params any, io InvokeIO) (data any, err error)
```

`WithInvokeResume(fn InvokeResumeFunc) InvokeOption` sets `InvokeSpec.Resume`,
compiled to `compiledInvoke.resume` alongside the existing
`start`/`params`/`finalize`/`autoForward` fields. Left `nil` — every
invocation today, zero migration cost — `Rehydrate`'s reconciliation keeps
ADR 0012's exact behavior: `error.communication`, unconditionally, for an
invocation with no way to check. When it is set, the reconciliation loop
calls `Resume(ctx, id, params, io)` instead of sending
`error.communication` directly, using `params` recomputed by calling the
located `compiledInvoke.params` against the restored `ExecContext` — the
same call `beginInvoke` makes for a live entry, just made again rather than
read back from anywhere, because nothing preserves the params a prior,
pre-restart invocation actually received.

This needs no new event type or interpreter-core concept, because
`Resume`'s return is threaded through *the same* done.invoke/
error.communication synthesis `startInvoke` already performs for `Start`:
a nil `err` produces an `EventExternal` named `"done.invoke." + id` carrying
`data` and `InvokeID: id` on `Deliver`; a non-nil `err` produces an
`EventPlatform` named `ErrEventCommunication` with `InvokeID: id`;
panics are recovered and reported as `error.communication` the same way;
`ctx` cancellation (should the invoking state be exited again before
`Resume` returns) suppresses both, per SCXML 6.4.3, exactly as it already
does for `Start`. The fix is which function gets called and when, not a
new outcome the interpreter has to learn to produce. This sharing is
expected to be implemented as one internal goroutine-wrapping helper,
parameterized by which function to run (`spec.start` vs `spec.resume`),
rather than duplicated — the panic recovery, `ctx` cancellation, and
event-synthesis logic must stay identical between the two paths for
`Resume` to genuinely behave like "the same invocation, still going" rather
than a parallel, subtly different mechanism with its own edge cases to keep
in sync by hand.

A `WithInvokeResume` invocation does not need `WithInvokeID` to get a
stable, predictable id across a restart. Gap 1's fix already preserves
whatever id — explicit or auto-generated — the invocation originally had,
verbatim, in `Snapshot.ActiveInvokes`; `Resume` receives that exact id as
its `id` parameter regardless of which InvokeSpec field originally produced
it.

## Consequences

- A resumable invoke's author is taking on two obligations `Start` never
  had. First, whatever identity `Resume` uses to find the real-world
  resource again must itself survive the restart — either `id` is already
  that identity (a subprocess whose PID *is* the invoke id, a queue job
  named after it), or `Resume`/`Params` must encode a durable identity into
  `params` at invoke time, since nothing else about the pre-restart
  invocation is preserved. Second, `Resume` must be able to tell "still
  running" from "gone" without side effects of its own: it may be called
  after a crash mid-write, after an idle-timeout page-out and page-in, or
  after a residency eviction, and none of those are the invocation's own
  business — an idempotent status check, not a retry-inducing action, is
  what makes `Resume` an honest improvement over `error.communication`
  rather than a way to double-run part of the invocation's own work.
- A non-resumable invoke — every invocation that does not call
  `WithInvokeResume`, including every invocation written before this ADR —
  is bit-for-bit unchanged from ADR 0012: `Rehydrate`'s reconciliation loop
  still sends `error.communication`, unconditionally, the moment replay
  catches up.
- `Snapshot`'s JSON shape gains one field, `ActiveInvokes`
  (`active_invokes` in `snapshotWire`, `omitempty` like `PendingSends`).
  This is additive-safe, not a breaking change: `snapshotVersion`
  (currently `1`) is carried through `Snapshot.Version`/`snapshotWire.Version`
  on every round trip but nothing in `Restore`/`restoreFrom`
  branches on its value today, so no version bump is required for
  correctness. Decoding an old, pre-this-ADR snapshot into the new
  `Snapshot` simply leaves `ActiveInvokes` nil — equivalent to Gap 1 still
  being open for that specific already-serialized checkpoint, which is the
  correct, unsurprising behavior for data that predates the fix, not a
  decode error. Decoding a new snapshot with old code ignores the unknown
  `active_invokes` key, per `encoding/json`'s default behavior.
- `actors.System`'s eviction guard needs no change. `HasActiveInvokes`,
  `pickEvictionVictim`, and `runSweep` already exist for the same
  underlying reason this ADR's Gap 2 does: a real invocation's real-world
  side is not something paging can safely disturb. Whether or not
  `WithInvokeResume` is set changes what happens the next time the actor
  *is* rehydrated; it does not change whether evicting it now is safe, so
  the guard's unconditional "skip if `HasActiveInvokes`" — with no
  awareness of `Resume` — remains exactly correct. `System.Stop`'s
  teardown loop is the one existing path that checkpoints past that guard
  (see Context); this ADR does not change that either, but it does mean a
  `Resume`-equipped invocation surviving a `System.Stop`-triggered restart
  now gets a real chance at continuity where before it only ever got
  `error.communication`.
