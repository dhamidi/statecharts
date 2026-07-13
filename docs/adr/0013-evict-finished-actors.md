# 13. Evicting actors that have reached their own final state

Date: 2026-07-13

## Status

Accepted

## Context

`System` (ADR 0009) already pages a durable actor out on idle timeout or
under residency pressure, and (ADR 0012) never picks one with an active
`<invoke>` as a victim for either. Neither mechanism, nor anything else in
`System`, ever asks whether a resident actor's `Instance` is still doing
anything at all. A chart that reaches its own top-level final
configuration stops its own goroutine (`Instance.run`'s loop exits once
`ip.running` goes false, closing `doneCh`) -- but the `actorEntry` in
`System`'s table keeps pointing at that now-inert `Instance` indefinitely.
Nothing ever calls `Stop` on it, nothing ever clears `entry.instance`, and
nothing ever reclaims its datamodel or interpreter state. Unlike an idle
actor (which might still receive a message later) or one with an active
`<invoke>` (which is doing real work), an actor that has reached a final
state is *guaranteed* to never do anything else -- SCXML has no way back
out of a top-level final configuration. Leaving it resident is pure waste,
and exactly the "hog memory" complaint this issue reports.

There is a second, latent consequence once this is considered together
with the existing eviction paths. `evictLocked` (used by residency-pressure
eviction, idle-timeout sweeping, and `Stop`) always calls
`Instance.Snapshot` first -- but `Snapshot` only works against a still-live
actor goroutine; called against an `Instance` that has already stopped on
its own, it always fails with `ErrInstanceStopped`. So a durable actor that
happens to reach its final state and is still resident by the time
`System.Stop` runs would make `Stop` itself report a spurious error for an
actor that had simply, correctly, finished.

## Decision

**`Instance` gains `Done() <-chan struct{}`,** returning the same channel
`Wait` already blocks on, for a non-blocking check instead of a blocking
one -- the same relationship `context.Context`'s `Done()` has to blocking
on `<-ctx.Done()` directly. `select { case <-inst.Done(): default: }` is
now the standard way to ask "has this Instance stopped on its own?"
without waiting for it.

**A finished `Instance` -- one where `Done()` is already closed -- is
freed the moment `System` notices, from three independent places, so no
configuration combination leaves one permanently stranded:**

- `System.deliver` checks right after every `Instance.Deliver` call, still
  holding `entry.mu` for that name. This is what catches the overwhelmingly
  common case -- a chart reaching its final state while processing a
  message -- immediately, with no extra delay.
- `System.admit` calls a new `System.reapFinished`, which sweeps every
  resident actor (durable or not) for one that has already finished,
  before it even asks whether the residency limit predicate says to make
  room. A finished actor is free real estate: no least-recently-active
  selection heuristic is needed to justify reclaiming it.
- `System.runSweep` also calls `reapFinished` on every periodic pass,
  independent of any individual actor's idle time -- the periodic-sweep
  counterpart to `deliver`'s inline check, for a final state reached
  entirely from an internal delayed `<send>` (a real wall-clock event with
  no `System.deliver` call involved at all) with nothing ever `Tell`ed to
  it again afterward.

**Freeing a finished actor never goes through the normal
checkpoint-then-stop path.** `evictLocked` itself now checks `Done()`
first: if already closed, it calls `Instance.Stop` (idempotent, and
already a no-op success on an already-stopped `Instance`) and clears
`entry.instance`, skipping `Snapshot`/`LastSeq`/`SnapshotStore.Save`
entirely -- there is nothing left to capture that isn't already durably
reflected in whatever `Log` entries led here, and attempting to capture it
anyway would only fail (`Snapshot` requires a live actor goroutine).
Putting this check inside `evictLocked` itself, rather than only at each
call site, is what also fixes `Stop`'s own latent bug: `Stop`'s durable
path already calls `evictLocked` unconditionally for every resident
durable actor, so a finished one reaching `Stop` before anything else
touched it no longer makes `Stop` report a spurious snapshot failure.

**Eviction this way applies to non-durable actors too.** Idle-timeout and
residency-limit eviction both still exclude non-durable actors (they have
no `Log` to rebuild themselves from, so paging one out would destroy it
rather than hibernate it) -- but a *finished* non-durable actor has nothing
left to lose either way, so `reapFinished` and `deliver`'s inline check
apply uniformly, and `evictLocked`'s finished-instance fast path never
touches `s.cfg.log`/`s.cfg.snapshots` (unlike its still-running path),
so it works even for a `System` with no durability configured at all.

## Consequences

- A `System` no longer accumulates memory indefinitely from actors that
  have legitimately finished -- the actor system now matches the
  intuition the issue reports: reaching a final state is, unconditionally,
  a safe and eventual (usually immediate) eviction.
- `Instance.Done()` is a small, reusable addition beyond this one use: any
  embedder wanting a non-blocking "has this stopped?" check -- previously
  only obtainable by racing a zero-timeout `Wait` -- now has one.
- A resident count observed through `residentCount` (and so
  `WithResidencyLimit`'s own predicate) can now drop between calls purely
  because actors finished, not just because something explicitly evicted
  or stopped them -- a caller inspecting residency for diagnostics should
  not assume a shrinking count always means idle-timeout or
  residency-pressure eviction happened.
- One narrow gap remains by design, not by oversight: an actor that
  reaches its final state entirely from an internal delayed `<send>`, with
  idle-timeout sweeping disabled (`WithIdleTimeout(0)`) and never `Tell`ed
  again, is only reaped the next time `admit` happens to run for a
  *different* Spawn/page-in, or when `Stop` tears down the whole `System`.
  Closing that last sliver would mean running a sweep loop unconditionally
  regardless of idle-timeout configuration, which is a larger change than
  this issue's complaint -- actors hogging memory with no eviction
  mechanism at all -- calls for.
