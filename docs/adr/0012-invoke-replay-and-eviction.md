# 12. Disabling `<invoke>` during replay, signaling mid-invoke recovery, and pausing eviction for it

Date: 2026-07-13

## Status

Accepted

## Context

ADR 0010 deliberately left `<invoke>` out of `Snapshot`/`Restore`/`Rehydrate`:
an invocation is a real-world side effect, the same kind `IOProcessor`
dispatch and `Logger` writes already are, and reconstructing history must
never repeat one of those a second time. `Rehydrate` (replay.go) already
gates `IOProcessor.Send`/`Cancel` and `Logger.Log` behind a `replayGate`
that stays suppressed until replay catches up (ADR 0006, ADR 0011) — but
`interpretation.startInvoke` (the seam `beginInvoke` calls to actually start
one) was never included in that gate. Both of `Rehydrate`'s two code
paths — the initial-configuration bootstrap `Instance.Start` runs when no
checkpoint exists, and replaying each subsequent `Log` entry through
`Instance.Send` — can enter a state carrying an `<invoke>`, and until now
each one really did restart it: a second real invocation, running
concurrently with (or instead of) whatever the original live one was doing,
every single time an actor is rehydrated or paged back in.

This has two further consequences once considered together with `actors`
(ADR 0009). First, once an actor is durable, `Rehydrate` is not just a
cold-start-after-a-crash path; `System.activateLocked` calls it for every
page-in, including a routine one after ordinary idle-timeout or
residency-limit eviction. So a chart with an `<invoke>` in its active
configuration would have that invocation silently duplicated on every page
cycle, not just after a crash. Second, precisely because `Rehydrate` cannot
resume a real invocation, evicting an actor while one is running is worse
than evicting one that is merely idle: the actor's own understanding of
that invocation (SCXML 6.4's "active until the owning state exits") and
whatever is actually still happening in the real world are guaranteed to
diverge the moment it is paged back in.

## Decision

**`startInvoke` is gated the same way `IOProcessor`/`Logger` already are,
but the interpreter-core bookkeeping still runs.** `Instance` gains an
`atomic.Bool` (`suppressInvoke`), checked at the top of
`Instance.startInvoke`: while set, it returns a no-op `cancel` and a `nil`
`incoming` without ever calling `spec.start` or spawning a goroutine.
Everything upstream of that call — `ip.invokeSeq`'s deterministic ID
assignment, the `runningInvoke` recorded into `activeInvokes`/
`invokesByID` — still happens exactly as it would live, because none of
that is itself a real-world side effect; it's the same "pure, deterministic
recomputation given the current configuration and the inbound event" ADR
0006 already relies on for everything else replay touches. This is what
lets a later replayed event still match a `<finalize>` or
`"#_<invokeid>"` send by the same `InvokeID` the live run would have
generated, and what lets the actor system ask, after replay, exactly which
invocations the restored configuration still considers active.

`Rehydrate` sets `suppressInvoke` before its bootstrap `Start` call and
clears it right before `goLive`, so the gate covers both of `Rehydrate`'s
paths into a state with an `<invoke>`, not just the explicit replay loop.

**Once replay catches up, Rehydrate synthesizes `error.communication` for
every invocation still active.** By definition, anything such an
invocation had actually delivered by the time this session stopped being
driven is already in the `Log` and was just replayed above; what
`Rehydrate` cannot do is resume the invocation itself, or confirm its
external process survived the restart. So immediately after `goLive`,
`Rehydrate` iterates `ip.invokesByID` (in the same deterministic order
`applyInvokeSideEffects` already uses) and delivers one
`Event{Name: ErrEventCommunication, Type: EventPlatform, InvokeID: id}`
per active invocation — the same event shape SCXML already gives a failed
`<send>` or a failing `InvokeFunc`, so a chart's existing
`error.communication` handling (retry, fail over, transition out of the
invoking state) is what decides what happens next, rather than the
invocation silently appearing to still be running.

**`HasActiveInvokes` lets the actor system avoid evicting into that
situation in the first place.** `Instance` gains an exported
`HasActiveInvokes(ctx) (bool, error)`, round-tripped through the actor's own
goroutine (the same request/reply pattern `Snapshot` already uses, since
`activeInvokes`/`invokesByID` are single-goroutine-owned interpreter-core
state). `System.pickEvictionVictim` (residency-limit eviction) and
`System.runSweep` (idle-timeout eviction) both skip a candidate this
reports true for, leaving it resident regardless of how long it has been
idle or how badly residency is needed — the same posture as skipping a
non-durable actor entirely, since paging either one out loses something
`Rehydrate` cannot get back. A query failure is treated the same as "yes,
active" (skip it) rather than risking eviction on an unknown answer;
`runSweep` reports it through the existing `WithOnSweepError` callback,
the same as any other sweep failure.

## Consequences

- Restarting an actor — after a crash, or after ordinary idle-timeout/
  residency-limit paging — no longer duplicates a real invocation. A chart
  relying on `<invoke>` to survive a restart still must re-`Invoke` it
  itself (ADR 0010's guidance is unchanged), but now does so from a clean,
  single `error.communication` signal instead of racing a second live copy
  of the original invocation.
- `error.communication` synthesized this way is not itself written to the
  `Log`: it is a deterministic function of the restored configuration
  (itself derived purely from the `Log`), the same way `Rehydrate`'s
  initial-entry bootstrap is deterministic and unlogged. Replaying to the
  same point a second time reproduces the identical event.
- An actor system with a residency limit can now legitimately run out of
  evictable actors while every resident actor is either non-durable or
  mid-invoke; `Spawn`/page-in surfaces this the same way it already
  reports `ErrResidencyExhausted` for any other case with nothing left to
  evict.
- `HasActiveInvokes`, like `Snapshot`, blocks until the actor's own
  goroutine answers, so it carries the same latency and cancellation
  characteristics `Snapshot` already has; it is not a lock-free peek.
