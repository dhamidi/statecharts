# 11. `<log>` as a fourth `ExecContext` primitive, routed through a configurable `Logger`

Date: 2026-07-13

## Status

Accepted

## Context

`ExecContext` currently exposes three ways for executable content to affect
anything outside the pure microstep computation: `Send` (real inter-session
or external communication, routed through whichever `IOProcessor` an
`Instance` was built with), `Raise` (same-session internal events, applied
directly to the internal queue), and `Cancel` (best-effort withdrawal of a
pending delayed `Send`). Every one of them is backed by a closure field on
`ExecContext` — `send func(name Identifier, opts SendOptions)`, `raise
func(Event)`, `cancel func(sendID Identifier)` — populated once, at the
`execContext()` construction site in interpreter.go, from a method on the
owning `*interpretation` (`ip.doSend`, `ip.enqueueInternal`, `ip.doCancel`).

None of the three covers diagnostic output: printing what a chart is doing,
for a human or a log aggregator to read, with no expectation that anything
reads it back in and no relationship to the chart's own event traffic.
SCXML's `<log>` executable-content element exists for exactly this case,
distinct from both neighbors it sits next to in the executable-content
section: unlike `<send>`, it never leaves the interpreter's own process or
crosses a session boundary, so it has nothing to do with `IOProcessor`;
unlike `<raise>`, it never produces an event another transition could match
against, so it has nothing to do with either queue. It takes a label and an
expression to evaluate and log, and the spec leaves where the result goes
entirely to the implementation.

Nothing in this codebase currently fills that gap. The `greet` example in
the README calls `fmt.Println` directly from inside an `ActionFunc`, which
is a real crack in the library's own story: every other side effect a chart
can produce is required to go through `IOProcessor`, specifically so an
embedder can observe, redirect, or suppress it (a replaying `Instance`
suppresses `IOProcessor` dispatch through `replayGate` for exactly this
reason). A `fmt.Println` inside an action is invisible to that machinery —
an embedder cannot redirect it, cannot suppress it during replay, and
cannot tell it apart from any other line a misbehaving dependency might
print. There is no sanctioned alternative to reach for instead: no
`<log>`-equivalent method exists on `ExecContext`, and neither
`NoopIOProcessor` nor `LocalIOProcessor` prints anything, nor should
either — `IOProcessor` is defined as the seam for genuinely external
dispatch, and stdout is not an external session.

## Decision

**`ExecContext` gains a fourth primitive, `Log(label string, data Value)`,
matching the existing three primitives' shape exactly.** A `log func(label
string, data Value)` closure field is added to `ExecContext` alongside
`raise`, `send`, and `cancel`; `Log` calls it if non-nil and is a silent
no-op otherwise, the same posture `Raise`, `Send`, and `Cancel` already
have for a bare `ExecContext{}` with no owning `interpretation`. The
signature takes a label plus canonical `Value`, mirroring `<log>`'s own
label-plus-expr shape rather than collapsing both into one pre-formatted
string. `Event.Data` and logger data share that syntax-neutral representation,
so callers hand over structured data and let whatever is on the other end of
the seam decide how to render it. Collapsing to a single `string` parameter would force every call site
to run its own `fmt.Sprintf` before handing the result off, which is the
formatting decision a `Logger` implementation exists to make instead. `Log`
is wired up at the same `execContext()` construction site as the other
three (interpreter.go), from a new `ip.doLog` method on `*interpretation`.

**Where the output goes is a `Logger`, configured per `Instance` via
`WithLogger`, defaulting to a no-op.** `Logger` is a one-method interface:

```go
type Logger interface {
	Log(label string, data Value)
}
```

`WithLogger(Logger) Option` follows `WithIOProcessor` and `WithClock`
exactly: a field on `instanceConfig`, set by the option, read by
`newInstance` onto the `interpretation`'s own `logger` field, with
`defaultInstanceConfig` defaulting it to an exported `NoopLogger` the same
way `io` defaults to `NoopIOProcessor`. A library that printed to stdout by
default would make every embedder's own logs a fight against ours; a chart
built with no `WithLogger` call must behave identically whether or not any
`ExecContext.Log` call sites exist in its action code; today, with the
`fmt.Println` in the README's `greet` example, that isn't true, and fixing
it is the entire point of this addition.

**A ready-made `WriterLogger` ships alongside the interface, so the
`fmt.Println` in the README has a one-line replacement.** `WriterLogger`
wraps an `io.Writer` and formats each call as a single line:

```go
type WriterLogger struct{ w io.Writer }

func NewWriterLogger(w io.Writer) *WriterLogger
func (l *WriterLogger) Log(label string, data Value)
```

`greet`'s `fmt.Println("alice received", ev.Name, "from", ev.Origin)`
becomes `ec.Log("received", ev.Data)` inside the action, plus one
`statecharts.WithLogger(statecharts.NewWriterLogger(os.Stdout))` at the call
site that builds the `Instance` — printing moves from being baked into the
chart's own logic to being a property of how that particular `Instance` was
configured, which is the same move `IOProcessor` already made for `<send>`.
A caller who wants JSON lines, a `log/slog` handler, or a metrics counter
instead writes their own three-line `Logger`; `WriterLogger` exists only to
cover the common case without forcing every example and every simple
program to hand-write one.

**`Log` never fails and is always synchronous, in-line — a stronger, not
weaker, guarantee than `Send`'s.** `Raise` already has no Go error return:
a broken `Logger` is the caller's own problem, exactly as a broken
downstream reactor to a raised event is the caller's own problem, not
something the interpreter treats as `error.execution`. `Send`, by contrast,
*can* produce an `error.communication` event, because it reaches a real
`IOProcessor` that talks to something outside this process and can
genuinely fail to reach it. `Log` never dispatches through an `IOProcessor`
at all — a `Logger` is invoked directly from `ip.doLog`, with no queue, no
delay bookkeeping, and no dispatch-failure path to report anywhere. That
also means `Log` carries none of `IOProcessor.Send`'s "must return quickly,
hand off to your own goroutine" obligation: that obligation exists because
`Send` is called synchronously in-line during a microstep's action list and
would otherwise stall the whole interpreter on real I/O, but a `Logger`
call is not real I/O in that sense — it is a local, in-process write, and
an implementation slow enough to matter is a problem for its own goroutine,
not a reason to give `Log` a deferred or queued execution model it doesn't
otherwise need.

**Replay gates `Logger` the same way it already gates `IOProcessor`.**
`Rehydrate` gates real `IOProcessor` dispatch behind a `replayGate` while
it replays historical `Log` entries, specifically so reconstructing a
session never repeats a real-world effect that already happened once, live.
A `Logger` write is a real-world effect by the same test — it leaves bytes
somewhere outside the interpreter — so replaying years of history through a
`WriterLogger` would otherwise reprint every diagnostic line a session ever
produced, live, a second time, on every restart. `Rehydrate` wraps whatever
`Logger` an `Instance` was configured with in the same suppress-until-live
gate it already builds for `IOProcessor`, going live at the same instant.

**A fourth primitive, not a specialized use of one of the other three.**
Routing `Log` through `Send` to some dedicated logging target would send
every diagnostic line through the full `IOProcessor`/`dispatchNow` path —
sendID bookkeeping, delay-timer plumbing, a target string a caller has to
invent and every `IOProcessor` implementation has to special-case — for a
side channel that has no delivery target, no reply, and no reason to be
delayed. It would also make logging unavailable to precisely the charts
most likely to want it: one built with no `IOProcessor` configured at all
(the common case while writing or testing a chart) routes any `Send` to
`NoopIOProcessor`, which silently discards it, so "just `Send` to a
logging target" would mean diagnostic output vanishes exactly when nothing
else is wired up yet. Calling `fmt.Println` directly from inside the action,
today's status quo, is the violation this decision exists to close, not an
alternative to weigh against it: it is neither observable nor swappable by
whoever embeds the chart, which is the one property every other seam in
this package — `IOProcessor`, `Clock`, now `Logger` — is built to have.

## Consequences

- The README's `greet` example, and any chart like it, has a sanctioned way
  to produce diagnostic output without calling a printing function directly
  from inside an `ActionFunc`. A chart that never calls `ExecContext.Log`
  and an `Instance` never configured with `WithLogger` behave identically
  to one built before this addition existed.
- `Logger` is configured per `Instance`, not per `Chart`, exactly like
  `IOProcessor` and `Clock` — the same chart can log to `os.Stdout` under
  one `Instance` and nowhere under another (a test), with no change to the
  chart's own build.
- Diagnostic output produced through `Log` is never part of `Snapshot`,
  never appended to a `Log` (the persistence interface in log.go), and
  never replayed as data the way a `KindTimerFired` entry is — a `Logger`
  call is recomputed by re-running the same deterministic action during
  replay, not read back from storage, the same way any other pure
  computation inside an action is. The persistence `Log` and the
  diagnostic `Logger` are deliberately unrelated seams despite the
  name overlap; nothing in this design lets one stand in for the other.
- A `Logger` implementation that panics brings down the actor goroutine
  exactly as a panicking `ActionFunc` already does — `Log` adds no new
  recovery behavior, since `runActions` doesn't specially guard any of the
  other three primitives either.
- A chart author who wants log lines tagged with the state that produced
  them gets nothing automatic from this design; `label` and `data` are the
  entire payload, matching `<log>`'s own label-plus-expr shape, and a
  caller who wants more context passes it explicitly through `data` or
  encodes it into `label` at the call site.
