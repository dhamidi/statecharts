<strong>A Go implementation of the W3C SCXML state machine semantics — the interpretation algorithm, without the XML.</strong>

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/dhamidi/statecharts"
)

func main() {
	chart, err := statecharts.Build(
		statecharts.Compound("conn", "disconnected",
			statecharts.Children(
				statecharts.Atomic("disconnected", statecharts.On("connect", statecharts.Target("open"))),
				statecharts.Atomic("open", statecharts.On("drop", statecharts.Target("disconnected"))),
			),
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	conn := statecharts.New(chart, nil)
	ctx := context.Background()
	if err := conn.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := conn.Stop(ctx); err != nil {
			log.Fatal(err)
		}
	}()

	if err := conn.Send(ctx, statecharts.Event{Name: "connect", Type: statecharts.EventExternal}); err != nil {
		log.Fatal(err)
	}
	fmt.Println(conn.Configuration()) // [open]
}
```

That's it!

statecharts handles transition selection, parallel-region conflicts, and
state entry/exit ordering exactly as the SCXML specification defines them,
so you can focus on the states and transitions your application actually
needs.

### Key features

- **Faithful**: implements SCXML's interpretation algorithm, not just its
  shape — parallel-region conflict resolution, shallow and deep history,
  and internal-vs-external transition semantics all behave as the spec
  defines
- **No XML**: charts are built directly in Go and validated once, at build
  time, with static errors instead of runtime surprises
- **Concurrency-safe by construction**: each running chart is a single
  goroutine; the public API is plain method calls, never channels
- **Persistent**: recording every message a chart receives is enough to
  reconstruct its exact state later, with a ready-made database/sql-backed
  log

<!-- omit in toc -->
## Table of Contents

- [Installation](#installation)
- [Quickstart](#quickstart)
- [What is SCXML?](#what-is-scxml)
- [Core Concepts](#core-concepts)
  - [Charts](#charts)
  - [Instances](#instances)
  - [The Datamodel](#the-datamodel)
  - [IOProcessor](#ioprocessor)
  - [Error events](#error-events)
  - [Log](#log)
  - [Invoke](#invoke)
  - [Persistence](#persistence)
- [Extras](#extras)
  - [Testing with a manual clock](#testing-with-a-manual-clock)
  - [The sqllog subpackage](#the-sqllog-subpackage)
- [Running actor systems](#running-actor-systems)
  - [Quickstart](#quickstart-1)
  - [Building a system](#building-a-system)
  - [Registering and spawning actors](#registering-and-spawning-actors)
  - [Addressing actors by name](#addressing-actors-by-name)
  - [Automatic paging](#automatic-paging)
  - [A full example](#a-full-example)
  - [Connecting two systems](#connecting-two-systems)

## Installation

```bash
go get github.com/dhamidi/statecharts
```

## Quickstart

A client connection's lifecycle in full: dialing, a guarded,
attempt-count-limited retry, and a drop that triggers reconnection
automatically.

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/dhamidi/statecharts"
)

// Conn is a live connection. A real deployment satisfies Dialer against an
// actual transport -- Go's standard library has no native WebSocket client,
// so gorilla/websocket or github.com/coder/websocket are the usual choices.
// flakyDialer below stands in for one so this example has no dependencies
// to install.
type Conn interface {
	Close() error
}

// Dialer opens a new Conn.
type Dialer interface {
	Dial(ctx context.Context) (Conn, error)
}

type fakeConn struct{}

func (fakeConn) Close() error { return nil }

// flakyDialer fails its first n dials, then succeeds -- standing in for a
// server that's briefly unreachable.
type flakyDialer struct {
	n       int
	attempt int
}

func (d *flakyDialer) Dial(ctx context.Context) (Conn, error) {
	d.attempt++
	if d.attempt <= d.n {
		return nil, fmt.Errorf("dial attempt %d: connection refused", d.attempt)
	}
	return fakeConn{}, nil
}

type Connection struct {
	Dialer     Dialer
	Retries    int
	MaxRetries int
	conn       Conn
}

func buildChart() (*statecharts.Chart, error) {
	underRetryLimit := statecharts.Cond(func(c *Connection, ec statecharts.ExecContext) bool {
		return c.Retries < c.MaxRetries
	})

	// dial calls out to the transport and reports the outcome with Raise,
	// not error.execution: a failed dial here is an expected outcome the
	// chart's own guarded retry already handles, not a bug in the action
	// itself -- see Error events below for the case that is a bug.
	dial := statecharts.Action(func(c *Connection, ec statecharts.ExecContext) error {
		c.Retries++
		conn, err := c.Dialer.Dial(context.Background())
		if err != nil {
			ec.Raise(statecharts.Event{Name: "dial.failed", Data: err})
			return nil
		}
		c.conn = conn
		ec.Raise(statecharts.Event{Name: "dial.ok"})
		return nil
	})
	recordOpen := statecharts.Action(func(c *Connection, ec statecharts.ExecContext) error {
		c.Retries = 0
		return nil
	})
	giveUp := statecharts.Action(func(c *Connection, ec statecharts.ExecContext) error {
		ec.Log("giving up", c.Retries)
		return nil
	})

	return statecharts.Build(
		statecharts.Compound("connection", "disconnected",
			statecharts.Children(
				statecharts.Atomic("disconnected",
					statecharts.On("connect", statecharts.Target("connecting")),
				),
				statecharts.Atomic("connecting",
					statecharts.OnEntry(dial),
					statecharts.On("dial.ok", statecharts.Target("open"), statecharts.Then(recordOpen)),
					statecharts.On("dial.failed", statecharts.Target("connecting"), statecharts.If(underRetryLimit)),
					statecharts.On("dial.failed", statecharts.Target("disconnected"), statecharts.Then(giveUp)),
				),
				statecharts.Atomic("open",
					statecharts.On("drop", statecharts.Target("connecting")),
					statecharts.On("close", statecharts.Target("disconnected")),
				),
			),
		),
	)
}

func run(ctx context.Context) (err error) {
	chart, err := buildChart()
	if err != nil {
		return err
	}

	conn := statecharts.New(chart, &Connection{Dialer: &flakyDialer{n: 2}, MaxRetries: 3})
	if err := conn.Start(ctx); err != nil {
		return err
	}
	defer func() {
		if stopErr := conn.Stop(ctx); stopErr != nil && err == nil {
			err = stopErr
		}
	}()

	if err := conn.Send(ctx, statecharts.Event{Name: "connect", Type: statecharts.EventExternal}); err != nil {
		return err
	}
	fmt.Println(conn.Configuration()) // [open] -- two failed dials, then a third that succeeds

	if err := conn.Send(ctx, statecharts.Event{Name: "drop", Type: statecharts.EventExternal}); err != nil {
		return err
	}
	fmt.Println(conn.Configuration()) // [open] -- reconnected before Send returned

	if err := conn.Send(ctx, statecharts.Event{Name: "close", Type: statecharts.EventExternal}); err != nil {
		return err
	}
	fmt.Println(conn.Configuration()) // [disconnected]

	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

`connecting`'s two `dial.failed` transitions are tried in document order: the
first, guarded by `underRetryLimit`, retries by re-entering `connecting`
itself, which re-runs `dial` on entry; the second, unconditional, is only
reached once the guard fails, and gives up. `open`'s `drop` handler routes a
dropped connection through the same `connecting` state a fresh `connect`
would, so reconnection and initial connection share one retry path.

## What is SCXML?

[SCXML](https://www.w3.org/TR/scxml/) (State Chart XML) is the W3C
recommendation for representing state machines and statecharts — Harel's
extension of finite state machines with hierarchy (states containing
states), orthogonality (parallel regions active at once), and history.
It defines a precise algorithm for how a chart processes one event at a
time: which transitions are enabled, how conflicts between them are
resolved, and in what order states are exited and entered.

This package implements that algorithm. It does not implement SCXML's XML
document format or its expression-language datamodels — charts are Go
values, and expressions are Go functions.

## Core Concepts

### Charts

<details>
<summary>Show Chart Examples</summary>

A chart is a tree of states. Five constructors build the tree; `Build`
compiles and validates it into a `*Chart`, which can then be run any
number of times by any number of `Instance`s.

- `Atomic` — a leaf state
- `Compound` — a state with children, exactly one of which is entered by
  default
- `Parallel` — a state whose children are all simultaneously active, each
  one its own region
- `Final` — marks completion of its parent
- `History` — a pseudostate that remembers which of its parent's children
  were last active

Transitions are attached to a state with `On` (matching one or more
space-separated event names) or `Eventless` (fires automatically once its
guard is true), and configured with `Target`, `If`, `Then`, and
`AsInternal`.

Parallel regions transition independently of one another. Generating
thumbnails at several sizes for one upload is three independent regions, one
per size, each moving from `pending` to `done` on its own:

```go
chart, err := statecharts.Build(
	statecharts.Parallel("thumbnails",
		statecharts.Children(
			statecharts.Compound("small", "small.pending",
				statecharts.Children(
					statecharts.Atomic("small.pending", statecharts.On("small.done", statecharts.Target("small.done"))),
					statecharts.Atomic("small.done"),
				),
			),
			statecharts.Compound("medium", "medium.pending",
				statecharts.Children(
					statecharts.Atomic("medium.pending", statecharts.On("medium.done", statecharts.Target("medium.done"))),
					statecharts.Atomic("medium.done"),
				),
			),
			statecharts.Compound("large", "large.pending",
				statecharts.Children(
					statecharts.Atomic("large.pending", statecharts.On("large.done", statecharts.Target("large.done"))),
					statecharts.Atomic("large.done"),
				),
			),
		),
	),
)
```

Sending `small.done` moves the `small` region from `small.pending` to
`small.done` and leaves `medium` and `large` untouched — all three regions
are active in the configuration at once.

A history pseudostate remembers where a compound state's children were
before it was exited, so re-entering it can resume there instead of
starting over. A multi-step job re-entering its parent state after being
paused resumes at whichever step it last reached:

```go
statecharts.Compound("job", "validating",
	statecharts.Children(
		statecharts.Atomic("validating", statecharts.On("next", statecharts.Target("thumbnailing"))),
		statecharts.Atomic("thumbnailing", statecharts.On("next", statecharts.Target("notifying"))),
		statecharts.Atomic("notifying"),
		statecharts.History("job.hist", statecharts.Shallow, "validating"),
	),
),
```

A transition targeting `"job.hist"` re-enters whichever of `validating`,
`thumbnailing`, or `notifying` was active when `job` was last exited — or
`validating`, the given default, the first time.

</details>

### Instances

<details>
<summary>Show Instance Examples</summary>

`New` builds an `Instance` from a `*Chart` and a datamodel value; `Start`
spawns its interpreter goroutine. From there, an `Instance` is driven
entirely through plain method calls:

```go
in := statecharts.New(chart, myJob)
if err := in.Start(ctx); err != nil {
	// ...
}

if err := in.Send(ctx, statecharts.Event{Name: "start", Type: statecharts.EventExternal}); err != nil {
	// ...
}

fmt.Println(in.Configuration())

if err := in.Stop(ctx); err != nil {
	// ...
}
if err := in.Wait(ctx); err != nil {
	// a non-nil error here means the instance terminated abnormally
}
```

No channel ever appears in this API, except `Done`, which returns the same
channel `Wait` blocks on for a non-blocking check instead: `select { case
<-in.Done(): default: }` reports whether the instance has already stopped
-- a top-level final state was reached, `Stop` was called, or it
terminated with an error -- without waiting for it.

Internally, `Send` and `Stop` hand a request to the instance's own
goroutine and wait for the resulting transition (if any) to finish
processing before returning, so `Configuration()` is always current
immediately afterward.

A callback running inside an `Instance` (an `ActionFunc` or `CondFunc`)
must never call `Send`, `Stop`, or `Wait` on that same instance — that
goroutine is the one that would have to service the call, so doing so
deadlocks. Use `ExecContext.Raise` to enqueue a follow-up event within the
same chart instead.

A raised event is processed within the same macrostep-processing chain as
the event that triggered it, so its effects are already visible by the
time the outer `Send` returns — unlike `Send` to a different actor, which
only enqueues delivery elsewhere:

```go
validate := statecharts.ActionFunc(func(ec statecharts.ExecContext) error {
	ec.Raise(statecharts.Event{Name: "validated"})
	return nil
})

chart, err := statecharts.Build(
	statecharts.Compound("job", "uploading",
		statecharts.Children(
			statecharts.Atomic("uploading", statecharts.On("upload.received", statecharts.Target("validating"))),
			statecharts.Atomic("validating",
				statecharts.OnEntry(validate),
				statecharts.On("validated", statecharts.Target("thumbnailing")),
			),
			statecharts.Atomic("thumbnailing"),
		),
	),
)
if err != nil {
	// ...
}

in := statecharts.New(chart, nil)
in.Start(ctx)
defer in.Stop(ctx)

in.Send(ctx, statecharts.Event{Name: "upload.received", Type: statecharts.EventExternal})
fmt.Println(in.Configuration()) // [thumbnailing] -- "validated" already fired
```

`validating` is never visible outside the action that raises past it: by the
time `Send` returns, `validated` has already moved the chart on to
`thumbnailing`.

</details>

### The Datamodel

<details>
<summary>Show Datamodel Examples</summary>

SCXML lets a chart's guards and executable content read and write a
"datamodel" through an expression language. This package has no
expression language: the datamodel is any Go value you choose, and guards
and actions are Go functions that operate on it directly.

`Action` and `Cond` adapt a callback written against your own datamodel
type into the `ActionFunc`/`CondFunc` a chart stores:

```go
type Job struct {
	Source string
	Sizes  []int
}

addSize := statecharts.Action(func(j *Job, ec statecharts.ExecContext) error {
	ev, ok := ec.Event()
	if !ok {
		return nil
	}
	if size, ok := statecharts.Payload[int](ev); ok {
		j.Sizes = append(j.Sizes, size)
	}
	return nil
})

hasSizes := statecharts.Cond(func(j *Job, ec statecharts.ExecContext) bool {
	return len(j.Sizes) > 0
})
```

`Send`'s own `Event.Data` field is where a caller attaches the payload
`Payload` recovers above:

```go
chart, err := statecharts.Build(
	statecharts.Atomic("job", statecharts.On("thumbnail.done", statecharts.Then(addSize))),
)
if err != nil {
	// ...
}

job := &Job{Source: "uploads/photo.png"}
in := statecharts.New(chart, job)
in.Start(ctx)
defer in.Stop(ctx)

in.Send(ctx, statecharts.Event{Name: "thumbnail.done", Type: statecharts.EventExternal, Data: 128})
fmt.Println(job.Sizes) // [128]
```

`ExecContext`, passed to every callback, gives access to the event
currently being processed (`Event`), the SCXML `In()` predicate for
testing whether a state is active, the ability to raise, send, or
cancel further events, and the session's own identity — `SessionID()`
and `Name()`, SCXML 5.10's `_sessionid` and `_name`.

</details>

### IOProcessor

<details>
<summary>Show IOProcessor Examples</summary>

Every side effect that reaches outside a running chart — dispatching an
event to something other than the chart itself — goes through an
`IOProcessor`. This keeps the interpreter core pure and gives an
application exactly one seam to control, mock, or suppress all outbound
effects.

`SendEvent` schedules delivery of an event as executable content, the
Go-API equivalent of SCXML's `<send>`, including delayed delivery:

```go
statecharts.On("job.start",
	statecharts.Target("thumbnailing"),
	statecharts.Then(statecharts.SendEvent("thumbnailing.timeout", statecharts.SendOptions{
		Delay: 30 * time.Second,
	})),
)
```

`CancelSend` best-effort cancels a still-pending delayed send by ID. A
default `Instance` uses `LocalIOProcessor`, which reports unreachable
external targets. `NoopIOProcessor` explicitly suppresses dispatch, and any
type satisfying `IOProcessor` can be registered under an exact processor
type with `WithIOProcessor`.

Updating a database record and sending a notification once a job finishes is
an ordinary `IOProcessor`: the chart never touches a database or a mail
client directly, only `SendEvent`/`ec.Send`, targeting whatever name the
`IOProcessor` recognizes:

```go
type notifyProcessor struct{}

func (p *notifyProcessor) Attach(d statecharts.Dispatcher) {}

func (p *notifyProcessor) Send(ctx context.Context, req statecharts.SendRequest) error {
	if req.Target != "notifier" {
		return fmt.Errorf("no transport for target %q", req.Target)
	}
	jobID, _ := req.Data.(string)
	// db.ExecContext(ctx, `UPDATE jobs SET status = 'done' WHERE id = ?`, jobID)
	// mailer.Send(ctx, ownerEmail(jobID), "your thumbnails are ready")
	return nil
}
```

```go
statecharts.On("job.done",
	statecharts.Target("done"),
	statecharts.Then(statecharts.SendEvent("notify", statecharts.SendOptions{
		Target: "notifier",
		Data:   "job-482",
	})),
)
```

`in := statecharts.New(chart, myJob, statecharts.WithIOProcessor(statecharts.SCXMLEventProcessor, &notifyProcessor{}))` is
what wires the two together -- `notifyProcessor.Send` runs whenever `notify`
reaches the `IOProcessor` seam, whether that's from this transition's
`SendEvent` or from an `ec.Send` call inside any other action.

An `IOProcessor` that also implements `IOProcessorDescriber` -- adding an
`IOProcessors() []IOProcessorInfo` method -- has an address to advertise for
the current session: one another session could set as its own
`SendOptions.Target` to reach it back. `ExecContext.IOProcessors()` (per
SCXML 5.10's `_ioprocessors`) exposes that list to any action, and
`ExecContext.IOProcessorLocation(typ)` looks up one processor's `Location`
by `Type` directly:

```go
notify := func(ec statecharts.ExecContext) error {
	replyTo, _ := ec.IOProcessorLocation(statecharts.SCXMLEventProcessor)
	ec.Send("job.notify", statecharts.SendOptions{Target: "notifier", Data: replyTo})
	return nil
}
```

`NoopIOProcessor`/`LocalIOProcessor` don't implement `IOProcessorDescriber`
-- neither has a real transport to advertise an address for -- so
`IOProcessors()` is empty and `IOProcessorLocation` reports `ok=false` on a
default `Instance`.

</details>

### Error events

<details>
<summary>Show Error Event Examples</summary>

An `ActionFunc` that returns a non-nil error doesn't propagate that error
to the caller of `Send`. Per SCXML's own error model, it's reported as an
`error.execution` event on the internal queue instead — an ordinary event
a sibling transition can match against, carrying the error itself as its
`Data`:

```go
validate := statecharts.ActionFunc(func(ec statecharts.ExecContext) error {
	return errors.New("corrupt image: unexpected EOF")
})

recordFailure := statecharts.ActionFunc(func(ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	ec.Log("validation failed", ev.Data) // "corrupt image: unexpected EOF"
	return nil
})

statecharts.Atomic("validating",
	statecharts.OnEntry(validate),
	statecharts.On(string(statecharts.ErrEventExecution), statecharts.Target("failed"), statecharts.Then(recordFailure)),
)
```

`error.communication` is `error.execution`'s counterpart for `Send`: a
dispatch that fails at the `IOProcessor` — no `IOProcessor` configured for
the target, or the configured one returning an error — produces
`error.communication` instead of a Go error at the `Send` call site, for
the same reason `error.execution` exists: SCXML executable content has no
synchronous error return, only further events. The `notifyProcessor` from
IOProcessor above failing to reach `"notifier"` -- an unreachable mail
server, say -- is exactly this case: `job.done` fails to dispatch, and
`error.communication` is what a job chart reacts to instead of the failure
vanishing at the `Send` call site.

</details>

### Log

<details>
<summary>Show Log Examples</summary>

`ExecContext.Log(label, data)` is SCXML's `<log>`: diagnostic output for a
human or a log aggregator to read, with no relationship to a chart's own
event traffic — unlike `Send`, it never leaves the interpreter's process,
and unlike `Raise`, it never produces an event a transition could match
against.

```go
logSize := statecharts.ActionFunc(func(ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	ec.Log("thumbnail.done", ev.Data)
	return nil
})
```

Where a `Log` call ends up is a `Logger`, configured per `Instance` with
`WithLogger`:

```go
in := statecharts.New(chart, myDatamodel, statecharts.WithLogger(statecharts.NewWriterLogger(os.Stdout)))
```

`WriterLogger` writes one line per call to an `io.Writer`. An `Instance`
built with no `WithLogger` option uses `NoopLogger`, which discards every
call, so a chart that never calls `Log` behaves identically either way.

</details>

### Invoke

<details>
<summary>Show Invoke Examples</summary>

`Invoke` attaches an external service to a state — SCXML's `<invoke>`,
minus XML: any Go function willing to run in its own goroutine for as
long as the state is active. It starts once the state's containing
macrostep settles, and is cancelled automatically if the state is exited
before the service finishes on its own:

```go
resize := statecharts.Invoke(
	func(ctx context.Context, params any, io statecharts.InvokeIO) (any, error) {
		size, _ := params.(int)
		// stand-in for the actual image-resize work
		return fmt.Sprintf("thumb-%dpx.jpg", size), nil
	},
	statecharts.WithInvokeID("resize"),
	statecharts.WithInvokeParams(func(ec statecharts.ExecContext) any { return 128 }),
)

statecharts.Atomic("thumbnailing", resize, statecharts.On("done.invoke.resize", statecharts.Target("notifying")))
```

A finished invocation's own (non-cancelled) return generates
`done.invoke.<id>` on the chart, carrying whatever data it returned.
`WithFinalize` attaches executable content that runs on every event
carrying that invocation's ID, before any transition's guard sees it —
useful for normalizing a returned payload first. `SendOptions{Target:
"#_" + id}` addresses a specific running invocation, delivered to it via
`InvokeIO.Incoming`; `WithAutoForward` instead forwards a copy of *every*
external event the chart processes there automatically.

`InvokeChart` runs another `*Chart` as a real child SCXML session instead
of a hand-written `InvokeFunc`:

```go
statecharts.Invoke(
	statecharts.InvokeChart(childChart, func(params any) any {
		return &ChildDatamodel{}
	}, nil),
	statecharts.WithInvokeID("child"),
	statecharts.WithAutoForward(),
)
```

The child's own `Send(name, statecharts.SendOptions{Target: "#_parent"})`
reaches back into the invoking chart, tagged with the invocation's ID —
the special `"#_parent"` target SCXML's Event I/O Processor defines.

An invocation is a real-world side effect, so `Rehydrate` never blindly
restarts one: a state's `<invoke>` still counts as active for as long as
that state stays in the restored configuration, but by default
`Rehydrate` delivers `error.communication` for it instead of running it
again, since a plain `InvokeFunc` gives no way to tell whether the
original process survived the restart. An actor system built on
`actors.System` also never pages out an actor with an active invocation —
see [Automatic paging](#automatic-paging).

The Go goroutine running an `InvokeFunc` dies the moment its process
does, but the real thing it was managing — a subprocess, a job in an
external queue, a container — might still be alive on the other side of a
restart. `WithInvokeResume` gives an invocation a way to check instead of
being written off automatically: once replay catches up, `Rehydrate`
calls it in place of `error.communication`, and its return is treated
exactly like `Start`'s — an error still becomes `error.communication`,
but this time an informed one, and a clean return (immediate, or after
blocking on `io.Incoming` like any other invocation) becomes
`done.invoke.<id>`. Extending the thumbnailing example with a real
subprocess, keyed by its PID:

```go
resize := statecharts.Invoke(
	func(ctx context.Context, params any, io statecharts.InvokeIO) (any, error) {
		cmd := exec.CommandContext(ctx, "resize-thumb", fmt.Sprint(params))
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		// recordPID("resize", cmd.Process.Pid) -- durable store WithInvokeResume reads back
		return fmt.Sprintf("thumb-%dpx.jpg", params), cmd.Wait()
	},
	statecharts.WithInvokeID("resize"),
	statecharts.WithInvokeParams(func(ec statecharts.ExecContext) any { return 128 }),
	statecharts.WithInvokeResume(func(ctx context.Context, id statecharts.Identifier, params any, io statecharts.InvokeIO) (any, error) {
		pid := 0 // loadPID(id) -- read back whatever the live invocation recorded
		proc, err := os.FindProcess(pid)
		if err != nil || proc.Signal(syscall.Signal(0)) != nil {
			return nil, fmt.Errorf("resize subprocess %d is gone", pid)
		}
		for proc.Signal(syscall.Signal(0)) == nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Second):
			}
		}
		return fmt.Sprintf("thumb-%dpx.jpg", params), nil
	}),
)
```

`id` is the same invocation ID either way — `"resize"`, from `WithInvokeID`
— so `Resume` needs no ID scheme of its own; it only needs whatever the
live invocation used to find its subprocess again to still be durable
across the restart, which a PID (or a queue job ID, or a container name)
already is.

</details>

### Persistence

<details>
<summary>Show Persistence Examples</summary>

An `Instance`'s state — including its datamodel, active configuration,
recorded history, queued events, outstanding delayed sends, and active
invocation bookkeeping — can be captured with `Instance.Snapshot` and later
restored with `Restore`. Snapshot datamodels use JSON by default, or the
chart's `WithDatamodelCodec` implementation:

```go
snap, err := in.Snapshot(ctx)
// ... persist snap however you like; it implements json.Marshaler ...
restored, err := statecharts.Restore(chart, myDatamodel, snap)
restored.Start(ctx)
```

The more durable mechanism is a `Log`: recording every message that
arrives at an instance (each call to `Send`, and each delayed send as it
fires) is enough to reconstruct its exact state later, by feeding those
same messages through a fresh instance. `Rehydrate` does this:

```go
in, err := statecharts.Rehydrate(ctx, chart, myDatamodel, log, snapshotStore, sessionID, realIOProcessor)
```

`Rehydrate` loads the latest checkpoint if one exists, to avoid replaying
from the very first message, then replays everything since. Real dispatch
through the `IOProcessor` is suppressed until replay catches up, so
reconstructing state never repeats a real-world effect. Once it catches
up, every invocation still active — restored from the checkpoint, replayed
back into its state, or both — is reconciled exactly once: `Rehydrate`
resumes it for real if its `<invoke>` was configured with
`WithInvokeResume`, or reports `error.communication` for it otherwise,
rather than leaving it looking alive with nothing actually running (see
[Invoke](#invoke)).

Snapshots are disposable caches, never a second source of truth. Give a
durable chart an application version with `WithVersion("v1")`; when chart
logic or its datamodel changes, bump that version. `Rehydrate` rejects the
old cache and transparently rebuilds state by replaying the reducer from the
Log. Applications do not migrate snapshot formats or snapshot datamodels.

</details>

## Extras

### Testing with a manual clock

`ManualClock` lets a test control delayed sends deterministically instead
of waiting on real timers:

```go
clock := statecharts.NewManualClock(time.Now())
in := statecharts.New(chart, myDatamodel, statecharts.WithClock(clock))
in.Start(ctx)

clock.Advance(30 * time.Second) // fires any delayed sends now due
```

### The sqllog subpackage

`sqllog` implements `DurableLog` and `SnapshotStore` on a caller-provided
`*sql.DB`. It does not register or select a database driver:

```go
import (
	"database/sql"

	"github.com/dhamidi/statecharts/sqllog"
	_ "modernc.org/sqlite" // or another SQLite database/sql driver
)

db, err := sql.Open("sqlite", "statecharts.db")
storage, err := sqllog.New(db, sqllog.SQLite)
```

For an opt-in, batteries-included SQLite configuration, import
`sqllog/sqlite3` instead:

```go
import "github.com/dhamidi/statecharts/sqllog/sqlite3"

storage, err := sqlite3.Open("statecharts.db")
defer storage.Close()
```

The SQLite adapter explicitly opts into the pure-Go ModernC driver, enables
WAL mode for file-backed databases, and configures every pooled connection.
Both constructors return storage satisfying the complete durable boundary
for one `actors.System`. Give each System its own database.

## Running actor systems

A `*statecharts.Instance` is a Go value: talking to one means holding a
reference to it. That works as long as everything that needs a session
lives in the same process and keeps every session it will ever run
resident in memory. The `actors` subpackage removes that constraint. It
lets a group of charts run as named actors that address each other by
name instead of by Go reference, that can be spawned durable so a process
restart resumes them exactly where they left off, and that are paged into
and out of memory automatically as they go idle or as the system comes
under resident-actor pressure -- the virtual actor pattern.

`actors` reuses the root package directly; there is no wrapper type
standing in for "actor definition":

| Actor-model term | Type |
| --- | --- |
| actor definition | `*statecharts.Chart`, given identity by `Chart.ID` and a datamodel factory by `WithNewDatamodel` |
| actor | `*statecharts.Instance`, spawned under a name |
| actor system | `*actors.System` |

### Quickstart

Two actors, addressed by name instead of by Go reference: a `conn-1`
connection actor, and a `gateway` actor that tells it to open:

```go
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
)

func run(ctx context.Context) error {
	onOpen := func(ec statecharts.ExecContext) error {
		ev, _ := ec.Event()
		ec.Log("opened", ev)
		return nil
	}
	connChart, err := statecharts.Build(
		statecharts.Atomic("conn", statecharts.On("open", statecharts.Then(onOpen))),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }),
		statecharts.WithVersion("v1"),
	)
	if err != nil {
		return err
	}

	notifyConn := func(ec statecharts.ExecContext) error {
		ec.Send("open", statecharts.SendOptions{Target: "conn-1"})
		return nil
	}
	gatewayChart, err := statecharts.Build(
		statecharts.Atomic("gateway", statecharts.On("accept", statecharts.Then(notifyConn))),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }),
		statecharts.WithVersion("v1"),
	)
	if err != nil {
		return err
	}

	sys := actors.NewSystem(actors.WithLogger(statecharts.NewWriterLogger(os.Stdout)))

	if err := sys.Register(connChart); err != nil {
		return err
	}
	if err := sys.Register(gatewayChart); err != nil {
		return err
	}

	if err := sys.Spawn(ctx, "conn-1", connChart.ID()); err != nil {
		return err
	}
	if err := sys.Spawn(ctx, "gateway", gatewayChart.ID()); err != nil {
		return err
	}

	if err := sys.Tell(ctx, "gateway", statecharts.Event{Name: "accept", Type: statecharts.EventExternal}); err != nil {
		return err
	}
	time.Sleep(50 * time.Millisecond) // peer delivery hops through the dispatcher

	return sys.Stop(ctx)
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

`gateway` addresses `conn-1` by name from inside its own chart, the same way
it would address any other executable content target. Neither actor holds
a Go reference to the other.

### Building a system

`NewSystem` takes functional options, so there are no positional
constructor arguments to get the order of wrong:

```go
sys := actors.NewSystem(
	actors.WithNodeName("main"),
	actors.WithStorage(storage),
	actors.WithIdleTimeout(5*time.Minute),
)
```

`WithNodeName` supplies the system's routing location. Actor IDs remain
stable `Identifier` values, while routable keys append the node with `@`:
actor `accounts.invoice-42` on node `main` is addressed as
`accounts.invoice-42@main`. The node is not part of the actor's session ID
or its key in this system's isolated storage, so changing hosts does not strand
durable history. `WithStorage` supplies the single durability boundary backing
every `Durable` actor. A `System` without storage still works -- `Spawn`
without `Durable()` never touches storage -- but `Spawn(..., Durable())`
fails. Each System must use its own SQLite database file (for example,
`main.db` and `billing.db`), even when their node names differ. `WithIdleTimeout` and
`WithResidencyLimit` (below) control automatic paging.

```go
mainStorage, _ := sqlite3.Open("data/main.db")
billingStorage, _ := sqlite3.Open("data/billing.db")
main := actors.NewSystem(actors.WithStorage(mainStorage))
billing := actors.NewSystem(actors.WithStorage(billingStorage))
```

### Registering and spawning actors

Register every chart the system will ever spawn before spawning any of
it -- paging an actor back in reconstructs its `Instance` from the
registered `Chart`, since the chart's Go value itself is never persisted:

```go
sys.Register(notifierChart)
sys.Register(jobChart)
```

`Spawn` gives an actor a stable ID and starts it under the chart registered
for `kind` (`chart.ID()`). IDs use `statecharts.Identifier`, so dots express
hierarchy without implying a routing location:

```go
sys.Spawn(ctx, "notifier", "notifier")             // not durable
sys.Spawn(ctx, "jobs.job-482", "job", actors.Durable()) // durable
```

Without `Durable()`, `Spawn` behaves like `statecharts.New` plus `Start`:
the actor begins in its chart's initial configuration and keeps no record
of what it does. If the process restarts, it's gone.

`Durable()` changes that. A durable actor's messages are appended to the
system's `Log` before they're applied, outbound sends are recorded before
dispatch, and its actor ID doubles as its session ID. Recipients deduplicate
retries by `DeliveryID`; an external `IOProcessor` can implement
`AcknowledgingIOProcessor` to keep an intent pending until transport
acceptance. Responses remain ordinary inbound actor events — the runtime
does not turn a send into a durable request/response promise. One call
handles both "start fresh" and "resume": if
`"job-482"` has no prior log entries, `Spawn` starts it fresh; if it
already has history -- because the process restarted, or because it was
previously paged out -- `Spawn` loads its latest checkpoint and replays
everything since, landing the actor back in the exact state it was in
before, ahead of the first new message being let through. This is what
"the durable attribute automatically hydrates the actor" means in
practice: creating an actor and resuming one are the same call. A name's
durability is fixed at its first `Spawn` -- a name spawned without
`Durable()` cannot later be spawned durable, and vice versa.

Durable charts must set `WithVersion`. Bump it when chart logic or the
datamodel changes; snapshots are then discarded and rebuilt transparently
from the Log. Any event payload crossing a durable ingress or outbox boundary
must implement `DataMarshaler` and have a registered `DataUnmarshaler`.

### Addressing actors by ID

Every actor a `System` spawns is wired to the same routing `IOProcessor`.
Addressing another actor in the same `System` is ordinary executable
content -- `Target` is its actor ID. A target on a named node uses
`<actor-id>@<node>`:

```go
sendNotify := statecharts.Action(func(j *JobData, ec statecharts.ExecContext) error {
	ec.Send("job.done", statecharts.SendOptions{
		Target: "notifier",
		Data:   j.Source,
	})
	return nil
})
```

The receiving actor doesn't need to be told who sent the message: every
event a `System` delivers carries `Origin` set to the sender's routable
`<actor-id>@<node>` key (or local ID when no node is configured), so a reply
is just another `Send` targeting `ev.Origin`:

```go
notify := statecharts.Action(func(n *Notifier, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	source, _ := statecharts.Payload[string](ev)
	ec.Log("notifying owner of", source) // stand-in for actually sending a notification
	ec.Send("notified", statecharts.SendOptions{Target: ev.Origin})
	return nil
})
```

Application code outside any chart addresses an actor the same way, with
`System.Tell`:

```go
sys.Tell(ctx, "job-482", statecharts.Event{
	Name: "job.start",
	Type: statecharts.EventExternal,
	Data: &sourcePayload{TypeName: "source", Value: "uploads/482.png"},
})
```

`Tell` and a chart's own `ec.Send` resolve names identically -- an actor
cannot tell whether a message came from `Tell` or from another actor in
the system.

This routing is scoped to one `System`: a name it never spawned is unknown
to it, full stop, unless it was built with `WithSCXMLPeer` -- see
[Connecting two systems](#connecting-two-systems) for addressing an actor
that lives in a different `System` entirely.

### Automatic paging

A `System` never requires every spawned actor to be resident in memory at
once. A durable actor idle past `WithIdleTimeout` is checkpointed and
stopped, freeing its goroutine and its datamodel; the system keeps only
the fact that the name exists and which kind it is. The next message
addressed to that name -- from `Tell` or from another actor's `Send` --
transparently pages it back in, through the same `Durable()`-hydration
path `Spawn` uses, with the message held until the actor is caught up and
ready to receive it. Nothing about sending to a durable actor reveals
whether it happened to already be resident.

`System.IsResident` provides an observational check for dashboards and
operational tooling. It accepts either a stable actor ID or this system's
qualified `ID@node` routing key, and never activates a paged-out actor.

`WithResidencyObserver` reports each actor's `hydrating`, `resident`, and
`paged out` lifecycle transitions. Observers receive the stable actor ID and
run synchronously, making them suitable for lightweight projections and
operational UIs that need to distinguish replay from ordinary residency.

Idle timeouts alone don't protect a node from holding more resident
actors than it has room for -- a system under heavy, broad traffic may
never see any one actor go idle. `WithResidencyLimit` gives the system a
predicate to consult before admitting a new activation, called with the
current resident count:

```go
sys := actors.NewSystem(
	actors.WithStorage(storage),
	actors.WithResidencyLimit(func(resident int) bool {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.Alloc > 2<<30 // evict once this node has 2 GiB resident
	}),
)
```

`WithMaxResident(n)` is sugar for the common case, a plain count cap.
When the predicate returns true, the system evicts durable resident
actors, least-recently-active first, until either the predicate is
satisfied or no durable resident actor is left to evict. If every
resident actor is pinned and the predicate still says to evict, admitting
the new activation fails rather than silently exceeding budget.

Paging applies to durable actors only. A non-durable actor has no `Log`
to rebuild itself from, so evicting one from memory would destroy it
rather than hibernate it; `actors` keeps non-durable actors resident for
as long as the system itself runs, which is the right behavior for
something meant to always be around, like a `"notifier"` singleton.

A durable actor with an active `<invoke>` is likewise never paged out,
by idle timeout or by residency pressure, however long it has been idle:
`Rehydrate` cannot resume a real invocation on page-in (see
[Invoke](#invoke)), so eviction leaves it resident until that invocation's
owning state exits on its own.

An actor that reaches its own top-level final state, by contrast, is
always freed -- durable or not, regardless of idle time or residency
pressure -- since SCXML has no way back out of a final configuration:
there is nothing left for it to ever do. This happens as soon as the
system notices, which is immediately for the common case of reaching the
final state while processing a message, and no later than the next
`Spawn`/page-in elsewhere or idle-timeout sweep otherwise.

### A full example

A notifier actor stands in for the outside world: it logs (in place of
actually sending mail) whenever a job finishes. A job actor validates,
thumbnails, and notifies in sequence, recording its own outcome. The job is
durable, so it survives a restart in whatever state it last reached:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
	"github.com/dhamidi/statecharts/sqllog/sqlite3"
)

type Notifier struct{}

type JobData struct {
	Source string
	Status string
}

// sourcePayload crosses a durable boundary in both directions: job.start is
// logged as durable ingress, and job.done is first recorded in the durable
// job actor's outbox. JSONData supplies both persistence methods.
type sourcePayload = statecharts.JSONData[string]

func init() {
	statecharts.RegisterDataType("source", func() statecharts.DataUnmarshaler {
		return &sourcePayload{TypeName: "source"}
	})
}

func buildNotifierChart() (*statecharts.Chart, error) {
	notify := statecharts.Action(func(n *Notifier, ec statecharts.ExecContext) error {
		ev, _ := ec.Event()
		source, _ := statecharts.Payload[*sourcePayload](ev)
		ec.Log("notifying owner of", source.Value) // stand-in for actually sending a notification
		ec.Send("notified", statecharts.SendOptions{Target: ev.Origin})
		return nil
	})
	return statecharts.Build(
		statecharts.Atomic("notifier", statecharts.On("job.done", statecharts.Then(notify))),
		statecharts.WithNewDatamodel(func() any { return &Notifier{} }),
		statecharts.WithVersion("v1"),
	)
}

func buildJobChart() (*statecharts.Chart, error) {
	recordSource := statecharts.Action(func(j *JobData, ec statecharts.ExecContext) error {
		ev, _ := ec.Event()
		if payload, ok := statecharts.Payload[*sourcePayload](ev); ok {
			j.Source = payload.Value
		}
		return nil
	})
	validate := statecharts.ActionFunc(func(ec statecharts.ExecContext) error {
		// stand-in for real image validation
		ec.Raise(statecharts.Event{Name: "validated"})
		return nil
	})
	thumbnail := statecharts.ActionFunc(func(ec statecharts.ExecContext) error {
		// stand-in for real thumbnail generation
		ec.Raise(statecharts.Event{Name: "thumbnailed"})
		return nil
	})
	sendNotify := statecharts.Action(func(j *JobData, ec statecharts.ExecContext) error {
		ec.Send("job.done", statecharts.SendOptions{
			Target: "notifier",
			Data:   &sourcePayload{TypeName: "source", Value: j.Source},
		})
		return nil
	})
	recordDone := statecharts.Action(func(j *JobData, ec statecharts.ExecContext) error {
		j.Status = "done"
		return nil
	})
	recordFailed := statecharts.Action(func(j *JobData, ec statecharts.ExecContext) error {
		j.Status = "failed"
		return nil
	})

	return statecharts.Build(
		statecharts.Compound("job", "queued",
			statecharts.Children(
				statecharts.Atomic("queued",
					statecharts.On("job.start", statecharts.Target("validating"), statecharts.Then(recordSource)),
				),
				statecharts.Atomic("validating",
					statecharts.OnEntry(validate),
					statecharts.On("validated", statecharts.Target("thumbnailing")),
					statecharts.On(string(statecharts.ErrEventExecution), statecharts.Target("failed"), statecharts.Then(recordFailed)),
				),
				statecharts.Atomic("thumbnailing",
					statecharts.OnEntry(thumbnail),
					statecharts.On("thumbnailed", statecharts.Target("notifying")),
					statecharts.On(string(statecharts.ErrEventExecution), statecharts.Target("failed"), statecharts.Then(recordFailed)),
				),
				statecharts.Atomic("notifying",
					statecharts.OnEntry(sendNotify),
					statecharts.On("notified", statecharts.Target("done"), statecharts.Then(recordDone)),
				),
				statecharts.Atomic("done"),
				statecharts.Atomic("failed"),
			),
		),
		statecharts.WithNewDatamodel(func() any { return &JobData{} }),
		statecharts.WithVersion("v1"),
	)
}

func buildSystem(storage actors.Storage) (*actors.System, error) {
	sys := actors.NewSystem(
		actors.WithNodeName("main"),
		actors.WithStorage(storage),
		actors.WithIdleTimeout(5*time.Minute),
		actors.WithMaxResident(10_000),
		actors.WithLogger(statecharts.NewWriterLogger(os.Stdout)),
	)
	notifierChart, err := buildNotifierChart()
	if err != nil {
		return nil, err
	}
	if err := sys.Register(notifierChart); err != nil {
		return nil, err
	}
	jobChart, err := buildJobChart()
	if err != nil {
		return nil, err
	}
	if err := sys.Register(jobChart); err != nil {
		return nil, err
	}
	return sys, nil
}

func run(ctx context.Context) error {
	storage, err := sqlite3.Open("actors.db")
	if err != nil {
		return err
	}
	defer storage.Close()

	sys, err := buildSystem(storage)
	if err != nil {
		return err
	}

	if err := sys.Spawn(ctx, "notifier", "notifier"); err != nil {
		return err
	}
	if err := sys.Spawn(ctx, "job-482", "job", actors.Durable()); err != nil {
		return err
	}

	if err := sys.Tell(ctx, "job-482", statecharts.Event{
		Name: "job.start",
		Type: statecharts.EventExternal,
		Data: &sourcePayload{TypeName: "source", Value: "uploads/482.png"},
	}); err != nil {
		return err
	}

	time.Sleep(50 * time.Millisecond) // peer delivery hops through a goroutine

	if err := sys.Stop(ctx); err != nil {
		return err
	}

	// Later, possibly in a different process entirely, against the same Log:
	sys2, err := buildSystem(storage)
	if err != nil {
		return err
	}
	if err := sys2.Spawn(ctx, "job-482", "job", actors.Durable()); err != nil {
		return err
	}
	// job-482 resumes in "done" without replaying "job.start" or "notified"
	// as new messages against the notifier -- they're already baked into
	// its checkpointed state.
	if err := sys2.Stop(ctx); err != nil {
		return err
	}

	fmt.Println("job-482 resumed")
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

### Connecting two systems

Every actor a `System` addresses by name lives inside that one `System`. A
name it never spawned is unknown to it, full stop -- there is no way for an
actor in one `System` to reach a name that belongs to a different `System`,
unless that `System` is built with `WithSCXMLPeer`.

`WithSCXMLPeer` adds the next SCXML `IOProcessor` hop behind the local actor
router. A name the `System` already knows — resident or paged out — resolves
locally first; the peer only sees an SCXML send when that lookup misses.
Custom processor types are registered separately with
`actors.WithIOProcessor(type, factory)` and never fall through to SCXML.

It's the same seam every actor already sends through, `IOProcessor`, doing
the same job one level up: routing between Systems or reaching another
application transport instead of routing between actors inside one.

`Bridge` is a ready-made SCXML peer `IOProcessor` for connecting two
`System`s. It separates the actor ID from its node at `@` and delivers the
ID to the target via `Tell`. An actor in `gatewaySystem` reaches actor
`"job-482"` in `jobsSystem` with `"job-482@jobs-system"`, once
`gatewaySystem` has a `Bridge` pointed at `jobsSystem`.

Replies work the same way, in reverse. `Bridge` stamps `Origin` with the
source node, so a reply -- an ordinary `Send` targeting `ev.Origin`, exactly
like a same-`System` reply -- carries a routing key the reverse `Bridge`
recognizes. Connecting two `System`s both ways takes one `Bridge` each.

Wiring them together is circular: each `Bridge`'s target is the other
`System`, but neither `System` can finish being built -- `WithSCXMLPeer`
wants a complete `IOProcessor` -- before the other one exists.
`NewBridge` accepts a `nil` target to break the cycle; `Bridge.SetTarget`
fills it in once both `System`s exist, before either receives any traffic:

```go
toJobs := actors.NewBridge("jobs-system", nil, "gateway-system")
gatewaySystem := actors.NewSystem(actors.WithSCXMLPeer(toJobs))
// ... register charts and spawn actors on gatewaySystem ...

jobsSystem := actors.NewSystem(actors.WithSCXMLPeer(
	actors.NewBridge("gateway-system", gatewaySystem, "jobs-system"),
))
// ... register charts and spawn actors on jobsSystem ...

toJobs.SetTarget(jobsSystem)
```

`Bridge.Send` never blocks on delivery, the same way a `System`'s own
routing `IOProcessor` never does: it checks the node and looks up the actor
ID in the target `System`'s table, then queues delivery on the source
System's bounded dispatcher. A slow target never blocks the sender's actor
goroutine.

A complete example: `gateway-system` holds a connection actor,
`jobs-system` holds a job actor. Connections churn fast and are numerous
but cheap to lose; jobs are heavier and would often use their own durable
storage boundary — different lifecycles and scaling characteristics are
exactly the reason to run them as two `System`s instead of one. This example
focuses only on routing. The connection actor forwards an upload to the
job actor across the bridge; the job actor, once it finishes, sends the
result back to the connection actor that started it, by name, across the
same bridge in reverse:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
)

type ConnData struct {
	Result string
}

func buildConnChart() (*statecharts.Chart, error) {
	forwardUpload := statecharts.Action(func(c *ConnData, ec statecharts.ExecContext) error {
		ec.Send("job.start", statecharts.SendOptions{
			Target: "job-482@jobs-system",
			Data:   "uploads/482.png",
		})
		return nil
	})
	recordResult := statecharts.Action(func(c *ConnData, ec statecharts.ExecContext) error {
		ev, _ := ec.Event()
		c.Result, _ = statecharts.Payload[string](ev)
		return nil
	})
	return statecharts.Build(
		statecharts.Compound("conn", "open",
			statecharts.Children(
				statecharts.Atomic("open",
					statecharts.On("upload", statecharts.Then(forwardUpload)),
					statecharts.On("job.result", statecharts.Target("delivered"), statecharts.Then(recordResult)),
				),
				statecharts.Atomic("delivered"),
			),
		),
		statecharts.WithNewDatamodel(func() any { return &ConnData{} }),
	)
}

type JobData struct {
	Origin statecharts.Identifier
	Status string
}

func buildJobChart() (*statecharts.Chart, error) {
	startJob := statecharts.Action(func(j *JobData, ec statecharts.ExecContext) error {
		ev, _ := ec.Event()
		j.Origin = ev.Origin
		// stand-in for validating, thumbnailing, and notifying
		ec.Raise(statecharts.Event{Name: "job.finished"})
		return nil
	})
	reply := statecharts.Action(func(j *JobData, ec statecharts.ExecContext) error {
		j.Status = "done"
		ec.Send("job.result", statecharts.SendOptions{Target: j.Origin, Data: "thumbnails ready"})
		return nil
	})
	return statecharts.Build(
		statecharts.Compound("job", "queued",
			statecharts.Children(
				statecharts.Atomic("queued",
					statecharts.On("job.start", statecharts.Target("processing"), statecharts.Then(startJob)),
				),
				statecharts.Atomic("processing",
					statecharts.On("job.finished", statecharts.Target("done"), statecharts.Then(reply)),
				),
				statecharts.Atomic("done"),
			),
		),
		statecharts.WithNewDatamodel(func() any { return &JobData{} }),
	)
}

func run(ctx context.Context) error {
	// gatewaySystem's Bridge needs jobsSystem as its target, but jobsSystem's
	// own Bridge needs gatewaySystem -- built first here -- as its target.
	// NewBridge accepts a nil target to break the cycle; toJobs is wired up
	// with SetTarget once jobsSystem exists.
	toJobs := actors.NewBridge("jobs-system", nil, "gateway-system")
	gatewaySystem := actors.NewSystem(actors.WithSCXMLPeer(toJobs))
	connChart, err := buildConnChart()
	if err != nil {
		return err
	}
	if err := gatewaySystem.Register(connChart); err != nil {
		return err
	}
	if err := gatewaySystem.Spawn(ctx, "gateway-1", "conn"); err != nil {
		return err
	}

	jobsSystem := actors.NewSystem(actors.WithSCXMLPeer(
		actors.NewBridge("gateway-system", gatewaySystem, "jobs-system"),
	))
	jobChart, err := buildJobChart()
	if err != nil {
		return err
	}
	if err := jobsSystem.Register(jobChart); err != nil {
		return err
	}
	if err := jobsSystem.Spawn(ctx, "job-482", "job"); err != nil {
		return err
	}
	toJobs.SetTarget(jobsSystem)

	if err := gatewaySystem.Tell(ctx, "gateway-1", statecharts.Event{
		Name: "upload",
		Type: statecharts.EventExternal,
	}); err != nil {
		return err
	}

	time.Sleep(50 * time.Millisecond) // the result round-trips through both bridges

	if err := gatewaySystem.Stop(ctx); err != nil {
		return err
	}
	if err := jobsSystem.Stop(ctx); err != nil {
		return err
	}

	fmt.Println("gateway-1 delivered a job result received from a different System entirely")
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```
