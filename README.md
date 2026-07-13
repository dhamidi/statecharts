<strong>A Go implementation of the W3C SCXML state machine semantics — the interpretation algorithm, without the XML.</strong>

```go
package main

import (
	"context"
	"fmt"

	"github.com/dhamidi/statecharts"
)

func main() {
	chart, err := statecharts.Build(
		statecharts.Compound("door", "closed",
			statecharts.Children(
				statecharts.Atomic("closed", statecharts.On("open", statecharts.Target("open"))),
				statecharts.Atomic("open", statecharts.On("close", statecharts.Target("closed"))),
			),
		),
	)
	if err != nil {
		panic(err)
	}

	door := statecharts.New(chart, nil)
	ctx := context.Background()
	door.Start(ctx)
	defer door.Stop(ctx)

	door.Send(ctx, statecharts.Event{Name: "open", Type: statecharts.EventExternal})
	fmt.Println(door.Configuration()) // [open]
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
  - [Invoke](#invoke)
  - [Persistence](#persistence)
- [Extras](#extras)
  - [Testing with a manual clock](#testing-with-a-manual-clock)
  - [The sqllog subpackage](#the-sqllog-subpackage)

## Installation

```bash
go get github.com/dhamidi/statecharts
```

## Quickstart

A slightly more realistic chart: a door with a lock, a guarded transition,
and an action that mutates the caller's own datamodel.

```go
package main

import (
	"context"
	"fmt"

	"github.com/dhamidi/statecharts"
)

type Door struct {
	Locked    bool
	OpenCount int
}

func main() {
	notLocked := statecharts.Cond(func(d *Door, ec statecharts.ExecContext) bool {
		return !d.Locked
	})
	recordOpen := statecharts.Action(func(d *Door, ec statecharts.ExecContext) error {
		d.OpenCount++
		return nil
	})

	chart, err := statecharts.Build(
		statecharts.Compound("door", "closed",
			statecharts.Children(
				statecharts.Atomic("closed",
					statecharts.On("open.request",
						statecharts.Target("open"),
						statecharts.If(notLocked),
						statecharts.Then(recordOpen),
					),
				),
				statecharts.Atomic("open",
					statecharts.On("close.request", statecharts.Target("closed")),
				),
			),
		),
	)
	if err != nil {
		panic(err)
	}

	door := &Door{Locked: true}
	in := statecharts.New(chart, door)
	ctx := context.Background()
	in.Start(ctx)
	defer in.Stop(ctx)

	in.Send(ctx, statecharts.Event{Name: "open.request", Type: statecharts.EventExternal})
	fmt.Println(in.Configuration(), door.OpenCount) // [closed] 0 -- still locked

	door.Locked = false
	in.Send(ctx, statecharts.Event{Name: "open.request", Type: statecharts.EventExternal})
	fmt.Println(in.Configuration(), door.OpenCount) // [open] 1
}
```

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

Parallel regions transition independently of one another:

```go
chart, err := statecharts.Build(
	statecharts.Parallel("machine",
		statecharts.Children(
			statecharts.Compound("motor", "off",
				statecharts.Children(
					statecharts.Atomic("off", statecharts.On("motor.start", statecharts.Target("on"))),
					statecharts.Atomic("on", statecharts.On("motor.stop", statecharts.Target("off"))),
				),
			),
			statecharts.Compound("light", "dark",
				statecharts.Children(
					statecharts.Atomic("dark", statecharts.On("light.on", statecharts.Target("lit"))),
					statecharts.Atomic("lit", statecharts.On("light.off", statecharts.Target("dark"))),
				),
			),
		),
	),
)
```

Sending `motor.start` moves the `motor` region from `off` to `on` and
leaves `light` untouched — both regions are active in the configuration at
once.

A history pseudostate remembers where a compound state's children were
before it was exited, so re-entering it can resume there instead of
starting over:

```go
statecharts.Compound("running", "step1",
	statecharts.Children(
		statecharts.Atomic("step1", statecharts.On("next", statecharts.Target("step2"))),
		statecharts.Atomic("step2", statecharts.On("next", statecharts.Target("step3"))),
		statecharts.Atomic("step3"),
		statecharts.History("running.hist", statecharts.Shallow, "step1"),
	),
),
```

A transition targeting `"running.hist"` re-enters whichever of `step1`,
`step2`, or `step3` was active when `running` was last exited — or
`step1`, the given default, the first time.

</details>

### Instances

<details>
<summary>Show Instance Examples</summary>

`New` builds an `Instance` from a `*Chart` and a datamodel value; `Start`
spawns its interpreter goroutine. From there, an `Instance` is driven
entirely through plain method calls:

```go
in := statecharts.New(chart, myDatamodel)
if err := in.Start(ctx); err != nil {
	// ...
}

if err := in.Send(ctx, statecharts.Event{Name: "go", Type: statecharts.EventExternal}); err != nil {
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

No channel ever appears in this API. Internally, `Send` and `Stop` hand a
request to the instance's own goroutine and wait for the resulting
transition (if any) to finish processing before returning, so
`Configuration()` is always current immediately afterward.

A callback running inside an `Instance` (an `ActionFunc` or `CondFunc`)
must never call `Send`, `Stop`, or `Wait` on that same instance — that
goroutine is the one that would have to service the call, so doing so
deadlocks. Use `ExecContext.Raise` to enqueue a follow-up event within the
same chart instead.

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
type Cart struct {
	Items []string
}

addItem := statecharts.Action(func(c *Cart, ec statecharts.ExecContext) error {
	ev, ok := ec.Event()
	if !ok {
		return nil
	}
	if item, ok := statecharts.Payload[string](ev); ok {
		c.Items = append(c.Items, item)
	}
	return nil
})

hasItems := statecharts.Cond(func(c *Cart, ec statecharts.ExecContext) bool {
	return len(c.Items) > 0
})
```

`ExecContext`, passed to every callback, gives access to the event
currently being processed (`Event`), the SCXML `In()` predicate for
testing whether a state is active, and the ability to raise, send, or
cancel further events.

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
statecharts.On("submit",
	statecharts.Target("waiting"),
	statecharts.Then(statecharts.SendEvent("timeout", statecharts.SendOptions{
		Delay: 30 * time.Second,
	})),
)
```

`CancelSend` best-effort cancels a still-pending delayed send by ID. A
default `Instance` uses `NoopIOProcessor`, which suppresses all outbound
dispatch; `LocalIOProcessor` is a starting point for a single-process
`IOProcessor` implementation, and any type satisfying the `IOProcessor`
interface can be supplied with `WithIOProcessor`.

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
clock := statecharts.Invoke(
	func(ctx context.Context, params any, io statecharts.InvokeIO) (any, error) {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil, nil
			case now := <-t.C:
				io.Deliver(statecharts.Event{Name: "tick", Data: now})
			}
		}
	},
	statecharts.WithInvokeID("clock"),
)

statecharts.Atomic("running", clock, statecharts.On("tick", ...))
```

A finished invocation's own (non-cancelled) return generates
`done.invoke.<id>` on the chart, carrying whatever data it returned.
`WithFinalize` attaches executable content that runs on every event
carrying that invocation's ID, before any transition's guard sees it —
useful for normalizing a returned payload first. `SendOptions{Target:
"#_" + id}` addresses a specific running invocation, delivered to it via
`InvokeIO.Incoming`.

</details>

### Persistence

<details>
<summary>Show Persistence Examples</summary>

An `Instance`'s state — its active configuration, recorded history, queued
events, and any outstanding delayed sends — can be captured at any point
with `Instance.Snapshot` and later restored with `Restore`:

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
reconstructing state never repeats a real-world effect.

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

`sqllog` implements `Log` and `SnapshotStore` against a `*sql.DB` from the
standard library's `database/sql` package:

```go
import "github.com/dhamidi/statecharts/sqllog"

db, err := sql.Open("sqlite", "statecharts.db")
log, err := sqllog.New(db, sqllog.SQLite)
```

The returned `*sqllog.Log` satisfies both interfaces, so it can be passed
as both the `log` and `snapshots` arguments to `Rehydrate`.
