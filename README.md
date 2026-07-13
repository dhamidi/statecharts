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

A raised event is processed within the same macrostep-processing chain as
the event that triggered it, so its effects are already visible by the
time the outer `Send` returns — unlike `Send` to a different actor, which
only enqueues delivery elsewhere:

```go
settle := statecharts.ActionFunc(func(ec statecharts.ExecContext) error {
	ec.Raise(statecharts.Event{Name: "settle"})
	return nil
})

chart, err := statecharts.Build(
	statecharts.Compound("door", "closed",
		statecharts.Children(
			statecharts.Atomic("closed", statecharts.On("open.request", statecharts.Target("opening"))),
			statecharts.Atomic("opening",
				statecharts.OnEntry(settle),
				statecharts.On("settle", statecharts.Target("open")),
			),
			statecharts.Atomic("open"),
		),
	),
)
if err != nil {
	// ...
}

in := statecharts.New(chart, nil)
in.Start(ctx)
defer in.Stop(ctx)

in.Send(ctx, statecharts.Event{Name: "open.request", Type: statecharts.EventExternal})
fmt.Println(in.Configuration()) // [open] -- "settle" already fired
```

`opening` is never visible outside the action that raises past it: by the
time `Send` returns, `settle` has already moved the chart on to `open`.

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

`Send`'s own `Event.Data` field is where a caller attaches the payload
`Payload` recovers above:

```go
chart, err := statecharts.Build(
	statecharts.Atomic("cart", statecharts.On("add", statecharts.Then(addItem))),
)
if err != nil {
	panic(err)
}

cart := &Cart{}
in := statecharts.New(chart, cart)
in.Start(ctx)
defer in.Stop(ctx)

in.Send(ctx, statecharts.Event{Name: "add", Type: statecharts.EventExternal, Data: "widget"})
fmt.Println(cart.Items) // [widget]
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
	return errors.New("payment declined")
})

recordFailure := statecharts.ActionFunc(func(ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	fmt.Println(ev.Data) // payment declined
	return nil
})

statecharts.Atomic("paying",
	statecharts.OnEntry(validate),
	statecharts.On(string(statecharts.ErrEventExecution), statecharts.Target("failed"), statecharts.Then(recordFailure)),
)
```

`error.communication` is `error.execution`'s counterpart for `Send`: a
dispatch that fails at the `IOProcessor` — no `IOProcessor` configured for
the target, or the configured one returning an error — produces
`error.communication` instead of a Go error at the `Send` call site, for
the same reason `error.execution` exists: SCXML executable content has no
synchronous error return, only further events.

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
greet := statecharts.ActionFunc(func(ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	ec.Log("received", ev.Name)
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

Two actors, addressed by name instead of by Go reference:

```go
package main

import (
	"context"
	"os"
	"time"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
)

func main() {
	ctx := context.Background()

	greet := func(ec statecharts.ExecContext) error {
		ev, _ := ec.Event()
		ec.Log("received", ev)
		return nil
	}
	greeterChart, err := statecharts.Build(
		statecharts.Atomic("greeter", statecharts.On("hello", statecharts.Then(greet))),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }),
	)
	if err != nil {
		panic(err)
	}

	sendHello := func(ec statecharts.ExecContext) error {
		ec.Send("hello", statecharts.SendOptions{Target: "alice"})
		return nil
	}
	callerChart, err := statecharts.Build(
		statecharts.Atomic("caller", statecharts.On("start", statecharts.Then(sendHello))),
		statecharts.WithNewDatamodel(func() any { return &struct{}{} }),
	)
	if err != nil {
		panic(err)
	}

	sys := actors.NewSystem(actors.WithLogger(statecharts.NewWriterLogger(os.Stdout)))
	sys.Register(greeterChart)
	sys.Register(callerChart)

	sys.Spawn(ctx, "alice", greeterChart.ID())
	sys.Spawn(ctx, "bob", callerChart.ID())

	sys.Tell(ctx, "bob", statecharts.Event{Name: "start", Type: statecharts.EventExternal})
	time.Sleep(50 * time.Millisecond) // peer delivery hops through a goroutine
	sys.Stop(ctx)
}
```

`bob` addresses `alice` by name from inside its own chart, the same way
it would address any other executable content target. Neither actor holds
a Go reference to the other.

### Building a system

`NewSystem` takes functional options, so there are no positional
constructor arguments to get the order of wrong:

```go
sys := actors.NewSystem(
	actors.WithNodeName("main"),
	actors.WithLog(log),
	actors.WithSnapshotStore(log),
	actors.WithIdleTimeout(5*time.Minute),
)
```

`WithNodeName` labels the system for diagnostics only -- an actor's name
means the same thing regardless of which node's `System` happens to have
it loaded right now. `WithLog` and `WithSnapshotStore` supply the
durability backing every `Durable` actor; `*sqllog.Log` satisfies both, so
the same value is commonly passed to each. A `System` with neither
configured still works -- `Spawn` without `Durable()` never touches them
-- but `Spawn(..., Durable())` fails if either is missing. `WithIdleTimeout`
and `WithResidencyLimit` (below) control automatic paging.

### Registering and spawning actors

Register every chart the system will ever spawn before spawning any of
it -- paging an actor back in reconstructs its `Instance` from the
registered `Chart`, since the chart's Go value itself is never persisted:

```go
sys.Register(warehouseChart)
sys.Register(orderChart)
```

`Spawn` gives an actor a name -- its address within the system -- and
starts it running under the chart registered for `kind` (`chart.ID()`):

```go
sys.Spawn(ctx, "warehouse", "warehouse")               // not durable
sys.Spawn(ctx, "order-482", "order", actors.Durable()) // durable
```

Without `Durable()`, `Spawn` behaves like `statecharts.New` plus `Start`:
the actor begins in its chart's initial configuration and keeps no record
of what it does. If the process restarts, it's gone.

`Durable()` changes that. A durable actor's messages are appended to the
system's `Log` before they're applied, and its name doubles as its
session ID. One call handles both "start fresh" and "resume": if
`"order-482"` has no prior log entries, `Spawn` starts it fresh; if it
already has history -- because the process restarted, or because it was
previously paged out -- `Spawn` loads its latest checkpoint and replays
everything since, landing the actor back in the exact state it was in
before, ahead of the first new message being let through. This is what
"the durable attribute automatically hydrates the actor" means in
practice: creating an actor and resuming one are the same call. A name's
durability is fixed at its first `Spawn` -- a name spawned without
`Durable()` cannot later be spawned durable, and vice versa.

### Addressing actors by name

Every actor a `System` spawns is wired to the same routing `IOProcessor`.
Addressing another actor from inside a chart is ordinary executable
content -- `Target` is just the other actor's name:

```go
placeOrder := statecharts.Action(func(o *OrderData, ec statecharts.ExecContext) error {
	ec.Send("reserve.request", statecharts.SendOptions{
		Target: "warehouse",
		Data:   o.SKU,
	})
	return nil
})
```

The receiving actor doesn't need to be told who sent the message: every
event a `System` delivers carries `Origin` set to the sender's own name,
so a reply is just another `Send` targeting `ev.Origin`:

```go
reserve := statecharts.Action(func(w *Warehouse, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	sku, _ := statecharts.Payload[string](ev)
	reply := statecharts.Identifier("reserve.denied")
	if w.Stock[sku] > 0 {
		w.Stock[sku]--
		reply = "reserve.ok"
	}
	ec.Send(reply, statecharts.SendOptions{Target: ev.Origin})
	return nil
})
```

Application code outside any chart addresses an actor the same way, with
`System.Tell`:

```go
sys.Tell(ctx, "order-482", statecharts.Event{
	Name: "order.place",
	Type: statecharts.EventExternal,
	Data: &skuPayload{TypeName: "sku", Value: "WIDGET-1"},
})
```

`Tell` and a chart's own `ec.Send` resolve names identically -- an actor
cannot tell whether a message came from `Tell` or from another actor in
the system.

This routing is scoped to one `System`: a name it never spawned is unknown
to it, full stop, unless it was built with `WithFallback` -- see
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

Idle timeouts alone don't protect a node from holding more resident
actors than it has room for -- a system under heavy, broad traffic may
never see any one actor go idle. `WithResidencyLimit` gives the system a
predicate to consult before admitting a new activation, called with the
current resident count:

```go
sys := actors.NewSystem(
	actors.WithLog(log),
	actors.WithSnapshotStore(log),
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
something meant to always be around, like a `"warehouse"` singleton.

### A full example

A warehouse actor holds stock. An order actor reserves against it and
records whether the reservation succeeded. The order is durable, so it
survives a restart in whatever state it last reached:

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
	"github.com/dhamidi/statecharts/sqllog"
)

type Warehouse struct {
	Stock map[string]int
}

type OrderData struct {
	SKU    string
	Status string
}

// skuPayload is the DataMarshaler wrapper "order.place" carries its SKU
// in. A durable actor's incoming events are appended to the Log before
// they're applied, so any Event.Data reaching one must implement
// statecharts.DataMarshaler; JSONData is a ready-made implementation.
// "reserve.request" and the replies below target actors that are either
// non-durable (warehouse) or carry no Data at all, so they need no such
// wrapper.
type skuPayload = statecharts.JSONData[string]

func init() {
	statecharts.RegisterDataType("sku", func() statecharts.DataUnmarshaler {
		return &skuPayload{TypeName: "sku"}
	})
}

func buildWarehouseChart() *statecharts.Chart {
	reserve := statecharts.Action(func(w *Warehouse, ec statecharts.ExecContext) error {
		ev, _ := ec.Event()
		sku, _ := statecharts.Payload[string](ev)
		reply := statecharts.Identifier("reserve.denied")
		if w.Stock[sku] > 0 {
			w.Stock[sku]--
			reply = "reserve.ok"
		}
		ec.Send(reply, statecharts.SendOptions{Target: ev.Origin})
		return nil
	})
	chart, err := statecharts.Build(
		statecharts.Atomic("warehouse", statecharts.On("reserve.request", statecharts.Then(reserve))),
		statecharts.WithNewDatamodel(func() any {
			return &Warehouse{Stock: map[string]int{"WIDGET-1": 100}}
		}),
	)
	if err != nil {
		panic(err)
	}
	return chart
}

func buildOrderChart() *statecharts.Chart {
	placeOrder := statecharts.Action(func(o *OrderData, ec statecharts.ExecContext) error {
		ev, _ := ec.Event()
		if payload, ok := statecharts.Payload[*skuPayload](ev); ok {
			o.SKU = payload.Value
		}
		ec.Send("reserve.request", statecharts.SendOptions{Target: "warehouse", Data: o.SKU})
		return nil
	})
	recordConfirmed := statecharts.Action(func(o *OrderData, ec statecharts.ExecContext) error {
		o.Status = "confirmed"
		return nil
	})
	recordFailed := statecharts.Action(func(o *OrderData, ec statecharts.ExecContext) error {
		o.Status = "failed"
		return nil
	})
	chart, err := statecharts.Build(
		statecharts.Compound("order", "new",
			statecharts.Children(
				statecharts.Atomic("new",
					statecharts.On("order.place", statecharts.Target("reserving"), statecharts.Then(placeOrder)),
				),
				statecharts.Atomic("reserving",
					statecharts.On("reserve.ok", statecharts.Target("confirmed"), statecharts.Then(recordConfirmed)),
					statecharts.On("reserve.denied", statecharts.Target("failed"), statecharts.Then(recordFailed)),
				),
				statecharts.Atomic("confirmed"),
				statecharts.Atomic("failed"),
			),
		),
		statecharts.WithNewDatamodel(func() any { return &OrderData{} }),
	)
	if err != nil {
		panic(err)
	}
	return chart
}

func buildSystem(log *sqllog.Log) *actors.System {
	sys := actors.NewSystem(
		actors.WithNodeName("main"),
		actors.WithLog(log),
		actors.WithSnapshotStore(log),
		actors.WithIdleTimeout(5*time.Minute),
		actors.WithMaxResident(10_000),
	)
	if err := sys.Register(buildWarehouseChart()); err != nil {
		panic(err)
	}
	if err := sys.Register(buildOrderChart()); err != nil {
		panic(err)
	}
	return sys
}

func main() {
	ctx := context.Background()

	db, err := sql.Open("sqlite", "actors.db")
	if err != nil {
		panic(err)
	}
	log, err := sqllog.New(db, sqllog.SQLite)
	if err != nil {
		panic(err)
	}

	sys := buildSystem(log)

	if err := sys.Spawn(ctx, "warehouse", "warehouse"); err != nil {
		panic(err)
	}
	if err := sys.Spawn(ctx, "order-482", "order", actors.Durable()); err != nil {
		panic(err)
	}

	if err := sys.Tell(ctx, "order-482", statecharts.Event{
		Name: "order.place",
		Type: statecharts.EventExternal,
		Data: &skuPayload{TypeName: "sku", Value: "WIDGET-1"},
	}); err != nil {
		panic(err)
	}

	time.Sleep(50 * time.Millisecond) // peer delivery hops through a goroutine

	if err := sys.Stop(ctx); err != nil {
		panic(err)
	}

	// Later, possibly in a different process entirely, against the same Log:
	sys2 := buildSystem(log)
	if err := sys2.Spawn(ctx, "order-482", "order", actors.Durable()); err != nil {
		panic(err)
	}
	// order-482 resumes in "confirmed" without replaying "order.place" or
	// "reserve.ok" as new messages against the warehouse -- they're already
	// baked into its checkpointed state.
	if err := sys2.Stop(ctx); err != nil {
		panic(err)
	}

	fmt.Println("order-482 resumed")
}
```

### Connecting two systems

Every actor a `System` addresses by name lives inside that one `System`. A
name it never spawned is unknown to it, full stop -- there is no way for an
actor in one `System` to reach a name that belongs to a different `System`,
unless that `System` is built with `WithFallback`.

`WithFallback` gives a `System` an `IOProcessor` to try once its own
routing table comes up empty for a `Send`'s target. A name the `System`
already knows -- spawned there, resident or not -- is always resolved
locally first; the fallback only ever sees a `Send` for a name outside that
table. It's the same seam every actor already sends through,
`IOProcessor`, doing the same job one level up: routing between Systems
instead of between actors inside one.

`Bridge` is a ready-made fallback `IOProcessor` for connecting two
`System`s. It's configured with a namespace and a target `System`: `Send`
accepts only targets whose first segment is that namespace, strips it, and
delivers what's left to the target via `Tell`. An actor in `sysA` reaches
an actor named `"billing"` in `sysB` by addressing
`"warehouse-b.billing"`, once `sysA` is built with a `Bridge` for the
`"warehouse-b"` namespace pointed at `sysB`.

Replies work the same way, in reverse. `Bridge` stamps `Origin` with its
own namespace, so a reply -- an ordinary `Send` targeting `ev.Origin`,
exactly like a same-`System` reply -- lands on a namespaced address a
`Bridge` on the other side recognizes and strips in turn. Connecting two
`System`s both ways takes one `Bridge` each, one per direction.

Wiring them together is circular: each `Bridge`'s target is the other
`System`, but neither `System` can finish being built -- `WithFallback`
wants a complete `IOProcessor` -- before the other one exists.
`NewBridge` accepts a `nil` target to break the cycle; `Bridge.SetTarget`
fills it in once both `System`s exist, before either receives any traffic:

```go
toOrders := actors.NewBridge("orders-system", nil, "warehouse-system")
warehouseSystem := actors.NewSystem(actors.WithFallback(toOrders))
// ... register charts and spawn actors on warehouseSystem ...

ordersSystem := actors.NewSystem(actors.WithFallback(
	actors.NewBridge("warehouse-system", warehouseSystem, "orders-system"),
))
// ... register charts and spawn actors on ordersSystem ...

toOrders.SetTarget(ordersSystem)
```

`Bridge.Send` never blocks on delivery, the same way a `System`'s own
routing `IOProcessor` never does: it checks the namespace and looks up the
target name in the target `System`'s table -- both cheap, synchronous
lookups -- and hands the actual delivery off to a goroutine before
returning. A slow or wedged actor on the far side of a bridge holds up only
that goroutine, never the sender's own.

A complete example: `warehouse-system` holds a warehouse actor,
`orders-system` holds an order actor, and an order in one reserves stock in
the other by addressing it across the bridge:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
)

type Warehouse struct {
	Stock map[string]int
}

type OrderData struct {
	Status string
}

func buildWarehouseChart() *statecharts.Chart {
	reserve := statecharts.Action(func(w *Warehouse, ec statecharts.ExecContext) error {
		ev, _ := ec.Event()
		sku, _ := statecharts.Payload[string](ev)
		reply := statecharts.Identifier("reserve.denied")
		if w.Stock[sku] > 0 {
			w.Stock[sku]--
			reply = "reserve.ok"
		}
		ec.Send(reply, statecharts.SendOptions{Target: ev.Origin})
		return nil
	})
	chart, err := statecharts.Build(
		statecharts.Atomic("warehouse", statecharts.On("reserve.request", statecharts.Then(reserve))),
		statecharts.WithNewDatamodel(func() any {
			return &Warehouse{Stock: map[string]int{"WIDGET-1": 100}}
		}),
	)
	if err != nil {
		panic(err)
	}
	return chart
}

func buildOrderChart() *statecharts.Chart {
	placeOrder := statecharts.Action(func(o *OrderData, ec statecharts.ExecContext) error {
		ec.Send("reserve.request", statecharts.SendOptions{
			Target: "warehouse-system.warehouse",
			Data:   "WIDGET-1",
		})
		return nil
	})
	recordConfirmed := statecharts.Action(func(o *OrderData, ec statecharts.ExecContext) error {
		o.Status = "confirmed"
		return nil
	})
	recordFailed := statecharts.Action(func(o *OrderData, ec statecharts.ExecContext) error {
		o.Status = "failed"
		return nil
	})
	chart, err := statecharts.Build(
		statecharts.Compound("order", "new",
			statecharts.Children(
				statecharts.Atomic("new",
					statecharts.On("order.place", statecharts.Target("reserving"), statecharts.Then(placeOrder)),
				),
				statecharts.Atomic("reserving",
					statecharts.On("reserve.ok", statecharts.Target("confirmed"), statecharts.Then(recordConfirmed)),
					statecharts.On("reserve.denied", statecharts.Target("failed"), statecharts.Then(recordFailed)),
				),
				statecharts.Atomic("confirmed"),
				statecharts.Atomic("failed"),
			),
		),
		statecharts.WithNewDatamodel(func() any { return &OrderData{} }),
	)
	if err != nil {
		panic(err)
	}
	return chart
}

func main() {
	ctx := context.Background()

	// warehouseSystem's Bridge needs ordersSystem as its target, but
	// ordersSystem's own Bridge needs warehouseSystem -- built first here
	// -- as its target. NewBridge accepts a nil target to break the cycle;
	// toOrders is wired up with SetTarget once ordersSystem exists.
	toOrders := actors.NewBridge("orders-system", nil, "warehouse-system")
	warehouseSystem := actors.NewSystem(actors.WithFallback(toOrders))
	if err := warehouseSystem.Register(buildWarehouseChart()); err != nil {
		panic(err)
	}
	if err := warehouseSystem.Spawn(ctx, "warehouse", "warehouse"); err != nil {
		panic(err)
	}

	ordersSystem := actors.NewSystem(actors.WithFallback(
		actors.NewBridge("warehouse-system", warehouseSystem, "orders-system"),
	))
	if err := ordersSystem.Register(buildOrderChart()); err != nil {
		panic(err)
	}
	if err := ordersSystem.Spawn(ctx, "order-482", "order"); err != nil {
		panic(err)
	}
	toOrders.SetTarget(ordersSystem)

	if err := ordersSystem.Tell(ctx, "order-482", statecharts.Event{
		Name: "order.place",
		Type: statecharts.EventExternal,
	}); err != nil {
		panic(err)
	}

	time.Sleep(50 * time.Millisecond) // the reservation round-trips through both bridges

	if err := warehouseSystem.Stop(ctx); err != nil {
		panic(err)
	}
	if err := ordersSystem.Stop(ctx); err != nil {
		panic(err)
	}

	fmt.Println("order-482 reserved WIDGET-1 in a different System entirely")
}
```
