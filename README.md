# statecharts

`statecharts` is a Go-first statechart runtime with precise hierarchical,
parallel, history, event-queue, and invocation semantics. Charts are ordinary
Go definitions. Their behavior is supplied by typed, explicitly named Go
functions, so complete definitions remain deterministic, inspectable, and
portable.

The runtime follows the W3C SCXML interpretation semantics, but XML is not the
authoring model. XML, JSON, a visual editor, or any other syntax can be a
surface over the same syntax-neutral `Definition`.

## Installation

```sh
go get github.com/dhamidi/statecharts
```

## Quickstart

Create a typed model, register behavior by stable application name and
version, build a chart, and start an instance:

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/dhamidi/statecharts"
)

type Door struct {
	OpenCount int
}

func main() {
	model := statecharts.NewGoModel(func() *Door { return &Door{} })
	recordOpen, err := model.Action(
		"example.door.record-open", "v1",
		func(door *Door, _ statecharts.ExecContext, _ []statecharts.Value) error {
			door.OpenCount++
			return nil
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	chart, err := statecharts.Build(
		statecharts.Compound("door", "closed",
			statecharts.Children(
				statecharts.Atomic("closed",
					statecharts.On("open", statecharts.Target("opened"), statecharts.Then(recordOpen.Do())),
				),
				statecharts.Atomic("opened",
					statecharts.On("close", statecharts.Target("closed")),
				),
			),
		),
		model,
	)
	if err != nil {
		log.Fatal(err)
	}

	instance, err := chart.NewInstance()
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer instance.Stop(ctx)

	if err := instance.Send(ctx, statecharts.Event{Name: "open", Type: statecharts.EventExternal}); err != nil {
		log.Fatal(err)
	}
	fmt.Println(instance.Configuration()) // [opened]
}
```

Each `Instance` owns a fresh `Door` from the model factory and processes events
serially on one goroutine. `Send` returns after the event's complete macrostep,
so `Configuration` is current immediately afterward.

## Core model

### Definitions and charts

The builder constructs a mutable `Definition`; `Build` validates it and
compiles it with a `Datamodel` into an immutable `Chart`:

- `Atomic` creates a leaf state.
- `Compound` creates a state with one active child.
- `Parallel` creates simultaneously active regions.
- `Final` marks completion of its parent.
- `History` remembers a shallow or deep prior configuration.
- `On` and `Eventless` add transitions.
- `Target`, `If`, `Then`, and `AsInternal` configure transitions.
- `OnEntry`, `OnExit`, `Invoke`, and the executable constructors add behavior.

Charts are safe to share. Every call to `Chart.NewInstance` creates an
isolated datamodel session and interpreter.

State and event IDs use `Identifier`. Dots express hierarchy in application
names, for example `orders.invoice-42`; they do not imply a network location.

### GoModel and stable function references

`GoModel[D]` is the default datamodel. It combines a factory for ordinary Go
state with a model-scoped registry:

```go
type Job struct {
	Attempts int
}

model := statecharts.NewGoModel(func() *Job { return &Job{} })

retry, err := model.Condition(
	"billing.job.can-retry", "v1",
	func(job *Job, _ statecharts.ExecContext, args []statecharts.Value) (bool, error) {
		limit, ok := args[0].AsInt64()
		return ok && int64(job.Attempts) < limit, nil
	},
)

record, err := model.Action(
	"billing.job.record-attempt", "v1",
	func(job *Job, _ statecharts.ExecContext, args []statecharts.Value) error {
		amount, _ := args[0].AsInt64()
		job.Attempts += int(amount)
		return nil
	},
)

working := statecharts.Atomic("working",
	statecharts.On("failed",
		statecharts.Target("working"),
		statecharts.If(retry.If(statecharts.GoLiteral(statecharts.Int64Value(3)))),
		statecharts.Then(record.Do(statecharts.GoLiteral(statecharts.Int64Value(1)))),
	),
)
```

The definition stores the names, versions, and canonical arguments, not Go
function values. Compilation resolves those references against the supplied
registry. This gives application authors a simple convention:

- use a package- or domain-qualified descriptive name such as
  `billing.job.record-attempt`;
- use a non-empty semantic implementation version such as `v1`;
- bump the function version when that named operation changes meaning;
- use `WithRevisionSalt` when chart behavior changes without changing a
  referenced function version.

Registries are deliberately model scoped. There is no global concrete-type or
function registry, and generated closure names never enter definitions.

Besides `Action` and `Condition`, a model can register a `Value` producer or a
readable/writable `Location`. References accept expression arguments, so a
single registration can be reused at many definition sites without captured
parameter closures. `GoLiteral` supplies canonical constants and `GoData`
addresses declared definition data.

### Canonical values

Events and definition operands carry `Value`, a closed, deterministic value
model: null, boolean, UTF-8 string, exact decimal number, list, string-keyed
map, or tagged application value. It has no process-global type registration.

```go
name, err := statecharts.StringValue("invoice-42")
if err != nil {
	// invalid UTF-8
}
payload, err := statecharts.TaggedValue("billing.invoice/v1", name)

event := statecharts.Event{
	Name: "invoice.open",
	Type: statecharts.EventExternal,
	Data: payload,
}
```

Use tagged values for application-level payload identities. `MapValue`,
`ListValue`, `ValueFromJSON`, and typed `As...` accessors handle nested data.
Numbers are exact and canonically encoded; use `AsInt64` for integer consumers
rather than parsing `AsNumber`, because canonical decimal text may use an
exponent.

### Instances and execution

An instance is driven with `Start`, `Send`, `Configuration`, `Snapshot`,
`Stop`, and `Wait`. Registered functions receive an `ExecContext`, which
exposes the current event, active-state checks, session metadata, logging, and
controlled `Raise`, `Send`, and `Cancel` operations.

`Raise` enqueues an internal event in the current macrostep. A registered
function must not call blocking methods on its own `Instance`; that instance's
single goroutine is already executing the function.

An action or condition error becomes a tagged `error.execution` platform
event. An outbound dispatch failure becomes `error.communication`. Charts can
handle both with ordinary transitions, and `PlatformErrorDetails` extracts the
stable classification and message.

### Outbound effects and IOProcessor

All event delivery outside the current chart crosses an `IOProcessor`.
Executable `Send` and `ExecContext.Send` select a processor type, target,
payload, and optional delay. This is the seam for local routing, HTTP or queue
transports, authorization, tracing, and test doubles; the core library does
not prescribe a network topology or transport.

```go
notify := statecharts.Send(
	"invoice.ready",
	statecharts.SendTarget("notifications"),
	statecharts.SendType("queue"),
	statecharts.SendDelay(2*time.Second),
)

instance, err := chart.NewInstance(
	statecharts.WithIOProcessor("queue", queueProcessor),
)
```

The default processor handles local interpreter targets. Custom processor
types are explicit and isolated. Processors that implement
`IOProcessorDescriber` can expose their type and location through
`ExecContext.IOProcessors`.

### Invoked services

Definitions declare an invocation by handler type and source. Runtime
capabilities arrive later through an `InvokeHandler` factory:

```go
request, err := model.Value(
	"billing.invoice.render-request", "v1",
	func(job *Job, _ statecharts.ExecContext, _ []statecharts.Value) (statecharts.Value, error) {
		return statecharts.Int64Value(int64(job.Attempts)), nil
	},
)

rendering := statecharts.Atomic("rendering",
	statecharts.Invoke(
		"renderer", "invoice-pdf",
		statecharts.WithInvokeID("render"),
		statecharts.WithInvokeContent(request.Get()),
	),
	statecharts.On("done.invoke.render", statecharts.Target("ready")),
)

instance, err := chart.NewInstance(
	statecharts.WithInvokeHandler("renderer", func() statecharts.InvokeHandler {
		return rendererHandler{Client: httpClient}
	}),
)
```

The definition contains only `renderer`, `invoice-pdf`, and the stable value
reference. Clients, channels, credentials, and other capabilities stay in the
runtime handler. A handler receives a context that is cancelled when its state
exits and an `InvokeIO` for delivering events or receiving forwarded events.
Its successful result becomes `done.invoke.<id>`; failures become
`error.communication`.

Durable invoked work can implement `ResumableInvokeHandler`. During rehydrate,
the runtime resumes active invocations through that interface; a handler that
cannot resume receives an explicit communication error rather than silently
pretending work is still running.

## Inspecting, editing, and recompiling definitions

Go is the primary authoring surface, not the storage format. `Chart.Definition`
returns an independently editable, normalized copy. The complete definition
can be encoded, decoded, inspected, changed, and recompiled against the same
application registry:

```go
definition := chart.Definition()

wire, err := json.MarshalIndent(definition, "", "  ")
if err != nil {
	return err
}
if err := os.WriteFile("door.json", wire, 0o644); err != nil {
	return err
}

// Later, after an operator or deployment tool edits the file:
wire, err = os.ReadFile("door.json")
if err != nil {
	return err
}
var edited statecharts.Definition
if err := json.Unmarshal(wire, &edited); err != nil {
	return err
}
nextChart, err := statecharts.Compile(edited, model)
if err != nil {
	return err // structural and model-reference errors are reported here
}
```

The same pattern supports a running program's inspection endpoint:

```go
func definitionHandler(chart *statecharts.Chart) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(chart.Definition())
	}
}
```

The durable counters example serves `GET /definitions/counter` and recompiles
its own encoded definition during startup. The ai-agent example performs the
same encode/decode/recompile path for every chart family before returning a
chart, so its behavioral tests run against transported definitions.

Compilation creates a new immutable chart; it never mutates an existing one.
That is the foundation for Erlang-style deployment: a publication layer can
pin existing instances to their old chart while routing newly spawned
instances to the new chart. Runtime chart publication and version pinning are
separate concerns and are not yet part of this package.

Definitions are syntax neutral. JSON is convenient for transport today, but a
different text syntax or editor only needs to produce the same `Definition`.
Likewise, another expression model can implement `Datamodel`,
`DatamodelProgram`, and `DatamodelSession` without changing the interpreter.

## Persistence

`Instance.Snapshot` captures a point-in-time cache: model bytes, active
configuration, history, queues, pending sends, and invocation bookkeeping.
The model owns encoding of its state; `GoModel` uses `encoding/json` by default
or an application-supplied `GoSnapshotCodec`.

Snapshots are transparent caches, not authoritative user data. The chart's
revision is derived from its canonical definition and referenced function
versions. When the revision no longer matches, rehydration ignores the stale
snapshot and replays the event reducer from the log. Applications should not
write snapshot migration code.

`Chart.Rehydrate` reconstructs a session from its `Log`, optionally using a
compatible snapshot to skip old entries. Replay suppresses real outbound
effects, then reconciles durable outbound intents and active invocations when
the instance goes live.

Runtime capabilities must not be placed in snapshot state. Keep clients,
channels, loggers, transports, and reply capabilities in instance options,
handler factories, IOProcessors, or out-of-band registries addressed by
canonical IDs.

For deterministic timer tests, use `ManualClock`:

```go
clock := statecharts.NewManualClock(time.Unix(0, 0))
instance, err := chart.NewInstance(statecharts.WithClock(clock))
clock.Advance(30 * time.Second)
```

## Actor systems

The `actors` package hosts many named chart instances in one process. An actor
is ephemeral by default; `actors.Durable()` opts that spawn into logging,
rehydration, checkpointing, and paging without changing its chart type.

```go
system := actors.NewSystem(
	actors.WithNodeName("host-a"),
	actors.WithStorage(storage),
	actors.WithIdleTimeout(5*time.Minute),
	actors.WithMaxResident(1_000),
)

if err := system.Register(chart); err != nil {
	return err
}
if err := system.Spawn(ctx, "invoice-42", chart.ID(), actors.Durable()); err != nil {
	return err
}
if err := system.Tell(ctx, "invoice-42", statecharts.Event{
	Name: "open",
	Type: statecharts.EventExternal,
}); err != nil {
	return err
}
```

Actor IDs are hierarchical `Identifier` values. Routing locations use `@`:
`billing.invoice-42@host-a`. The node name is routing metadata, not durable
identity. Each system has an isolated storage boundary, so moving a system to
a differently named host does not move or rename actor history.

Chart actions reach peers through the same IOProcessor seam:

```go
sendReceipt, err := model.Action(
	"billing.invoice.send-receipt", "v1",
	func(_ *Invoice, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ec.Send("receipt.ready", statecharts.SendOptions{
			Target: "mailer@host-b",
			Data:   statecharts.Int64Value(42),
		})
		return nil
	},
)
```

Local actor IDs route inside the system first. A missing local SCXML target can
fall through to the configured peer processor. `actors.Bridge` is an in-process
demonstration of this seam; real applications can supply their existing
transport. The library intentionally does not define network discovery,
topology, authentication, authorization, or telemetry.

Durable actors can page out after an idle timeout or under residency pressure.
The next message transparently rehydrates them before delivery.
`System.IsResident` observes residency without activating an actor, and
`WithResidencyObserver` reports `hydrating`, `resident`, and `paged out`
transitions. Non-durable actors remain resident because they have no history
from which to reconstruct.

Each pending durable outbound send is recorded before dispatch. A processor
can implement `AcknowledgingIOProcessor` so the intent remains pending until
transport acceptance. Outcomes still arrive as ordinary events; actor charts
remain responsible for domain timeout and error behavior.

## SQL storage

`sqllog` is opt-in and works over a caller-provided `database/sql` connection:

```go
db, err := sql.Open(driverName, dsn)
if err != nil {
	return err
}
storage, err := sqllog.New(db, dialect)
```

The optional `sqllog/sqlite3` package supplies the pure-Go ModernC driver and
the current SQLite dialect:

```go
storage, err := sqlite3.Open("data/actors.db")
if err != nil {
	return err
}
defer storage.Close()
```

File-backed SQLite databases use WAL mode and configured pooled connections.
Use a different database file for each actor system; one process may host many
systems, but each system owns an isolated log.

## Examples

- [`examples/counters`](examples/counters) runs seven durable actors with a
  three-actor residency limit, a live Datastar UI, and reconnecting writer and
  reader load instruments. It demonstrates paging, hydration visibility,
  canonical definition inspection, bridges, and custom IOProcessors.
- [`examples/ai-agent`](examples/ai-agent) is a multi-conversation durable
  workspace with ephemeral connection actors, resumable clients, streamed
  provider work, tool leasing, and capability registries kept outside model
  snapshots.

## Standards coverage

The interpreter follows SCXML's transition selection, conflict resolution,
entry/exit ordering, hierarchy, parallel regions, history, event queues,
executable content, sends, cancellation, invocation, finalization, done data,
and platform error behavior. `scxml.html` in this repository is the normative
reference used by the conformance tests.

This library provides the runtime semantics and extension seams, not every
ecosystem concern. Network transports and topologies, authentication and
authorization, and tracing or metrics integrations belong in application
IOProcessors, handlers, and wrappers.
