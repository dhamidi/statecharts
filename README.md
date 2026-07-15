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

### Optional ECMAScript datamodel

Applications that need runtime-editable source expressions can opt into the
pure-Go ModernC QuickJS integration without changing the interpreter:

```go
model, err := ecmascript.New(
	ecmascript.WithEvaluationTimeout(100 * time.Millisecond),
	ecmascript.WithMemoryLimit(64 << 20),
)
initial, err := ecmascript.Source("0")
condition, err := ecmascript.Source("count < 3")

chart, err := statecharts.Build(
	statecharts.Compound("root", "working", statecharts.Children(
		statecharts.Atomic("working", statecharts.Eventless(
			statecharts.If(condition), statecharts.Target("done"),
		)),
		statecharts.Final("done"),
	)),
	model,
	statecharts.WithData(statecharts.DataDefinition{ID: "count", Expr: &initial}),
)
```

Each instance owns an isolated VM. System bindings such as `_event`,
`_sessionid`, `_name`, `_ioprocessors`, `_x`, and `In()` are refreshed for
each evaluation and cannot be overwritten. Synchronously queued Promise jobs
drain within the same interpreter turn; no browser, filesystem, timer, or
network APIs are installed.

Declared data is the only VM state stored in snapshots. Cycles, functions,
accessors, unsupported objects, and undeclared global mutations make the
snapshot cache unavailable instead of being silently discarded; durable log
replay remains authoritative. Exact integers outside JavaScript's safe range
cross as `BigInt`. Decimal values that cannot round-trip through an ECMAScript
`Number` are rejected rather than rounded. Configure finite evaluation,
memory, and stack limits for runtime-edited or otherwise untrusted source.

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

wire, err := statejson.MarshalIndent(definition, "", "  ")
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
edited, err := statejson.Unmarshal(wire)
if err != nil {
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
		data, err := statejson.MarshalIndent(chart.Definition(), "", "  ")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}
}
```

Here `statejson` is the optional
`github.com/dhamidi/statecharts/syntax/json` package. JSON is one editable
surface syntax, not the canonical in-memory program or revision encoding. The
durable counters example serves `GET /definitions/counter`, validates and
publishes edited complete definitions, and recompiles its own encoded
definition during startup. The ai-agent example performs an equivalent
encode/decode/recompile path for every chart family before returning a chart,
so its behavioral tests run against transported definitions.

For standards-based interchange, the optional
`github.com/dhamidi/statecharts/syntax/scxml` package maps the same complete
`Definition` to and from SCXML. Expressions remain owned by the selected
datamodel, so callers provide that model's text codec rather than asking the
XML adapter to interpret program text:

```go
wire, err := scxml.MarshalIndent(
	chart.Definition(),
	"  ",
	scxml.WithTextExpressionCodec(model.TextExpressionCodec()),
)
edited, err := scxml.Unmarshal(
	wire,
	scxml.WithTextExpressionCodec(model.TextExpressionCodec()),
)
nextChart, err := statecharts.Compile(edited, model)
```

`GoModel.TextExpressionCodec` stores stable function names, versions, and
canonical arguments as editable expression text; it never serializes function
pointers. The optional ECMAScript model's `TextExpressionCodec` preserves
source text verbatim. Encoding fails at the exact definition path if the
chosen codec cannot faithfully represent an expression. Unknown executable
XML is rejected, while explicit `ExtensionDefinition` nodes preserve their
namespace, name, and canonical payload.

Compilation creates a new immutable chart; it never mutates an existing one.
`actors.System.Publish` provides Erlang-style deployment: existing actors stay
pinned to their old chart while newly spawned actors select the new current
revision. The counters example documents the complete export → edit → validate
→ publish → canary loop; its dashboard cards and actor-inspection endpoint make
the corresponding revision pins visible.

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

`Chart.Revision()` returns that identity as `sha256:<64 lowercase hex digits>`.
Revision envelope version 1 hashes length-prefixed canonical definition bytes,
the datamodel name, and the datamodel program's deterministic fingerprint.
Canonical definition bytes include every expression/function reference and
the optional `WithRevisionSalt` value. Runtime pointers, registry insertion
order, clocks, process IDs, and compiled engine artifacts are excluded. A
datamodel must return a stable, non-empty `DatamodelProgram.Fingerprint`.

`Chart.DefinitionArtifact()` packages the canonical definition, chart and
datamodel identities, program fingerprint, envelope version, and revision
integrity needed to recompile that exact revision after restart. The
`DefinitionStore` and `ActorStore` contracts define immutable artifact storage,
atomic durable actor pin plus session-start creation, terminal pin release,
and atomic reference-checked deletion without exposing database concepts.
`storagetest.Run` is a reusable conformance suite; `storagetest.MemoryStore`
provides the same complete durable boundary for tests.

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

`Register` establishes the Go-built chart's stable identity, compiler, and
first current revision. To hot-deploy a complete edited definition, publish it
only after decoding the whole file:

```go
definition, revision, ok := system.CurrentDefinition(chart.ID())
if !ok {
	return fmt.Errorf("chart is not registered")
}
_ = revision // expose this in administrative output

definition.RevisionSalt = "invoice-v2"
newRevision, err := system.Publish(ctx, definition)
if err != nil {
	return err
}
```

Publication compiles all expressions and handlers and stores the immutable
artifact before one atomic current-pointer change. Actors already spawned stay
on their pinned revision; only future actors select `newRevision`. Definitions
returned by `CurrentDefinition` and `Definition` are independent editable
copies.

For durable actors, first spawn atomically stores the actor identity, chart ID,
revision, session ID, and session-start entry before initial behavior runs.
Page-in and process restart load that recorded revision—even when a newer one
is current—and fail before replay if its definition, datamodel implementation,
named function version, invoke handler, or pending outbox processor is missing.
Snapshots are only same-revision caches; a mismatch falls back to replay.

Reaching a top-level final state marks a durable actor terminal before its
completion is acknowledged and releases its stored revision reference. Actor
IDs are tombstones after terminal: `Spawn` and `Tell` return
`statecharts.ErrActorTerminal` rather than silently creating a new generation.
After publishing a replacement and allowing the last old actor to finish, use
`system.CollectDefinition(ctx, chart.ID(), oldRevision)` to remove that
non-current revision from storage and the compiled registry. Collection is
idempotent, refuses the current revision, and reports
`statecharts.DefinitionReferenced` while any resident, paged-out, or ephemeral
non-terminal actor still uses it.

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

`sqllog` stores immutable definition artifacts and durable actor revision pins
alongside the event log and snapshot cache. File-backed SQLite databases use
WAL mode, configured pooled connections, and immediate write transactions so
actor start and revision deletion remain linearizable across handles. Use a
different database file for each actor system; one process may host many
systems, but each system owns isolated durable storage.

## Examples

- [`examples/counters`](examples/counters) runs seven durable actors with a
  three-actor residency limit, a live Datastar UI, and reconnecting writer and
  reader load instruments. It demonstrates paging, hydration visibility,
  complete-definition hot publication, revision-pinned canaries, bridges, and
  custom IOProcessors.
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
