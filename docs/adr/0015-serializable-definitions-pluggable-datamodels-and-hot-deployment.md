# 15. Serializable definitions, pluggable datamodels, and revision-pinned hot deployment

Date: 2026-07-15

## Status

Accepted

## Context

The first implementation treats a chart definition and its executable form
as nearly the same thing. `StateSpec` and `TransitionSpec` contain Go
function values, and `Build` compiles those values directly into a `Chart`.
This gives Go callers a small and convenient API, but opaque function values
create three limits:

1. A running program cannot return a complete definition for inspection or
   editing. It can list states, but it cannot faithfully describe the
   conditions, actions, data, invocation declarations, or other behavior
   that connects them.
2. A definition cannot be encoded, edited outside the process, decoded, and
   compiled again. Go function values have no stable serialized form, and a
   runtime-derived function name is neither a durable identity nor a
   sufficient description of a closure.
3. The interpreter is coupled to one datamodel implementation: an arbitrary
   Go value operated on by Go callbacks. Supporting another expression and
   data environment would require adding a parallel set of special cases to
   the chart and interpreter APIs.

The intended user experience remains Go-first. Most callers should build a
chart with typed Go callbacks and never need to know how its definition is
encoded. At the same time, a definition built in Go must be available as
ordinary inspectable data so tooling can display it, store it, edit it, and
compile it again.

The motivating operational loop is:

1. Ask a running program for a chart definition that was originally built
   and compiled into that program with the Go builder.
2. Encode the definition to a text file.
3. Edit the text file.
4. Load and compile the complete edited definition in the running program.
5. Publish it so newly spawned actors and newly created chart instances use
   it.
6. Leave existing actors and instances on the definition they started with
   until they reach a terminal state.

This is code-version hot deployment in the Erlang sense: publishing a new
definition does not rewrite a currently executing actor underneath its
mailbox. Old and new revisions coexist. Each actor continues against the
revision it already knows, while future actors select the newly published
revision.

The library must also make adding a datamodel a bounded integration task.
The default distribution includes a Go datamodel. A separate optional
package will demonstrate the extension point with an ECMAScript datamodel
implemented using `modernc.org/quickjs`, following the same dependency shape
as the optional SQLite-backed log package.

## Decision

### Definition is the canonical program; Chart is a compiled revision

Introduce a serializable, mutable `Definition` as the canonical description
of a statechart program. A definition contains no raw Go function values. It
contains the complete state tree, transitions, executable operations, data
declarations, expressions, invocation declarations, and stable references
to host-provided behavior.

`Chart` remains immutable and safe to share between instances, but becomes
explicitly a compiled revision:

```text
Go builder ──▶ Definition ──▶ compile ──▶ Chart
                    ▲                       │
                    │                       │
                    └──── inspect/copy ─────┘
```

The Go builder constructs the same `Definition` that any decoder or editor
would construct, then sends it through the same compiler. There must not be a
second builder-only compilation path. A definition obtained from a compiled
chart therefore contains the behavior originally supplied through the Go
builder, not a lossy reconstruction of the compiled state tree.

The names `Definition` and `Chart` are deliberate:

- `Definition` is syntax-neutral program data.
- `Chart` is its validated executable form.
- Surface encodings are adapters around `Definition`, not architectural
  centers of the interpreter.

The ordinary Go API remains close to the current builder experience. The
following is illustrative; exact constructor names may change during
implementation:

```go
type Counter struct {
	Value int
}

model := statecharts.NewGoModel(func() *Counter {
	return &Counter{}
})

increment := model.Action(
	"increment", "v1",
	func(c *Counter, ec statecharts.ExecContext, args []statecharts.Value) error {
		c.Value++
		return nil
	},
)

belowLimit := model.Condition(
	"below-limit", "v1",
	func(c *Counter, ec statecharts.ExecContext, args []statecharts.Value) (bool, error) {
		return c.Value < 100, nil
	},
)

chart, err := statecharts.Build(
	statecharts.Compound("counter", "ready",
		statecharts.Children(
			statecharts.Atomic("ready",
				statecharts.On("increment",
					statecharts.If(belowLimit),
					statecharts.Then(increment),
				),
			),
		),
	),
	statecharts.WithDatamodel(model),
)
```

The callbacks are ordinary typed Go functions. The values returned by
`model.Action` and `model.Condition` carry both the resolved implementation
used by the compiler and a stable definition used for inspection and
encoding.

### Definitions contain structured behavior

Every operation that can affect interpretation must have a definition form.
This includes:

- ordered action blocks;
- boolean conditions;
- value and location expressions;
- entry, exit, and transition behavior;
- raise, send, cancel, and log operations;
- assignment;
- conditional branches and iteration;
- scripts or model-specific action calls;
- invocation type, source, parameters, identity, forwarding, and finalize
  behavior;
- final-state result data;
- initial and history default transitions, including their behavior.

These are statechart operations, not artifacts of a particular surface
encoding. Public Go constructors should use idiomatic names and types. A
surface codec is responsible for mapping its representation onto these
operations.

Expressions are model-owned, structured values rather than universally
being strings:

```go
type Expression struct {
	Kind Identifier
	Data Value
}
```

For one model an expression may contain source text. For the Go model it may
be a function call, a literal, or a composition such as `and`, `or`, or
`not`. Keeping this representation structured allows a text editor, visual
editor, or another codec to preserve the expression without forcing every
datamodel into one expression syntax.

### Go behavior round-trips as stable references

Compiled Go code cannot itself be serialized. The serializable identity of a
Go action, condition, value producer, location handler, or other host
function is an explicit reference:

```go
type FunctionRef struct {
	Name    Identifier
	Version string
	Args    []Expression
}
```

The actual implementation lives in an immutable registry owned by a Go
datamodel value. It is not stored in global process state. Compilation
resolves every reference before a chart can be published; an unknown
reference is a compile error, never a delayed runtime surprise.

Arguments are part of the reference so parameterized behavior remains
expressive. Without them, callers would need to register a distinct closure
for every use of operations such as `set-count(5)` or `send-reminder(30s)`.

The function version is a compatibility promise. Changing the behavior of a
registered function requires publishing it under a new version. The
definition therefore round-trips faithfully, and its behavior can be
resolved in any process that supplies the same named and versioned function
implementations. The body of a Go function is inspectable only through its
declared metadata; editing arbitrary compiled Go instructions is explicitly
not a goal.

### Datamodels have shared-program and per-instance lifecycles

A datamodel integration has two necessary ownership levels:

1. A shared definition/compiler value validates expressions, resolves model
   functions, and prepares immutable program data for a chart revision.
2. A session value belongs to exactly one running chart instance and owns its
   mutable data and model runtime.

The final interfaces will be refined during implementation, but preserve
this shape:

```go
type Datamodel interface {
	Name() Identifier
	Compile(*Definition) (DatamodelProgram, error)
}

type DatamodelProgram interface {
	Fingerprint() []byte
	NewSession(SessionOptions) (DatamodelSession, error)
}

type DatamodelSession interface {
	EvaluateBoolean(ExecContext, CompiledExpression) (bool, error)
	EvaluateValue(ExecContext, CompiledExpression) (any, error)
	Assign(ExecContext, CompiledExpression, any) error
	Execute(ExecContext, CompiledExpression) error
	ForEach(ExecContext, CompiledExpression, IterationBindings, func() error) error

	EncodeSnapshot() ([]byte, error)
	DecodeSnapshot([]byte) error
	Close() error
}
```

`NewSession` always returns a fresh session. Restoring a snapshot decodes
into a fresh session and replaces all of its restorable model state. If the
snapshot is incompatible, that whole session is closed and discarded before
another fresh session is created for same-revision replay. Replay never
continues against a session that a failed decode may have partially mutated.

The interpreter owns the execution environment: the current event, active
configuration, session identity, chart name, available I/O processors, and
platform capabilities. It passes that environment to the datamodel for each
evaluation. A datamodel must not maintain a second copy of interpreter state.

Datamodel methods return errors. The interpreter remains solely responsible
for applying statechart error semantics and placing platform error events on
the correct queue. A model cannot directly mutate interpreter queues.

Snapshot encoding belongs to the datamodel session. Snapshots remain
transparent, disposable caches. Users do not write snapshot migration code;
an incompatible snapshot is ignored and the actor is reconstructed from its
authoritative input log using its pinned chart revision.

`Close` is part of the session contract so eviction and terminal shutdown
release model resources deterministically.

### The Go datamodel is the default

The root package ships the Go datamodel and typed helpers. It retains the
ergonomics of callbacks over `*D`, while the returned action and expression
values carry stable function references for definitions.

A Go model owns:

- the factory for a fresh `D`;
- named and versioned function registrations;
- compilation of structured Go expressions into direct function calls;
- import and export between canonical values and Go values;
- transparent snapshot encoding for `D`;
- optional codecs for applications that need non-default data encoding.

Users should not separately register a function and then repeat its name in
the builder. A registration helper both installs the implementation and
returns the definition value used by the builder.

### ECMAScript is an optional datamodel package

An optional package, expected to be named along the lines of
`datamodel/ecmascript`, implements the same interfaces using
`modernc.org/quickjs`.

Each running instance owns one QuickJS VM through its datamodel session. This
matches the interpreter's single-owner execution model because a QuickJS VM
is not safe for concurrent use. Paging out or terminating an actor closes
the VM; paging in creates a new VM and restores or replays the actor's data.

The implementation owns:

- expression and script compilation;
- model data initialization;
- binding the interpreter-provided execution environment;
- protection of system-owned bindings;
- conversion to and from canonical values;
- value-level snapshot encoding;
- evaluation time, memory, and stack limits;
- explicit QuickJS value lifetimes and pending-job handling.

Engine bytecode is an in-memory optimization only. It is engine-version
specific and is never a durable definition or snapshot format.

The dependency is isolated in the optional package. Applications that use
only the default Go datamodel do not import or compile the engine package.

### Values crossing runtime boundaries have a canonical representation

Arbitrary `any` values and a global type registry do not provide a sound
boundary between different datamodels. Events, invocation parameters,
results, sends, logs, and function arguments need a persistable
representation that is independent of either runtime. A datamodel snapshot
is different: it remains a model-owned opaque payload interpreted only by
the same datamodel program and revision.

Introduce a canonical `Value` representation supporting scalar values,
lists, maps, and explicitly tagged application values. Datamodel sessions
import canonical values into their native representation and export native
values back to canonical form.

The Go model may reconstruct a tagged value as a concrete Go type. Another
model may inspect the same value as a map or list. Native opaque capabilities
may be supported inside one ephemeral Go instance, but cannot cross a
durability, process, actor, or datamodel boundary.

This model-local conversion eventually replaces the process-global event
data registry.

### Surface syntaxes are optional adapters

The root package does not require a particular text representation. Surface
codecs encode and decode `Definition`; they do not compile charts or execute
model code.

The datamodel interface does not require textual parsing or formatting.
Models or syntax packages may additionally implement an optional adapter:

```go
type TextExpressionCodec interface {
	ParseExpression(ExpressionKind, string) (Expression, error)
	FormatExpression(ExpressionKind, Expression) (string, error)
}
```

A source-oriented model naturally provides such an adapter. A model used
only through the Go builder, structured files, or a visual editor does not
need one. Adding a datamodel is therefore an execution integration first,
not a commitment to any one authoring format.

The normal documentation and examples continue to lead with Go builders,
typed callbacks, actors, durability, and hot deployment. Interchange formats
remain discoverable but peripheral packages.

### Publishing a definition is atomic and affects only future instances

A definition has a stable chart identity and a distinct revision identity.
The revision identity is derived from:

- the canonical definition;
- the datamodel name and program fingerprint;
- every referenced host function name and version;
- an optional explicit compatibility salt when an application needs one.

Revision identity must be deterministic across processes and restarts. Its
inputs use canonical ordering and exclude pointers, process-local registry
state, compiled engine bytecode, and other transient compilation artifacts.
The same definition, datamodel implementation version, host-function
references, and compatibility salt must always produce the same revision
identity.

Loading a text file follows a prepare-then-publish sequence:

1. Decode the entire definition.
2. Validate its state structure and references.
3. Resolve and compile every datamodel expression and host function.
4. Compute its revision identity.
5. Atomically make that revision current for its chart identity.

Any failure before the final step leaves the currently published revision
unchanged. There is no partially updated chart visible to a spawning actor.

Spawning selects the current revision once and pins it before any initial
behavior runs. Re-publishing the same chart identity changes the revision
selected by later spawns, not the revision stored by an existing actor or
instance.

For example, in illustrative pseudocode (the final editing and codec APIs are
not fixed by this decision):

```go
// v1 was originally built using the Go builder.
v1, err := system.Definition("counter")
if err != nil {
	return err
}

if err := textcodec.EncodeFile("counter.chart", v1); err != nil {
	return err
}

// An operator edits counter.chart outside the process.
edited, err := textcodec.DecodeFile("counter.chart")
if err != nil {
	return err
}

// PublishDefinition compiles and validates the complete candidate before
// atomically changing the revision selected by later spawns.
if err := system.PublishDefinition(edited, models); err != nil {
	return err
}
```

If actor `red` was spawned before publication, and actor `blue` afterward:

```text
red  ──▶ revision v1 ──▶ remains on v1 until terminal
blue ──▶ revision v2 ──▶ remains on v2 until terminal
```

There is no operation that mutates a compiled `Chart` in place. Reloading a
definition does not reinterpret an existing actor's history, map its active
configuration, replace its datamodel, restart its invocations, or change its
pending timers. These would be actor migration semantics, not hot code
publication, and are outside this decision.

### Durable actors persist their pinned revision

An actor's revision selection is durable actor metadata. The chart identity
and revision identity are written atomically in the session-start record
before the actor's initial behavior is allowed to run. That record is the
source of truth for page-in and restart when no in-memory actor entry exists;
recovery never infers an existing actor's revision from whichever definition
is currently published.

Paging out and paging in do not count as spawning a new actor. A paged-in
actor resolves and uses its recorded revision even when a newer revision is
currently published for its chart identity. Falling back to the newest
revision when the pinned revision is unavailable would silently change actor
behavior and is forbidden.

Consequently, an actor system maintains multiple compiled revisions for one
chart identity:

```text
counter
  current ──▶ revision v3
  retained ─▶ revision v1 (two live actors)
  retained ─▶ revision v2 (one paged-out durable actor)
```

An old revision may be released only when no non-terminal actor, resident or
paged out, refers to it. Ordinary non-actor `Instance` values retain their
`*Chart` directly, so normal Go reachability provides the same pinning.

After a process restart, every revision referenced by a non-terminal durable
actor must still be resolvable. The canonical definition, datamodel identity,
and serializable model configuration needed to compile it are stored in a
durable definition store keyed by revision identity. A deployment artifact
may provide that store, but an in-memory compiled-chart registry alone is not
sufficient. Definitions containing Go function references additionally
require the new binary to provide the same named and versioned
implementations. Removing an implementation while durable actors still
reference it makes those actors unavailable and must fail loudly before
replay or initial behavior.

A snapshot records its chart revision and is valid only for that revision.
An invalid or unavailable snapshot is discarded and rebuilt by replaying the
actor's input log against the same pinned revision. Snapshot migrations are
never required from application users.

### Terminal state is the revision handoff boundary

An existing actor never begins using a newer revision. Its terminal state is
the end of that actor identity and releases its revision pin. A later spawn
under another identity uses the currently published revision.

If an application wants a long-lived logical entity to adopt new behavior,
it models that as an application protocol: allow the old actor to finish and
spawn its successor, or explicitly create a separate migration mechanism.
The core hot-deployment path does not guess how to transform live state.

Ephemeral actors have no durable metadata or replay history. They simply
retain their in-memory chart revision until terminal. If the process stops,
they cease to exist as before.

## Consequences

- The Go builder remains the primary and simplest authoring API.
- A chart built in Go can be inspected and encoded without reverse
  engineering its compiled state tree.
- Complete definitions can be edited and atomically republished at runtime.
- New and old chart revisions coexist safely; existing actors never change
  behavior because another definition was loaded.
- Durable paging preserves code-version identity instead of accidentally
  upgrading an old actor to the current revision.
- Supporting a new datamodel requires implementing one shared compiler and
  one per-instance session contract, without modifying interpreter control
  flow.
- The default Go model stays dependency-light. The QuickJS engine is isolated
  in an optional package.
- Standard operations become first-class definition data instead of being
  hidden inside callbacks.
- Go function implementations remain process capabilities. Definitions
  serialize stable references and arguments, not machine code or closure
  captures.
- Function names and versions become durable compatibility commitments.
- Actor systems need a multi-revision registry, durable revision pins, and a
  way to retain or reload definitions referenced by non-terminal actors.
- Event and function data must move from arbitrary `any` values toward a
  canonical cross-model value representation.
- The current single-Go-datamodel decision in ADR 0004 is superseded.
- Snapshot version checks become revision checks while preserving the rule
  that snapshots are transparent caches and input logs are authoritative.
