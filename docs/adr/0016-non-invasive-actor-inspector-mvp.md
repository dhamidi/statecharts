# 16. A non-invasive actor and statechart inspector

Date: 2026-07-27

## Status

Accepted

## Context

An actor runtime needs an operational view that answers more than whether a
process is running. An operator needs to find an actor, see the chart revision
it executes, understand its active state configuration, inspect its data, and
relate an incoming event to the transitions it selected. A virtual actor
runtime also needs to expose whether an actor is resident, hydrating, or paged
out without loading every durable identity into memory.

Actor dashboards commonly provide discovery, state inspection, commands,
logs, active connections, and lifecycle controls. This runtime has additional
statechart-specific information worth exposing:

- the complete immutable definition and pinned revision;
- the active state configuration and history values;
- the external event and internal microsteps in one macrostep;
- the transitions selected in document order;
- pending delayed sends and active invocations;
- the distinction between live state, persisted input history, and transient
  diagnostic traces;
- residency and hydration changes;
- the current published revision compared with the revision pinned by an
  existing actor.

The existing APIs expose parts of this information but do not form a safe
inspection contract. `Instance.Configuration` returns active states but not
the datamodel, queues, pending work, or one mutually consistent view of all of
them. `Instance.Snapshot` is a persistence cache whose datamodel bytes are
opaque and model-owned. It must not become a public debugging representation.
`System.IsResident`, `System.ActorRevision`, and `ActorStore.GetActor` answer
point questions but do not provide actor discovery. `ActorStore` lists every
non-terminal durable actor at once, which cannot back a large paginated
directory. Residency callbacks report changes but do not retain the current
hydrating state for a later query.

Inspection must not change the behavior being inspected. In particular:

- listing durable actors must not hydrate them;
- selecting a paged-out actor must not make it resident;
- a slow or disconnected browser must not delay a macrostep;
- a trace subscriber must not introduce an interpreter failure;
- replay during hydration must not appear as new live behavior;
- reading state must not write snapshots, events, or actor metadata;
- inspecting an actor must not keep it resident;
- inspecting one system must not expose another system that happens to share
  the process;
- arbitrary datamodel mutation must not invalidate the relationship between
  a durable input log and the state derived from it.

The library does not own deployment topology, authentication, authorization,
or an observability backend. The inspector needs explicit seams for those
concerns rather than embedding one answer to them. It must also work for a
system with no durable storage and must not depend on SQLite.

## Decision

### The inspector is a service over explicit runtime capabilities

Add an `inspector` package that composes public inspection capabilities from
`actors.System`. It does not reach into a system's maps, an `Instance`'s
interpreter, or a storage implementation's tables.

One process may host several isolated systems. An inspector service registers
each system under a host-supplied name:

```go
service := inspector.New(...)

err := service.RegisterSystem("orders", orders)
err = service.RegisterSystem("billing", billing)
```

The registration name is unique within that service. It is an inspector
namespace only. It does not alter actor addresses, node names, routing, chart
revisions, session IDs, or storage keys. The service does not own the systems
and does not stop them when it closes. There is no package-global system
registry.

The service exposes ordinary Go operations for:

- listing registered systems;
- listing and looking up actors;
- reading one actor's metadata and live state when available;
- reading one actor's pinned definition and the chart's current definition;
- reading durable input history when the system has storage;
- subscribing to bounded live observations;
- sending an external event when the host enables commands.

An optional HTTP package adapts these operations to JSON, Server-Sent Events,
and an embedded browser UI. The core service does not open a listener. A host
mounts the handler behind its own network, authentication, and authorization
policy. The adapter checks a host-supplied authorization function for system
listing, actor reads, history reads, definition reads, stream subscriptions,
and commands. It does not define identities or roles; an outer middleware
attaches the host's authenticated identity to the request context.

### Actor discovery is paginated and does not activate actors

`actors.System` gains a public actor-directory query. Its result combines:

1. durable actor metadata from the configured `ActorStore`; and
2. process-local entries, including ephemeral actors and live overlays for
   durable actors.

An actor appears once when both sources know it. The process-local entry
supplies current residency, last activity, and runtime lifecycle. Persisted
metadata supplies durable identity and revision information after a process
restart, before that actor has been activated in the new process.

Each actor summary contains at least:

```go
type ActorInfo struct {
    ID           ActorID
    Address      statecharts.Identifier
    Node         string
    Kind         statecharts.Identifier
    Revision     statecharts.RevisionID
    SessionID    statecharts.SessionID
    Durable      bool
    Adopted      bool
    Lifecycle    ActorLifecycle
    Residency    ResidencyState
    StartedAt    time.Time
    TerminalAt   time.Time
    LastActiveAt time.Time
}
```

The final API may reuse the existing lifecycle and metadata types rather than
duplicate them. The behavioral contract is fixed:

- `ID` is the stable actor identity without a node suffix.
- `Address` is the routing key and includes `@node` when the system has a node
  name.
- `Adopted` reports whether this process has a local actor entry and can route
  to it. Durable storage may contain an actor the application has not adopted
  since process start.
- `Lifecycle` distinguishes active and terminal actors without treating a
  paged-out actor as stopped. A terminal error is transient diagnostic data
  unless the application persists it separately; durable actor metadata does
  not invent one after a restart.
- `Residency` distinguishes resident, hydrating, and paged out. It is retained
  as queryable state rather than existing only as a callback argument.
- Times are UTC. An unknown time is zero rather than fabricated from the time
  of inspection.

The directory uses keyset pagination in actor-ID order. A query accepts an
exclusive `after` cursor, a bounded limit, and filters for actor ID prefix,
kind, revision, durability, lifecycle, and residency. The default page size is
50 and the maximum is 200. A page returns an opaque next cursor when more
matching actors exist. Inserting an actor before a cursor does not rewrite an
already returned page; refreshing starts a new traversal.

`ActorStore` replaces its all-at-once list operation with a paginated query
capable of applying the durable subset of those filters. Revision collection
continues to use `ReferencedRevisions` and the atomic
`DeleteDefinitionIfUnreferenced` check; paginated discovery must not replace
those operations or race a concurrent actor start. A storage implementation
may optimize the directory query, but the result order and cursor semantics
are storage-independent.

Each process-local actor entry retains its current residency in an atomic
field updated at the existing residency-change points. Directory reads use
that field and never wait for the per-actor activation lock merely to learn
that activation is in progress.

Directory reads never call `Spawn`, `Tell`, actor activation, `Rehydrate`, or
`SnapshotStore.Load`. They do not acquire every actor lock at once. They may
be eventually consistent across entries while an individual returned summary
is internally valid. An actor that terminates or disappears between a list
and a point lookup produces an ordinary not-found or no-longer-live result.

### Live inspection is a stable-boundary request, not a snapshot

`Instance` gains an inspection request processed on its own goroutine. The
request runs after every previously accepted request and before every later
one. It therefore observes a stable macrostep boundary and returns one
mutually consistent view of interpreter and datamodel state.

The result contains:

```go
type InstanceInspection struct {
    SessionID       SessionID
    Revision        RevisionID
    Running         bool
    Configuration   []Identifier
    HistoryValue    map[Identifier][]Identifier
    Datamodel       Value
    InternalQueue   []Event
    ExternalQueue   []Event
    PendingSends    []PendingSend
    ActiveInvokes   []ActiveInvoke
}
```

Every slice, map, event, and value in the result is independently owned. A
caller cannot mutate the running instance through the result. State IDs,
pending sends, and invocations use deterministic document or identifier order
where the runtime already defines one.

`Instance.Inspect` does not call `Instance.Snapshot`. It does not encode
snapshot bytes, checkpoint the actor, touch the log, reset idle time, or count
as actor activity. An inspection failure is returned to the inspector caller
and does not become a platform event or stop the actor.

`System.InspectActor` first resolves metadata without activation. It returns
`ActorInfo` for a paged-out or hydrating actor with no live
`InstanceInspection`. It calls `Instance.Inspect` only when the actor is
already resident. It does not acquire the actor entry's lifecycle lock: it
loads the resident instance atomically and submits a context-bounded
inspection request to that instance. An inspection concurrent with an
ordinary delivery may wait for that delivery's macrostep to finish. Concurrent
eviction may instead return the ordinary no-longer-live result. Neither case
initiates hydration, so opening the inspector cannot defeat a residency
limit or wait forever on a wedged activation lock.

The MVP does not reconstruct full current state for a paged-out actor. Doing
that correctly requires an isolated replay mode that never goes live, resumes
an invocation, arms a timer, or dispatches through an IO processor. Until that
mode exists, the UI shows persisted metadata and history while explicitly
marking live state unavailable.

### Every datamodel exports inspectable canonical state

`DatamodelSession` gains a read-only inspection operation:

```go
Inspect() (Value, error)
```

It returns the session's application-owned model state as canonical `Value`.
It excludes interpreter-owned state such as the active configuration, current
event, session ID, queues, and IO processor metadata; those fields come from
`InstanceInspection`. The returned value is independently owned.

Inspection is part of the datamodel contract rather than a Go-specific type
assertion. A new datamodel becomes inspectable by implementing the same
interface it already implements for evaluation and snapshot caching. The Go
datamodel returns a tagged `statecharts.go-datamodel/v1` value containing the
user's `D` under `data` and the model's declared values under `variables`.
Its default `D` conversion round-trips through `encoding/json`, decodes with
`UseNumber`, and passes the resulting JSON-shaped value to `ValueFromJSON`.
This covers ordinary JSON-shaped Go data without involving snapshot bytes. A
new `GoInspectionCodec[D]`, configured with `WithGoInspectionCodec`, replaces
that conversion for tagged or otherwise non-default data. An unsupported
value returns an inspection error without changing or terminating the actor.

Snapshot bytes remain opaque and model-owned. The inspector never decodes,
renders, or edits them. This keeps snapshot representation a transparent cache
that can change with a chart revision without becoming an operator-facing
schema.

The MVP does not import inspected state back into a live session. Operators
change behavior by sending events. Direct mutation would produce state that
cannot be derived from the durable input log and would make replay disagree
with the live actor.

### Live observations describe completed macrosteps

`actors.System` exposes a bounded observation subscription. A subscription
does not install an arbitrary callback on an actor goroutine. The system
copies an observation into each subscriber's bounded queue with a non-blocking
send. A full queue drops observations for that subscriber and reports the
number dropped with the next delivered item. Closing a subscription releases
its queue and registration.

System observations include:

- actor discovery and terminal lifecycle changes;
- residency changes, including the start and end of hydration;
- definition publication changes;
- one trace for each completed live macrostep.

The core `statecharts` package gains a macrostep-observer instance option.
`System` installs its internal observer on every activation, including actors
already resident when an external inspector subscriber later attaches. The
observer first checks an atomic subscriber count. With no subscribers it does
not build a trace, walk transition sets for diagnostics, or clone events. With
subscribers it builds the trace on the instance goroutine and hands it to the
system's non-blocking bounded fan-out; it never calls browser, transport, or
host callback code there. `Rehydrate` gates this observer alongside IO
processors and the diagnostic logger, so bootstrap and replayed macrosteps
remain suppressed until the instance goes live.

A macrostep trace contains:

```go
type MacrostepTrace struct {
    Sequence      uint64
    Timestamp     time.Time
    Trigger       *Event
    Before        []Identifier
    Microsteps    []MicrostepTrace
    After         []Identifier
    Terminal      bool
    TerminalError string
}

type MicrostepTrace struct {
    Trigger     *Event
    Transitions []TransitionRef
    Exited      []Identifier
    Entered     []Identifier
}

type TransitionRef struct {
    Source Identifier
    Index  int
}
```

`TransitionRef.Index` is the transition's zero-based position in its source
state's `Definition.Transitions` list. Source plus index is stable within one
immutable revision and lets the UI locate the exact transition without adding
operator-facing IDs to authored definitions.

The outer trigger is the event presented to the macrostep, or nil for initial
entry. A microstep trigger is nil for an eventless transition. Raised internal
events and other internally processed events appear on the microstep that
consumes them. `Before`, `After`, `Exited`, and `Entered` use document order.
Events and values are cloned before publication.

The MVP records selected transitions, not every candidate transition, failed
guard, expression evaluation, or executable operation. It is sufficient to
highlight the path the interpreter took without turning every expression into
a diagnostic side effect.

Trace sequence numbers are monotonic within one actor activation and are not
durable log sequence numbers. Live traces are suppressed while `Rehydrate`
replays existing input. Hydration produces residency observations and a final
resident summary, not a burst of apparently new macrosteps. Traces resume when
the actor goes live.

Observations are diagnostics. Dropping one does not alter interpreter,
routing, logging, snapshot, or actor lifecycle behavior. A gap is visible in
the stream and causes the UI to refresh the selected actor's current state.
The inspector service drains system subscriptions into a bounded in-memory
ring. It never allocates an unbounded queue per system, actor, or browser.

### Durable history and live traces remain distinct

For a durable actor, the inspector reads accepted input history through the
system's public actor-history operation. The system resolves the actor's
persisted session ID and reads through its configured `Log` interface. It
returns `LogEntry` records in sequence order and stops iteration at the
requested page size. It may start near the end using `LastSeq`, but neither
the system nor inspector queries driver-specific tables. The operation reports
history unavailable for an ephemeral actor or a system without storage.

Persisted history is authoritative input. A live macrostep trace is transient
diagnostic detail. The UI may place them on one chronological timeline, but
each row identifies its source as one of:

- persisted input;
- live runtime trace;
- actor lifecycle or residency observation;
- a stream gap.

The inspector does not claim that a live trace survives a restart. It does
not persist traces into the actor log because transitions and state changes
are deterministic consequences of the input log and pinned definition.
Historical transition reconstruction is outside the MVP.

Ephemeral actors have live observations and a bounded in-memory recent
timeline only. They do not gain persistence because an inspector is attached.

### Definition viewing compares pinned and current revisions

An actor detail response includes its pinned chart identity and revision plus
the revision currently published for that chart identity. The inspector can
return independently editable copies of both complete `Definition` values.
If a durable actor's pinned revision is not resident in the compiled registry,
the system resolves its immutable definition artifact from `DefinitionStore`
without compiling, activating, or replaying the actor.

The browser renders the state hierarchy and transitions from `Definition`.
For a resident actor it overlays `InstanceInspection.Configuration` and the
last live `MacrostepTrace`. For a paged-out actor it renders the definition
without claiming an active configuration. Missing pinned artifacts are shown
as operational errors rather than silently falling back to the current
revision.

The MVP is read-only with respect to definitions. It does not edit, publish,
collect, diff, or roll out revisions. The pinned/current comparison makes hot
deployment visible without combining inspection with deployment control.

### Sending an event is the only mutating MVP command

The service can expose one command:

```go
func (s *Service) SendEvent(
    ctx context.Context,
    systemName string,
    actorID actors.ActorID,
    eventName statecharts.Identifier,
    data statecharts.Value,
) error
```

It validates the actor ID, event name, and canonical value, constructs an
external event, and calls `System.Tell` exactly once. Inspector callers cannot
forge platform events, invocation IDs, send IDs, origins, or delivery IDs.
`Tell` retains its normal semantics: a durable paged-out target may hydrate,
the event is write-ahead logged before application, and actor-to-actor sends
still route through IO processors. The inspector does not inject directly
into an `Instance` or storage table.

A durable actor found in storage after a process restart is not necessarily
adopted into the new `System`'s process-local actor table. The directory marks
this case as known to storage but not adopted by this process. `SendEvent`
does not call `Spawn` on the operator's behalf and does not widen `Tell`'s
resolution semantics. It returns the ordinary unknown-actor error until the
application adopts that actor through its normal spawn protocol.

The browser provides a structured canonical-value form rather than an
untyped JSON text box. It shows that sending to a paged-out durable actor can
hydrate it and reports the resulting command error without retrying
automatically. A host may add application-specific event catalogues or forms,
but payload schemas are not inferred from dynamic expressions in a
definition.

Command support is disabled unless the host supplies an authorization
function. The function receives the request context, operation, system, and
actor. The host is responsible for attaching identity to the context. Every
attempt is passed to a host-supplied audit sink with its target and outcome.
The inspector does not implement users, sessions, roles, or an audit storage
backend.

There is no live datamodel edit, stop, restart, evict, hydrate, delete-history,
definition-publication, or rollout command in the MVP.

### Redaction happens before data leaves the inspector service

Events, datamodel values, invocation parameters, pending sends, and diagnostic
errors may contain secrets. The service accepts a host-provided redaction
policy with typed operations for canonical values and diagnostic text. Each
operation receives the system, actor, and data category and returns an
independently owned replacement.

Redaction runs in the inspector's subscription-draining goroutine, never on
the actor goroutine. Streamed values are redacted before they enter the
service's recent-event ring. Point-in-time actor inspection and durable
history values are redacted before the service returns them to a remote
adapter. A redaction failure drops a streamed record or fails a request,
increments a visible stream gap where applicable, and is reported through a
host-supplied diagnostic error sink. It does not expose the original value as
a fallback.

Definitions are not rewritten field by field. Redacting their literal values
would produce a document that no longer matches the advertised immutable
revision. Definition viewing is therefore a separately authorized read
capability that the host may withhold entirely.

The runtime inspection APIs themselves return unredacted values to their Go
caller. They are process-local capabilities. The inspector service is the
boundary that prepares those values for remote presentation.

### The browser UI is embedded, reconnecting, and transport-thin

An optional HTTP adapter ships an embedded, vendored UI with no CDN or runtime
package-manager dependency. It exposes a root custom element and smaller Web
Components for:

- system selection;
- actor search, filters, and virtualized pages;
- actor identity, lifecycle, revision, and residency summary;
- the state hierarchy and transition view;
- canonical datamodel values;
- queues, pending sends, and active invocations;
- persisted history and live macrostep traces;
- the external-event form.

The UI is a thin client. It does not reproduce transition selection, merge
snapshots, infer actor lifecycle, or edit definitions locally. It renders the
service's typed responses and sends explicit commands.

Server-Sent Events carry one-way live observations. Ordinary request/response
operations perform listing, lookup, history reads, and event sends. Each SSE
record has a service-local sequence ID. Reconnection uses `Last-Event-ID` when
the requested record remains in the bounded ring. If it has expired, the
adapter emits a gap record and the UI refreshes current actor and directory
state. Reconnection never resends an external event.

The UI remains usable without live streaming. It marks the view disconnected
and continues to support explicit refresh. The page has no document-level
horizontal scrolling; dense definition and value panels own their overflow.
Keyboard navigation, text labels, and color-independent residency and active
state markers are required.

The HTTP adapter defines a versioned wire protocol. Canonical values use the
existing versioned `Value` wire representation. Actor IDs and routing
addresses are data fields rather than unescaped URL path fragments. The
adapter accepts an `http.Handler` mount prefix and emits only relative URLs so
it works behind a reverse proxy.

### Inspection capabilities do not define deployment policy

The core and actor packages expose inspection data and bounded observations.
They do not choose:

- where the HTTP handler is mounted;
- whether it is reachable outside the process;
- TLS termination;
- users, credentials, roles, or tokens;
- network topology or peer discovery;
- an OpenTelemetry, logging, or metrics backend;
- data-retention policy beyond the inspector's bounded recent-event ring.

The host supplies those concerns. The inspector may include external trace or
log links supplied by the host, but it does not become an observability
database.

## MVP acceptance criteria

The implementation is complete when all of the following hold:

1. Two systems registered in one inspector remain isolated even when they use
   the same actor ID and node name.
2. A paginated directory lists resident ephemeral actors, resident durable
   actors, and paged-out durable actors recovered from storage after a process
   restart.
3. Listing, exact lookup, definition viewing, and opening the detail page for
   a paged-out actor produce no hydration or last-activity change.
4. A hydration already in progress appears as hydrating rather than paged out
   or resident.
5. Inspecting a resident actor returns configuration and datamodel from one
   stable macrostep boundary. Mutating the returned value cannot affect the
   actor.
6. The default Go datamodel exports ordinary nested Go data as canonical
   `Value`. Unsupported data returns an inspection error while the actor keeps
   processing events.
7. A macrostep trace identifies selected transitions by source state and
   document-order index. The references resolve against the actor's pinned
   definition.
8. Replay during page-in produces no live macrostep traces. Hydration and the
   resulting resident state remain visible.
9. A deliberately stalled subscriber cannot delay `Tell`, event processing,
   eviction, or shutdown. Its next delivered observation reports a gap.
10. Closing a browser connection releases its subscription and bounded
    buffers. Repeated reconnects do not leak goroutines.
11. Durable input history is read through `Log`, is labeled persisted, and is
    still available when no live trace exists. Ephemeral actors acquire no
    log entries from inspection.
12. The definition view distinguishes pinned and current revisions. Missing
    pinned artifacts never fall back to the current definition.
13. Event sending is unavailable without an authorization function. An
    authorized send to an actor adopted by the process calls `System.Tell`
    once, is audited, and produces the same persistence, hydration, and
    transition behavior as an application send. A storage-only actor remains
    unaddressable until the application adopts it.
14. Redacted values, not originals, are stored in the inspector's bounded
    ring and returned by point-in-time and history endpoints.
15. The embedded UI reconnects after an SSE interruption, handles an expired
    cursor as a visible gap, refreshes current state, and never repeats a
    command.
16. The inspector works against an in-memory, non-durable actor system with no
    SQLite dependency.
17. The Go race suite covers concurrent listing, inspection, event delivery,
    termination, hydration, subscription, and disconnect. Browser interaction
    tests cover filtering, actor selection, trace updates, gaps, event sends,
    and reconnects.

## Out of scope

The following features are deliberately excluded from this MVP:

- arbitrary live datamodel mutation;
- chart editing, publication, revision diffing, collection, or fleet rollout;
- actor stop, restart, eviction, forced hydration, or history deletion;
- historical transition reconstruction by replay;
- forking an actor at a log sequence and testing a different definition;
- pause, step, and breakpoint semantics;
- recording every guard result, expression, or executable operation;
- cross-actor causal graphs and processor-specific delivery panels;
- active network-connection inspection;
- supervision trees and restart controls;
- authentication, authorization policy, or audit storage;
- log aggregation, metrics storage, and distributed tracing;
- actor discovery across processes or nodes.

These features can build on the directory, point-in-time inspection,
definition, durable-history, and bounded-observation contracts without
changing their semantics.

## Consequences

- Operators can discover and inspect actors without changing residency or
  durable state.
- The default Go datamodel and every future datamodel have one explicit
  canonical state-export obligation. Snapshot encoding remains unrelated and
  opaque.
- Adding `Inspect` changes the datamodel session interface. The Go and
  ECMAScript datamodels implement it together, and datamodel conformance tests
  verify independently owned canonical output and non-mutating failures.
- `InstanceInspection` duplicates some fields present in `Snapshot`, but the
  duplication protects two different contracts: inspection is stable public
  state, while a snapshot is a disposable restoration cache.
- Actor storage gains a paginated listing contract. SQLite can implement it
  with indexed keyset queries, while another `database/sql` or in-memory
  implementation retains the same behavior. This changes `ActorStore`; the
  SQLite store, `storagetest.MemoryStore`, counters example, and storage
  conformance suite move to the paginated operation together. The conformance
  suite defines ordering, filtering, cursor, and concurrent-start behavior.
- Live traces add bounded diagnostic work to each observed macrostep. They are
  lossy by design and never become an execution dependency or durable source
  of truth.
- A paged-out actor does not expose a current configuration or datamodel until
  isolated read-only replay exists. The UI presents that absence honestly
  instead of hydrating the actor or trusting a possibly absent cache.
- Sending an event remains the only state-changing operation and passes
  through the same `System.Tell` ingress as application traffic. No inspector
  back door bypasses logging, routing, or actor serialization.
- Applications retain control over network exposure, authorization,
  redaction, auditing, and observability integration.
- The embedded UI dogfoods the same public service API available to another
  frontend. It has no privileged access to runtime or storage internals.
