# 8. Event.Data persisted as one canonical Value

Date: 2026-07-13

## Status

Accepted (revised by the pre-v1 payload cutover in issue #9)

## Context

Events cross datamodel, session, actor, process, snapshot, log, and durable
outbox boundaries. Allowing each concrete Go type to choose its own encoding
made those boundaries depend on process-global registration and prevented
other datamodel runtimes from interpreting the same payload.

The canonical `Value` introduced for the serializable-definition architecture
is a closed, syntax-neutral data union: null, boolean, UTF-8 string, exact
decimal number, list, string-keyed map, and an application-tagged value. Its
versioned marshal representation is deterministic and does not require a Go
concrete type to decode.

## Decision

`Event.Data` and every other cross-runtime payload field use `Value` directly.
Durable stores persist only `Value.MarshalBinary` bytes and reconstruct them
with `Value.UnmarshalBinary`. Application payload identities use stable tags
whose payloads are themselves canonical values.

There is one durable codec. There is no process-global concrete-type registry,
reflection fallback, or caller-selected alternate payload encoding. Platform
errors that cross a session or durability boundary are explicit tagged values
with stable classification and message fields rather than raw Go errors.

## Consequences

- Event, send, invoke, log, snapshot, actor bridge, and durable outbox payloads
  share one representation and clone at their semantic boundaries.
- Snapshot event queues and SQL log/outbox rows reuse the same event codec.
- A zero `Value` is canonical null, so signal-only events require no special
  case or registration.
- This was a direct pre-v1 replacement. Persisted payloads and SQL schemas from
  the earlier concrete-type encoding are rejected rather than migrated or read
  through a compatibility path.
