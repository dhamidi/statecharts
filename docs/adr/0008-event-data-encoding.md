# 8. Event.Data persisted via explicit marshaler interfaces, not gob

Date: 2026-07-13

## Status

Accepted

## Context

`Event.Data` is an arbitrary Go value (there is no expression language or
generic datamodel to introspect, per ADR 0004), so persisting an `Event` —
in a `Snapshot`'s queues/pending-sends, or in a `Log` entry — needs a way to
encode and later reconstruct whatever concrete type a caller put in `Data`.

Two approaches were considered:

1. `encoding/gob` with `gob.Register`, relying on the application
   registering every concrete `Data` type once at startup.
2. An explicit `DataMarshaler`/`DataUnmarshaler` interface pair
   (event_codec.go) plus an explicit `RegisterDataType(typeName, factory)`
   registry, with the wire format itself left up to each `Data` type's own
   implementation (a `JSONData[T]` convenience wrapper is provided via
   `encoding/json` for callers who don't want to hand-write one).

## Decision

Option 2. A `Log` is meant to be a long-lived, debuggable, replay-safe
durability mechanism (ADR 0006) — `gob.Register`'s global mutable
registration state (init-order fragility, awkward when tests run multiple
type universes) and gob's opaque binary wire format (unreadable without
decoding, which hurts the "debug a stuck replay in production" story this
layer exists for) are the wrong trade for that. The explicit interface pair
makes the registry's failure mode a clear, actionable error ("no registered
data type %q") rather than a silent gob decode mismatch, and lets a caller
choose any wire format for their own payload (JSON, protobuf, a hand-rolled
format with an embedded schema version) without the persistence layer
needing to know which.

A `nil` or non-`DataMarshaler` `Data` value is an error when encoding
(`EncodeEvent`, event_codec.go) — there is no reflection-based fallback,
since silently dropping payload data would break replay fidelity.

## Consequences

- Every concrete `Data` type that will ever be persisted must implement
  `DataMarshaler`/`DataUnmarshaler` (or be wrapped in `JSONData[T]`) and be
  registered once via `RegisterDataType` at program startup.
- `Snapshot`'s JSON envelope (`MarshalJSON`/`UnmarshalJSON` in snapshot.go)
  and the `sqllog` subpackage's `data_type`/`data_payload` columns both
  reuse `EncodeEvent`/`DecodeEvent`, so there is exactly one encoding path
  to keep correct and to extend if a new wire format is ever needed.
- Events with `Data == nil` (the common case for pure signal events) encode
  and decode with zero registry involvement.
