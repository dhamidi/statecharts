# 2. Faithful SCXML semantics without SCXML syntax

Date: 2026-07-13

## Status

Accepted

## Context

SCXML (https://www.w3.org/TR/scxml/, vendored at /scxml.html) defines both an
XML document format and the operational semantics of a statechart
interpreter (microstep/macrostep algorithm, event queues, transition
selection and conflict resolution, history states, etc.).

Implementing the XML syntax up front adds parsing, schema validation, and
serialization concerns before the interpreter semantics are even correct.

## Decision

For the initial implementation we will implement the W3C SCXML operational
semantics as a Go library, with charts constructed programmatically (Go
values/builder API) rather than parsed from SCXML documents. The interpreter
behavior (event processing, transition conflict resolution, entry/exit
ordering, history, etc.) must match the SCXML spec 1:1.

XML document parsing may be added later as a separate concern once the
interpreter semantics are validated, but is out of scope for now.

## Consequences

- Faster path to a correct, spec-conformant interpreter core.
- No XML parser/schema validation dependency yet.
- Consumers must build charts via the Go API instead of loading .scxml files,
  until an XML front end is added.
