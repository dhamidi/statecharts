// Package statecharts implements statechart semantics with a Go-first
// authoring API.
//
// Atomic, Compound, Parallel, Final, History, On, and Eventless construct a
// mutable, syntax-neutral Definition. Build validates that definition and
// compiles it with a Datamodel into an immutable *Chart. Compile accepts the
// same Definition directly, so a definition obtained from Chart.Definition
// can be encoded, edited, decoded, and compiled through exactly the same path.
// Chart.Definition always returns an independently editable deep copy.
//
// GoModel is the default datamodel. Applications register typed actions,
// conditions, value producers, and locations under explicit names and
// versions. Builder nodes store those stable references rather than Go
// function values, making the complete definition deterministic and
// inspectable while each running Instance still owns ordinary typed Go data.
// Other datamodels can implement Datamodel, DatamodelProgram, and
// DatamodelSession without changing the interpreter.
//
// An Instance processes events serially, selecting transitions, resolving
// conflicts between parallel regions, and entering and exiting states in
// document order. Send, Stop, Wait, and Configuration provide its lifecycle
// API. ExecContext supplies registered model functions with the current event,
// active-state predicate, session metadata, and controlled raise, send,
// cancel, and logging capabilities.
//
// Communication outside an Instance goes through IOProcessor. Longer-lived
// services are declared with Invoke and supplied at instance creation as
// environment-scoped InvokeHandler factories; handler capabilities never
// enter a Definition or model snapshot. Logger is the separate diagnostic
// output seam.
//
// Instance.Snapshot captures a disposable restoration cache. Chart.Restore
// reconstructs from that cache, while Chart.Rehydrate rebuilds authoritative
// state from a Log and uses a compatible snapshot only to skip older replay.
// The sqllog package provides database/sql-backed durable storage, and the
// optional sqllog/sqlite3 package supplies a configured SQLite driver.
package statecharts
