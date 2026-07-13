// Package statecharts implements the state machine semantics defined by
// the W3C SCXML specification (State Chart XML), without SCXML's XML
// document syntax: charts are built directly in Go.
//
// A chart is a tree of states built with Atomic, Compound, Parallel,
// Final, and History, with transitions attached using On or Eventless.
// Build compiles and validates a tree into a *Chart, which can then be run
// any number of times.
//
// Running a chart produces an Instance: a single goroutine that processes
// events one at a time, applying the SCXML interpretation algorithm --
// selecting transitions, resolving conflicts between simultaneously active
// parallel regions, and entering and exiting states in the correct order.
// Instance's methods (Send, Stop, Wait, Configuration) are plain function
// calls; no channel appears anywhere in the public API.
//
// There is no expression language. Wherever SCXML would evaluate an
// expression against a datamodel -- a transition's condition, or the
// executable content of a transition or an onentry/onexit handler -- this
// package calls a Go function instead. Action and Cond adapt a callback
// written against the chart's own datamodel type into the untyped
// ActionFunc and CondFunc that a chart's tree stores; ExecContext gives
// that callback access to the event being processed, the In() predicate,
// and the ability to raise, send, cancel, or log against the owning
// Instance.
//
// All communication with the world outside a running Instance goes through
// an IOProcessor, isolating every real side effect behind one interface.
// Invoke attaches a longer-lived external service to a state instead: an
// InvokeFunc runs in its own goroutine for as long as the state is active,
// delivering events back through InvokeIO and cancelled automatically if
// the state is exited first. Diagnostic output -- what a chart is doing,
// for a human or a log aggregator to read -- goes through a separate,
// simpler seam, Logger, since it never crosses a session boundary and
// never produces an event a transition could match against.
//
// A running chart's state -- its active configuration, recorded history,
// queued events, and outstanding delayed sends -- can be captured with
// Instance.Snapshot and later restored with Restore. The primary way to
// persist a chart across restarts, though, is a Log: recording every
// message that arrives at an Instance (an application's calls to Send, and
// delayed-send timers firing) is enough to reconstruct its state exactly,
// by feeding those same messages back through a fresh Instance. Rehydrate
// does this, using a Snapshot only as a shortcut so a long-running session
// doesn't need to replay from its very first message. The sqllog
// subpackage provides a database/sql-backed Log.
package statecharts
