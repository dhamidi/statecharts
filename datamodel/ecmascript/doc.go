// Package ecmascript provides an optional ECMAScript datamodel backed by the
// pure-Go modernc QuickJS port.
//
// Definitions remain Go-first and syntax-neutral:
//
//	model, _ := ecmascript.New(ecmascript.WithEvaluationTimeout(100 * time.Millisecond))
//	initial, _ := ecmascript.Source("0")
//	result, _ := ecmascript.Source("count")
//	chart, _ := statecharts.Build(
//		statecharts.Compound("root", "done", statecharts.Children(
//			statecharts.Final("done", statecharts.WithDone(result)),
//		)),
//		model,
//		statecharts.WithData(statecharts.DataDefinition{ID: "count", Expr: &initial}),
//	)
//
// Each chart instance owns one VM. Evaluations and their synchronously queued
// Promise jobs run to completion on the instance goroutine. Promise results
// are not valid statechart values, and no browser, filesystem, network, or
// timer APIs are installed. Use a finite evaluation timeout for untrusted or
// runtime-edited definitions, and set memory and stack limits appropriate to
// the host process.
package ecmascript
