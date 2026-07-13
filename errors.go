package statecharts

// Platform event names placed on the internal queue by the interpreter
// itself, per SCXML 5.10.2 / C.1. These are ordinary event names, not Go
// errors: a chart reacts to them with transitions exactly like any other
// event.
const (
	ErrEventExecution     Identifier = "error.execution"
	ErrEventCommunication Identifier = "error.communication"
)
