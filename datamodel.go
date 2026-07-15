package statecharts

// Datamodel compiles syntax-neutral definitions for one model implementation.
// Compile must not mutate definition. It returns an immutable program only
// after every model-owned expression has been validated and compiled.
type Datamodel interface {
	Name() Identifier
	Compile(definition *Definition) (DatamodelProgram, error)
}

// DatamodelProgram is immutable compiled model state shared safely by every
// instance of one chart revision.
type DatamodelProgram interface {
	// Fingerprint returns deterministic implementation identity used as chart
	// revision material. Callers must treat the returned bytes as immutable.
	Fingerprint() []byte
	// ResolveExpression returns the immutable handle compiled for expression.
	// It rejects expressions that were not part of the compiled Definition.
	ResolveExpression(Expression) (CompiledExpression, error)
	// ResolveFunction returns the immutable executable handle compiled for a
	// named host-function reference in the Definition.
	ResolveFunction(FunctionRef) (CompiledExpression, error)
	// ResolveDataLocation returns an assignable handle for a declared data ID.
	// This lets the syntax-neutral chart compiler implement ordered early and
	// late initialization and foreach bindings without knowing the model.
	ResolveDataLocation(Identifier) (CompiledExpression, error)
	// NewSession creates fresh mutable model state owned by one Instance.
	NewSession(SessionOptions) (DatamodelSession, error)
}

// SessionOptions configures creation of one datamodel session. Interpreter
// environment such as the current event, configuration, identity, and
// platform values is deliberately supplied per operation through ExecContext
// rather than cached here.
type SessionOptions struct{}

// CompiledExpression is an opaque immutable handle produced by a
// DatamodelProgram. The interpreter passes it back to that program's sessions
// without inspecting its concrete representation.
type CompiledExpression any

// IterationBindings identifies model-owned locations receiving the current
// item and optional index during ForEach.
type IterationBindings struct {
	Item  CompiledExpression
	Index CompiledExpression
}

// DatamodelSession owns exactly one Instance's mutable model state and any
// model runtime resources. Methods return ordinary Go errors; only the
// interpreter translates them into statechart error events and abort rules.
// Implementations are single-owner and need not be safe for concurrent use.
type DatamodelSession interface {
	EvaluateBoolean(ExecContext, CompiledExpression) (bool, error)
	EvaluateValue(ExecContext, CompiledExpression) (Value, error)
	Assign(ExecContext, CompiledExpression, Value) error
	Execute(ExecContext, CompiledExpression) error
	ForEach(ExecContext, CompiledExpression, IterationBindings, func() error) error

	// EncodeSnapshot returns opaque, model-owned cache bytes.
	EncodeSnapshot() ([]byte, error)
	// DecodeSnapshot atomically replaces this fresh session's restorable model
	// state. A failed session is closed and discarded rather than reused.
	DecodeSnapshot([]byte) error
	// Close releases model resources. Instance calls it exactly once.
	Close() error
}
