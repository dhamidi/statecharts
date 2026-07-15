package ecmascript

import (
	"fmt"
	"time"

	"github.com/dhamidi/statecharts"
)

const defaultEvaluationTimeout = time.Second

type config struct {
	evaluationTimeout time.Duration
	memoryLimit       uintptr
	stackLimit        uintptr
	gcThreshold       uintptr
	interrupt         <-chan struct{}
}

// Option configures the resource policy of every session created by a Model.
type Option func(*config) error

// WithEvaluationTimeout limits each evaluation and synchronous pending-job
// drain. Zero disables the timeout. The default is one second.
func WithEvaluationTimeout(timeout time.Duration) Option {
	return func(config *config) error {
		if timeout < 0 {
			return fmt.Errorf("ecmascript: evaluation timeout cannot be negative")
		}
		config.evaluationTimeout = timeout
		return nil
	}
}

// WithMemoryLimit sets the QuickJS runtime memory limit in bytes. Zero leaves
// the engine default in place.
func WithMemoryLimit(bytes uintptr) Option {
	return func(config *config) error {
		config.memoryLimit = bytes
		return nil
	}
}

// WithStackLimit sets the QuickJS call-depth limit in engine stack slots.
// Zero leaves the engine default in place.
func WithStackLimit(slots uintptr) Option {
	return func(config *config) error {
		config.stackLimit = slots
		return nil
	}
}

// WithGCThreshold sets the number of allocated bytes at which QuickJS runs
// garbage collection. Zero leaves the engine default in place.
func WithGCThreshold(bytes uintptr) Option {
	return func(config *config) error {
		config.gcThreshold = bytes
		return nil
	}
}

// WithInterrupt arranges for a session created by the model to be permanently
// interrupted when signal becomes readable or is closed. Close signal to
// broadcast shutdown to every session; one sent value wakes one waiter.
// Evaluation timeout remains the usual per-operation guard.
func WithInterrupt(signal <-chan struct{}) Option {
	return func(config *config) error {
		config.interrupt = signal
		return nil
	}
}

// Model compiles syntax-neutral Definitions containing package-owned source
// expressions into isolated QuickJS sessions.
type Model struct {
	config config
}

// New creates an ECMAScript datamodel. Root users that do not import this
// package do not link QuickJS.
func New(options ...Option) (*Model, error) {
	configuration := config{evaluationTimeout: defaultEvaluationTimeout}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&configuration); err != nil {
			return nil, err
		}
	}
	return &Model{config: configuration}, nil
}

// Name implements statecharts.Datamodel.
func (*Model) Name() statecharts.Identifier { return "ecmascript" }

// Compile implements statecharts.Datamodel.
func (model *Model) Compile(definition *statecharts.Definition) (statecharts.DatamodelProgram, error) {
	if model == nil {
		return nil, fmt.Errorf("ecmascript: nil model")
	}
	return compileDefinition(definition, model.config)
}
