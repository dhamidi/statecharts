package statecharts

import (
	"fmt"
)

// Chart is an immutable, validated, indexed chart definition produced by
// Build. It is safe for concurrent use by multiple Instances.
type Chart struct {
	root                  *compiledState
	name                  string
	byID                  map[Identifier]*compiledState
	order                 []*compiledState // document order (pre-order traversal of the state tree)
	program               DatamodelProgram
	programFingerprint    []byte
	canonicalDefinition   []byte
	revision              RevisionID
	definition            Definition
	data                  []compiledData
	dataBinding           DataBinding
	invokesByDefinitionID map[Identifier]*compiledInvoke
}

// Definition returns an independently editable copy of the normalized
// definition used to compile c.
func (c *Chart) Definition() Definition { return c.definition.Clone() }

// DefinitionArtifact returns an immutable-storage representation of c that
// can be recompiled after a process restart. Returned byte slices are owned by
// the caller.
func (c *Chart) DefinitionArtifact() DefinitionArtifact {
	return DefinitionArtifact{
		Revision:                c.revision,
		RevisionEnvelopeVersion: RevisionEnvelopeVersion,
		ChartID:                 c.definition.ID,
		Datamodel:               c.definition.Datamodel,
		CanonicalDefinition:     append([]byte(nil), c.canonicalDefinition...),
		ProgramFingerprint:      append([]byte(nil), c.programFingerprint...),
	}
}

// BuildOption configures the mutable Definition assembled by Build before it
// is validated and compiled. Options must store only definition data.
type BuildOption func(*Definition)

// WithName sets the SCXML document name exposed as _name. It is independent
// of the root state's structural ID.
func WithName(name string) BuildOption { return func(d *Definition) { d.Name = name } }

// WithRevisionSalt adds explicit application-controlled revision material.
// Change it when registered behavior changes without a corresponding function
// version change.
func WithRevisionSalt(salt string) BuildOption {
	return func(d *Definition) { d.RevisionSalt = salt }
}

// WithDataBinding selects early or late initialization for data declarations.
func WithDataBinding(binding DataBinding) BuildOption {
	return func(d *Definition) { d.DataBinding = binding }
}

// WithData appends document-level data declarations.
func WithData(data ...DataDefinition) BuildOption {
	owned := cloneDataDefinitions(data)
	return func(d *Definition) { d.Data = append(d.Data, cloneDataDefinitions(owned)...) }
}

// Revision returns the deterministic identity of this compiled definition and
// datamodel program.
func (c *Chart) Revision() RevisionID { return c.revision }

// ID returns the chart's root state's ID, which identifies the chart itself
// wherever a chart-level identity is needed. A Chart is otherwise
// anonymous -- only its states have names.
func (c *Chart) ID() Identifier {
	return c.root.id
}

// Name returns the SCXML document's name attribute, or empty if omitted.
func (c *Chart) Name() string { return c.name }

// DatamodelProgram returns the immutable model program shared by this chart's
// instances.
func (c *Chart) DatamodelProgram() DatamodelProgram { return c.program }

type compiledState struct {
	id           Identifier
	kind         StateKind
	historyKind  HistoryKind
	initial      *compiledTransition // compound initial or history default transition
	parent       *compiledState
	children     []*compiledState // document order
	onEntry      []actionBlock
	onExit       []actionBlock
	transitions  []*compiledTransition
	invokes      []*compiledInvoke
	modelPayload *compiledPayload
	data         []compiledData
	docOrder     int
}

type compiledTransition struct {
	events            []Identifier
	target            []Identifier
	modelCondition    CompiledExpression
	hasModelCondition bool
	actions           []actionBlock
	internal          bool
	source            *compiledState
}

// States returns every state's ID in document order.
func (c *Chart) States() []Identifier {
	ids := make([]Identifier, len(c.order))
	for i, cs := range c.order {
		ids[i] = cs.id
	}
	return ids
}

// Build assembles a canonical Go-authored Definition and sends it through the
// same compiler used by decoded or edited definitions.
func Build(root StateDefinition, model Datamodel, opts ...BuildOption) (*Chart, error) {
	if model == nil {
		return nil, fmt.Errorf("statecharts: nil datamodel")
	}
	id := root.ID.Value
	if id == "" {
		id = "chart"
	}
	d := Definition{ID: id, Datamodel: model.Name(), Root: root.clone()}
	for _, opt := range opts {
		if opt != nil {
			opt(&d)
		}
	}
	return Compile(d, model)
}

func validateEventDescriptor(id Identifier) error {
	if _, err := NewIdentifier(string(id)); err != nil {
		return fmt.Errorf("statecharts: invalid event descriptor %q: %w", id, err)
	}
	return nil
}
