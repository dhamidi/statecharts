package statecharts

import "fmt"

// Chart is an immutable, validated, indexed chart definition produced by
// Build. It is safe for concurrent use by multiple Instances.
type Chart struct {
	root         *compiledState
	byID         map[Identifier]*compiledState
	order        []*compiledState // document order (pre-order traversal of the state tree)
	newDatamodel func() any
}

// BuildOption configures a Chart being built by Build.
type BuildOption func(*Chart)

// WithNewDatamodel gives chart a way to produce a fresh datamodel value on
// its own, for callers that build Instances of chart without constructing a
// datamodel themselves. The actors package depends on this to page a
// chart's instances into and out of memory on demand, reconstructing each
// one's datamodel from scratch as it pages back in.
func WithNewDatamodel(fn func() any) BuildOption {
	return func(c *Chart) { c.newDatamodel = fn }
}

// ID returns the chart's root state's ID, which identifies the chart itself
// wherever a chart-level identity is needed. A Chart is otherwise
// anonymous -- only its states have names.
func (c *Chart) ID() Identifier {
	return c.root.id
}

// NewDatamodel calls the factory registered with WithNewDatamodel, if one
// was. ok is false if none was.
func (c *Chart) NewDatamodel() (v any, ok bool) {
	if c.newDatamodel == nil {
		return nil, false
	}
	return c.newDatamodel(), true
}

type compiledState struct {
	id          Identifier
	kind        StateKind
	historyKind HistoryKind
	initial     Identifier // compound: default child id. history: default target id.
	parent      *compiledState
	children    []*compiledState // document order
	onEntry     []ActionFunc
	onExit      []ActionFunc
	transitions []*compiledTransition
	invokes     []*compiledInvoke
	done        DoneDataFunc
	docOrder    int
}

type compiledTransition struct {
	events   []Identifier
	target   []Identifier
	cond     CondFunc
	actions  []ActionFunc
	internal bool
	source   *compiledState
}

// States returns every state's ID in document order.
func (c *Chart) States() []Identifier {
	ids := make([]Identifier, len(c.order))
	for i, cs := range c.order {
		ids[i] = cs.id
	}
	return ids
}

// Build compiles root and its descendants into a Chart, resolving and
// validating every Initial/Target/history-default Identifier reference.
// Errors are static, discovered here rather than at interpretation time.
func Build(root StateSpec, opts ...BuildOption) (*Chart, error) {
	c := &Chart{byID: make(map[Identifier]*compiledState)}
	docOrder := 0
	compiled, err := compileState(c, root, nil, &docOrder)
	if err != nil {
		return nil, err
	}
	c.root = compiled
	if err := c.validateReferences(); err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

func compileState(c *Chart, spec StateSpec, parent *compiledState, counter *int) (*compiledState, error) {
	if spec.ID == "" {
		return nil, fmt.Errorf("statecharts: state has empty ID")
	}
	if _, exists := c.byID[spec.ID]; exists {
		return nil, fmt.Errorf("statecharts: duplicate state ID %q", spec.ID)
	}

	cs := &compiledState{
		id:          spec.ID,
		kind:        spec.Kind,
		historyKind: spec.HistoryKind,
		initial:     spec.Initial,
		parent:      parent,
		onEntry:     spec.OnEntry,
		onExit:      spec.OnExit,
		done:        spec.Done,
		docOrder:    *counter,
	}
	*counter++
	c.byID[spec.ID] = cs
	c.order = append(c.order, cs)

	for _, t := range spec.Transitions {
		cs.transitions = append(cs.transitions, &compiledTransition{
			events:   t.Events,
			target:   t.Target,
			cond:     t.Cond,
			actions:  t.Actions,
			internal: t.Internal,
			source:   cs,
		})
	}

	for _, inv := range spec.Invokes {
		cs.invokes = append(cs.invokes, &compiledInvoke{
			id:          inv.ID,
			start:       inv.Start,
			params:      inv.Params,
			finalize:    inv.Finalize,
			autoForward: inv.AutoForward,
		})
	}

	for _, childSpec := range spec.Children {
		child, err := compileState(c, childSpec, cs, counter)
		if err != nil {
			return nil, err
		}
		cs.children = append(cs.children, child)
	}
	return cs, nil
}

func (c *Chart) validateReferences() error {
	for _, cs := range c.order {
		switch cs.kind {
		case KindCompound:
			if len(cs.children) == 0 {
				return fmt.Errorf("statecharts: compound state %q has no children", cs.id)
			}
			if cs.initial == "" {
				return fmt.Errorf("statecharts: compound state %q has no initial child", cs.id)
			}
			// SCXML 3.11 requires only that the target of a state's
			// 'initial' attribute be a descendant of that state, not a
			// direct child -- entry fills in every intervening ancestor
			// (interpreter.go's addAncestorStatesToEnter), so a deeper
			// target is entered correctly.
			child, ok := c.byID[cs.initial]
			if !ok || !isDescendant(child, cs) {
				return fmt.Errorf("statecharts: compound state %q initial %q is not a descendant of it", cs.id, cs.initial)
			}
		case KindParallel:
			if len(cs.children) == 0 {
				return fmt.Errorf("statecharts: parallel state %q has no children", cs.id)
			}
		case KindAtomic, KindFinal:
			if len(cs.children) != 0 {
				return fmt.Errorf("statecharts: %s state %q must not have children", cs.kind, cs.id)
			}
		case KindHistory:
			if len(cs.children) != 0 {
				return fmt.Errorf("statecharts: history state %q must not have children", cs.id)
			}
			if cs.parent == nil {
				return fmt.Errorf("statecharts: history state %q must have a parent", cs.id)
			}
			if cs.initial == "" {
				return fmt.Errorf("statecharts: history state %q has no default target", cs.id)
			}
			if _, ok := c.byID[cs.initial]; !ok {
				return fmt.Errorf("statecharts: history state %q default target %q does not exist", cs.id, cs.initial)
			}
		}

		for _, t := range cs.transitions {
			for _, target := range t.target {
				if _, ok := c.byID[target]; !ok {
					return fmt.Errorf("statecharts: state %q transition targets unresolved state %q", cs.id, target)
				}
			}
		}
	}
	return nil
}
