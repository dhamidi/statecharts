package statecharts

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// Chart is an immutable, validated, indexed chart definition produced by
// Build. It is safe for concurrent use by multiple Instances.
type Chart struct {
	root         *compiledState
	name         string
	byID         map[Identifier]*compiledState
	order        []*compiledState // document order (pre-order traversal of the state tree)
	newDatamodel func() any
	version      string
	codec        DatamodelCodec
}

// DatamodelCodec serializes the opaque chart datamodel. Decode must return a
// fresh value shaped like prototype and must not mutate prototype.
type DatamodelCodec interface {
	Encode(any) ([]byte, error)
	Decode([]byte, any) (any, error)
}

type jsonDatamodelCodec struct{}

func (jsonDatamodelCodec) Encode(v any) ([]byte, error) { return json.Marshal(v) }
func (jsonDatamodelCodec) Decode(b []byte, prototype any) (any, error) {
	if prototype == nil {
		var v any
		err := json.Unmarshal(b, &v)
		return v, err
	}
	t := reflect.TypeOf(prototype)
	var v any
	if t.Kind() == reflect.Pointer {
		v = reflect.New(t.Elem()).Interface()
	} else {
		v = reflect.New(t).Interface()
	}
	if err := json.Unmarshal(b, v); err != nil {
		return nil, err
	}
	if t.Kind() != reflect.Pointer {
		v = reflect.ValueOf(v).Elem().Interface()
	}
	return v, nil
}

// BuildOption configures a Chart being built by Build.
type BuildOption func(*Chart)

// WithName sets the SCXML document name exposed as _name. It is independent
// of the root state's structural ID.
func WithName(name string) BuildOption { return func(c *Chart) { c.name = name } }

// WithNewDatamodel gives chart a way to produce a fresh datamodel value on
// its own, for callers that build Instances of chart without constructing a
// datamodel themselves. The actors package depends on this to page a
// chart's instances into and out of memory on demand, reconstructing each
// one's datamodel from scratch as it pages back in.
func WithNewDatamodel(fn func() any) BuildOption {
	return func(c *Chart) { c.newDatamodel = fn }
}

// WithVersion assigns the opaque application version used to validate
// snapshot caches. Change it whenever chart/reducer behavior or datamodel
// compatibility changes; Rehydrate then ignores the old snapshot and
// rebuilds state from the authoritative Log.
func WithVersion(version string) BuildOption { return func(c *Chart) { c.version = version } }

// WithDatamodelCodec overrides the default JSON datamodel codec used only
// for transparent snapshot caches. The Log remains the source of truth.
func WithDatamodelCodec(codec DatamodelCodec) BuildOption { return func(c *Chart) { c.codec = codec } }

// Version returns the chart's opaque application version.
func (c *Chart) Version() string { return c.version }

// ID returns the chart's root state's ID, which identifies the chart itself
// wherever a chart-level identity is needed. A Chart is otherwise
// anonymous -- only its states have names.
func (c *Chart) ID() Identifier {
	return c.root.id
}

// Name returns the SCXML document's name attribute, or empty if omitted.
func (c *Chart) Name() string { return c.name }

// NewDatamodel calls the factory registered with WithNewDatamodel, if one
// was. ok is false if none was.
func (c *Chart) NewDatamodel() (v any, ok bool) {
	if c.newDatamodel == nil {
		return nil, false
	}
	return c.newDatamodel(), true
}

func (c *Chart) freshDatamodel(prototype any) any {
	if v, ok := c.NewDatamodel(); ok {
		return v
	}
	if prototype == nil {
		return nil
	}
	t := reflect.TypeOf(prototype)
	if t.Kind() == reflect.Pointer {
		return reflect.New(t.Elem()).Interface()
	}
	return reflect.New(t).Elem().Interface()
}

type compiledState struct {
	id          Identifier
	kind        StateKind
	historyKind HistoryKind
	initial     *compiledTransition // compound initial or history default transition
	parent      *compiledState
	children    []*compiledState // document order
	onEntry     []actionBlock
	onExit      []actionBlock
	transitions []*compiledTransition
	invokes     []*compiledInvoke
	done        DoneDataFunc
	docOrder    int
}

type compiledTransition struct {
	events   []Identifier
	target   []Identifier
	cond     CondFunc
	actions  []actionBlock
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
	c := &Chart{byID: make(map[Identifier]*compiledState), codec: jsonDatamodelCodec{}}
	explicit := make(map[Identifier]bool)
	var collect func(StateSpec)
	collect = func(s StateSpec) {
		if s.ID != "" {
			explicit[s.ID] = true
		}
		for _, child := range s.Children {
			collect(child)
		}
	}
	collect(root)
	generated := 0
	var assign func(StateSpec) StateSpec
	assign = func(s StateSpec) StateSpec {
		if s.ID == "" {
			for {
				generated++
				candidate := Identifier(fmt.Sprintf("state.%d", generated))
				if !explicit[candidate] {
					s.ID = candidate
					explicit[candidate] = true
					break
				}
			}
		}
		children := make([]StateSpec, len(s.Children))
		for i, child := range s.Children {
			children[i] = assign(child)
		}
		s.Children = children
		return s
	}
	root = assign(root)
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
	if err := validateStateID(spec.ID); err != nil {
		return nil, err
	}
	if spec.Kind > KindHistory {
		return nil, fmt.Errorf("statecharts: state %q has invalid kind %d", spec.ID, spec.Kind)
	}
	if spec.Kind == KindHistory && spec.HistoryKind > Deep {
		return nil, fmt.Errorf("statecharts: history state %q has invalid history kind %d", spec.ID, spec.HistoryKind)
	}
	if _, exists := c.byID[spec.ID]; exists {
		return nil, fmt.Errorf("statecharts: duplicate state ID %q", spec.ID)
	}
	onEntry, err := reconcileActionBlocks(spec.OnEntry, spec.onEntryBlocks)
	if err != nil {
		return nil, fmt.Errorf("statecharts: state %q onentry: %w", spec.ID, err)
	}
	onExit, err := reconcileActionBlocks(spec.OnExit, spec.onExitBlocks)
	if err != nil {
		return nil, fmt.Errorf("statecharts: state %q onexit: %w", spec.ID, err)
	}

	var initial *compiledTransition
	if spec.DefaultTransition != nil || spec.Initial != "" {
		defaultSpec := TransitionSpec{}
		if spec.DefaultTransition != nil {
			defaultSpec = *spec.DefaultTransition
		}
		if spec.Initial != "" {
			defaultSpec.Target = append([]Identifier{spec.Initial}, defaultSpec.Target...)
		}
		actions, err := reconcileActionBlocks(defaultSpec.Actions, defaultSpec.actionBlocks)
		if err != nil {
			return nil, fmt.Errorf("statecharts: state %q default transition actions: %w", spec.ID, err)
		}
		initial = &compiledTransition{events: defaultSpec.Events, target: defaultSpec.Target, cond: defaultSpec.Cond, actions: actions, internal: defaultSpec.Internal}
	}
	cs := &compiledState{
		id:          spec.ID,
		kind:        spec.Kind,
		historyKind: spec.HistoryKind,
		initial:     initial,
		parent:      parent,
		onEntry:     onEntry,
		onExit:      onExit,
		done:        spec.Done,
		docOrder:    *counter,
	}
	if initial != nil {
		initial.source = cs
	}
	*counter++
	c.byID[spec.ID] = cs
	c.order = append(c.order, cs)

	for _, t := range spec.Transitions {
		actions, err := reconcileActionBlocks(t.Actions, t.actionBlocks)
		if err != nil {
			return nil, fmt.Errorf("statecharts: state %q transition actions: %w", spec.ID, err)
		}
		cs.transitions = append(cs.transitions, &compiledTransition{
			events:   t.Events,
			target:   t.Target,
			cond:     t.Cond,
			actions:  actions,
			internal: t.Internal,
			source:   cs,
		})
	}

	for _, inv := range spec.Invokes {
		finalize, err := reconcileActionBlocks(inv.Finalize, inv.finalizeBlocks)
		if err != nil {
			return nil, fmt.Errorf("statecharts: state %q invoke finalize: %w", spec.ID, err)
		}
		cs.invokes = append(cs.invokes, &compiledInvoke{
			id:          inv.ID,
			start:       inv.Start,
			params:      inv.Params,
			finalize:    finalize,
			autoForward: inv.AutoForward,
			resume:      inv.Resume,
			idLocation:  inv.IDLocation,
		})
	}

	for _, childSpec := range spec.Children {
		child, err := compileState(c, childSpec, cs, counter)
		if err != nil {
			return nil, err
		}
		cs.children = append(cs.children, child)
	}
	if cs.kind == KindCompound && cs.initial == nil {
		if children := realChildren(cs); len(children) > 0 {
			cs.initial = &compiledTransition{target: []Identifier{children[0].id}, source: cs}
		}
	}
	return cs, nil
}

// reconcileActionBlocks keeps the exported action slices authoritative while
// retaining the block boundaries recorded by builder options. Actions added
// directly to a public slice after using a builder option form one additional
// block instead of being silently dropped.
func reconcileActionBlocks(actions []ActionFunc, recorded []actionBlock) ([]actionBlock, error) {
	if len(recorded) == 0 {
		if len(actions) == 0 {
			return nil, nil
		}
		return []actionBlock{append(actionBlock(nil), actions...)}, nil
	}
	blocks := make([]actionBlock, 0, len(recorded)+1)
	offset := 0
	for _, block := range recorded {
		end := offset + len(block)
		if end > len(actions) {
			return nil, fmt.Errorf("public action list was shortened after builder options recorded its block boundaries")
		}
		blocks = append(blocks, append(actionBlock(nil), actions[offset:end]...))
		offset = end
	}
	if offset < len(actions) {
		blocks = append(blocks, append(actionBlock(nil), actions[offset:]...))
	}
	return blocks, nil
}

func validateStateID(id Identifier) error {
	if _, err := NewIdentifier(string(id)); err != nil {
		return fmt.Errorf("statecharts: invalid state ID %q: %w", id, err)
	}
	s := string(id)
	if strings.HasPrefix(s, "#") || s == "*" || strings.HasSuffix(s, ".") || strings.HasSuffix(s, ".*") {
		return fmt.Errorf("statecharts: invalid state ID %q", id)
	}
	return nil
}

func validateEventDescriptor(id Identifier) error {
	if _, err := NewIdentifier(string(id)); err != nil {
		return fmt.Errorf("statecharts: invalid event descriptor %q: %w", id, err)
	}
	return nil
}

func (c *Chart) validateReferences() error {
	explicitInvokeIDs := map[Identifier]Identifier{}
	for _, cs := range c.order {
		if cs.kind != KindFinal && cs.done != nil {
			return fmt.Errorf("statecharts: non-final state %q must not have done data", cs.id)
		}
		if cs == c.root && (cs.kind == KindCompound || cs.kind == KindParallel) {
			if len(cs.onEntry) > 0 || len(cs.onExit) > 0 || len(cs.invokes) > 0 {
				return fmt.Errorf("statecharts: document root %q must not have onentry, onexit, or invoke content", cs.id)
			}
		}
		switch cs.kind {
		case KindCompound:
			if len(realChildren(cs)) == 0 {
				return fmt.Errorf("statecharts: compound state %q has no children", cs.id)
			}
			if cs.initial == nil || len(cs.initial.target) == 0 {
				return fmt.Errorf("statecharts: compound state %q has no initial child", cs.id)
			}
			// SCXML 3.11 requires only that the target of a state's
			// 'initial' attribute be a descendant of that state, not a
			// direct child -- entry fills in every intervening ancestor
			// (interpreter.go's addAncestorStatesToEnter), so a deeper
			// target is entered correctly.
			for _, id := range cs.initial.target {
				child, ok := c.byID[id]
				if !ok || !isDescendant(child, cs) {
					return fmt.Errorf("statecharts: compound state %q initial %q is not a descendant of it", cs.id, id)
				}
			}
			if err := validateDefaultTransition(cs); err != nil {
				return err
			}
			if cs == c.root && (len(cs.initial.actions) > 0 || cs.initial.internal) {
				return fmt.Errorf("statecharts: document root %q initial attribute may only specify targets", cs.id)
			}
			if err := c.validateStateSpecification(cs.initial.target); err != nil {
				return fmt.Errorf("statecharts: compound state %q has invalid initial target: %w", cs.id, err)
			}
		case KindParallel:
			if cs.initial != nil {
				return fmt.Errorf("statecharts: parallel state %q must not have a default transition", cs.id)
			}
			if len(realChildren(cs)) == 0 {
				return fmt.Errorf("statecharts: parallel state %q has no children", cs.id)
			}
			for _, child := range realChildren(cs) {
				if child.kind == KindFinal {
					return fmt.Errorf("statecharts: parallel state %q must not contain final child %q", cs.id, child.id)
				}
			}
		case KindAtomic, KindFinal:
			if cs.initial != nil {
				return fmt.Errorf("statecharts: %s state %q must not have a default transition", cs.kind, cs.id)
			}
			if len(cs.children) != 0 {
				return fmt.Errorf("statecharts: %s state %q must not have children", cs.kind, cs.id)
			}
			if cs.kind == KindFinal && (len(cs.transitions) > 0 || len(cs.invokes) > 0) {
				return fmt.Errorf("statecharts: final state %q must not have transitions or invokes", cs.id)
			}
		case KindHistory:
			if len(cs.children) != 0 {
				return fmt.Errorf("statecharts: history state %q must not have children", cs.id)
			}
			if cs.parent == nil {
				return fmt.Errorf("statecharts: history state %q must have a parent", cs.id)
			}
			if cs.initial == nil || len(cs.initial.target) == 0 {
				return fmt.Errorf("statecharts: history state %q has no default target", cs.id)
			}
			if len(cs.transitions) > 0 || len(cs.invokes) > 0 || len(cs.onEntry) > 0 || len(cs.onExit) > 0 || cs.done != nil {
				return fmt.Errorf("statecharts: history state %q contains unsupported state content", cs.id)
			}
			if err := validateDefaultTransition(cs); err != nil {
				return err
			}
			for _, id := range cs.initial.target {
				target := c.byID[id]
				if target == nil {
					return fmt.Errorf("statecharts: history state %q default target %q does not exist", cs.id, id)
				}
				if !isDescendant(target, cs.parent) {
					return fmt.Errorf("statecharts: history state %q default target %q is outside parent %q", cs.id, target.id, cs.parent.id)
				}
				if cs.historyKind == Shallow && target.parent != cs.parent {
					return fmt.Errorf("statecharts: shallow history state %q default target %q is not an immediate child", cs.id, target.id)
				}
			}
			if err := c.validateStateSpecification(cs.initial.target); err != nil {
				return fmt.Errorf("statecharts: history state %q has invalid default target: %w", cs.id, err)
			}
		}

		for _, t := range cs.transitions {
			if len(t.events) == 0 && t.cond == nil && len(t.target) == 0 {
				return fmt.Errorf("statecharts: state %q has transition without event, condition, or target", cs.id)
			}
			for _, event := range t.events {
				if err := validateEventDescriptor(event); err != nil {
					return fmt.Errorf("statecharts: state %q: %w", cs.id, err)
				}
			}
			for _, target := range t.target {
				if _, ok := c.byID[target]; !ok {
					return fmt.Errorf("statecharts: state %q transition targets unresolved state %q", cs.id, target)
				}
			}
			if err := c.validateStateSpecification(t.target); err != nil {
				return fmt.Errorf("statecharts: state %q has invalid transition target: %w", cs.id, err)
			}
		}
		for _, inv := range cs.invokes {
			if inv.id != "" && inv.idLocation != nil {
				return fmt.Errorf("statecharts: state %q invoke id and idlocation are mutually exclusive", cs.id)
			}
			if inv.start == nil {
				return fmt.Errorf("statecharts: state %q has invoke without a start function", cs.id)
			}
			if inv.id == "" {
				continue
			}
			if err := validateStateID(inv.id); err != nil {
				return fmt.Errorf("statecharts: state %q has invalid invoke ID: %w", cs.id, err)
			}
			if owner, exists := explicitInvokeIDs[inv.id]; exists {
				return fmt.Errorf("statecharts: duplicate invoke ID %q on states %q and %q", inv.id, owner, cs.id)
			}
			explicitInvokeIDs[inv.id] = cs.id
		}
	}
	return nil
}

func validateDefaultTransition(s *compiledState) error {
	if len(s.initial.events) != 0 || s.initial.cond != nil {
		return fmt.Errorf("statecharts: state %q default transition must be eventless and unconditional", s.id)
	}
	return nil
}

func (c *Chart) validateStateSpecification(ids []Identifier) error {
	if len(ids) == 0 {
		return nil
	}
	states := make([]*compiledState, 0, len(ids))
	seen := map[*compiledState]bool{}
	for _, id := range ids {
		s := c.byID[id]
		if s == nil {
			continue // unresolved references receive the more specific caller error
		}
		if seen[s] {
			return fmt.Errorf("state %q occurs more than once", id)
		}
		seen[s] = true
		states = append(states, s)
	}
	for i, a := range states {
		for _, b := range states[i+1:] {
			if isDescendant(a, b) || isDescendant(b, a) {
				return fmt.Errorf("states %q and %q have an ancestor/descendant relationship", a.id, b.id)
			}
		}
	}

	atomSet := map[*compiledState]bool{}
	for _, s := range states {
		if err := c.collectDefaultAtomicStates(s, atomSet, map[*compiledState]bool{}); err != nil {
			return err
		}
	}
	atoms := make([]*compiledState, 0, len(atomSet))
	for atom := range atomSet {
		atoms = append(atoms, atom)
	}
	if len(atoms) == 0 {
		return fmt.Errorf("target does not expand to an atomic state")
	}
	for i, a := range atoms {
		for _, b := range atoms[i+1:] {
			lca := leastCommonAncestor(a, b)
			if lca == nil || lca.kind != KindParallel {
				return fmt.Errorf("states %q and %q cannot be active together", a.id, b.id)
			}
		}
	}
	return nil
}

func (c *Chart) collectDefaultAtomicStates(s *compiledState, atoms, visiting map[*compiledState]bool) error {
	if visiting[s] {
		return fmt.Errorf("default target cycle through state %q", s.id)
	}
	if isAtomicKind(s) {
		atoms[s] = true
		return nil
	}
	visiting[s] = true
	defer delete(visiting, s)
	switch s.kind {
	case KindHistory, KindCompound:
		if s.initial == nil || len(s.initial.target) == 0 {
			return fmt.Errorf("state %q has no default target", s.id)
		}
		for _, id := range s.initial.target {
			target := c.byID[id]
			if target == nil {
				return fmt.Errorf("state %q has unresolved default target %q", s.id, id)
			}
			if err := c.collectDefaultAtomicStates(target, atoms, visiting); err != nil {
				return err
			}
		}
		return nil
	case KindParallel:
		for _, child := range realChildren(s) {
			if err := c.collectDefaultAtomicStates(child, atoms, visiting); err != nil {
				return err
			}
		}
	}
	return nil
}

func leastCommonAncestor(a, b *compiledState) *compiledState {
	ancestors := map[*compiledState]bool{}
	for s := a; s != nil; s = s.parent {
		ancestors[s] = true
	}
	for s := b; s != nil; s = s.parent {
		if ancestors[s] {
			return s
		}
	}
	return nil
}
