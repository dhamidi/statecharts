package statecharts

import "strings"

// StateKind is the kind of node in a chart's state tree.
type StateKind uint8

const (
	KindAtomic StateKind = iota
	KindCompound
	KindParallel
	KindFinal
	KindHistory
)

// String implements fmt.Stringer.
func (k StateKind) String() string {
	switch k {
	case KindAtomic:
		return "atomic"
	case KindCompound:
		return "compound"
	case KindParallel:
		return "parallel"
	case KindFinal:
		return "final"
	case KindHistory:
		return "history"
	default:
		return "unknown"
	}
}

// HistoryKind distinguishes shallow from deep history, per SCXML "history"
// element semantics.
type HistoryKind uint8

const (
	Shallow HistoryKind = iota
	Deep
)

// String implements fmt.Stringer.
func (k HistoryKind) String() string {
	if k == Deep {
		return "deep"
	}
	return "shallow"
}

// StateSpec is the uncompiled description of one node in a chart's state
// tree, built via Atomic/Compound/Parallel/Final/History and StateOptions,
// then compiled once by Build.
type StateSpec struct {
	ID          Identifier
	Kind        StateKind
	Initial     Identifier // compound: default child. history: default target.
	HistoryKind HistoryKind
	OnEntry     []ActionFunc
	OnExit      []ActionFunc
	Transitions []TransitionSpec
	Children    []StateSpec // preserved in call order == SCXML document order
	Done        DoneDataFunc
}

// StateOption configures a StateSpec being built by Atomic/Compound/etc.
type StateOption func(*StateSpec)

// Children attaches child states, in document order.
func Children(children ...StateSpec) StateOption {
	return func(s *StateSpec) { s.Children = append(s.Children, children...) }
}

// OnEntry attaches executable content run when the state is entered.
func OnEntry(actions ...ActionFunc) StateOption {
	return func(s *StateSpec) { s.OnEntry = append(s.OnEntry, actions...) }
}

// OnExit attaches executable content run when the state is exited.
func OnExit(actions ...ActionFunc) StateOption {
	return func(s *StateSpec) { s.OnExit = append(s.OnExit, actions...) }
}

// WithDone sets the done-data callback for a Final state.
func WithDone(fn DoneDataFunc) StateOption {
	return func(s *StateSpec) { s.Done = fn }
}

// On attaches a transition matching the space-separated event descriptors.
func On(events string, opts ...TransitionOption) StateOption {
	return func(s *StateSpec) {
		t := TransitionSpec{Events: parseEventDescriptors(events)}
		for _, opt := range opts {
			opt(&t)
		}
		s.Transitions = append(s.Transitions, t)
	}
}

// Eventless attaches an eventless (automatic) transition, evaluated whenever
// no event is required for it to fire -- only its Cond gates it.
func Eventless(opts ...TransitionOption) StateOption {
	return func(s *StateSpec) {
		t := TransitionSpec{}
		for _, opt := range opts {
			opt(&t)
		}
		s.Transitions = append(s.Transitions, t)
	}
}

func parseEventDescriptors(events string) []Identifier {
	fields := strings.Fields(events)
	ids := make([]Identifier, len(fields))
	for i, f := range fields {
		ids[i] = Identifier(f)
	}
	return ids
}

func newSpec(id Identifier, kind StateKind, opts ...StateOption) StateSpec {
	s := StateSpec{ID: id, Kind: kind}
	for _, opt := range opts {
		opt(&s)
	}
	return s
}

// Atomic declares a leaf state with no children.
func Atomic(id Identifier, opts ...StateOption) StateSpec {
	return newSpec(id, KindAtomic, opts...)
}

// Compound declares a state with children, exactly one of which (initial)
// is entered by default.
func Compound(id Identifier, initial Identifier, opts ...StateOption) StateSpec {
	s := newSpec(id, KindCompound, opts...)
	s.Initial = initial
	return s
}

// Parallel declares a state whose children are all simultaneously active
// (each child is one region).
func Parallel(id Identifier, opts ...StateOption) StateSpec {
	return newSpec(id, KindParallel, opts...)
}

// Final declares a final state.
func Final(id Identifier, opts ...StateOption) StateSpec {
	return newSpec(id, KindFinal, opts...)
}

// History declares a history pseudostate belonging to whichever compound or
// parallel state contains it. defaultTarget is entered when the history has
// never recorded a configuration (i.e. on first entry to the parent).
func History(id Identifier, kind HistoryKind, defaultTarget Identifier) StateSpec {
	return StateSpec{ID: id, Kind: KindHistory, HistoryKind: kind, Initial: defaultTarget}
}
