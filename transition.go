package statecharts

// TransitionSpec is the uncompiled description of one transition, built via
// On/Eventless plus TransitionOptions.
type TransitionSpec struct {
	Events   []Identifier // event descriptors; empty = eventless
	Target   []Identifier // resolved state IDs; empty = targetless (internal-only actions)
	Cond     CondFunc     // nil = always true
	Actions  []ActionFunc
	Internal bool // SCXML transition type="internal"
}

// TransitionOption configures a TransitionSpec being built by On/Eventless.
type TransitionOption func(*TransitionSpec)

// Target sets the transition's target state(s) (more than one for a
// transition that enters multiple parallel regions).
func Target(ids ...Identifier) TransitionOption {
	return func(t *TransitionSpec) { t.Target = append(t.Target, ids...) }
}

// If sets the transition's guard condition.
func If(cond CondFunc) TransitionOption {
	return func(t *TransitionSpec) { t.Cond = cond }
}

// Then attaches executable content run when the transition fires.
func Then(actions ...ActionFunc) TransitionOption {
	return func(t *TransitionSpec) { t.Actions = append(t.Actions, actions...) }
}

// AsInternal marks the transition as SCXML transition type="internal".
func AsInternal() TransitionOption {
	return func(t *TransitionSpec) { t.Internal = true }
}
