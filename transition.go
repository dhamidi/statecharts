package statecharts

// TransitionOption appends serializable authoring data to a transition.
type TransitionOption func(*TransitionDefinition)

// Target sets the transition's target state(s) (more than one for a
// transition that enters multiple parallel regions).
func Target(ids ...Identifier) TransitionOption {
	return func(t *TransitionDefinition) { t.Targets = append(t.Targets, ids...) }
}

// If sets the transition's guard condition.
func If(condition Expression) TransitionOption {
	return func(t *TransitionDefinition) {
		value := condition.Clone()
		t.Condition = &value
	}
}

// Then attaches executable content run when the transition fires.
func Then(actions ...Executable) TransitionOption {
	return func(t *TransitionDefinition) {
		t.Actions = append(t.Actions, cloneExecutableBlock(actions))
	}
}

// AsInternal marks the transition as SCXML transition type="internal".
func AsInternal() TransitionOption {
	return func(t *TransitionDefinition) { t.Type = TransitionInternal }
}
