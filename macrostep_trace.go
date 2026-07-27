package statecharts

import "time"

// MacrostepObserver receives diagnostic descriptions of completed live
// macrosteps. Enabled and ObserveMacrostep execute on the Instance's actor
// goroutine and must return promptly. Panics are recovered and ignored.
type MacrostepObserver interface {
	Enabled() bool
	ObserveMacrostep(MacrostepTrace)
}

// MacrostepObserverFunc adapts a function to an always-enabled observer.
type MacrostepObserverFunc func(MacrostepTrace)

func (f MacrostepObserverFunc) Enabled() bool { return true }

func (f MacrostepObserverFunc) ObserveMacrostep(trace MacrostepTrace) { f(trace) }

// MacrostepTrace describes one completed macrostep. All values in a delivered
// trace are independently owned and may be mutated by the observer. Sequence
// starts at one for each live Instance activation and is unrelated to any
// durable log sequence. Timestamp is read from the configured Clock and
// normalized to UTC.
type MacrostepTrace struct {
	Sequence      uint64
	Timestamp     time.Time
	Trigger       *Event
	Before        []Identifier
	Microsteps    []MicrostepTrace
	After         []Identifier
	Terminal      bool
	TerminalError string
}

// MicrostepTrace describes one selected transition set and its state changes.
type MicrostepTrace struct {
	Trigger     *Event
	Transitions []TransitionRef
	Exited      []Identifier
	Entered     []Identifier
}

// TransitionRef identifies a transition in the pinned immutable definition.
// Index is zero-based within Source's StateDefinition.Transitions.
type TransitionRef struct {
	Source Identifier
	Index  int
}

func cloneTraceEvent(event *Event) *Event {
	if event == nil {
		return nil
	}
	clone := cloneEvent(*event)
	return &clone
}

func cloneMacrostepTrace(trace MacrostepTrace) MacrostepTrace {
	trace.Trigger = cloneTraceEvent(trace.Trigger)
	trace.Before = append([]Identifier(nil), trace.Before...)
	trace.After = append([]Identifier(nil), trace.After...)
	trace.Microsteps = append([]MicrostepTrace(nil), trace.Microsteps...)
	for i := range trace.Microsteps {
		step := &trace.Microsteps[i]
		step.Trigger = cloneTraceEvent(step.Trigger)
		step.Transitions = append([]TransitionRef(nil), step.Transitions...)
		step.Exited = append([]Identifier(nil), step.Exited...)
		step.Entered = append([]Identifier(nil), step.Entered...)
	}
	return trace
}
