package actors

import (
	"fmt"
	"sync"
	"time"

	"github.com/dhamidi/statecharts"
)

const (
	// DefaultObservationBuffer is used when SubscribeObservations receives zero.
	DefaultObservationBuffer = 256
	// MaxObservationBuffer is the largest accepted subscription buffer.
	MaxObservationBuffer = 4096
)

// ObservationKind is a stable, machine-readable observation category.
type ObservationKind string

const (
	// ObservationActorDiscovered reports an actor entering the local directory.
	ObservationActorDiscovered ObservationKind = "actor.discovered"
	// ObservationActorTerminal reports an actor reaching terminal lifecycle state.
	ObservationActorTerminal ObservationKind = "actor.terminal"
	// ObservationResidencyChanged reports a local residency transition.
	ObservationResidencyChanged ObservationKind = "residency.changed"
	// ObservationDefinitionPublished reports a change to a chart's current revision.
	ObservationDefinitionPublished ObservationKind = "definition.published"
	// ObservationMacrostep reports one completed live (never replayed) macrostep.
	ObservationMacrostep ObservationKind = "macrostep"
)

// DefinitionPublication identifies a successful current-revision change.
type DefinitionPublication struct {
	// ChartID is the stable chart identifier.
	ChartID statecharts.Identifier
	// PreviousRevision is empty when no prior current revision existed.
	PreviousRevision statecharts.RevisionID
	// Revision is the newly current revision.
	Revision statecharts.RevisionID
}

// Observation is one best-effort system diagnostic event. Payload pointers
// are non-nil only when relevant to Kind and are independently owned.
type Observation struct {
	// Sequence is a system-wide contiguous publication sequence while subscribers exist.
	Sequence uint64
	// Timestamp is the publication time normalized to UTC.
	Timestamp time.Time
	// Kind classifies the payload.
	Kind ObservationKind
	// Dropped counts observations lost by this subscriber since its prior delivery.
	Dropped uint64
	// Actor is present for actor lifecycle, residency, and macrostep observations.
	Actor *ActorInfo
	// Definition is present for definition publication observations.
	Definition *DefinitionPublication
	// Macrostep is present for live macrostep observations.
	Macrostep *statecharts.MacrostepTrace
}

type observationSubscriber struct {
	channel chan Observation
	dropped uint64
}

// ObservationSubscription is a bounded, non-blocking observation stream.
// Close is safe to call concurrently and more than once.
type ObservationSubscription struct {
	// C receives observations and is closed by Close or System.Stop.
	C    <-chan Observation
	s    *System
	id   uint64
	once sync.Once
}

// SubscribeObservations registers a best-effort stream with the requested
// bounded buffer. Zero selects DefaultObservationBuffer. Publication never
// blocks actor execution; overflow is reported by the next delivered event.
func (s *System) SubscribeObservations(buffer int) (*ObservationSubscription, error) {
	if buffer == 0 {
		buffer = DefaultObservationBuffer
	}
	if buffer < 1 || buffer > MaxObservationBuffer {
		return nil, fmt.Errorf("actors: observation buffer must be between 1 and %d", MaxObservationBuffer)
	}
	s.observationsMu.Lock()
	defer s.observationsMu.Unlock()
	if s.stopped.Load() {
		return nil, ErrSystemStopped
	}
	s.observationID++
	id := s.observationID
	ch := make(chan Observation, buffer)
	s.observations[id] = &observationSubscriber{channel: ch}
	s.observerCount.Add(1)
	return &ObservationSubscription{C: ch, s: s, id: id}, nil
}

// Close unregisters the subscription and closes C. It is safe to call Close
// concurrently or more than once; the zero value is a no-op.
func (sub *ObservationSubscription) Close() {
	if sub == nil || sub.s == nil {
		return
	}
	sub.once.Do(func() {
		sub.s.observationsMu.Lock()
		if registered, ok := sub.s.observations[sub.id]; ok {
			delete(sub.s.observations, sub.id)
			sub.s.observerCount.Add(-1)
			close(registered.channel)
		}
		sub.s.observationsMu.Unlock()
	})
}

func (s *System) closeObservationSubscriptions() {
	s.observationsMu.Lock()
	for id, sub := range s.observations {
		delete(s.observations, id)
		close(sub.channel)
	}
	s.observerCount.Store(0)
	s.observationsMu.Unlock()
}

func actorInfoPointer(info ActorInfo) *ActorInfo { return &info }

// Clone returns an independently owned copy, including nested macrostep data.
func (in Observation) Clone() Observation {
	out := in
	if in.Actor != nil {
		actor := *in.Actor
		out.Actor = &actor
	}
	if in.Definition != nil {
		definition := *in.Definition
		out.Definition = &definition
	}
	if in.Macrostep != nil {
		trace := in.Macrostep.Clone()
		out.Macrostep = &trace
	}
	return out
}

func (s *System) publishObservation(observation Observation) {
	if s.observerCount.Load() == 0 {
		return
	}
	s.observationsMu.Lock()
	if len(s.observations) == 0 {
		s.observationsMu.Unlock()
		return
	}
	s.observationSeq++
	observation.Sequence = s.observationSeq
	observation.Timestamp = s.cfg.clock.Now().UTC()
	for _, sub := range s.observations {
		copy := observation.Clone()
		copy.Dropped = sub.dropped
		select {
		case sub.channel <- copy:
			sub.dropped = 0
		default:
			sub.dropped++
		}
	}
	s.observationsMu.Unlock()
}

func (s *System) notifyDiscovered(entry *actorEntry) {
	if entry.discovered.CompareAndSwap(false, true) && s.observerCount.Load() != 0 {
		s.publishObservation(Observation{Kind: ObservationActorDiscovered, Actor: actorInfoPointer(s.infoForEntry(entry))})
	}
}

func (s *System) notifyTerminal(entry *actorEntry) {
	if entry.observedEnd.CompareAndSwap(false, true) && s.observerCount.Load() != 0 {
		s.publishObservation(Observation{Kind: ObservationActorTerminal, Actor: actorInfoPointer(s.infoForEntry(entry))})
	}
}

type systemMacrostepObserver struct {
	s     *System
	entry *actorEntry
}

func (o systemMacrostepObserver) Enabled() bool { return o.s.observerCount.Load() != 0 }

func (o systemMacrostepObserver) ObserveMacrostep(trace statecharts.MacrostepTrace) {
	if o.s.observerCount.Load() == 0 {
		return
	}
	o.s.publishObservation(Observation{Kind: ObservationMacrostep, Actor: actorInfoPointer(o.s.infoForEntry(o.entry)), Macrostep: &trace})
}

var _ statecharts.MacrostepObserver = systemMacrostepObserver{}
