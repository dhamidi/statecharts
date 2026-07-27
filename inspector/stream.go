package inspector

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dhamidi/statecharts/actors"
)

// StreamKind classifies a service stream record.
type StreamKind string

const (
	// StreamObservation carries a redacted actors observation.
	StreamObservation StreamKind = "observation"
	// StreamGap reports records dropped before this delivery.
	StreamGap StreamKind = "gap"
)

// ErrStreamClosed reports that one registered system's observation source has
// ended while the inspector service itself remains available for retained reads.
var ErrStreamClosed = errors.New("inspector: stream closed")

// StreamRecord has a per-system inspector sequence. It is unrelated to the
// durable log sequence, the source Observation.Sequence, and a macrostep's
// actor-activation Sequence.
type StreamRecord struct {
	Sequence    uint64
	Timestamp   time.Time
	Kind        StreamKind
	System      string
	Observation *actors.Observation
	Dropped     uint64
	Reason      string
}

// Clone returns an independently owned record.
func (r StreamRecord) Clone() StreamRecord {
	if r.Observation != nil {
		x := r.Observation.Clone()
		r.Observation = &x
	}
	return r
}

// RingPage is one exclusive-cursor page of retained records. For reconnect-safe
// consumption, subscribe first, fetch Recent, then emit and deduplicate both
// sources by Sequence. A live Dropped value requires refetching from the ring.
type RingPage struct {
	Records []StreamRecord
	Expired bool
	// Oldest and Latest are the retained boundary and latest assigned sequence.
	Oldest uint64
	Latest uint64
	// Next is the exclusive cursor represented by the last returned record (or
	// the input cursor when no records were returned).
	Next uint64
}

// Subscription is a bounded live stream. Its zero value is safe to close.
type Subscription struct {
	C     <-chan StreamRecord
	once  sync.Once
	close func()
}

// Close releases the subscription and closes C; it is concurrent-safe and idempotent.
func (s *Subscription) Close() {
	if s != nil {
		s.once.Do(func() {
			if s.close != nil {
				s.close()
			}
		})
	}
}

type subscriber struct {
	ch      chan StreamRecord
	dropped uint64
}

// Recent returns retained records strictly after the cursor. A zero cursor
// means all retained records and never expires. A zero limit selects 100.
func (s *Service) Recent(ctx context.Context, system string, after uint64, limit int) (RingPage, error) {
	if e := s.authorize(ctx, OperationSubscribe, system, ""); e != nil {
		return RingPage{}, e
	}
	r, e := s.lookup(system)
	if e != nil {
		return RingPage{}, e
	}
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 1000 {
		return RingPage{}, fmt.Errorf("inspector: ring limit must be between 1 and 1000")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	p := RingPage{Latest: r.sequence, Next: after}
	if len(r.ring) > 0 {
		p.Oldest = r.ring[0].Sequence
	}
	cursor := after
	if after > r.sequence {
		p.Expired = true
		cursor = 0
		gapSequence := r.sequence
		if len(r.ring) > 0 {
			gapSequence = r.ring[0].Sequence - 1
		}
		p.Records = append(p.Records, StreamRecord{Sequence: gapSequence, Timestamp: s.cfg.clock.Now().UTC(), Kind: StreamGap, System: system, Reason: "cursor ahead of stream"})
	} else if after != 0 && len(r.ring) > 0 && after < r.ring[0].Sequence-1 {
		p.Expired = true
		p.Records = append(p.Records, StreamRecord{Sequence: r.ring[0].Sequence - 1, Timestamp: s.cfg.clock.Now().UTC(), Kind: StreamGap, System: system, Dropped: r.ring[0].Sequence - after - 1, Reason: "cursor expired"})
	}
	for _, x := range r.ring {
		if x.Sequence > cursor && len(p.Records) < limit {
			p.Records = append(p.Records, x.Clone())
		}
	}
	if len(p.Records) > 0 {
		p.Next = p.Records[len(p.Records)-1].Sequence
	}
	return p, nil
}

// Subscribe opens a non-blocking live stream. Zero selects a buffer of 64.
func (s *Service) Subscribe(ctx context.Context, system string, buffer int) (*Subscription, error) {
	if e := s.authorize(ctx, OperationSubscribe, system, ""); e != nil {
		return nil, e
	}
	if buffer == 0 {
		buffer = 64
	}
	if buffer < 1 || buffer > 4096 {
		return nil, fmt.Errorf("inspector: subscription buffer must be between 1 and 4096")
	}
	r, e := s.lookup(system)
	if e != nil {
		return nil, e
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, ErrStreamClosed
	}
	r.nextSub++
	id := r.nextSub
	ch := make(chan StreamRecord, buffer)
	r.subscribers[id] = &subscriber{ch: ch}
	r.mu.Unlock()
	return &Subscription{C: ch, close: func() {
		r.mu.Lock()
		if x := r.subscribers[id]; x != nil {
			delete(r.subscribers, id)
			close(x.ch)
		}
		r.mu.Unlock()
	}}, nil
}

func (s *Service) drain(name string, r *registered) {
	defer s.wg.Done()
	defer func() { r.mu.Lock(); r.closed = true; r.mu.Unlock(); r.closeSubscribers() }()
	for o := range r.source.C {
		dropped := o.Dropped
		o.Dropped = 0
		if dropped > 0 {
			r.add(s, name, StreamGap, nil, dropped, "source observations dropped")
		}
		x, e := s.redactObservation(context.Background(), name, o)
		if e != nil {
			r.add(s, name, StreamGap, nil, 1, "observation redaction failed")
			s.diagnostic(e)
			continue
		}
		r.add(s, name, StreamObservation, &x, 0, "")
	}
}
func (s *Service) diagnostic(e error) {
	if s.cfg.diagnostic == nil {
		return
	}
	defer func() { _ = recover() }()
	s.cfg.diagnostic(context.Background(), e)
}
func (s *Service) redactObservation(ctx context.Context, system string, o actors.Observation) (actors.Observation, error) {
	x := o.Clone()
	if x.Macrostep == nil {
		return x, nil
	}
	id := actors.ActorID("")
	if x.Actor != nil {
		id = x.Actor.ID
	}
	var e error
	if x.Macrostep.Trigger != nil {
		x.Macrostep.Trigger.Data, e = s.redactValue(ctx, system, id, CategoryEvent, x.Macrostep.Trigger.Data)
		if e != nil {
			return actors.Observation{}, e
		}
	}
	for i := range x.Macrostep.Microsteps {
		if x.Macrostep.Microsteps[i].Trigger != nil {
			x.Macrostep.Microsteps[i].Trigger.Data, e = s.redactValue(ctx, system, id, CategoryEvent, x.Macrostep.Microsteps[i].Trigger.Data)
			if e != nil {
				return actors.Observation{}, e
			}
		}
	}
	if s.cfg.redactor != nil {
		x.Macrostep.TerminalError, e = s.redactText(ctx, RedactionContext{system, id, CategoryDiagnostic}, x.Macrostep.TerminalError)
		if e != nil {
			return actors.Observation{}, e
		}
	}
	return x, nil
}
func (r *registered) add(s *Service, system string, k StreamKind, o *actors.Observation, d uint64, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sequence++
	x := StreamRecord{Sequence: r.sequence, Timestamp: s.cfg.clock.Now().UTC(), Kind: k, System: system, Observation: o, Dropped: d, Reason: reason}
	r.ring = append(r.ring, x.Clone())
	if n := len(r.ring) - s.cfg.ringSize; n > 0 {
		copy(r.ring, r.ring[n:])
		r.ring = r.ring[:len(r.ring)-n]
	}
	for _, sub := range r.subscribers {
		delivery := x.Clone()
		if sub.dropped > 0 {
			delivery.Dropped += sub.dropped
			if delivery.Reason == "" {
				delivery.Reason = "subscriber observations dropped"
			} else {
				delivery.Reason += "; subscriber observations dropped"
			}
		}
		select {
		case sub.ch <- delivery:
			sub.dropped = 0
		default:
			sub.dropped++
		}
	}
}
func (r *registered) closeSubscribers() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, x := range r.subscribers {
		delete(r.subscribers, id)
		close(x.ch)
	}
}
