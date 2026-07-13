package statecharts

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// Snapshot is a point-in-time capture of everything about a running chart's
// state except the datamodel: the session id, the active configuration,
// recorded history, both event queues, the running flag, and any
// outstanding delayed sends. The datamodel is explicitly excluded -- it is
// the caller's own Go value(s), serialized by the caller if desired.
//
// Snapshot is a derivable checkpoint, not an independent source of truth: it
// captures exactly what replaying a Log up to some point would produce, and
// exists purely so a cold start doesn't need to replay from the beginning
// every time (see Checkpoint, Rehydrate in replay.go).
type Snapshot struct {
	Version       int
	ID            string // SCXML 5.10's _sessionid for this session; Restore preserves it unless WithSessionID overrides it
	Configuration []Identifier
	HistoryValue  map[Identifier][]Identifier
	InternalQueue []Event
	ExternalQueue []Event
	Running       bool
	PendingSends  []PendingSend
}

// PendingSend describes one delayed <send> that has not yet fired or been
// cancelled. FireAt is absolute so Restore can re-arm a real timer relative
// to time.Until(FireAt), firing immediately if already overdue.
type PendingSend struct {
	SendID Identifier
	Target Identifier
	Type   Identifier
	Event  Event
	FireAt time.Time
}

// Checkpoint pairs a Snapshot with the Log sequence number it reflects.
// Seq == 0 means the Snapshot was taken independent of any Log.
type Checkpoint struct {
	Snapshot Snapshot
	Seq      uint64
}

const snapshotVersion = 1

// Snapshot captures this Instance's current state (safely, by running on
// the interpreter's own goroutine), suitable for persisting and later
// passing to Restore.
func (in *Instance) Snapshot(ctx context.Context) (Snapshot, error) {
	req := actorRequest{kind: reqSnapshot, snapOut: make(chan Snapshot, 1)}
	if err := in.submit(ctx, req); err != nil {
		return Snapshot{}, err
	}
	select {
	case snap := <-req.snapOut:
		return snap, nil
	case <-in.doneCh:
		select {
		case snap := <-req.snapOut:
			return snap, nil
		default:
			return Snapshot{}, ErrInstanceStopped
		}
	case <-ctx.Done():
		return Snapshot{}, ctx.Err()
	}
}

func (in *Instance) buildSnapshot() Snapshot {
	ip := in.ip
	snap := Snapshot{
		Version:       snapshotVersion,
		ID:            ip.sessionID,
		Configuration: ip.activeStates(),
		HistoryValue:  map[Identifier][]Identifier{},
		InternalQueue: append([]Event(nil), ip.internalQueue...),
		ExternalQueue: append([]Event(nil), ip.externalQueue...),
		Running:       ip.running,
	}
	for hs, states := range ip.historyValue {
		ids := make([]Identifier, len(states))
		for i, s := range states {
			ids[i] = s.id
		}
		snap.HistoryValue[hs.id] = ids
	}
	for _, rec := range ip.pending {
		snap.PendingSends = append(snap.PendingSends, PendingSend{
			SendID: rec.sendID,
			Target: rec.target,
			Type:   rec.typ,
			Event:  rec.event,
			FireAt: rec.fireAt,
		})
	}
	sort.Slice(snap.PendingSends, func(i, j int) bool {
		return snap.PendingSends[i].SendID < snap.PendingSends[j].SendID
	})
	return snap
}

// Restore reconstructs a paused Instance directly from a Snapshot, without
// re-running any onentry/initial-transition executable content (those
// already ran historically). Its session id follows the same precedence as
// Instance.ID: an explicit WithSessionID, then snap.ID, then the configured
// IDGenerator. It validates that every state ID mentioned in snap still
// exists in chart, returning an error on drift. Pending sends are
// re-armed as real timers relative to time.Until(FireAt) (firing
// immediately if already overdue). The Instance is constructed but not
// started; call Start to spawn its goroutine.
func Restore(chart *Chart, datamodel any, snap Snapshot, opts ...Option) (*Instance, error) {
	cfg := defaultInstanceConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	// Precedence, highest to lowest: an explicit WithSessionID, an id
	// already recorded in snap, then the configured IDGenerator -- resolved
	// here, before constructing anything, so the generator is never invoked
	// speculatively only to be discarded.
	id := cfg.sessionID
	if id == "" {
		id = snap.ID
	}
	if id == "" {
		id = cfg.idGen.NewID()
	}
	ip := newInterpretation(chart, datamodel)
	in := newInstance(chart, ip, cfg, id)
	if err := ip.restoreFrom(chart, snap); err != nil {
		return nil, err
	}
	return in, nil
}

func (ip *interpretation) restoreFrom(chart *Chart, snap Snapshot) error {
	configuration := map[*compiledState]bool{}
	for _, id := range snap.Configuration {
		s, ok := chart.byID[id]
		if !ok {
			return fmt.Errorf("statecharts: restore: chart has no state %q (from snapshot configuration)", id)
		}
		configuration[s] = true
	}

	historyValue := map[*compiledState][]*compiledState{}
	for histID, recordedIDs := range snap.HistoryValue {
		h, ok := chart.byID[histID]
		if !ok {
			return fmt.Errorf("statecharts: restore: chart has no history state %q", histID)
		}
		recorded := make([]*compiledState, 0, len(recordedIDs))
		for _, rid := range recordedIDs {
			rs, ok := chart.byID[rid]
			if !ok {
				return fmt.Errorf("statecharts: restore: chart has no state %q (from history %q)", rid, histID)
			}
			recorded = append(recorded, rs)
		}
		historyValue[h] = recorded
	}

	ip.configuration = configuration
	ip.historyValue = historyValue
	ip.internalQueue = append([]Event(nil), snap.InternalQueue...)
	ip.externalQueue = append([]Event(nil), snap.ExternalQueue...)
	ip.running = snap.Running

	ip.pending = map[Identifier]*pendingSendRecord{}
	for _, ps := range snap.PendingSends {
		sendID := ps.SendID
		rec := &pendingSendRecord{
			sendID: sendID,
			target: ps.Target,
			typ:    ps.Type,
			event:  ps.Event,
			fireAt: ps.FireAt,
		}
		ip.pending[sendID] = rec
		delay := time.Until(ps.FireAt)
		if delay < 0 {
			delay = 0
		}
		rec.stop = ip.clock.AfterFunc(delay, func() { ip.handleTimerFire(sendID) })
	}

	ip.restored = true
	return nil
}

// --- JSON envelope -------------------------------------------------------
//
// Snapshot's own fields are otherwise JSON-friendly; only Event.Data (an
// arbitrary Go value) needs help, via EncodeEvent/DecodeEvent (event_codec.go).

type snapshotWire struct {
	Version       int                         `json:"version"`
	ID            string                      `json:"id,omitempty"`
	Configuration []Identifier                `json:"configuration"`
	HistoryValue  map[Identifier][]Identifier `json:"history_value,omitempty"`
	InternalQueue []EncodedEvent              `json:"internal_queue,omitempty"`
	ExternalQueue []EncodedEvent              `json:"external_queue,omitempty"`
	Running       bool                        `json:"running"`
	PendingSends  []pendingSendWire           `json:"pending_sends,omitempty"`
}

type pendingSendWire struct {
	SendID Identifier   `json:"send_id"`
	Target Identifier   `json:"target,omitempty"`
	Type   Identifier   `json:"type,omitempty"`
	Event  EncodedEvent `json:"event"`
	FireAt time.Time    `json:"fire_at"`
}

// MarshalJSON implements json.Marshaler.
func (s Snapshot) MarshalJSON() ([]byte, error) {
	wire := snapshotWire{
		Version:       s.Version,
		ID:            s.ID,
		Configuration: s.Configuration,
		HistoryValue:  s.HistoryValue,
		Running:       s.Running,
	}
	for _, ev := range s.InternalQueue {
		enc, err := EncodeEvent(ev)
		if err != nil {
			return nil, fmt.Errorf("statecharts: Snapshot: encode internal queue event: %w", err)
		}
		wire.InternalQueue = append(wire.InternalQueue, enc)
	}
	for _, ev := range s.ExternalQueue {
		enc, err := EncodeEvent(ev)
		if err != nil {
			return nil, fmt.Errorf("statecharts: Snapshot: encode external queue event: %w", err)
		}
		wire.ExternalQueue = append(wire.ExternalQueue, enc)
	}
	for _, ps := range s.PendingSends {
		enc, err := EncodeEvent(ps.Event)
		if err != nil {
			return nil, fmt.Errorf("statecharts: Snapshot: encode pending send event: %w", err)
		}
		wire.PendingSends = append(wire.PendingSends, pendingSendWire{
			SendID: ps.SendID, Target: ps.Target, Type: ps.Type, Event: enc, FireAt: ps.FireAt,
		})
	}
	return json.Marshal(wire)
}

// UnmarshalJSON implements json.Unmarshaler.
func (s *Snapshot) UnmarshalJSON(b []byte) error {
	var wire snapshotWire
	if err := json.Unmarshal(b, &wire); err != nil {
		return err
	}
	s.Version = wire.Version
	s.ID = wire.ID
	s.Configuration = wire.Configuration
	s.HistoryValue = wire.HistoryValue
	s.Running = wire.Running

	s.InternalQueue = nil
	for _, enc := range wire.InternalQueue {
		ev, err := DecodeEvent(enc)
		if err != nil {
			return fmt.Errorf("statecharts: Snapshot: decode internal queue event: %w", err)
		}
		s.InternalQueue = append(s.InternalQueue, ev)
	}
	s.ExternalQueue = nil
	for _, enc := range wire.ExternalQueue {
		ev, err := DecodeEvent(enc)
		if err != nil {
			return fmt.Errorf("statecharts: Snapshot: decode external queue event: %w", err)
		}
		s.ExternalQueue = append(s.ExternalQueue, ev)
	}
	s.PendingSends = nil
	for _, pw := range wire.PendingSends {
		ev, err := DecodeEvent(pw.Event)
		if err != nil {
			return fmt.Errorf("statecharts: Snapshot: decode pending send event: %w", err)
		}
		s.PendingSends = append(s.PendingSends, PendingSend{
			SendID: pw.SendID, Target: pw.Target, Type: pw.Type, Event: ev, FireAt: pw.FireAt,
		})
	}
	return nil
}
