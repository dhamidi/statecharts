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
// recorded history, both event queues, the running flag, any outstanding
// delayed sends, and which invocations were active. The datamodel is
// explicitly excluded -- it is the caller's own Go value(s), serialized by
// the caller if desired.
//
// Snapshot is a derivable checkpoint, not an independent source of truth: it
// captures exactly what replaying a Log up to some point would produce, and
// exists purely so a cold start doesn't need to replay from the beginning
// every time (see Checkpoint, Rehydrate in replay.go).
type Snapshot struct {
	Version       int
	ID            SessionID // SCXML 5.10's _sessionid for this session; Restore preserves it unless WithSessionID overrides it
	Configuration []Identifier
	HistoryValue  map[Identifier][]Identifier
	InternalQueue []Event
	ExternalQueue []Event
	Running       bool
	SendSeq       int // high-water mark for auto-generated send.<n> IDs
	InvokeSeq     int // high-water mark for auto-generated <state>.invoke<n> IDs
	PendingSends  []PendingSend
	ActiveInvokes []ActiveInvoke
}

// PendingSend describes one delayed <send> that has not yet fired or been
// cancelled. FireAt is absolute so a restored Instance can re-arm it against
// its configured Clock, firing it during Start if it is already overdue.
type PendingSend struct {
	SendID Identifier
	Target Identifier
	Type   Identifier
	Event  Event
	FireAt time.Time
}

// ActiveInvoke records one <invoke> that was active in Configuration when a
// Snapshot was taken.
type ActiveInvoke struct {
	State     Identifier // the state that owns this invocation
	SpecIndex int        // this invocation's position among State's <invoke> elements, in document order
	ID        Identifier // the invocation's own id, exactly as assigned when it started
}

// Checkpoint pairs a Snapshot with the Log sequence number it reflects.
// Seq == 0 means the Snapshot was taken independent of any Log.
type Checkpoint struct {
	Snapshot Snapshot
	Seq      uint64
}

const snapshotVersion = 2

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
		SendSeq:       ip.sendSeq,
		InvokeSeq:     ip.invokeSeq,
	}
	for hs, states := range ip.historyValue {
		ids := make([]Identifier, len(states))
		for i, s := range states {
			ids[i] = s.id
		}
		snap.HistoryValue[hs.id] = ids
	}
	pending := make([]*pendingSendRecord, 0, len(ip.pending))
	for rec := range ip.pending {
		pending = append(pending, rec)
	}
	sort.Slice(pending, func(i, j int) bool {
		if !pending[i].fireAt.Equal(pending[j].fireAt) {
			return pending[i].fireAt.Before(pending[j].fireAt)
		}
		if pending[i].order != pending[j].order {
			return pending[i].order < pending[j].order
		}
		return pending[i].sendID < pending[j].sendID
	})
	for _, rec := range pending {
		snap.PendingSends = append(snap.PendingSends, PendingSend{
			SendID: rec.sendID,
			Target: rec.target,
			Type:   rec.typ,
			Event:  rec.event,
			FireAt: rec.fireAt,
		})
	}
	for _, invokes := range ip.activeInvokes {
		for _, ri := range invokes {
			snap.ActiveInvokes = append(snap.ActiveInvokes, ActiveInvoke{
				State:     ri.state.id,
				SpecIndex: ri.specIndex,
				ID:        ri.id,
			})
		}
	}
	sort.Slice(snap.ActiveInvokes, func(i, j int) bool {
		return snap.ActiveInvokes[i].ID < snap.ActiveInvokes[j].ID
	})
	return snap
}

// Restore reconstructs a paused Instance directly from a Snapshot, without
// re-running any onentry/initial-transition executable content (those
// already ran historically). Its session id follows the same precedence as
// Instance.ID: an explicit WithSessionID, then snap.ID, then the configured
// IDGenerator. It validates that every state ID mentioned in snap still
// exists in chart, returning an error on drift. Pending sends are rebuilt as
// inert records, then armed against the configured Clock by Start (or, for
// Rehydrate, only after log replay catches up); Start fires already-overdue
// sends before returning. ActiveInvokes are reconstructed as bookkeeping
// only -- routing a <finalize> or a "#_<invokeid>" send still works, but no
// invocation goroutine is started; Rehydrate is what decides whether each
// one gets error.communication or a real InvokeResumeFunc call. The Instance
// is constructed but not started; call Start to spawn its goroutine.
func Restore(chart *Chart, datamodel any, snap Snapshot, opts ...Option) (*Instance, error) {
	if snap.Version != snapshotVersion {
		return nil, fmt.Errorf("statecharts: restore: unsupported snapshot version %d (want %d)", snap.Version, snapshotVersion)
	}
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
	ip.sendSeq = snap.SendSeq
	ip.invokeSeq = snap.InvokeSeq

	ip.pending = map[*pendingSendRecord]bool{}
	for i, ps := range snap.PendingSends {
		sendID := ps.SendID
		rec := &pendingSendRecord{
			sendID: sendID,
			target: ps.Target,
			typ:    ps.Type,
			event:  ps.Event,
			fireAt: ps.FireAt,
			order:  i + 1,
		}
		ip.pending[rec] = true
	}
	ip.pendingSeq = len(snap.PendingSends)

	// Reconstructed as bookkeeping, not as a running invocation: cancel and
	// incoming are left as the same no-op/nil shape startInvoke returns
	// while suppressed, exactly mirroring how PendingSends are rebuilt as
	// records rather than replayed as historical events. This is what lets
	// Rehydrate's post-replay reconciliation loop see this invocation as
	// active regardless of whether it got here via replay or straight off a
	// checkpoint.
	ip.activeInvokes = map[*compiledState][]*runningInvoke{}
	ip.invokesByID = map[Identifier]*runningInvoke{}
	for _, ai := range snap.ActiveInvokes {
		s, ok := chart.byID[ai.State]
		if !ok {
			return fmt.Errorf("statecharts: restore: chart has no state %q (from active invoke)", ai.State)
		}
		if !configuration[s] {
			return fmt.Errorf("statecharts: restore: active invoke references state %q not in Configuration", ai.State)
		}
		if ai.SpecIndex < 0 || ai.SpecIndex >= len(s.invokes) {
			return fmt.Errorf("statecharts: restore: state %q has no invoke at index %d", ai.State, ai.SpecIndex)
		}
		spec := s.invokes[ai.SpecIndex]
		ri := &runningInvoke{
			id:          ai.ID,
			state:       s,
			specIndex:   ai.SpecIndex,
			finalize:    spec.finalize,
			autoForward: spec.autoForward,
			cancel:      func() {},
			incoming:    nil,
		}
		ip.activeInvokes[s] = append(ip.activeInvokes[s], ri)
		ip.invokesByID[ai.ID] = ri
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
	ID            SessionID                   `json:"id,omitempty"`
	Configuration []Identifier                `json:"configuration"`
	HistoryValue  map[Identifier][]Identifier `json:"history_value,omitempty"`
	InternalQueue []EncodedEvent              `json:"internal_queue,omitempty"`
	ExternalQueue []EncodedEvent              `json:"external_queue,omitempty"`
	Running       bool                        `json:"running"`
	SendSeq       int                         `json:"send_seq,omitempty"`
	InvokeSeq     int                         `json:"invoke_seq,omitempty"`
	PendingSends  []pendingSendWire           `json:"pending_sends,omitempty"`
	ActiveInvokes []ActiveInvoke              `json:"active_invokes,omitempty"`
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
		SendSeq:       s.SendSeq,
		InvokeSeq:     s.InvokeSeq,
		ActiveInvokes: s.ActiveInvokes,
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
	s.SendSeq = wire.SendSeq
	s.InvokeSeq = wire.InvokeSeq
	s.ActiveInvokes = wire.ActiveInvokes

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
