package statecharts

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// Snapshot is a point-in-time cache of a running chart's datamodel and
// interpreter state: the session id, active configuration, recorded history,
// both event queues, running flag, outstanding delayed sends, and active
// invocation bookkeeping. Datamodel bytes are an opaque cache owned by the
// instance's DatamodelSession.
//
// Snapshot is a derivable checkpoint, not an independent source of truth: it
// captures exactly what replaying a Log up to some point would produce, and
// exists purely so a cold start doesn't need to replay from the beginning
// every time (see Checkpoint, Rehydrate in replay.go).
type Snapshot struct {
	Version           int
	ChartVersion      string
	Datamodel         []byte
	ID                SessionID // SCXML 5.10's _sessionid for this session; Restore preserves it unless WithSessionID overrides it
	Configuration     []Identifier
	HistoryValue      map[Identifier][]Identifier
	InternalQueue     []Event
	ExternalQueue     []Event
	Running           bool
	SendSeq           int // high-water mark for auto-generated send.<n> IDs
	InvokeSeq         int // high-water mark for auto-generated <state>.invoke<n> IDs
	DispatchSeq       uint64
	DeliveryNamespace string
	PendingSends      []PendingSend
	ActiveInvokes     []ActiveInvoke
	InitializedData   []Identifier
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
	State        Identifier `json:"state"`            // the state that owns this invocation
	DefinitionID Identifier `json:"definition_id"`    // stable declaration identity in the pinned chart definition
	ID           Identifier `json:"id"`               // the invocation's own id, exactly as assigned when it started
	Type         Identifier `json:"type,omitempty"`   // evaluated handler type selected when this invocation began
	Source       string     `json:"source,omitempty"` // evaluated source selected when this invocation began
}

// Checkpoint pairs a Snapshot with the Log sequence number it reflects.
// Seq == 0 means the Snapshot was taken independent of any Log.
type Checkpoint struct {
	Snapshot Snapshot
	Seq      uint64
}

const snapshotVersion = 6

// Snapshot captures this Instance's current state (safely, by running on
// the interpreter's own goroutine), suitable for persisting and later
// passing to Restore.
func (in *Instance) Snapshot(ctx context.Context) (Snapshot, error) {
	req := actorRequest{kind: reqSnapshot, snapOut: make(chan snapshotResult, 1)}
	if err := in.submit(ctx, req); err != nil {
		return Snapshot{}, err
	}
	select {
	case result := <-req.snapOut:
		return result.snapshot, result.err
	case <-in.doneCh:
		select {
		case result := <-req.snapOut:
			return result.snapshot, result.err
		default:
			return Snapshot{}, ErrInstanceStopped
		}
	case <-ctx.Done():
		return Snapshot{}, ctx.Err()
	}
}

func (in *Instance) buildSnapshot() (Snapshot, error) {
	ip := in.ip
	datamodel, err := in.session.EncodeSnapshot()
	if err != nil {
		return Snapshot{}, fmt.Errorf("statecharts: snapshot datamodel: %w", err)
	}
	snap := Snapshot{
		Version:           snapshotVersion,
		ChartVersion:      in.chart.version,
		Datamodel:         datamodel,
		ID:                ip.sessionID,
		Configuration:     ip.activeStates(),
		HistoryValue:      map[Identifier][]Identifier{},
		InternalQueue:     cloneEvents(ip.internalQueue),
		ExternalQueue:     cloneEvents(ip.externalQueue),
		Running:           ip.running,
		SendSeq:           ip.sendSeq,
		InvokeSeq:         ip.invokeSeq,
		DispatchSeq:       ip.dispatchSeq,
		DeliveryNamespace: ip.deliveryNamespace,
	}
	for hs, states := range ip.historyValue {
		ids := make([]Identifier, len(states))
		for i, s := range states {
			ids[i] = s.id
		}
		snap.HistoryValue[hs.id] = ids
	}
	for _, s := range ip.chart.order {
		if ip.initializedData[s] {
			snap.InitializedData = append(snap.InitializedData, s.id)
		}
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
			Event:  cloneEvent(rec.event),
			FireAt: rec.fireAt,
		})
	}
	for _, invokes := range ip.activeInvokes {
		for _, ri := range invokes {
			snap.ActiveInvokes = append(snap.ActiveInvokes, ActiveInvoke{
				State:        ri.state.id,
				DefinitionID: ri.spec.definitionID,
				ID:           ri.id,
				Type:         ri.typ,
				Source:       ri.source,
			})
		}
	}
	sort.Slice(snap.ActiveInvokes, func(i, j int) bool {
		return snap.ActiveInvokes[i].ID < snap.ActiveInvokes[j].ID
	})
	return snap, nil
}

// Restore reconstructs a paused Instance from snap with a fresh session from
// the chart's DatamodelProgram. The session owns decoding the opaque model
// snapshot bytes. The returned instance is not started.
func (c *Chart) Restore(snap Snapshot, opts ...Option) (*Instance, error) {
	if c.program == nil {
		return nil, fmt.Errorf("statecharts: chart has no datamodel program")
	}
	if err := c.Prepare(opts...); err != nil {
		return nil, err
	}
	return restoreInstanceFromFactory(c, snap, func() (DatamodelSession, error) {
		return c.program.NewSession(SessionOptions{})
	}, opts...)
}

func restoreInstanceFromFactory(chart *Chart, snap Snapshot, factory datamodelSessionFactory, opts ...Option) (*Instance, error) {
	if err := validateSnapshotHeader(chart, snap); err != nil {
		return nil, err
	}
	session, err := factory()
	if err != nil {
		return nil, fmt.Errorf("statecharts: create datamodel session: %w", err)
	}
	if session == nil {
		return nil, fmt.Errorf("statecharts: create datamodel session: program returned nil session")
	}
	return restoreInstanceForSession(chart, session, snap, opts...)
}

func validateSnapshotHeader(chart *Chart, snap Snapshot) error {
	if snap.Version != snapshotVersion {
		return fmt.Errorf("%w: unsupported snapshot version %d (want %d)", ErrInvalidSnapshot, snap.Version, snapshotVersion)
	}
	if snap.ChartVersion != chart.version {
		return fmt.Errorf("%w: chart version mismatch", ErrInvalidSnapshot)
	}
	return nil
}

func restoreInstanceForSession(chart *Chart, session DatamodelSession, snap Snapshot, opts ...Option) (_ *Instance, resultErr error) {
	keepSession := false
	defer func() {
		if !keepSession {
			_ = closeSession(session)
		}
	}()
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
	ip := newInterpretation(chart)
	ip.session = session
	if cfg.deliveryNamespace != "" {
		ip.deliveryNamespace = cfg.deliveryNamespace
	} else {
		ip.deliveryNamespace = fmt.Sprintf("incarnation-%d", incarnationSeq.Add(1))
	}
	if err := ip.restoreFrom(chart, snap); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidSnapshot, err)
	}
	if err := session.DecodeSnapshot(snap.Datamodel); err != nil {
		return nil, fmt.Errorf("%w: decode datamodel: %v", ErrInvalidSnapshot, err)
	}
	in := newInstance(chart, ip, session, cfg, id)
	keepSession = true
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
	ip.initializedData = map[*compiledState]bool{}
	for _, id := range snap.InitializedData {
		s, ok := chart.byID[id]
		if !ok {
			return fmt.Errorf("statecharts: restore: chart has no initialized state %q", id)
		}
		ip.initializedData[s] = true
	}
	ip.historyValue = historyValue
	ip.internalQueue = cloneEvents(snap.InternalQueue)
	ip.externalQueue = cloneEvents(snap.ExternalQueue)
	ip.running = snap.Running
	ip.sendSeq = snap.SendSeq
	ip.invokeSeq = snap.InvokeSeq
	ip.dispatchSeq = snap.DispatchSeq
	if snap.DeliveryNamespace != "" {
		ip.deliveryNamespace = snap.DeliveryNamespace
	}

	ip.pending = map[*pendingSendRecord]bool{}
	for i, ps := range snap.PendingSends {
		sendID := ps.SendID
		rec := &pendingSendRecord{
			sendID: sendID,
			target: ps.Target,
			typ:    ps.Type,
			event:  cloneEvent(ps.Event),
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
	restoredDefinitions := map[Identifier]bool{}
	for _, ai := range snap.ActiveInvokes {
		s, ok := chart.byID[ai.State]
		if !ok {
			return fmt.Errorf("statecharts: restore: chart has no state %q (from active invoke)", ai.State)
		}
		if !configuration[s] {
			return fmt.Errorf("statecharts: restore: active invoke references state %q not in Configuration", ai.State)
		}
		spec := chart.invokesByDefinitionID[ai.DefinitionID]
		if spec == nil {
			return fmt.Errorf("statecharts: restore: chart has no invoke definition %q", ai.DefinitionID)
		}
		if ai.ID == "" {
			return fmt.Errorf("statecharts: restore: invoke definition %q has an empty runtime ID", ai.DefinitionID)
		}
		if spec.owner != s {
			return fmt.Errorf("statecharts: restore: invoke definition %q belongs to state %q, not %q", ai.DefinitionID, spec.owner.id, ai.State)
		}
		if ai.Type == "" {
			return fmt.Errorf("statecharts: restore: invoke definition %q has an empty handler type", ai.DefinitionID)
		}
		if !spec.hasTypeExpr && ai.Type != spec.staticType {
			return fmt.Errorf("statecharts: restore: invoke definition %q handler type %q does not match static type %q", ai.DefinitionID, ai.Type, spec.staticType)
		}
		if restoredDefinitions[ai.DefinitionID] {
			return fmt.Errorf("statecharts: restore: duplicate active invoke definition %q", ai.DefinitionID)
		}
		if ip.invokesByID[ai.ID] != nil {
			return fmt.Errorf("statecharts: restore: duplicate active invoke runtime ID %q", ai.ID)
		}
		restoredDefinitions[ai.DefinitionID] = true
		ri := &runningInvoke{
			id:          ai.ID,
			state:       s,
			spec:        spec,
			typ:         ai.Type,
			source:      ai.Source,
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
// Snapshot's event payloads use EncodeEvent/DecodeEvent so the only durable
// representation of Data is canonical Value marshal bytes.

type snapshotWire struct {
	Version           int                         `json:"version"`
	ChartVersion      string                      `json:"chart_version"`
	Datamodel         []byte                      `json:"datamodel"`
	ID                SessionID                   `json:"id,omitempty"`
	Configuration     []Identifier                `json:"configuration"`
	HistoryValue      map[Identifier][]Identifier `json:"history_value,omitempty"`
	InternalQueue     []EncodedEvent              `json:"internal_queue,omitempty"`
	ExternalQueue     []EncodedEvent              `json:"external_queue,omitempty"`
	Running           bool                        `json:"running"`
	SendSeq           int                         `json:"send_seq,omitempty"`
	InvokeSeq         int                         `json:"invoke_seq,omitempty"`
	DispatchSeq       uint64                      `json:"dispatch_seq,omitempty"`
	DeliveryNamespace string                      `json:"delivery_namespace,omitempty"`
	PendingSends      []pendingSendWire           `json:"pending_sends,omitempty"`
	ActiveInvokes     []ActiveInvoke              `json:"active_invokes,omitempty"`
	InitializedData   []Identifier                `json:"initialized_data,omitempty"`
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
		Version:           s.Version,
		ChartVersion:      s.ChartVersion,
		Datamodel:         s.Datamodel,
		ID:                s.ID,
		Configuration:     s.Configuration,
		HistoryValue:      s.HistoryValue,
		Running:           s.Running,
		SendSeq:           s.SendSeq,
		InvokeSeq:         s.InvokeSeq,
		DispatchSeq:       s.DispatchSeq,
		DeliveryNamespace: s.DeliveryNamespace,
		ActiveInvokes:     s.ActiveInvokes,
		InitializedData:   s.InitializedData,
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
	s.ChartVersion = wire.ChartVersion
	s.Datamodel = wire.Datamodel
	s.ID = wire.ID
	s.Configuration = wire.Configuration
	s.HistoryValue = wire.HistoryValue
	s.Running = wire.Running
	s.SendSeq = wire.SendSeq
	s.InvokeSeq = wire.InvokeSeq
	s.DispatchSeq = wire.DispatchSeq
	s.DeliveryNamespace = wire.DeliveryNamespace
	s.ActiveInvokes = wire.ActiveInvokes
	s.InitializedData = wire.InitializedData

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
