package statecharts

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"time"
)

// Snapshot is a point-in-time cache of a running chart's datamodel and
// interpreter state: the session id, active configuration, recorded history,
// both event queues, running flag, outstanding delayed sends, and active
// invocation bookkeeping. Datamodel encoding is controlled by the Chart's
// DatamodelCodec (JSON by default).
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

const snapshotVersion = 4

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
	datamodel, err := in.chart.codec.Encode(ip.datamodel)
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
				State:     ri.state.id,
				SpecIndex: ri.specIndex,
				ID:        ri.id,
			})
		}
	}
	sort.Slice(snap.ActiveInvokes, func(i, j int) bool {
		return snap.ActiveInvokes[i].ID < snap.ActiveInvokes[j].ID
	})
	return snap, nil
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
		return nil, fmt.Errorf("%w: unsupported snapshot version %d (want %d)", ErrInvalidSnapshot, snap.Version, snapshotVersion)
	}
	if snap.ChartVersion != chart.version {
		return nil, fmt.Errorf("%w: chart version mismatch", ErrInvalidSnapshot)
	}
	var decoded any
	var err error
	if datamodel == nil && len(snap.Datamodel) == 0 {
		// A nil datamodel has no payload to encode. In particular, preserve
		// control-state-only snapshots written with an empty SQL blob.
		decoded = nil
	} else {
		// Never let a codec mutate the caller's observable value before all
		// snapshot control state has also passed validation.
		decoded, err = chart.codec.Decode(snap.Datamodel, freshDecodePrototype(datamodel))
		if err != nil {
			return nil, fmt.Errorf("%w: decode datamodel: %v", ErrInvalidSnapshot, err)
		}
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
	ip := newInterpretation(chart, decoded)
	in := newInstance(chart, ip, cfg, id)
	if err := ip.restoreFrom(chart, snap); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidSnapshot, err)
	}
	// Validation is complete. Commit the decoded state to the supplied
	// datamodel where its shape is mutable, then ensure future actions see
	// that same caller-observable value.
	ip.datamodel = commitDecodedDatamodel(datamodel, decoded)
	return in, nil
}

func freshDecodePrototype(datamodel any) any {
	if datamodel == nil {
		return nil
	}
	t := reflect.TypeOf(datamodel)
	if t.Kind() == reflect.Pointer {
		return reflect.New(t.Elem()).Interface()
	}
	return reflect.New(t).Elem().Interface()
}

func commitDecodedDatamodel(datamodel, decoded any) any {
	if datamodel == nil || decoded == nil {
		return decoded
	}
	dst, src := reflect.ValueOf(datamodel), reflect.ValueOf(decoded)
	switch dst.Kind() {
	case reflect.Pointer:
		if !dst.IsNil() {
			if src.Type() == dst.Type() && !src.IsNil() {
				dst.Elem().Set(src.Elem())
				return datamodel
			}
			if src.Type().AssignableTo(dst.Elem().Type()) {
				dst.Elem().Set(src)
				return datamodel
			}
		}
	case reflect.Map:
		if !dst.IsNil() && src.Type() == dst.Type() {
			dst.Clear()
			for _, key := range src.MapKeys() {
				dst.SetMapIndex(key, src.MapIndex(key))
			}
			return datamodel
		}
	case reflect.Slice:
		// A slice header passed through an interface is not settable. It is
		// nevertheless fully observable when its length already matches.
		if !dst.IsNil() && src.Type() == dst.Type() && dst.Len() == src.Len() {
			reflect.Copy(dst, src)
			return datamodel
		}
	}
	// Non-pointer scalars (and shapes that cannot be replaced through the
	// interface) are explicitly allowed to use the decoded value.
	return decoded
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
