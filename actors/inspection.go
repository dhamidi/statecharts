package actors

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dhamidi/statecharts"
)

const (
	// DefaultActorPageLimit is used when ActorQuery.Limit is zero.
	DefaultActorPageLimit = 50
	// MaxActorPageLimit is the largest accepted actor directory or history page.
	MaxActorPageLimit = 200
)

const (
	residencyPagedOutCode uint32 = iota
	residencyHydratingCode
	residencyResidentCode
)

// ActorInfo is a non-activating directory view of an actor. Zero times mean
// unknown. Adopted means the current process has a routing entry; it does not
// mean that the actor is resident.
type ActorInfo struct {
	ID           ActorID
	Address      statecharts.Identifier
	Node         string
	Kind         statecharts.Identifier
	Revision     statecharts.RevisionID
	SessionID    statecharts.SessionID
	Durable      bool
	Adopted      bool
	Lifecycle    statecharts.ActorLifecycle
	Residency    ResidencyState
	StartedAt    time.Time
	TerminalAt   time.Time
	LastActiveAt time.Time
}

// ActorCursor is an opaque exclusive position in actor-ID order.
type ActorCursor = statecharts.ActorMetadataCursor

// ActorQuery selects actors. After is exclusive and all filters are combined.
type ActorQuery struct {
	After     ActorCursor
	Limit     int
	IDPrefix  string
	Kind      statecharts.Identifier
	Revision  statecharts.RevisionID
	Durable   *bool
	Lifecycle statecharts.ActorLifecycle
	Residency ResidencyState
}

// ActorPage is one independently owned page in actor-ID order. Next is empty
// when no later matching actor was observed.
type ActorPage struct {
	Actors []ActorInfo
	Next   ActorCursor
}

func residencyCode(state ResidencyState) uint32 {
	switch state {
	case ResidencyHydrating:
		return residencyHydratingCode
	case ResidencyResident:
		return residencyResidentCode
	default:
		return residencyPagedOutCode
	}
}
func residencyState(code uint32) ResidencyState {
	switch code {
	case residencyHydratingCode:
		return ResidencyHydrating
	case residencyResidentCode:
		return ResidencyResident
	default:
		return ResidencyPagedOut
	}
}

func normalizeActorQuery(q ActorQuery) (ActorQuery, statecharts.Identifier, error) {
	if q.Limit == 0 {
		q.Limit = DefaultActorPageLimit
	}
	if q.Limit < 1 || q.Limit > MaxActorPageLimit {
		return q, "", fmt.Errorf("actors: query limit must be between 1 and %d", MaxActorPageLimit)
	}
	_, after, err := (statecharts.ActorMetadataQuery{After: q.After, Limit: q.Limit, ChartID: q.Kind, Revision: q.Revision, Lifecycle: q.Lifecycle}).Validate()
	if err != nil {
		return q, "", err
	}
	if q.Residency != "" && q.Residency != ResidencyPagedOut && q.Residency != ResidencyHydrating && q.Residency != ResidencyResident {
		return q, "", fmt.Errorf("actors: unknown residency %q", q.Residency)
	}
	return q, after, nil
}

func (s *System) localInfosAfter(ctx context.Context, after ActorID, q ActorQuery) ([]ActorInfo, error) {
	s.tableMu.Lock()
	if s.actorIDsDirty {
		sort.Slice(s.actorIDs, func(i, j int) bool { return s.actorIDs[i] < s.actorIDs[j] })
		s.actorIDsDirty = false
	}
	start := sort.Search(len(s.actorIDs), func(i int) bool { return s.actorIDs[i] > after })
	infos := make([]ActorInfo, 0, q.Limit+1)
	for _, id := range s.actorIDs[start:] {
		if err := ctx.Err(); err != nil {
			s.tableMu.Unlock()
			return nil, err
		}
		e := s.table[id]
		if e.initialized.Load() {
			info := s.infoForEntry(e)
			if actorMatches(info, q) {
				infos = append(infos, info)
				if len(infos) == q.Limit+1 {
					break
				}
			}
		}
	}
	s.tableMu.Unlock()
	return infos, nil
}

func (s *System) infoForEntry(e *actorEntry) ActorInfo {
	lifecycle := statecharts.ActorLifecycleActive
	var terminal time.Time
	if e.terminal.Load() {
		lifecycle = statecharts.ActorLifecycleTerminal
		if n := e.terminalAt.Load(); n != 0 {
			terminal = time.Unix(0, n).UTC()
		}
	}
	var active time.Time
	if n := e.lastActive.Load(); n != 0 {
		active = time.Unix(0, n).UTC()
	}
	return ActorInfo{ID: e.name, Address: s.address(e.name), Node: s.cfg.nodeName, Kind: e.kind, Revision: e.revision, SessionID: e.sessionID, Durable: e.durable, Adopted: true, Lifecycle: lifecycle, Residency: residencyState(e.residency.Load()), StartedAt: e.startedAt.UTC(), TerminalAt: terminal, LastActiveAt: active}
}

func (s *System) infoForMetadata(m statecharts.ActorMetadata) ActorInfo {
	return ActorInfo{ID: m.ActorID, Address: s.address(m.ActorID), Node: s.cfg.nodeName, Kind: m.ChartID, Revision: m.Revision, SessionID: m.SessionID, Durable: true, Lifecycle: m.Lifecycle, Residency: ResidencyPagedOut, StartedAt: m.StartedAt.UTC(), TerminalAt: m.TerminalAt.UTC()}
}

func actorMatches(i ActorInfo, q ActorQuery) bool {
	return strings.HasPrefix(string(i.ID), q.IDPrefix) && (q.Kind == "" || i.Kind == q.Kind) && (q.Revision == "" || i.Revision == q.Revision) && (q.Durable == nil || i.Durable == *q.Durable) && (q.Lifecycle == "" || i.Lifecycle == q.Lifecycle) && (q.Residency == "" || i.Residency == q.Residency)
}

// QueryActors merges process-local actors with durable metadata without
// activation. Results are ordered by ID; Next is non-empty exactly when a
// further matching result was observed. The cursor is exclusive.
func (s *System) QueryActors(ctx context.Context, query ActorQuery) (ActorPage, error) {
	if err := ctx.Err(); err != nil {
		return ActorPage{}, err
	}
	q, after, err := normalizeActorQuery(query)
	if err != nil {
		return ActorPage{}, err
	}
	local, err := s.localInfosAfter(ctx, after, q)
	if err != nil {
		return ActorPage{}, err
	}
	merged := make(map[ActorID]ActorInfo, len(local))
	for _, info := range local {
		merged[info.ID] = info
	}
	// Resident and hydrating are necessarily process-local. Avoid touching
	// storage for filters that cannot possibly match storage-only metadata.
	if s.cfg.storage != nil && (q.Durable == nil || *q.Durable) && q.Residency != ResidencyResident && q.Residency != ResidencyHydrating {
		cursor := q.After
		for {
			if err := ctx.Err(); err != nil {
				return ActorPage{}, err
			}
			page, err := s.cfg.storage.QueryActors(ctx, statecharts.ActorMetadataQuery{After: cursor, Limit: MaxActorPageLimit, ActorIDPrefix: q.IDPrefix, ChartID: q.Kind, Revision: q.Revision, Lifecycle: q.Lifecycle})
			if err != nil {
				return ActorPage{}, err
			}
			for _, m := range page.Actors {
				info := s.infoForMetadata(m)
				if entry, ok := s.resolve(m.ActorID); ok {
					info = s.infoForEntry(entry)
				}
				if actorMatches(info, q) {
					merged[m.ActorID] = info
				}
			}
			ids := make([]ActorID, 0, len(merged))
			for id := range merged {
				ids = append(ids, id)
			}
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			if page.Next == "" || (len(ids) > q.Limit && len(page.Actors) > 0 && page.Actors[len(page.Actors)-1].ActorID >= ids[q.Limit]) {
				break
			}
			cursor = page.Next
		}
	}
	ids := make([]ActorID, 0, len(merged))
	for id := range merged {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	page := ActorPage{}
	n := len(ids)
	if n > q.Limit {
		n = q.Limit
		page.Next = statecharts.ActorMetadataCursorFor(ids[n-1])
	}
	for _, id := range ids[:n] {
		page.Actors = append(page.Actors, merged[id])
	}
	return page, nil
}

// actorMetadata performs an exact, metadata-only lookup. It never asks an
// Instance or datamodel for live state and never activates an actor.
func (s *System) actorMetadata(ctx context.Context, id ActorID) (ActorInfo, error) {
	if err := ctx.Err(); err != nil {
		return ActorInfo{}, err
	}
	if e, ok := s.resolve(id); ok {
		return s.infoForEntry(e), nil
	}
	if s.cfg.storage != nil {
		m, found, err := s.cfg.storage.GetActor(ctx, id)
		if err != nil {
			return ActorInfo{}, err
		}
		if found {
			return s.infoForMetadata(m), nil
		}
	}
	return ActorInfo{}, fmt.Errorf("actors: inspect %q: %w", id, ErrUnknownActor)
}

// InspectActor returns metadata and, only when already resident, a stable live view.
func (s *System) InspectActor(ctx context.Context, id ActorID) (ActorInfo, *statecharts.InstanceInspection, error) {
	if e, ok := s.resolve(id); ok {
		info := s.infoForEntry(e)
		inst := e.instance.Load()
		if inst == nil || info.Residency != ResidencyResident {
			return s.infoForEntry(e), nil, nil
		}
		inspection, err := inst.Inspect(ctx)
		if err != nil {
			if errors.Is(err, statecharts.ErrInstanceStopped) {
				return s.infoForEntry(e), nil, nil
			}
			return info, nil, err
		}
		return info, &inspection, nil
	}
	info, err := s.actorMetadata(ctx, id)
	return info, nil, err
}

// ActorDefinition contains one actor's pinned definition and, when registered,
// the revision that future actors of the same kind currently select.
type ActorDefinition struct {
	Pinned           statecharts.Definition
	PinnedRevision   statecharts.RevisionID
	Current          statecharts.Definition
	CurrentRevision  statecharts.RevisionID
	CurrentAvailable bool
}

// InspectActorDefinition resolves the pinned artifact without compiling or
// activating it. It never asks the resident actor's datamodel for live state.
func (s *System) InspectActorDefinition(ctx context.Context, id ActorID) (ActorDefinition, error) {
	info, err := s.actorMetadata(ctx, id)
	if err != nil {
		return ActorDefinition{}, err
	}
	var pinned statecharts.Definition
	if info.Durable {
		if s.cfg.storage == nil {
			return ActorDefinition{}, statecharts.ErrDefinitionNotFound
		}
		a, found, e := s.cfg.storage.GetDefinition(ctx, info.Revision)
		if e != nil {
			return ActorDefinition{}, e
		}
		if !found {
			return ActorDefinition{}, statecharts.ErrDefinitionNotFound
		}
		if a.ChartID != info.Kind {
			return ActorDefinition{}, fmt.Errorf("%w: pinned artifact chart %q does not match actor chart %q", statecharts.ErrInvalidDefinitionArtifact, a.ChartID, info.Kind)
		}
		pinned, e = a.Definition()
		if e != nil {
			return ActorDefinition{}, e
		}
	} else if d, ok := s.Definition(info.Kind, info.Revision); ok {
		pinned = d
	} else {
		return ActorDefinition{}, statecharts.ErrDefinitionNotFound
	}
	result := ActorDefinition{Pinned: pinned.Clone(), PinnedRevision: info.Revision}
	result.Current, result.CurrentRevision, result.CurrentAvailable = s.CurrentDefinition(info.Kind)
	result.Current = result.Current.Clone()
	return result, nil
}

// ErrHistoryUnavailable means an actor has no configured durable event log.
var ErrHistoryUnavailable = errors.New("actors: durable history unavailable")

// ErrInvalidHistoryQuery means history options contradict one another or a
// sequence cursor cannot have a successor.
var ErrInvalidHistoryQuery = errors.New("actors: invalid history query")

// ActorHistoryQuery selects durable input history. After is an exclusive
// sequence cursor. Tail returns the latest Limit entries and cannot be
// combined with After. Limit defaults to 50 and is bounded at 200.
type ActorHistoryQuery struct {
	After uint64
	Limit int
	Tail  bool
}

// ActorHistoryPage owns its entries and their event Values. Next is the last
// returned sequence only when another forward entry was observed.
type ActorHistoryPage struct {
	Entries []statecharts.LogEntry
	Next    uint64
}

// QueryActorHistory reads durable inputs without adopting or activating an
// actor. It never asks the resident actor's datamodel for live state.
func (s *System) QueryActorHistory(ctx context.Context, id ActorID, query ActorHistoryQuery) (ActorHistoryPage, error) {
	if query.Tail && query.After != 0 {
		return ActorHistoryPage{}, fmt.Errorf("%w: Tail and After are mutually exclusive", ErrInvalidHistoryQuery)
	}
	if query.After == ^uint64(0) {
		return ActorHistoryPage{}, fmt.Errorf("%w: After has no successor", ErrInvalidHistoryQuery)
	}
	if query.Limit == 0 {
		query.Limit = DefaultActorPageLimit
	}
	if query.Limit < 1 || query.Limit > MaxActorPageLimit {
		return ActorHistoryPage{}, fmt.Errorf("actors: history limit must be between 1 and %d", MaxActorPageLimit)
	}
	info, err := s.actorMetadata(ctx, id)
	if err != nil {
		return ActorHistoryPage{}, err
	}
	if !info.Durable || s.cfg.storage == nil {
		return ActorHistoryPage{}, ErrHistoryUnavailable
	}
	from := query.After + 1
	if query.Tail {
		last, e := s.cfg.storage.LastSeq(ctx, info.SessionID)
		if e != nil {
			return ActorHistoryPage{}, e
		}
		if last > uint64(query.Limit) {
			from = last - uint64(query.Limit) + 1
		} else {
			from = 1
		}
	}
	result := ActorHistoryPage{}
	for entry, e := range s.cfg.storage.Read(ctx, info.SessionID, from) {
		if e != nil {
			return ActorHistoryPage{}, e
		}
		entry.Event.Data = entry.Event.Data.Clone()
		result.Entries = append(result.Entries, entry)
		if len(result.Entries) > query.Limit {
			result.Entries = result.Entries[:query.Limit]
			result.Next = result.Entries[len(result.Entries)-1].Seq
			break
		}
	}
	return result, nil
}
