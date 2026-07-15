package storagetest

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/dhamidi/statecharts"
)

type beginFaultStage string

const (
	faultAfterActorStaged  beginFaultStage = "after actor staged"
	faultAfterStartStaged  beginFaultStage = "after session start staged"
	faultBeforeBeginCommit beginFaultStage = "before begin commit"
)

// MemoryStore is a concurrent in-memory implementation of Store. It is
// intended for tests and process-local prototypes, not durable deployments.
type MemoryStore struct {
	mu sync.Mutex

	definitions   map[statecharts.RevisionID]statecharts.DefinitionArtifact
	actors        map[statecharts.Identifier]statecharts.ActorMetadata
	logs          map[statecharts.SessionID][]statecharts.LogEntry
	ingress       map[statecharts.SessionID]map[statecharts.DeliveryID]struct{}
	outbounds     map[statecharts.SessionID]map[statecharts.DeliveryID]statecharts.OutboundMessage
	outboundOrder map[statecharts.SessionID][]statecharts.DeliveryID
	snapshots     map[statecharts.SessionID]statecharts.Checkpoint

	beginFault func(beginFaultStage) error
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		definitions:   make(map[statecharts.RevisionID]statecharts.DefinitionArtifact),
		actors:        make(map[statecharts.Identifier]statecharts.ActorMetadata),
		logs:          make(map[statecharts.SessionID][]statecharts.LogEntry),
		ingress:       make(map[statecharts.SessionID]map[statecharts.DeliveryID]struct{}),
		outbounds:     make(map[statecharts.SessionID]map[statecharts.DeliveryID]statecharts.OutboundMessage),
		outboundOrder: make(map[statecharts.SessionID][]statecharts.DeliveryID),
		snapshots:     make(map[statecharts.SessionID]statecharts.Checkpoint),
	}
}

func (s *MemoryStore) PutDefinition(ctx context.Context, artifact statecharts.DefinitionArtifact) (statecharts.DefinitionPutResult, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if stored, exists := s.definitions[artifact.Revision]; exists {
		if stored.Equal(artifact) {
			if err := stored.Validate(); err != nil {
				return 0, err
			}
			return statecharts.DefinitionUnchanged, nil
		}
		return 0, statecharts.ErrDefinitionCollision
	}
	if err := artifact.Validate(); err != nil {
		return 0, err
	}
	s.definitions[artifact.Revision] = artifact.Clone()
	return statecharts.DefinitionStored, nil
}

func (s *MemoryStore) GetDefinition(ctx context.Context, revision statecharts.RevisionID) (statecharts.DefinitionArtifact, bool, error) {
	if err := ctx.Err(); err != nil {
		return statecharts.DefinitionArtifact{}, false, err
	}
	s.mu.Lock()
	artifact, exists := s.definitions[revision]
	artifact = artifact.Clone()
	s.mu.Unlock()
	if !exists {
		return statecharts.DefinitionArtifact{}, false, nil
	}
	if err := artifact.Validate(); err != nil {
		return statecharts.DefinitionArtifact{}, false, err
	}
	return artifact, true, nil
}

func (s *MemoryStore) DeleteDefinitionIfUnreferenced(ctx context.Context, revision statecharts.RevisionID) (statecharts.DefinitionDeleteResult, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.definitions[revision]; !exists {
		return statecharts.DefinitionNotFound, nil
	}
	for _, actor := range s.actors {
		if actor.Revision != revision {
			continue
		}
		if err := s.validateStoredActorLocked(actor); err != nil {
			return 0, err
		}
		if actor.Lifecycle == statecharts.ActorLifecycleActive {
			return statecharts.DefinitionReferenced, nil
		}
	}
	delete(s.definitions, revision)
	return statecharts.DefinitionDeleted, nil
}

func (s *MemoryStore) BeginActor(ctx context.Context, metadata statecharts.ActorMetadata) (statecharts.ActorMetadata, statecharts.ActorBeginResult, error) {
	if err := ctx.Err(); err != nil {
		return statecharts.ActorMetadata{}, 0, err
	}
	if _, err := statecharts.NewIdentifier(string(metadata.ActorID)); err != nil {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("%w: actor ID: %v", statecharts.ErrInvalidActorMetadata, err)
	}
	if err := metadata.Validate(); err != nil {
		return statecharts.ActorMetadata{}, 0, err
	}
	if metadata.Lifecycle != statecharts.ActorLifecycleActive {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("%w: begin lifecycle is %q, want %q", statecharts.ErrInvalidActorMetadata, metadata.Lifecycle, statecharts.ActorLifecycleActive)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if stored, exists := s.actors[metadata.ActorID]; exists {
		if err := s.validateStoredActorLocked(stored); err != nil {
			return statecharts.ActorMetadata{}, 0, err
		}
		if stored.Lifecycle == statecharts.ActorLifecycleTerminal {
			return stored, 0, statecharts.ErrActorTerminal
		}
		if stored.ChartID != metadata.ChartID || stored.Revision != metadata.Revision || stored.SessionID != metadata.SessionID || stored.Durable != metadata.Durable {
			return stored, 0, statecharts.ErrActorCollision
		}
		artifact, exists := s.definitions[stored.Revision]
		if !exists {
			return stored, 0, statecharts.ErrDefinitionNotFound
		}
		if err := artifact.Validate(); err != nil {
			return stored, 0, err
		}
		return stored, statecharts.ActorAlreadyActive, nil
	}
	artifact, exists := s.definitions[metadata.Revision]
	if !exists {
		return statecharts.ActorMetadata{}, 0, statecharts.ErrDefinitionNotFound
	}
	if err := artifact.Validate(); err != nil {
		return statecharts.ActorMetadata{}, 0, err
	}
	if artifact.ChartID != metadata.ChartID {
		return statecharts.ActorMetadata{}, 0, statecharts.ErrActorCollision
	}
	if len(s.logs[metadata.SessionID]) != 0 {
		return statecharts.ActorMetadata{}, 0, statecharts.ErrActorCollision
	}

	stagedActor := metadata
	stagedActor.StartedAt = stagedActor.StartedAt.UTC()
	if err := s.failBegin(faultAfterActorStaged); err != nil {
		return statecharts.ActorMetadata{}, 0, err
	}
	stagedEntries := []statecharts.LogEntry{{
		SessionID: stagedActor.SessionID,
		Seq:       1,
		Kind:      statecharts.KindSessionStarted,
		Timestamp: stagedActor.StartedAt,
	}}
	if err := s.failBegin(faultAfterStartStaged); err != nil {
		return statecharts.ActorMetadata{}, 0, err
	}
	if err := s.failBegin(faultBeforeBeginCommit); err != nil {
		return statecharts.ActorMetadata{}, 0, err
	}
	s.actors[stagedActor.ActorID] = stagedActor
	s.logs[stagedActor.SessionID] = stagedEntries
	return stagedActor, statecharts.ActorStarted, nil
}

func (s *MemoryStore) failBegin(stage beginFaultStage) error {
	if s.beginFault != nil {
		return s.beginFault(stage)
	}
	return nil
}

func (s *MemoryStore) GetActor(ctx context.Context, actorID statecharts.Identifier) (statecharts.ActorMetadata, bool, error) {
	if err := ctx.Err(); err != nil {
		return statecharts.ActorMetadata{}, false, err
	}
	s.mu.Lock()
	metadata, exists := s.actors[actorID]
	if !exists {
		s.mu.Unlock()
		return statecharts.ActorMetadata{}, false, nil
	}
	if err := s.validateStoredActorLocked(metadata); err != nil {
		s.mu.Unlock()
		return statecharts.ActorMetadata{}, false, err
	}
	s.mu.Unlock()
	return metadata, true, nil
}

func (s *MemoryStore) ListNonTerminalActors(ctx context.Context) ([]statecharts.ActorMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]statecharts.ActorMetadata, 0, len(s.actors))
	for _, actor := range s.actors {
		if err := s.validateStoredActorLocked(actor); err != nil {
			return nil, err
		}
		if actor.Lifecycle == statecharts.ActorLifecycleActive {
			result = append(result, actor)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ActorID < result[j].ActorID })
	return result, nil
}

func (s *MemoryStore) MarkActorTerminal(ctx context.Context, actorID statecharts.Identifier, terminalAt time.Time) (statecharts.ActorMetadata, statecharts.ActorTerminalResult, error) {
	if err := ctx.Err(); err != nil {
		return statecharts.ActorMetadata{}, 0, err
	}
	if _, err := statecharts.NewIdentifier(string(actorID)); err != nil {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("%w: actor ID: %v", statecharts.ErrInvalidActorMetadata, err)
	}
	if terminalAt.IsZero() {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("%w: terminal time is zero", statecharts.ErrInvalidActorMetadata)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	metadata, exists := s.actors[actorID]
	if !exists {
		return statecharts.ActorMetadata{}, statecharts.ActorNotFound, nil
	}
	if err := s.validateStoredActorLocked(metadata); err != nil {
		return statecharts.ActorMetadata{}, 0, err
	}
	if metadata.Lifecycle == statecharts.ActorLifecycleTerminal {
		return metadata, statecharts.ActorAlreadyTerminal, nil
	}
	if terminalAt.Before(metadata.StartedAt) {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("%w: terminal time precedes start time", statecharts.ErrInvalidActorMetadata)
	}
	metadata.Lifecycle = statecharts.ActorLifecycleTerminal
	metadata.TerminalAt = terminalAt.UTC()
	s.actors[actorID] = metadata
	return metadata, statecharts.ActorMarkedTerminal, nil
}

func (s *MemoryStore) ReferencedRevisions(ctx context.Context) ([]statecharts.RevisionID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	set := make(map[statecharts.RevisionID]struct{})
	for _, actor := range s.actors {
		if err := s.validateStoredActorLocked(actor); err != nil {
			return nil, err
		}
		if actor.Lifecycle == statecharts.ActorLifecycleActive {
			set[actor.Revision] = struct{}{}
		}
	}
	result := make([]statecharts.RevisionID, 0, len(set))
	for revision := range set {
		result = append(result, revision)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result, nil
}

func (s *MemoryStore) validateStoredActorLocked(actor statecharts.ActorMetadata) error {
	if err := actor.Validate(); err != nil {
		return err
	}
	entries := s.logs[actor.SessionID]
	if len(entries) == 0 {
		return fmt.Errorf("%w: actor %q has no session-start record", statecharts.ErrInvalidActorMetadata, actor.ActorID)
	}
	startCount := 0
	for _, entry := range entries {
		if entry.Kind == statecharts.KindSessionStarted {
			startCount++
		}
	}
	start := entries[0]
	if startCount != 1 || start.Seq != 1 || start.Kind != statecharts.KindSessionStarted || start.SessionID != actor.SessionID || !start.Timestamp.Equal(actor.StartedAt) || !reflect.DeepEqual(start.Event, statecharts.Event{}) || start.SendID != "" || start.Target != "" || start.Type != "" {
		return fmt.Errorf("%w: actor %q has an inconsistent session-start record", statecharts.ErrInvalidActorMetadata, actor.ActorID)
	}
	if actor.Lifecycle == statecharts.ActorLifecycleActive {
		artifact, exists := s.definitions[actor.Revision]
		if !exists {
			return statecharts.ErrDefinitionNotFound
		}
		if err := artifact.Validate(); err != nil {
			return err
		}
		if artifact.ChartID != actor.ChartID {
			return fmt.Errorf("%w: actor %q chart identity does not match revision", statecharts.ErrInvalidActorMetadata, actor.ActorID)
		}
	}
	return nil
}

func (s *MemoryStore) Append(ctx context.Context, entry statecharts.LogEntry) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendLocked(entry), nil
}

func (s *MemoryStore) appendLocked(entry statecharts.LogEntry) uint64 {
	entries := s.logs[entry.SessionID]
	entry = cloneLogEntry(entry)
	entry.Seq = uint64(len(entries) + 1)
	s.logs[entry.SessionID] = append(entries, entry)
	return entry.Seq
}

func (s *MemoryStore) Read(ctx context.Context, sessionID statecharts.SessionID, from uint64) iter.Seq2[statecharts.LogEntry, error] {
	return func(yield func(statecharts.LogEntry, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(statecharts.LogEntry{}, err)
			return
		}
		s.mu.Lock()
		entries := make([]statecharts.LogEntry, 0, len(s.logs[sessionID]))
		for _, entry := range s.logs[sessionID] {
			if entry.Seq >= from {
				entries = append(entries, cloneLogEntry(entry))
			}
		}
		s.mu.Unlock()
		for _, entry := range entries {
			if !yield(entry, nil) {
				return
			}
		}
	}
}

func (s *MemoryStore) LastSeq(ctx context.Context, sessionID statecharts.SessionID) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := s.logs[sessionID]
	if len(entries) == 0 {
		return 0, nil
	}
	return entries[len(entries)-1].Seq, nil
}

func (s *MemoryStore) AppendIngress(ctx context.Context, entry statecharts.LogEntry, deliveryID statecharts.DeliveryID) (uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	deliveries := s.ingress[entry.SessionID]
	if deliveries == nil {
		deliveries = make(map[statecharts.DeliveryID]struct{})
		s.ingress[entry.SessionID] = deliveries
	}
	if _, exists := deliveries[deliveryID]; exists {
		return 0, false, nil
	}
	seq := s.appendLocked(entry)
	deliveries[deliveryID] = struct{}{}
	return seq, true, nil
}

func (s *MemoryStore) StoreOutbound(ctx context.Context, message statecharts.OutboundMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	message = cloneOutbound(message)
	s.mu.Lock()
	defer s.mu.Unlock()
	messages := s.outbounds[message.SessionID]
	if messages == nil {
		messages = make(map[statecharts.DeliveryID]statecharts.OutboundMessage)
		s.outbounds[message.SessionID] = messages
	}
	if existing, exists := messages[message.DeliveryID]; exists {
		if reflect.DeepEqual(existing, message) {
			return nil
		}
		return statecharts.ErrOutboundCollision
	}
	messages[message.DeliveryID] = message
	s.outboundOrder[message.SessionID] = append(s.outboundOrder[message.SessionID], message.DeliveryID)
	return nil
}

func (s *MemoryStore) ResolveOutbound(ctx context.Context, sessionID statecharts.SessionID, deliveryID statecharts.DeliveryID, result statecharts.OutboundResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	message, exists := s.outbounds[sessionID][deliveryID]
	if !exists {
		return fmt.Errorf("storagetest: outbound %q not found", deliveryID)
	}
	message.Status = statecharts.OutboundResolved
	message.Result = result
	s.outbounds[sessionID][deliveryID] = message
	return nil
}

func (s *MemoryStore) Outbounds(ctx context.Context, sessionID statecharts.SessionID) ([]statecharts.OutboundMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]statecharts.OutboundMessage, 0, len(s.outboundOrder[sessionID]))
	for _, deliveryID := range s.outboundOrder[sessionID] {
		result = append(result, cloneOutbound(s.outbounds[sessionID][deliveryID]))
	}
	return result, nil
}

func (s *MemoryStore) Save(ctx context.Context, sessionID statecharts.SessionID, checkpoint statecharts.Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	clone, err := cloneCheckpoint(checkpoint)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.snapshots[sessionID] = clone
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Load(ctx context.Context, sessionID statecharts.SessionID) (statecharts.Checkpoint, bool, error) {
	if err := ctx.Err(); err != nil {
		return statecharts.Checkpoint{}, false, err
	}
	s.mu.Lock()
	checkpoint, exists := s.snapshots[sessionID]
	s.mu.Unlock()
	if !exists {
		return statecharts.Checkpoint{}, false, nil
	}
	clone, err := cloneCheckpoint(checkpoint)
	return clone, true, err
}

func cloneLogEntry(entry statecharts.LogEntry) statecharts.LogEntry {
	entry.Event.Data = entry.Event.Data.Clone()
	return entry
}

func cloneOutbound(message statecharts.OutboundMessage) statecharts.OutboundMessage {
	message.Request.Data = message.Request.Data.Clone()
	return message
}

func cloneCheckpoint(checkpoint statecharts.Checkpoint) (statecharts.Checkpoint, error) {
	wire, err := json.Marshal(checkpoint.Snapshot)
	if err != nil {
		return statecharts.Checkpoint{}, err
	}
	var snapshot statecharts.Snapshot
	if err := json.Unmarshal(wire, &snapshot); err != nil {
		return statecharts.Checkpoint{}, err
	}
	return statecharts.Checkpoint{Snapshot: snapshot, Seq: checkpoint.Seq}, nil
}
