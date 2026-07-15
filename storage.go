package statecharts

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// DefinitionPutResult describes an idempotent immutable artifact write.
type DefinitionPutResult uint8

const (
	DefinitionStored DefinitionPutResult = iota + 1
	DefinitionUnchanged
)

// DefinitionDeleteResult describes an atomic reference-check-and-delete.
type DefinitionDeleteResult uint8

const (
	DefinitionDeleted DefinitionDeleteResult = iota + 1
	DefinitionReferenced
	DefinitionNotFound
)

// DefinitionStore persists immutable, content-addressed chart definitions.
// Implementations must copy caller-owned bytes on writes and returns.
type DefinitionStore interface {
	// PutDefinition validates artifact and stores it when its revision is
	// absent. An exact retry returns DefinitionUnchanged. If the revision is
	// already associated with any different field or byte, it returns
	// ErrDefinitionCollision without changing the stored artifact.
	PutDefinition(ctx context.Context, artifact DefinitionArtifact) (DefinitionPutResult, error)

	// GetDefinition returns a validated, independently owned artifact. Corrupt
	// stored data returns ErrInvalidDefinitionArtifact rather than found=true.
	GetDefinition(ctx context.Context, revision RevisionID) (artifact DefinitionArtifact, found bool, err error)

	// DeleteDefinitionIfUnreferenced atomically checks non-terminal actor
	// references and deletes revision only when none exist. It must linearize
	// with BeginActor on the same storage implementation. Publication state is
	// intentionally separate; callers must not request deletion of a current
	// revision.
	DeleteDefinitionIfUnreferenced(ctx context.Context, revision RevisionID) (DefinitionDeleteResult, error)
}

var (
	// ErrInvalidActorMetadata marks malformed durable actor identity, pin,
	// lifecycle data, or its required session-start boundary.
	ErrInvalidActorMetadata = errors.New("statecharts: invalid actor metadata")
	// ErrActorCollision marks reuse of an actor ID with a different chart,
	// revision, session, or durability.
	ErrActorCollision = errors.New("statecharts: actor identity collision")
	// ErrActorTerminal marks an attempt to begin an actor ID whose durable
	// lifecycle has already reached terminal.
	ErrActorTerminal = errors.New("statecharts: actor is terminal")
)

// ActorLifecycle is the durable lifecycle of a started actor. Absence from an
// ActorStore means never started; no never-started metadata row is persisted.
type ActorLifecycle string

const (
	ActorLifecycleActive   ActorLifecycle = "active"
	ActorLifecycleTerminal ActorLifecycle = "terminal"
)

// ActorMetadata is the authoritative durable revision pin for one actor ID.
// Ephemeral actors retain their Chart directly and are not written to an
// ActorStore; Durable remains explicit so identity retries cannot silently
// cross the durable/ephemeral boundary.
type ActorMetadata struct {
	ActorID    Identifier
	ChartID    Identifier
	Revision   RevisionID
	SessionID  SessionID
	Durable    bool
	Lifecycle  ActorLifecycle
	StartedAt  time.Time
	TerminalAt time.Time
}

// Validate checks active or terminal persisted metadata. BeginActor accepts
// only ActorLifecycleActive values.
func (m ActorMetadata) Validate() error {
	if _, err := NewIdentifier(string(m.ActorID)); err != nil {
		return fmt.Errorf("%w: actor ID: %v", ErrInvalidActorMetadata, err)
	}
	if _, err := NewIdentifier(string(m.ChartID)); err != nil {
		return fmt.Errorf("%w: chart ID: %v", ErrInvalidActorMetadata, err)
	}
	if m.Revision == "" {
		return fmt.Errorf("%w: revision is empty", ErrInvalidActorMetadata)
	}
	if m.SessionID == "" {
		return fmt.Errorf("%w: session ID is empty", ErrInvalidActorMetadata)
	}
	if !m.Durable {
		return fmt.Errorf("%w: durable is false", ErrInvalidActorMetadata)
	}
	if m.StartedAt.IsZero() {
		return fmt.Errorf("%w: start time is zero", ErrInvalidActorMetadata)
	}
	switch m.Lifecycle {
	case ActorLifecycleActive:
		if !m.TerminalAt.IsZero() {
			return fmt.Errorf("%w: terminal time is set for an active actor", ErrInvalidActorMetadata)
		}
	case ActorLifecycleTerminal:
		if m.TerminalAt.IsZero() {
			return fmt.Errorf("%w: terminal time is zero for a terminal actor", ErrInvalidActorMetadata)
		}
		if m.TerminalAt.Before(m.StartedAt) {
			return fmt.Errorf("%w: terminal time precedes start time", ErrInvalidActorMetadata)
		}
	case "":
		return fmt.Errorf("%w: lifecycle is empty", ErrInvalidActorMetadata)
	default:
		return fmt.Errorf("%w: lifecycle %q is unknown", ErrInvalidActorMetadata, m.Lifecycle)
	}
	return nil
}

// ActorBeginResult describes an idempotent atomic actor start.
type ActorBeginResult uint8

const (
	ActorStarted ActorBeginResult = iota + 1
	ActorAlreadyActive
)

// ActorTerminalResult describes an idempotent terminal transition.
type ActorTerminalResult uint8

const (
	ActorMarkedTerminal ActorTerminalResult = iota + 1
	ActorAlreadyTerminal
	ActorNotFound
)

// ActorStore persists durable actor revision pins and their lifecycle. The
// same concrete storage object must also implement Log and DefinitionStore so
// BeginActor and DeleteDefinitionIfUnreferenced can provide their documented
// cross-record atomicity. A SessionID is owned by exactly one ActorID. Its
// first log entry is the actor's sole KindSessionStarted record, at Seq 1 and
// the stored StartedAt; every actor read verifies that boundary before
// returning metadata.
type ActorStore interface {
	// BeginActor atomically stores metadata and appends exactly one
	// KindSessionStarted entry at metadata.StartedAt before any initial chart
	// behavior may run. It requires a valid matching DefinitionArtifact.
	// Exact identity retries return the originally stored metadata and
	// ActorAlreadyActive without another log entry. Different pins return
	// ErrActorCollision; terminal IDs return ErrActorTerminal. Reusing a
	// SessionID or beginning against a pre-populated session log also returns
	// ErrActorCollision.
	BeginActor(ctx context.Context, metadata ActorMetadata) (ActorMetadata, ActorBeginResult, error)

	// GetActor returns authoritative metadata regardless of residency or
	// terminal state. Absence means the actor was never started. Corrupt actor
	// metadata or a missing/inconsistent session-start record returns
	// ErrInvalidActorMetadata before replay can begin.
	GetActor(ctx context.Context, actorID Identifier) (ActorMetadata, bool, error)

	// ListNonTerminalActors returns all active durable actors in ActorID order.
	ListNonTerminalActors(ctx context.Context) ([]ActorMetadata, error)

	// MarkActorTerminal atomically releases actorID's revision reference. It
	// is idempotent and preserves the first terminal timestamp.
	MarkActorTerminal(ctx context.Context, actorID Identifier, terminalAt time.Time) (ActorMetadata, ActorTerminalResult, error)

	// ReferencedRevisions returns the sorted, deduplicated revisions pinned by
	// non-terminal durable actors.
	ReferencedRevisions(ctx context.Context) ([]RevisionID, error)
}
