package storagetest

import (
	"context"
	"errors"
	"testing"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
)

var _ actors.Storage = (*MemoryStore)(nil)

func TestMemoryStoreConformance(t *testing.T) {
	Run(t, func(*testing.T) Store { return NewMemoryStore() })
}

func TestMemoryStoreBeginFailureNeverPublishesHalfStart(t *testing.T) {
	for _, stage := range []beginFaultStage{faultAfterActorStaged, faultAfterStartStaged, faultBeforeBeginCommit} {
		t.Run(string(stage), func(t *testing.T) {
			store := NewMemoryStore()
			artifact := fixtureArtifact(t, "counter", "v1")
			putArtifact(t, store, artifact)
			actor := fixtureActor(artifact, "red")
			injected := errors.New("injected begin failure")
			store.beginFault = func(got beginFaultStage) error {
				if got == stage {
					return injected
				}
				return nil
			}
			if _, _, err := store.BeginActor(context.Background(), actor); !errors.Is(err, injected) {
				t.Fatalf("BeginActor error = %v", err)
			}
			if _, ok, err := store.GetActor(context.Background(), actor.ActorID); err != nil || ok {
				t.Fatalf("actor visible after failure = ok %v, err %v", ok, err)
			}
			if seq, err := store.LastSeq(context.Background(), actor.SessionID); err != nil || seq != 0 {
				t.Fatalf("session start visible after failure = seq %d, err %v", seq, err)
			}
		})
	}
}

func TestMemoryStoreRejectsCorruptReferencedArtifactBeforeActorStart(t *testing.T) {
	store := NewMemoryStore()
	artifact := fixtureArtifact(t, "counter", "v1")
	putArtifact(t, store, artifact)
	store.mu.Lock()
	corrupt := store.definitions[artifact.Revision]
	corrupt.CanonicalDefinition[0] ^= 0xff
	store.definitions[artifact.Revision] = corrupt
	store.mu.Unlock()
	actor := fixtureActor(artifact, "red")
	if _, _, err := store.BeginActor(context.Background(), actor); !errors.Is(err, statecharts.ErrInvalidDefinitionArtifact) {
		t.Fatalf("BeginActor error = %v, want ErrInvalidDefinitionArtifact", err)
	}
	if _, ok, err := store.GetActor(context.Background(), actor.ActorID); err != nil || ok {
		t.Fatalf("actor visible after corrupt definition = ok %v, err %v", ok, err)
	}
}

func TestMemoryStoreRejectsCorruptActorBeforeReferenceRelease(t *testing.T) {
	store := NewMemoryStore()
	artifact := fixtureArtifact(t, "counter", "v1")
	putArtifact(t, store, artifact)
	actor := fixtureActor(artifact, "red")
	if _, _, err := store.BeginActor(context.Background(), actor); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	corrupt := store.actors[actor.ActorID]
	corrupt.Lifecycle = "corrupt"
	store.actors[actor.ActorID] = corrupt
	store.mu.Unlock()
	if _, err := store.DeleteDefinitionIfUnreferenced(context.Background(), artifact.Revision); !errors.Is(err, statecharts.ErrInvalidActorMetadata) {
		t.Fatalf("DeleteDefinitionIfUnreferenced error = %v, want ErrInvalidActorMetadata", err)
	}
	if _, ok, err := store.GetDefinition(context.Background(), artifact.Revision); err != nil || !ok {
		t.Fatalf("definition after rejected deletion = ok %v, err %v", ok, err)
	}
}

func TestMemoryStoreRejectsMissingSessionStartBeforeActorReadOrRetry(t *testing.T) {
	store := NewMemoryStore()
	artifact := fixtureArtifact(t, "counter", "v1")
	putArtifact(t, store, artifact)
	actor := fixtureActor(artifact, "red")
	if _, _, err := store.BeginActor(context.Background(), actor); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	delete(store.logs, actor.SessionID)
	store.mu.Unlock()
	if _, _, err := store.GetActor(context.Background(), actor.ActorID); !errors.Is(err, statecharts.ErrInvalidActorMetadata) {
		t.Fatalf("GetActor error = %v, want ErrInvalidActorMetadata", err)
	}
	if _, _, err := store.BeginActor(context.Background(), actor); !errors.Is(err, statecharts.ErrInvalidActorMetadata) {
		t.Fatalf("BeginActor error = %v, want ErrInvalidActorMetadata", err)
	}
}
