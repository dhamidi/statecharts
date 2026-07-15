// Package storagetest provides a reusable storage conformance suite and an
// in-memory implementation for statecharts storage implementations.
package storagetest

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/dhamidi/statecharts"
)

// Store is the complete durable boundary exercised by Run. A factory must
// return one object implementing every capability so cross-capability
// atomicity can be verified.
type Store interface {
	statecharts.DurableLog
	statecharts.SnapshotStore
	statecharts.DefinitionStore
	statecharts.ActorStore
}

// Factory returns a fresh empty Store for one conformance subtest.
type Factory func(t *testing.T) Store

type fixtureModel struct{}

func fixtureArtifact(t *testing.T, chartID, salt string) statecharts.DefinitionArtifact {
	t.Helper()
	chart, err := statecharts.Build(
		statecharts.Atomic(statecharts.Identifier(chartID)),
		statecharts.NewGoModel(func() *fixtureModel { return &fixtureModel{} }),
		statecharts.WithRevisionSalt(salt),
	)
	if err != nil {
		t.Fatal(err)
	}
	return chart.DefinitionArtifact()
}

func fixtureActor(artifact statecharts.DefinitionArtifact, actorID string) statecharts.ActorMetadata {
	return statecharts.ActorMetadata{
		ActorID:   statecharts.Identifier(actorID),
		ChartID:   artifact.ChartID,
		Revision:  artifact.Revision,
		SessionID: statecharts.SessionID(actorID),
		Durable:   true,
		Lifecycle: statecharts.ActorLifecycleActive,
		StartedAt: time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC),
	}
}

func putArtifact(t *testing.T, store Store, artifact statecharts.DefinitionArtifact) {
	t.Helper()
	result, err := store.PutDefinition(context.Background(), artifact)
	if err != nil {
		t.Fatalf("PutDefinition: %v", err)
	}
	if result != statecharts.DefinitionStored {
		t.Fatalf("PutDefinition result = %v, want DefinitionStored", result)
	}
}

// Run executes the reusable DefinitionStore and ActorStore contract tests.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("definitions are immutable and collisions fail", func(t *testing.T) {
		store := factory(t)
		ctx := context.Background()
		artifact := fixtureArtifact(t, "counter", "v1")
		want := artifact.Clone()
		result, err := store.PutDefinition(ctx, artifact)
		if err != nil || result != statecharts.DefinitionStored {
			t.Fatalf("first PutDefinition = %v, %v", result, err)
		}
		artifact.CanonicalDefinition[0] ^= 0xff
		artifact.ProgramFingerprint[0] ^= 0xff
		got, ok, err := store.GetDefinition(ctx, want.Revision)
		if err != nil || !ok || !got.Equal(want) {
			t.Fatalf("GetDefinition = %#v, %v, %v", got, ok, err)
		}
		got.CanonicalDefinition[0] ^= 0xff
		got.ProgramFingerprint[0] ^= 0xff
		again, ok, err := store.GetDefinition(ctx, want.Revision)
		if err != nil || !ok || !again.Equal(want) {
			t.Fatalf("GetDefinition after output mutation = %#v, %v, %v", again, ok, err)
		}
		result, err = store.PutDefinition(ctx, want)
		if err != nil || result != statecharts.DefinitionUnchanged {
			t.Fatalf("duplicate PutDefinition = %v, %v", result, err)
		}
		collision := want.Clone()
		collision.CanonicalDefinition[len(collision.CanonicalDefinition)-1] ^= 1
		if _, err := store.PutDefinition(ctx, collision); !errors.Is(err, statecharts.ErrDefinitionCollision) {
			t.Fatalf("collision error = %v", err)
		}
		if _, ok, err := store.GetDefinition(ctx, "sha256:missing"); err != nil || ok {
			t.Fatalf("missing GetDefinition = ok %v, err %v", ok, err)
		}
		invalid := fixtureArtifact(t, "other", "v1")
		invalid.Revision = "sha256:invalid"
		if _, err := store.PutDefinition(ctx, invalid); !errors.Is(err, statecharts.ErrInvalidDefinitionArtifact) {
			t.Fatalf("invalid artifact error = %v", err)
		}
	})

	t.Run("concurrent identical definition puts are idempotent", func(t *testing.T) {
		store := factory(t)
		artifact := fixtureArtifact(t, "counter", "v1")
		const callers = 32
		results := make(chan statecharts.DefinitionPutResult, callers)
		errs := make(chan error, callers)
		var wg sync.WaitGroup
		for range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				result, err := store.PutDefinition(context.Background(), artifact)
				results <- result
				errs <- err
			}()
		}
		wg.Wait()
		close(results)
		close(errs)
		stored := 0
		for err := range errs {
			if err != nil {
				t.Fatalf("PutDefinition: %v", err)
			}
		}
		for result := range results {
			switch result {
			case statecharts.DefinitionStored:
				stored++
			case statecharts.DefinitionUnchanged:
			default:
				t.Fatalf("PutDefinition result = %v", result)
			}
		}
		if stored != 1 {
			t.Fatalf("DefinitionStored results = %d, want 1", stored)
		}
	})

	t.Run("actor begin atomically pins and appends session start", func(t *testing.T) {
		store := factory(t)
		ctx := context.Background()
		artifact := fixtureArtifact(t, "counter", "v1")
		actor := fixtureActor(artifact, "red")
		if _, _, err := store.BeginActor(ctx, actor); !errors.Is(err, statecharts.ErrDefinitionNotFound) {
			t.Fatalf("BeginActor missing definition error = %v", err)
		}
		if _, ok, err := store.GetActor(ctx, actor.ActorID); err != nil || ok {
			t.Fatalf("actor visible after failed begin = ok %v, err %v", ok, err)
		}
		if seq, err := store.LastSeq(ctx, actor.SessionID); err != nil || seq != 0 {
			t.Fatalf("LastSeq after failed begin = %d, %v", seq, err)
		}
		putArtifact(t, store, artifact)
		stored, result, err := store.BeginActor(ctx, actor)
		if err != nil || result != statecharts.ActorStarted {
			t.Fatalf("BeginActor = %#v, %v, %v", stored, result, err)
		}
		if stored.Lifecycle != statecharts.ActorLifecycleActive || stored.Revision != artifact.Revision {
			t.Fatalf("stored actor = %#v", stored)
		}
		var entries []statecharts.LogEntry
		for entry, err := range store.Read(ctx, actor.SessionID, 1) {
			if err != nil {
				t.Fatal(err)
			}
			entries = append(entries, entry)
		}
		if len(entries) != 1 || entries[0].Seq != 1 || entries[0].Kind != statecharts.KindSessionStarted || !entries[0].Timestamp.Equal(actor.StartedAt) {
			t.Fatalf("session start entries = %#v", entries)
		}
		seq, err := store.Append(ctx, statecharts.LogEntry{SessionID: actor.SessionID, Kind: statecharts.KindExternalEvent, Timestamp: actor.StartedAt.Add(time.Second)})
		if err != nil || seq != 2 {
			t.Fatalf("Append after begin = %d, %v", seq, err)
		}

		retry := actor
		retry.StartedAt = retry.StartedAt.Add(time.Hour)
		stored, result, err = store.BeginActor(ctx, retry)
		if err != nil || result != statecharts.ActorAlreadyActive || !stored.StartedAt.Equal(actor.StartedAt) {
			t.Fatalf("retry BeginActor = %#v, %v, %v", stored, result, err)
		}
		if seq, err := store.LastSeq(ctx, actor.SessionID); err != nil || seq != 2 {
			t.Fatalf("LastSeq after retry = %d, %v", seq, err)
		}
	})

	t.Run("actor identity cannot move between pins", func(t *testing.T) {
		store := factory(t)
		ctx := context.Background()
		v1 := fixtureArtifact(t, "counter", "v1")
		v2 := fixtureArtifact(t, "counter", "v2")
		other := fixtureArtifact(t, "gauge", "v1")
		putArtifact(t, store, v1)
		putArtifact(t, store, v2)
		putArtifact(t, store, other)
		actor := fixtureActor(v1, "red")
		if _, _, err := store.BeginActor(ctx, actor); err != nil {
			t.Fatal(err)
		}
		attempts := []statecharts.ActorMetadata{
			func() statecharts.ActorMetadata { value := actor; value.Revision = v2.Revision; return value }(),
			func() statecharts.ActorMetadata {
				value := actor
				value.ChartID = other.ChartID
				value.Revision = other.Revision
				return value
			}(),
			func() statecharts.ActorMetadata { value := actor; value.SessionID = "other-session"; return value }(),
		}
		for _, attempt := range attempts {
			if _, _, err := store.BeginActor(ctx, attempt); !errors.Is(err, statecharts.ErrActorCollision) {
				t.Errorf("BeginActor(%#v) error = %v, want ErrActorCollision", attempt, err)
			}
		}
	})

	t.Run("actor sessions are unique and start at sequence one", func(t *testing.T) {
		artifact := fixtureArtifact(t, "counter", "v1")
		t.Run("prepopulated session", func(t *testing.T) {
			store := factory(t)
			putArtifact(t, store, artifact)
			actor := fixtureActor(artifact, "red")
			if seq, err := store.Append(context.Background(), statecharts.LogEntry{SessionID: actor.SessionID, Kind: statecharts.KindExternalEvent, Timestamp: actor.StartedAt}); err != nil || seq != 1 {
				t.Fatalf("prepopulate session = %d, %v", seq, err)
			}
			if _, _, err := store.BeginActor(context.Background(), actor); !errors.Is(err, statecharts.ErrActorCollision) {
				t.Fatalf("BeginActor error = %v, want ErrActorCollision", err)
			}
		})
		t.Run("cross actor reuse", func(t *testing.T) {
			store := factory(t)
			putArtifact(t, store, artifact)
			first := fixtureActor(artifact, "red")
			if _, _, err := store.BeginActor(context.Background(), first); err != nil {
				t.Fatal(err)
			}
			second := fixtureActor(artifact, "blue")
			second.SessionID = first.SessionID
			if _, _, err := store.BeginActor(context.Background(), second); !errors.Is(err, statecharts.ErrActorCollision) {
				t.Fatalf("BeginActor error = %v, want ErrActorCollision", err)
			}
		})
	})

	t.Run("invalid actor metadata is rejected without a start record", func(t *testing.T) {
		artifact := fixtureArtifact(t, "counter", "v1")
		valid := fixtureActor(artifact, "red")
		tests := []struct {
			name   string
			mutate func(*statecharts.ActorMetadata)
		}{
			{"actor ID", func(actor *statecharts.ActorMetadata) { actor.ActorID = "bad@actor" }},
			{"chart ID", func(actor *statecharts.ActorMetadata) { actor.ChartID = "bad@chart" }},
			{"revision", func(actor *statecharts.ActorMetadata) { actor.Revision = "" }},
			{"session ID", func(actor *statecharts.ActorMetadata) { actor.SessionID = "" }},
			{"durability", func(actor *statecharts.ActorMetadata) { actor.Durable = false }},
			{"lifecycle", func(actor *statecharts.ActorMetadata) {
				actor.Lifecycle = statecharts.ActorLifecycleTerminal
				actor.TerminalAt = actor.StartedAt
			}},
			{"start time", func(actor *statecharts.ActorMetadata) { actor.StartedAt = time.Time{} }},
			{"terminal time", func(actor *statecharts.ActorMetadata) { actor.TerminalAt = actor.StartedAt }},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				store := factory(t)
				putArtifact(t, store, artifact)
				actor := valid
				test.mutate(&actor)
				if _, _, err := store.BeginActor(context.Background(), actor); !errors.Is(err, statecharts.ErrInvalidActorMetadata) {
					t.Fatalf("BeginActor error = %v, want ErrInvalidActorMetadata", err)
				}
				if seq, err := store.LastSeq(context.Background(), actor.SessionID); err != nil || seq != 0 {
					t.Fatalf("LastSeq after invalid begin = %d, %v", seq, err)
				}
			})
		}

		store := factory(t)
		putArtifact(t, store, artifact)
		if _, _, err := store.BeginActor(context.Background(), valid); err != nil {
			t.Fatal(err)
		}
		for _, test := range []struct {
			name   string
			mutate func(*statecharts.ActorMetadata)
		}{
			{"retry terminal lifecycle", func(actor *statecharts.ActorMetadata) {
				actor.Lifecycle = statecharts.ActorLifecycleTerminal
				actor.TerminalAt = actor.StartedAt
			}},
			{"retry terminal time", func(actor *statecharts.ActorMetadata) { actor.TerminalAt = actor.StartedAt }},
			{"retry start time", func(actor *statecharts.ActorMetadata) { actor.StartedAt = time.Time{} }},
			{"retry durability", func(actor *statecharts.ActorMetadata) { actor.Durable = false }},
		} {
			t.Run(test.name, func(t *testing.T) {
				actor := valid
				test.mutate(&actor)
				if _, _, err := store.BeginActor(context.Background(), actor); !errors.Is(err, statecharts.ErrInvalidActorMetadata) {
					t.Fatalf("BeginActor error = %v, want ErrInvalidActorMetadata", err)
				}
			})
		}
	})

	t.Run("concurrent actor begin has one winner", func(t *testing.T) {
		store := factory(t)
		artifact := fixtureArtifact(t, "counter", "v1")
		putArtifact(t, store, artifact)
		actor := fixtureActor(artifact, "red")
		const callers = 32
		results := make(chan statecharts.ActorBeginResult, callers)
		errs := make(chan error, callers)
		var wg sync.WaitGroup
		for range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, result, err := store.BeginActor(context.Background(), actor)
				results <- result
				errs <- err
			}()
		}
		wg.Wait()
		close(results)
		close(errs)
		started := 0
		for err := range errs {
			if err != nil {
				t.Fatalf("BeginActor: %v", err)
			}
		}
		for result := range results {
			switch result {
			case statecharts.ActorStarted:
				started++
			case statecharts.ActorAlreadyActive:
			default:
				t.Fatalf("BeginActor result = %v", result)
			}
		}
		if started != 1 {
			t.Fatalf("ActorStarted results = %d, want 1", started)
		}
		if seq, err := store.LastSeq(context.Background(), actor.SessionID); err != nil || seq != 1 {
			t.Fatalf("LastSeq = %d, %v", seq, err)
		}
	})

	t.Run("concurrent incompatible actor begins cannot move the winner", func(t *testing.T) {
		store := factory(t)
		v1 := fixtureArtifact(t, "counter", "v1")
		v2 := fixtureArtifact(t, "counter", "v2")
		putArtifact(t, store, v1)
		putArtifact(t, store, v2)
		actors := []statecharts.ActorMetadata{fixtureActor(v1, "red"), fixtureActor(v2, "red")}
		start := make(chan struct{})
		results := make(chan statecharts.ActorBeginResult, len(actors))
		errs := make(chan error, len(actors))
		var wg sync.WaitGroup
		for _, actor := range actors {
			actor := actor
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, result, err := store.BeginActor(context.Background(), actor)
				results <- result
				errs <- err
			}()
		}
		close(start)
		wg.Wait()
		close(results)
		close(errs)
		started, collisions := 0, 0
		for result := range results {
			if result == statecharts.ActorStarted {
				started++
			}
		}
		for err := range errs {
			if errors.Is(err, statecharts.ErrActorCollision) {
				collisions++
			} else if err != nil {
				t.Fatalf("BeginActor error = %v", err)
			}
		}
		if started != 1 || collisions != 1 {
			t.Fatalf("started = %d, collisions = %d; want 1, 1", started, collisions)
		}
		stored, ok, err := store.GetActor(context.Background(), "red")
		if err != nil || !ok || (stored.Revision != v1.Revision && stored.Revision != v2.Revision) {
			t.Fatalf("stored actor = %#v, %v, %v", stored, ok, err)
		}
	})

	t.Run("listing references and terminal release are authoritative", func(t *testing.T) {
		store := factory(t)
		ctx := context.Background()
		v1 := fixtureArtifact(t, "counter", "v1")
		v2 := fixtureArtifact(t, "counter", "v2")
		putArtifact(t, store, v1)
		putArtifact(t, store, v2)
		actors := []statecharts.ActorMetadata{fixtureActor(v1, "red"), fixtureActor(v1, "blue"), fixtureActor(v2, "green")}
		for _, actor := range actors {
			if _, _, err := store.BeginActor(ctx, actor); err != nil {
				t.Fatal(err)
			}
		}
		active, err := store.ListNonTerminalActors(ctx)
		if err != nil || len(active) != 3 {
			t.Fatalf("ListNonTerminalActors = %#v, %v", active, err)
		}
		if got := []statecharts.Identifier{active[0].ActorID, active[1].ActorID, active[2].ActorID}; !reflect.DeepEqual(got, []statecharts.Identifier{"blue", "green", "red"}) {
			t.Fatalf("ListNonTerminalActors order = %v", got)
		}
		wantRevisions := []statecharts.RevisionID{v1.Revision, v2.Revision}
		sort.Slice(wantRevisions, func(i, j int) bool { return wantRevisions[i] < wantRevisions[j] })
		revisions, err := store.ReferencedRevisions(ctx)
		if err != nil || !reflect.DeepEqual(revisions, wantRevisions) {
			t.Fatalf("ReferencedRevisions = %#v, %v", revisions, err)
		}
		terminalAt := actors[0].StartedAt.Add(time.Minute)
		stored, result, err := store.MarkActorTerminal(ctx, actors[0].ActorID, terminalAt)
		if err != nil || result != statecharts.ActorMarkedTerminal || stored.Lifecycle != statecharts.ActorLifecycleTerminal || !stored.TerminalAt.Equal(terminalAt) {
			t.Fatalf("MarkActorTerminal = %#v, %v, %v", stored, result, err)
		}
		stored, result, err = store.MarkActorTerminal(ctx, actors[0].ActorID, terminalAt.Add(time.Hour))
		if err != nil || result != statecharts.ActorAlreadyTerminal || !stored.TerminalAt.Equal(terminalAt) {
			t.Fatalf("repeated MarkActorTerminal = %#v, %v, %v", stored, result, err)
		}
		if _, result, err := store.MarkActorTerminal(ctx, "missing", terminalAt); err != nil || result != statecharts.ActorNotFound {
			t.Fatalf("missing MarkActorTerminal = %v, %v", result, err)
		}
		if _, _, err := store.MarkActorTerminal(ctx, actors[1].ActorID, actors[1].StartedAt.Add(-time.Second)); !errors.Is(err, statecharts.ErrInvalidActorMetadata) {
			t.Fatalf("early MarkActorTerminal error = %v, want ErrInvalidActorMetadata", err)
		}
		if _, _, err := store.BeginActor(ctx, actors[0]); !errors.Is(err, statecharts.ErrActorTerminal) {
			t.Fatalf("BeginActor after terminal error = %v", err)
		}
		before, err := store.LastSeq(ctx, actors[0].SessionID)
		if err != nil {
			t.Fatal(err)
		}
		if _, appended, err := store.AppendIngress(ctx, statecharts.LogEntry{
			SessionID: actors[0].SessionID,
			Kind:      statecharts.KindExternalEvent,
			Timestamp: terminalAt.Add(time.Second),
			Event:     statecharts.Event{Name: "late", Type: statecharts.EventExternal},
		}, "late-delivery"); !errors.Is(err, statecharts.ErrActorTerminal) || appended {
			t.Fatalf("AppendIngress after terminal = appended %v, err %v; want ErrActorTerminal", appended, err)
		}
		after, err := store.LastSeq(ctx, actors[0].SessionID)
		if err != nil || after != before {
			t.Fatalf("LastSeq after terminal ingress = %d, %v; want %d", after, err, before)
		}
		if _, _, err := store.MarkActorTerminal(ctx, actors[1].ActorID, terminalAt); err != nil {
			t.Fatal(err)
		}
		revisions, err = store.ReferencedRevisions(ctx)
		if err != nil || !reflect.DeepEqual(revisions, []statecharts.RevisionID{v2.Revision}) {
			t.Fatalf("ReferencedRevisions after terminal = %#v, %v", revisions, err)
		}
	})

	t.Run("definition deletion is atomic with actor references", func(t *testing.T) {
		store := factory(t)
		ctx := context.Background()
		artifact := fixtureArtifact(t, "counter", "v1")
		putArtifact(t, store, artifact)
		result, err := store.DeleteDefinitionIfUnreferenced(ctx, artifact.Revision)
		if err != nil || result != statecharts.DefinitionDeleted {
			t.Fatalf("DeleteDefinitionIfUnreferenced = %v, %v", result, err)
		}
		result, err = store.DeleteDefinitionIfUnreferenced(ctx, artifact.Revision)
		if err != nil || result != statecharts.DefinitionNotFound {
			t.Fatalf("repeated DeleteDefinitionIfUnreferenced = %v, %v", result, err)
		}

		putArtifact(t, store, artifact)
		actor := fixtureActor(artifact, "red")
		if _, _, err := store.BeginActor(ctx, actor); err != nil {
			t.Fatal(err)
		}
		result, err = store.DeleteDefinitionIfUnreferenced(ctx, artifact.Revision)
		if err != nil || result != statecharts.DefinitionReferenced {
			t.Fatalf("referenced DeleteDefinitionIfUnreferenced = %v, %v", result, err)
		}
		if _, _, err := store.MarkActorTerminal(ctx, actor.ActorID, actor.StartedAt.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
		result, err = store.DeleteDefinitionIfUnreferenced(ctx, artifact.Revision)
		if err != nil || result != statecharts.DefinitionDeleted {
			t.Fatalf("terminal-only DeleteDefinitionIfUnreferenced = %v, %v", result, err)
		}
	})

	t.Run("concurrent delete and begin never leave an unresolvable actor", func(t *testing.T) {
		for i := 0; i < 32; i++ {
			store := factory(t)
			artifact := fixtureArtifact(t, "counter", "v1")
			putArtifact(t, store, artifact)
			actor := fixtureActor(artifact, "red")
			start := make(chan struct{})
			var beginResult statecharts.ActorBeginResult
			var beginErr error
			var deleteResult statecharts.DefinitionDeleteResult
			var deleteErr error
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				_, beginResult, beginErr = store.BeginActor(context.Background(), actor)
			}()
			go func() {
				defer wg.Done()
				<-start
				deleteResult, deleteErr = store.DeleteDefinitionIfUnreferenced(context.Background(), artifact.Revision)
			}()
			close(start)
			wg.Wait()
			if deleteErr != nil {
				t.Fatalf("delete error = %v", deleteErr)
			}
			switch deleteResult {
			case statecharts.DefinitionReferenced:
				if beginErr != nil || beginResult != statecharts.ActorStarted {
					t.Fatalf("referenced race begin = %v, %v", beginResult, beginErr)
				}
			case statecharts.DefinitionDeleted:
				if !errors.Is(beginErr, statecharts.ErrDefinitionNotFound) {
					t.Fatalf("deleted race begin error = %v", beginErr)
				}
			default:
				t.Fatalf("delete race result = %v", deleteResult)
			}
			metadata, actorExists, err := store.GetActor(context.Background(), actor.ActorID)
			if err != nil {
				t.Fatal(err)
			}
			_, definitionExists, err := store.GetDefinition(context.Background(), artifact.Revision)
			if err != nil {
				t.Fatal(err)
			}
			if actorExists && (!definitionExists || metadata.Revision != artifact.Revision) {
				t.Fatalf("race left actor %#v without its definition", metadata)
			}
		}
	})
}
