package actors

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dhamidi/statecharts"
)

func publicationDefinition(target, salt string) statecharts.Definition {
	return statecharts.Definition{
		ID: "published-counter", Datamodel: "go", RevisionSalt: salt,
		Root: statecharts.StateDefinition{
			ID: statecharts.StateDefinitionID{Value: "root"}, Kind: statecharts.KindCompound,
			Initial: &statecharts.TransitionDefinition{Targets: []statecharts.Identifier{"waiting"}},
			Children: []statecharts.StateDefinition{
				{
					ID: statecharts.StateDefinitionID{Value: "waiting"}, Kind: statecharts.KindAtomic,
					Transitions: []statecharts.TransitionDefinition{{Events: []statecharts.Identifier{"finish"}, Targets: []statecharts.Identifier{statecharts.Identifier(target)}}},
				},
				{ID: statecharts.StateDefinitionID{Value: "finished-v1"}, Kind: statecharts.KindAtomic},
				{ID: statecharts.StateDefinitionID{Value: "finished-v2"}, Kind: statecharts.KindAtomic},
			},
		},
	}
}

func publicationChart(t *testing.T, model statecharts.Datamodel, target, salt string) *statecharts.Chart {
	t.Helper()
	chart, err := statecharts.Compile(publicationDefinition(target, salt), model)
	if err != nil {
		t.Fatal(err)
	}
	return chart
}

func waitForPublishedState(t *testing.T, system *System, actor, state statecharts.Identifier) {
	t.Helper()
	waitFor(t, time.Second, func() bool {
		instance := testInstanceFor(system, actor)
		return instance != nil && hasStateID(instance.Configuration(), state)
	})
}

func TestPublishPinsExistingActorsAndSelectsNewRevisionForFutureSpawns(t *testing.T) {
	ctx := context.Background()
	model := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	v1 := publicationChart(t, model, "finished-v1", "v1")
	system := NewSystem()
	if err := system.Register(v1); err != nil {
		t.Fatal(err)
	}
	if v1.ID() != "published-counter" {
		t.Fatalf("Chart.ID = %q, want stable definition ID %q", v1.ID(), "published-counter")
	}
	if err := system.Spawn(ctx, "red", v1.ID()); err != nil {
		t.Fatal(err)
	}

	v2Definition := publicationDefinition("finished-v2", "v2")
	v2Revision, err := system.Publish(ctx, v2Definition)
	if err != nil {
		t.Fatal(err)
	}
	if v2Revision == v1.Revision() {
		t.Fatal("publication did not produce a new revision")
	}
	if err := system.Spawn(ctx, "blue", v1.ID()); err != nil {
		t.Fatal(err)
	}
	if revision, ok := system.ActorRevision("red"); !ok || revision != v1.Revision() {
		t.Fatalf("red revision = %q, %v; want %q", revision, ok, v1.Revision())
	}
	if revision, ok := system.ActorRevision("blue"); !ok || revision != v2Revision {
		t.Fatalf("blue revision = %q, %v; want %q", revision, ok, v2Revision)
	}
	for _, actor := range []statecharts.Identifier{"red", "blue"} {
		if err := system.Tell(ctx, actor, statecharts.Event{Name: "finish", Type: statecharts.EventExternal}); err != nil {
			t.Fatal(err)
		}
	}
	waitForPublishedState(t, system, "red", "finished-v1")
	waitForPublishedState(t, system, "blue", "finished-v2")
	if err := system.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPublishIsAtomicIdempotentInspectableAndFailureSafe(t *testing.T) {
	ctx := context.Background()
	model := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	v1 := publicationChart(t, model, "finished-v1", "v1")
	storage := &publicationStorage{Storage: openTestLog(t)}
	system := NewSystem(WithStorage(storage))
	if err := system.Register(v1); err != nil {
		t.Fatal(err)
	}
	if _, found, err := storage.GetDefinition(ctx, v1.Revision()); err != nil || !found {
		t.Fatalf("registered definition persisted = %v, %v", found, err)
	}

	definition, revision, ok := system.CurrentDefinition(v1.ID())
	if !ok || revision != v1.Revision() {
		t.Fatalf("CurrentDefinition = %q, %v; want %q", revision, ok, v1.Revision())
	}
	definition.Root.Children[0].Transitions[0].Targets[0] = "mutated"
	again, _, _ := system.CurrentDefinition(v1.ID())
	if got := again.Root.Children[0].Transitions[0].Targets[0]; got != "finished-v1" {
		t.Fatalf("inspection result aliases registry: target = %q", got)
	}

	if got, err := system.Publish(ctx, v1.Definition()); err != nil || got != v1.Revision() {
		t.Fatalf("identical Publish = %q, %v", got, err)
	}
	v2Definition := publicationDefinition("finished-v2", "v2")
	v2Revision, err := system.Publish(ctx, v2Definition)
	if err != nil {
		t.Fatal(err)
	}
	retained, found := system.Definition(v1.ID(), v1.Revision())
	if !found || retained.RevisionSalt != "v1" {
		t.Fatalf("retained v1 = %#v, %v", retained, found)
	}
	retained.Root.Children[0].Transitions[0].Targets[0] = "mutated"
	retainedAgain, _ := system.Definition(v1.ID(), v1.Revision())
	if got := retainedAgain.Root.Children[0].Transitions[0].Targets[0]; got != "finished-v1" {
		t.Fatalf("retained definition result aliases registry: target = %q", got)
	}

	invalid := v2Definition.Clone()
	invalid.Root.Initial.Targets[0] = "missing"
	if _, err := system.Publish(ctx, invalid); err == nil {
		t.Fatal("invalid publication succeeded")
	}
	if _, current, _ := system.CurrentDefinition(v1.ID()); current != v2Revision {
		t.Fatalf("invalid publication changed current to %q", current)
	}
	unresolved := v2Definition.Clone()
	unresolved.Root.OnEntry = []statecharts.ExecutableBlock{{statecharts.NewCallExecutable(statecharts.CallDefinition{
		Function: statecharts.FunctionRef{Name: "missing", Version: "v1"},
	})}}
	if _, err := system.Publish(ctx, unresolved); err == nil {
		t.Fatal("publication with an unresolved host function succeeded")
	}
	if _, current, _ := system.CurrentDefinition(v1.ID()); current != v2Revision {
		t.Fatalf("unresolved publication changed current to %q", current)
	}

	storage.setPutError(statecharts.ErrDefinitionCollision)
	v3 := publicationDefinition("finished-v1", "v3")
	if _, err := system.Publish(ctx, v3); !errors.Is(err, statecharts.ErrDefinitionCollision) {
		t.Fatalf("collision publication error = %v", err)
	}
	if _, current, _ := system.CurrentDefinition(v1.ID()); current != v2Revision {
		t.Fatalf("failed durable publication changed current to %q", current)
	}
	if err := system.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

type publicationStorage struct {
	Storage
	mu     sync.Mutex
	putErr error
}

func (s *publicationStorage) setPutError(err error) {
	s.mu.Lock()
	s.putErr = err
	s.mu.Unlock()
}

func (s *publicationStorage) PutDefinition(ctx context.Context, artifact statecharts.DefinitionArtifact) (statecharts.DefinitionPutResult, error) {
	s.mu.Lock()
	err := s.putErr
	s.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return s.Storage.PutDefinition(ctx, artifact)
}

func TestConcurrentPublishAndSpawnPinsOneCompleteRevision(t *testing.T) {
	ctx := context.Background()
	model := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	v1 := publicationChart(t, model, "finished-v1", "v1")
	system := NewSystem()
	if err := system.Register(v1); err != nil {
		t.Fatal(err)
	}
	v2 := publicationChart(t, model, "finished-v2", "v2")
	start := make(chan struct{})
	errs := make(chan error, 65)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		_, err := system.Publish(ctx, v2.Definition())
		errs <- err
	}()
	for i := 0; i < 64; i++ {
		name := statecharts.Identifier(fmt.Sprintf("counter-%d", i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- system.Spawn(ctx, name, v1.ID())
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 64; i++ {
		name := statecharts.Identifier(fmt.Sprintf("counter-%d", i))
		revision, ok := system.ActorRevision(name)
		if !ok || revision != v1.Revision() && revision != v2.Revision() {
			t.Fatalf("%s revision = %q, %v; want complete v1 or v2", name, revision, ok)
		}
	}
	if err := system.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSystemsPublishCurrentRevisionsIndependently(t *testing.T) {
	ctx := context.Background()
	model := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	v1 := publicationChart(t, model, "finished-v1", "v1")
	left, right := NewSystem(), NewSystem()
	for _, system := range []*System{left, right} {
		if err := system.Register(v1); err != nil {
			t.Fatal(err)
		}
	}
	v2Revision, err := left.Publish(ctx, publicationDefinition("finished-v2", "v2"))
	if err != nil {
		t.Fatal(err)
	}
	if _, revision, _ := left.CurrentDefinition(v1.ID()); revision != v2Revision {
		t.Fatalf("left current revision = %q, want %q", revision, v2Revision)
	}
	if _, revision, _ := right.CurrentDefinition(v1.ID()); revision != v1.Revision() {
		t.Fatalf("right current revision = %q, want %q", revision, v1.Revision())
	}
	if err := left.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := right.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}
