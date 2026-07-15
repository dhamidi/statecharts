package actors

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dhamidi/statecharts"
)

func TestDurableActorKeepsPinnedRevisionAcrossPageOutAndRestart(t *testing.T) {
	ctx := context.Background()
	storage := openTestLog(t)
	model := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	v1 := publicationChart(t, model, "finished-v1", "v1")
	first := NewSystem(WithStorage(storage), WithMaxResident(1), WithIdleTimeout(0))
	if err := first.Register(v1); err != nil {
		t.Fatal(err)
	}
	if err := first.Spawn(ctx, "red", v1.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	metadata, found, err := storage.GetActor(ctx, "red")
	if err != nil || !found {
		t.Fatalf("GetActor after first Spawn = %#v, %v, %v", metadata, found, err)
	}
	if metadata.Revision != v1.Revision() || metadata.ChartID != v1.ID() {
		t.Fatalf("stored actor pin = %#v, want chart %q revision %q", metadata, v1.ID(), v1.Revision())
	}

	v2Revision, err := first.Publish(ctx, publicationDefinition("finished-v2", "v2"))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Spawn(ctx, "blue", v1.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	if first.IsResident("red") {
		t.Fatal("red remained resident after blue displaced it")
	}
	if err := first.Tell(ctx, "red", statecharts.Event{Name: "finish", Type: statecharts.EventExternal}); err != nil {
		t.Fatal(err)
	}
	waitForPublishedState(t, first, "red", "finished-v1")
	if err := first.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	checkpoint, found, err := storage.Load(ctx, metadata.SessionID)
	if err != nil || !found {
		t.Fatalf("Load checkpoint = found %v, err %v", found, err)
	}
	checkpoint.Snapshot.Revision = v2Revision
	if err := storage.Save(ctx, metadata.SessionID, checkpoint); err != nil {
		t.Fatal(err)
	}

	// A fresh process knows v2 as current but must load red's durable v1 pin.
	restartedModel := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	v2 := publicationChart(t, restartedModel, "finished-v2", "v2")
	restarted := NewSystem(WithStorage(storage), WithIdleTimeout(0))
	if err := restarted.Register(v2); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Spawn(ctx, "red", v2.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	if revision, ok := restarted.ActorRevision("red"); !ok || revision != v1.Revision() {
		t.Fatalf("restarted red revision = %q, %v; want %q", revision, ok, v1.Revision())
	}
	waitForPublishedState(t, restarted, "red", "finished-v1")
	if err := restarted.Spawn(ctx, "green", v2.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	if revision, ok := restarted.ActorRevision("green"); !ok || revision != v2Revision {
		t.Fatalf("new green revision = %q, %v; want %q", revision, ok, v2Revision)
	}
	if err := restarted.Tell(ctx, "green", statecharts.Event{Name: "finish", Type: statecharts.EventExternal}); err != nil {
		t.Fatal(err)
	}
	waitForPublishedState(t, restarted, "green", "finished-v2")
	if err := restarted.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDurableSpawnFailsBeforeReplayWhenPinnedArtifactIsMissing(t *testing.T) {
	ctx := context.Background()
	storage := openTestLog(t)
	model := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	v1 := publicationChart(t, model, "finished-v1", "v1")
	first := NewSystem(WithStorage(storage), WithIdleTimeout(0))
	if err := first.Register(v1); err != nil {
		t.Fatal(err)
	}
	if err := first.Spawn(ctx, "red", v1.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	if err := first.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.DB().ExecContext(ctx, `DELETE FROM statechart_definition WHERE revision=?`, v1.Revision()); err != nil {
		t.Fatal(err)
	}

	var sessions atomic.Int64
	restartedModel := statecharts.NewGoModel(func() *struct{} {
		sessions.Add(1)
		return &struct{}{}
	})
	v2 := publicationChart(t, restartedModel, "finished-v2", "v2")
	restarted := NewSystem(WithStorage(storage), WithIdleTimeout(0))
	if err := restarted.Register(v2); err != nil {
		t.Fatal(err)
	}
	before, err := storage.LastSeq(ctx, "red")
	if err != nil {
		t.Fatal(err)
	}
	err = restarted.Spawn(ctx, "red", v2.ID(), Durable())
	if !errors.Is(err, statecharts.ErrDefinitionNotFound) {
		t.Fatalf("Spawn error = %v, want ErrDefinitionNotFound", err)
	}
	if sessions.Load() != 0 {
		t.Fatalf("missing artifact created %d datamodel sessions before failing", sessions.Load())
	}
	after, err := storage.LastSeq(ctx, "red")
	if err != nil || after != before {
		t.Fatalf("log changed during failed activation: before %d, after %d, err %v", before, after, err)
	}
	if restarted.IsResident("red") {
		t.Fatal("actor with missing pinned artifact became resident")
	}
	if err := restarted.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

type commitThenFailStorage struct {
	Storage
	once sync.Once
}

var errLostActorStartAcknowledgement = errors.New("injected lost actor-start commit acknowledgement")

func (s *commitThenFailStorage) BeginActor(ctx context.Context, metadata statecharts.ActorMetadata) (statecharts.ActorMetadata, statecharts.ActorBeginResult, error) {
	stored, result, err := s.Storage.BeginActor(ctx, metadata)
	if err != nil {
		return stored, result, err
	}
	failed := false
	s.once.Do(func() { failed = true })
	if failed && result == statecharts.ActorStarted {
		return stored, result, errLostActorStartAcknowledgement
	}
	return stored, result, nil
}

func TestCommittedActorStartIsRecoveredWithoutRepeatingInitialInvoke(t *testing.T) {
	ctx := context.Background()
	base := openTestLog(t)
	storage := &commitThenFailStorage{Storage: base}
	var starts atomic.Int64
	invokeHandler := func() statecharts.InvokeHandler {
		return statecharts.InvokeHandlerFunc(func(ctx context.Context, _ statecharts.InvokeRequest, _ statecharts.InvokeIO) (statecharts.Value, error) {
			starts.Add(1)
			<-ctx.Done()
			return statecharts.NullValue(), nil
		})
	}
	build := func() *statecharts.Chart {
		chart, err := statecharts.Build(
			statecharts.Compound("start-boundary", "invoking", statecharts.Children(
				statecharts.Atomic("invoking",
					statecharts.Invoke("durability-test", "work", statecharts.WithInvokeID("work")),
					statecharts.On(string(statecharts.ErrEventCommunication), statecharts.Target("recovered")),
				),
				statecharts.Atomic("recovered"),
			)),
			statecharts.NewGoModel(func() *struct{} { return &struct{}{} }),
			statecharts.WithRevisionSalt("v1"),
		)
		if err != nil {
			t.Fatal(err)
		}
		return chart
	}

	first := NewSystem(WithStorage(storage), WithIdleTimeout(0), WithInvokeHandler("durability-test", invokeHandler))
	chart := build()
	if err := first.Register(chart); err != nil {
		t.Fatal(err)
	}
	if err := first.Spawn(ctx, "worker", chart.ID(), Durable()); !errors.Is(err, errLostActorStartAcknowledgement) {
		t.Fatalf("Spawn error = %v, want injected lost acknowledgement", err)
	}
	if starts.Load() != 0 {
		t.Fatalf("initial invoke starts before retry = %d, want 0", starts.Load())
	}
	metadata, found, err := base.GetActor(ctx, "worker")
	if err != nil || !found || metadata.Revision != chart.Revision() {
		t.Fatalf("committed actor metadata = %#v, %v, %v", metadata, found, err)
	}
	if seq, err := base.LastSeq(ctx, metadata.SessionID); err != nil || seq != 1 {
		t.Fatalf("committed session-start sequence = %d, %v; want 1", seq, err)
	}

	restarted := NewSystem(WithStorage(base), WithIdleTimeout(0), WithInvokeHandler("durability-test", invokeHandler))
	if err := restarted.Register(build()); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Spawn(ctx, "worker", chart.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	if starts.Load() != 0 {
		t.Fatalf("initial invoke starts after committed-boundary recovery = %d, want 0", starts.Load())
	}
	if instance := testInstanceFor(restarted, "worker"); instance == nil || !hasStateID(instance.Configuration(), "recovered") {
		t.Fatal("committed-boundary recovery did not synthesize interrupted invoke outcome")
	}
	if err := first.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDurableRecoveryRejectsMissingFunctionFromPinnedRevision(t *testing.T) {
	ctx := context.Background()
	storage := openTestLog(t)
	v1Model := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	action, err := v1Model.Action("old-action", "v1", func(*struct{}, statecharts.ExecContext, []statecharts.Value) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	v1Definition := publicationDefinition("finished-v1", "v1")
	v1Definition.Root.Children[0].OnEntry = []statecharts.ExecutableBlock{{action.Do()}}
	v1, err := statecharts.Compile(v1Definition, v1Model)
	if err != nil {
		t.Fatal(err)
	}
	first := NewSystem(WithStorage(storage), WithIdleTimeout(0))
	if err := first.Register(v1); err != nil {
		t.Fatal(err)
	}
	if err := first.Spawn(ctx, "red", v1.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	if err := first.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	v2Model := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	v2 := publicationChart(t, v2Model, "finished-v2", "v2")
	restarted := NewSystem(WithStorage(storage), WithIdleTimeout(0))
	if err := restarted.Register(v2); err != nil {
		t.Fatal(err)
	}
	err = restarted.Spawn(ctx, "red", v2.ID(), Durable())
	if err == nil {
		t.Fatal("recovery succeeded without the pinned revision's function implementation")
	}
	if restarted.IsResident("red") {
		t.Fatal("actor became resident without its pinned function implementation")
	}
	if err := restarted.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDurableRecoveryRejectsMissingInvokeHandlerFromPinnedRevision(t *testing.T) {
	ctx := context.Background()
	storage := openTestLog(t)
	v1Model := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	v1Definition := publicationDefinition("finished-v1", "v1-invoke")
	v1Definition.Root.Children[0].Invokes = []statecharts.InvokeDefinition{{
		DefinitionID: "waiting.worker", ID: "work", Type: "old-worker",
	}}
	v1, err := statecharts.Compile(v1Definition, v1Model)
	if err != nil {
		t.Fatal(err)
	}
	registered := NewSystem(WithStorage(storage), WithInvokeHandler("old-worker", func() statecharts.InvokeHandler {
		return statecharts.InvokeHandlerFunc(func(context.Context, statecharts.InvokeRequest, statecharts.InvokeIO) (statecharts.Value, error) {
			return statecharts.NullValue(), nil
		})
	}))
	if err := registered.Register(v1); err != nil {
		t.Fatal(err)
	}
	metadata := statecharts.ActorMetadata{
		ActorID: "red", ChartID: v1.ID(), Revision: v1.Revision(), SessionID: "red",
		Durable: true, Lifecycle: statecharts.ActorLifecycleActive, StartedAt: time.Unix(1, 0).UTC(),
	}
	if _, result, err := storage.BeginActor(ctx, metadata); err != nil || result != statecharts.ActorStarted {
		t.Fatalf("BeginActor = %v, %v", result, err)
	}
	if err := registered.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	v2Model := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	v2 := publicationChart(t, v2Model, "finished-v2", "v2")
	restarted := NewSystem(WithStorage(storage))
	if err := restarted.Register(v2); err != nil {
		t.Fatal(err)
	}
	err = restarted.Spawn(ctx, "red", v2.ID(), Durable())
	if err == nil || !strings.Contains(err.Error(), "old-worker") {
		t.Fatalf("Spawn error = %v, want missing pinned invoke handler", err)
	}
	if restarted.IsResident("red") {
		t.Fatal("actor became resident without its pinned invoke handler")
	}
	if err := restarted.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDurableActorMetadataUsesOneSessionStartBoundary(t *testing.T) {
	ctx := context.Background()
	storage := openTestLog(t)
	model := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	chart := publicationChart(t, model, "finished-v1", "v1")
	system := NewSystem(WithStorage(storage), WithIdleTimeout(0))
	if err := system.Register(chart); err != nil {
		t.Fatal(err)
	}
	if err := system.Spawn(ctx, "red", chart.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	metadata, found, err := storage.GetActor(ctx, "red")
	if err != nil || !found {
		t.Fatalf("GetActor = %#v, %v, %v", metadata, found, err)
	}
	var starts int
	for entry, err := range storage.Read(ctx, metadata.SessionID, 1) {
		if err != nil {
			t.Fatal(err)
		}
		if entry.Kind == statecharts.KindSessionStarted {
			starts++
			if entry.Seq != 1 || !entry.Timestamp.Equal(metadata.StartedAt) {
				t.Fatalf("session start = %#v, metadata = %#v", entry, metadata)
			}
		}
	}
	if starts != 1 {
		t.Fatalf("session-start entries = %d, want 1", starts)
	}
	if err := system.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestFailedWrongKindRecoveryDoesNotPoisonActorEntry(t *testing.T) {
	ctx := context.Background()
	storage := openTestLog(t)
	model := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	correct := publicationChart(t, model, "finished-v1", "v1")
	first := NewSystem(WithStorage(storage), WithIdleTimeout(0))
	if err := first.Register(correct); err != nil {
		t.Fatal(err)
	}
	if err := first.Spawn(ctx, "red", correct.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	if err := first.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	restartedModel := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	current := publicationChart(t, restartedModel, "finished-v2", "v2")
	wrong, err := statecharts.Build(statecharts.Atomic("wrong-kind"), restartedModel, statecharts.WithRevisionSalt("wrong"))
	if err != nil {
		t.Fatal(err)
	}
	restarted := NewSystem(WithStorage(storage), WithIdleTimeout(0))
	for _, chart := range []*statecharts.Chart{current, wrong} {
		if err := restarted.Register(chart); err != nil {
			t.Fatal(err)
		}
	}
	if err := restarted.Spawn(ctx, "red", wrong.ID(), Durable()); !errors.Is(err, ErrKindMismatch) {
		t.Fatalf("wrong-kind Spawn error = %v, want ErrKindMismatch", err)
	}
	if err := restarted.Spawn(ctx, "red", current.ID(), Durable()); err != nil {
		t.Fatalf("correct retry after wrong kind: %v", err)
	}
	if revision, ok := restarted.ActorRevision("red"); !ok || revision != correct.Revision() {
		t.Fatalf("correct retry revision = %q, %v; want %q", revision, ok, correct.Revision())
	}
	if err := restarted.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentDurableFirstSpawnAndPublishPersistOneCoherentPin(t *testing.T) {
	ctx := context.Background()
	storage := openTestLog(t)
	model := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	v1 := publicationChart(t, model, "finished-v1", "v1")
	v2 := publicationChart(t, model, "finished-v2", "v2")
	system := NewSystem(WithStorage(storage), WithIdleTimeout(0))
	if err := system.Register(v1); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 33)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		_, err := system.Publish(ctx, v2.Definition())
		errs <- err
	}()
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- system.Spawn(ctx, "red", v1.ID(), Durable())
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
	metadata, found, err := storage.GetActor(ctx, "red")
	if err != nil || !found {
		t.Fatalf("GetActor = %#v, %v, %v", metadata, found, err)
	}
	if metadata.Revision != v1.Revision() && metadata.Revision != v2.Revision() {
		t.Fatalf("persisted revision = %q, want complete v1 or v2", metadata.Revision)
	}
	if revision, ok := system.ActorRevision("red"); !ok || revision != metadata.Revision {
		t.Fatalf("resident revision = %q, %v; stored = %q", revision, ok, metadata.Revision)
	}
	var starts int
	for entry, err := range storage.Read(ctx, metadata.SessionID, 1) {
		if err != nil {
			t.Fatal(err)
		}
		if entry.Kind == statecharts.KindSessionStarted {
			starts++
		}
	}
	if starts != 1 {
		t.Fatalf("session-start entries = %d, want exactly 1", starts)
	}
	if err := system.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}
