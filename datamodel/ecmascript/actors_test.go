package ecmascript_test

import (
	"context"
	"testing"
	"time"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
	"github.com/dhamidi/statecharts/storagetest"
)

type replayOnlyStore struct {
	*storagetest.MemoryStore
}

func (*replayOnlyStore) Save(context.Context, statecharts.SessionID, statecharts.Checkpoint) error {
	return nil
}

func (*replayOnlyStore) Load(context.Context, statecharts.SessionID) (statecharts.Checkpoint, bool, error) {
	return statecharts.Checkpoint{}, false, nil
}

func TestDurableActorReplaysECMAScriptThroughPaging(t *testing.T) {
	chart, err := statecharts.Build(
		statecharts.Compound("counter", "active", statecharts.Children(
			statecharts.Atomic("active",
				statecharts.On("increment", statecharts.Then(
					statecharts.NewScriptExecutable(statecharts.ScriptDefinition{Expr: source(t, "count++")}),
				)),
				statecharts.On("verify", statecharts.If(source(t, "count === 3")), statecharts.Target("done")),
			),
			statecharts.Final("done", statecharts.WithDone(source(t, "count"))),
		)),
		model(t),
		statecharts.WithRevisionSalt("ecmascript-durable-v1"),
		statecharts.WithData(statecharts.DataDefinition{ID: "count", Expr: ptr(source(t, "0"))}),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	storage := &replayOnlyStore{MemoryStore: storagetest.NewMemoryStore()}
	system := actors.NewSystem(
		actors.WithStorage(storage),
		actors.WithMaxResident(1),
		actors.WithIdleTimeout(0),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := system.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := system.Spawn(ctx, "red", chart.ID(), actors.Durable()); err != nil {
		t.Fatalf("Spawn red: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := system.Tell(ctx, "red", statecharts.Event{Name: "increment"}); err != nil {
			t.Fatalf("Tell red increment %d: %v", i, err)
		}
	}
	if err := system.Spawn(ctx, "blue", chart.ID(), actors.Durable()); err != nil {
		t.Fatalf("Spawn blue: %v", err)
	}
	if system.IsResident("red") {
		t.Fatal("red remained resident after blue was admitted under max resident 1")
	}
	if err := system.Tell(ctx, "red", statecharts.Event{Name: "increment"}); err != nil {
		t.Fatalf("Tell red after page-out: %v", err)
	}
	if !system.IsResident("red") || system.IsResident("blue") {
		t.Fatalf("residency after page-in: red=%v blue=%v, want true false", system.IsResident("red"), system.IsResident("blue"))
	}
	if err := system.Tell(ctx, "red", statecharts.Event{Name: "verify"}); err != nil {
		t.Fatalf("Tell red verify: %v", err)
	}
	metadata, found, err := storage.GetActor(ctx, "red")
	if err != nil {
		t.Fatalf("GetActor red: %v", err)
	}
	if !found || metadata.Lifecycle != statecharts.ActorLifecycleTerminal {
		t.Fatalf("red metadata = %#v, found=%v, want terminal", metadata, found)
	}
	deadline := time.Now().Add(time.Second)
	for system.IsResident("red") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if system.IsResident("red") {
		t.Fatal("terminal red actor remained resident")
	}
	if err := system.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
