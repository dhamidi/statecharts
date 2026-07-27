package inspector

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"

	statecharts "github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
	"github.com/dhamidi/statecharts/sqllog"
	_ "modernc.org/sqlite"
)

func inspectorTestStorage(t *testing.T) *sqllog.Storage {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	storage, err := sqllog.New(db, sqllog.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	return storage
}

func TestStorageOnlyReadsDoNotAdoptOrHydrateAndHistoryIsRedacted(t *testing.T) {
	ctx := t.Context()
	storage := inspectorTestStorage(t)
	var sessions atomic.Int64
	model := statecharts.NewGoModel(func() *struct{ Secret string } {
		sessions.Add(1)
		return &struct{ Secret string }{Secret: "model-secret"}
	})
	chart, err := statecharts.Build(
		statecharts.Atomic("durable-inspection"),
		model,
		statecharts.WithRevisionSalt("durable-inspection-v1"),
	)
	if err != nil {
		t.Fatal(err)
	}
	seed := actors.NewSystem(actors.WithStorage(storage), actors.WithIdleTimeout(0))
	if err := seed.Register(chart); err != nil {
		t.Fatal(err)
	}
	if err := seed.Spawn(ctx, "stored", chart.ID(), actors.Durable()); err != nil {
		t.Fatal(err)
	}
	secret, err := statecharts.StringValue("history-secret")
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.Tell(ctx, "stored", statecharts.Event{Name: "remember", Type: statecharts.EventExternal, Data: secret}); err != nil {
		t.Fatal(err)
	}
	if err := seed.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	fresh := actors.NewSystem(actors.WithStorage(storage), actors.WithMaxResident(1), actors.WithIdleTimeout(0))
	t.Cleanup(func() { _ = fresh.Stop(context.Background()) })
	if err := fresh.Register(chart); err != nil {
		t.Fatal(err)
	}
	redacted, err := statecharts.StringValue("redacted")
	if err != nil {
		t.Fatal(err)
	}
	service := New(
		WithAuthorizer(AllowAll()),
		WithRedactor(RedactorFuncs{Value: func(_ context.Context, rc RedactionContext, value statecharts.Value) (statecharts.Value, error) {
			if rc.Category == CategoryEvent {
				return redacted, nil
			}
			return value, nil
		}}),
	)
	t.Cleanup(service.Close)
	if err := service.RegisterSystem("durable", fresh); err != nil {
		t.Fatal(err)
	}
	before := sessions.Load()
	page, err := service.QueryActors(ctx, "durable", actors.ActorQuery{})
	if err != nil || len(page.Actors) != 1 || page.Actors[0].Adopted || page.Actors[0].Residency != actors.ResidencyPagedOut {
		t.Fatalf("directory = %#v, %v", page, err)
	}
	info, live, err := service.InspectActor(ctx, "durable", "stored")
	if err != nil || live != nil || info.Adopted {
		t.Fatalf("inspection = %#v, %#v, %v", info, live, err)
	}
	if _, err := service.InspectActorDefinition(ctx, "durable", "stored"); err != nil {
		t.Fatal(err)
	}
	history, err := service.QueryActorHistory(ctx, "durable", "stored", actors.ActorHistoryQuery{Limit: 10})
	if err != nil || len(history.Entries) < 2 {
		t.Fatalf("history = %#v, %v", history, err)
	}
	last := history.Entries[len(history.Entries)-1]
	if value, ok := last.Event.Data.AsString(); !ok || value != "redacted" {
		t.Fatalf("history data = %#v", last.Event.Data)
	}
	if recent, err := service.Recent(ctx, "durable", 0, 10); err != nil || len(recent.Records) != 0 {
		t.Fatalf("recent = %#v, %v", recent, err)
	}
	if sessions.Load() != before || fresh.IsResident("stored") {
		t.Fatalf("reads hydrated actor: sessions %d -> %d, resident=%v", before, sessions.Load(), fresh.IsResident("stored"))
	}
	page, err = service.QueryActors(ctx, "durable", actors.ActorQuery{})
	if err != nil || page.Actors[0].Adopted {
		t.Fatalf("reads adopted actor: %#v, %v", page, err)
	}
	if err := service.SendEvent(ctx, "durable", "stored", "remember", statecharts.NullValue()); !errors.Is(err, actors.ErrUnknownActor) {
		t.Fatalf("storage-only SendEvent = %v", err)
	}

	if err := fresh.Spawn(ctx, "stored", chart.ID(), actors.Durable()); err != nil {
		t.Fatal(err)
	}
	if err := fresh.Spawn(ctx, "other", chart.ID(), actors.Durable()); err != nil {
		t.Fatal(err)
	}
	if fresh.IsResident("stored") {
		t.Fatal("stored remained resident after residency pressure")
	}
	if err := service.SendEvent(ctx, "durable", "stored", "remember", statecharts.NullValue()); err != nil {
		t.Fatal(err)
	}
	if !fresh.IsResident("stored") {
		t.Fatal("authorized send did not use normal Tell hydration")
	}
}

func TestEphemeralInspectionWorksWithoutStorageAndHistoryStaysUnavailable(t *testing.T) {
	system := testSystem(t, "ephemeral-inspection", "ephemeral")
	service := New(WithAuthorizer(AllowAll()))
	t.Cleanup(service.Close)
	if err := service.RegisterSystem("memory", system); err != nil {
		t.Fatal(err)
	}
	page, err := service.QueryActors(t.Context(), "memory", actors.ActorQuery{})
	if err != nil || len(page.Actors) != 1 || page.Actors[0].Durable {
		t.Fatalf("directory = %#v, %v", page, err)
	}
	if _, live, err := service.InspectActor(t.Context(), "memory", "ephemeral"); err != nil || live == nil {
		t.Fatalf("inspection = %#v, %v", live, err)
	}
	if _, err := service.QueryActorHistory(t.Context(), "memory", "ephemeral", actors.ActorHistoryQuery{}); !errors.Is(err, actors.ErrHistoryUnavailable) {
		t.Fatalf("history = %v", err)
	}
}
