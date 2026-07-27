package actors

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/dhamidi/statecharts"
)

func inspectionCountingChart(t *testing.T, created *atomic.Int64) *statecharts.Chart {
	t.Helper()
	chart, err := statecharts.Build(
		statecharts.Atomic("inspection-counting"),
		statecharts.NewGoModel(func() *struct{ Value int } {
			created.Add(1)
			return &struct{ Value int }{Value: 7}
		}),
		statecharts.WithRevisionSalt("inspection-counting"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return chart
}

func inspectionTestChart(t *testing.T) *statecharts.Chart {
	t.Helper()
	chart, err := statecharts.Build(
		statecharts.Atomic("inspection"),
		statecharts.NewGoModel(func() *struct{} { return &struct{}{} }),
		statecharts.WithRevisionSalt("inspection-test"),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return chart
}

type cappedActorQueryStorage struct {
	Storage
	limit int
	calls atomic.Int64
}

func (s *cappedActorQueryStorage) QueryActors(ctx context.Context, query statecharts.ActorMetadataQuery) (statecharts.ActorMetadataPage, error) {
	s.calls.Add(1)
	if query.Limit > s.limit {
		query.Limit = s.limit
	}
	return s.Storage.QueryActors(ctx, query)
}

func TestQueryActorsLocalCursorTraversalAndExactBoundary(t *testing.T) {
	ctx := context.Background()
	chart := inspectionTestChart(t)
	sys := NewSystem()
	t.Cleanup(func() { _ = sys.Stop(context.Background()) })
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	for _, id := range []ActorID{"mixed.e", "mixed.a", "other", "mixed.c"} {
		if err := sys.Spawn(ctx, id, chart.ID()); err != nil {
			t.Fatalf("Spawn(%q): %v", id, err)
		}
	}

	var got []ActorID
	query := ActorQuery{Limit: 2, IDPrefix: "mixed."}
	for {
		page, err := sys.QueryActors(ctx, query)
		if err != nil {
			t.Fatalf("QueryActors: %v", err)
		}
		for _, actor := range page.Actors {
			got = append(got, actor.ID)
			if !actor.Adopted || actor.Residency != ResidencyResident || actor.Kind == "" || actor.Revision == "" || actor.SessionID == "" {
				t.Fatalf("malformed actor: %#v", actor)
			}
		}
		if page.Next == "" {
			break
		}
		query.After = page.Next
	}
	if want := []ActorID{"mixed.a", "mixed.c", "mixed.e"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("IDs = %v, want %v", got, want)
	}
	page, err := sys.QueryActors(ctx, ActorQuery{Limit: 3, IDPrefix: "mixed."})
	if err != nil {
		t.Fatalf("exact-boundary QueryActors: %v", err)
	}
	if page.Next != "" {
		t.Fatalf("exact-boundary Next = %q, want empty", page.Next)
	}
}

func TestInspectionQueryValidationAndCancellation(t *testing.T) {
	sys := NewSystem()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sys.QueryActors(canceled, ActorQuery{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("QueryActors canceled error = %v", err)
	}
	if _, err := sys.QueryActorHistory(context.Background(), "actor", ActorHistoryQuery{After: 1, Tail: true}); !errors.Is(err, ErrInvalidHistoryQuery) {
		t.Fatalf("Tail+After error = %v", err)
	}
	if _, err := sys.QueryActorHistory(context.Background(), "actor", ActorHistoryQuery{After: ^uint64(0)}); !errors.Is(err, ErrInvalidHistoryQuery) {
		t.Fatalf("maximum After error = %v", err)
	}
}

func TestResidencyStateIsPublishedBeforeObserver(t *testing.T) {
	ctx := context.Background()
	chart := inspectionTestChart(t)
	var sys *System
	var observed []ResidencyState
	sys = NewSystem(WithResidencyObserver(func(change ResidencyChange) {
		entry, ok := sys.resolve(change.ActorID)
		if !ok {
			t.Errorf("observer could not resolve %q", change.ActorID)
			return
		}
		observed = append(observed, residencyState(entry.residency.Load()))
	}))
	t.Cleanup(func() { _ = sys.Stop(context.Background()) })
	if err := sys.Register(chart); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sys.Spawn(ctx, "observed", chart.ID()); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if len(observed) == 0 || observed[len(observed)-1] != ResidencyResident {
		t.Fatalf("observer saw states %v", observed)
	}
}

func TestQueryActorsStorageOnlyDoesNotAdoptOrHydrate(t *testing.T) {
	ctx := context.Background()
	storage := openTestLog(t)
	var firstCreated, freshCreated atomic.Int64
	chart := inspectionCountingChart(t, &firstCreated)
	first := NewSystem(WithStorage(storage), WithIdleTimeout(0))
	if err := first.Register(chart); err != nil {
		t.Fatal(err)
	}
	if err := first.Spawn(ctx, "stored", chart.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	if firstCreated.Load() != 1 {
		t.Fatalf("first sessions = %d, want 1", firstCreated.Load())
	}

	freshChart := inspectionCountingChart(t, &freshCreated)
	fresh := NewSystem(WithStorage(storage), WithNodeName("node-b"), WithIdleTimeout(0))
	t.Cleanup(func() { _ = fresh.Stop(context.Background()); _ = first.Stop(context.Background()) })
	if err := fresh.Register(freshChart); err != nil {
		t.Fatal(err)
	}
	page, err := fresh.QueryActors(ctx, ActorQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Actors) != 1 {
		t.Fatalf("actors = %#v, want one", page.Actors)
	}
	got := page.Actors[0]
	if got.ID != "stored" || got.Adopted || got.Residency != ResidencyPagedOut || got.Address != "stored@node-b" {
		t.Fatalf("storage-only actor = %#v", got)
	}
	if len(fresh.table) != 0 {
		t.Fatalf("fresh routing table has %d entries", len(fresh.table))
	}
	if freshCreated.Load() != 0 {
		t.Fatalf("directory created %d datamodel sessions", freshCreated.Load())
	}
}

func TestQueryActorsMergesLocalDurableAndEphemeralWithFilters(t *testing.T) {
	ctx := context.Background()
	storage := openTestLog(t)
	chart := inspectionTestChart(t)
	sys := NewSystem(WithStorage(storage), WithIdleTimeout(0))
	t.Cleanup(func() { _ = sys.Stop(context.Background()) })
	if err := sys.Register(chart); err != nil {
		t.Fatal(err)
	}
	if err := sys.Spawn(ctx, "a-ephemeral", chart.ID()); err != nil {
		t.Fatal(err)
	}
	if err := sys.Spawn(ctx, "b-durable", chart.ID(), Durable()); err != nil {
		t.Fatal(err)
	}

	page, err := sys.QueryActors(ctx, ActorQuery{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if page.Next != "" || len(page.Actors) != 2 || page.Actors[0].ID != "a-ephemeral" || page.Actors[1].ID != "b-durable" {
		t.Fatalf("merged page = %#v, next %q", page.Actors, page.Next)
	}
	if !page.Actors[1].Adopted || page.Actors[1].Residency != ResidencyResident {
		t.Fatalf("persisted local row was not overlaid: %#v", page.Actors[1])
	}
	for name, query := range map[string]ActorQuery{
		"prefix": {IDPrefix: "b-"}, "kind": {Kind: chart.ID()}, "revision": {Revision: chart.Revision()},
		"durable":  {Durable: func() *bool { v := true; return &v }()},
		"resident": {Residency: ResidencyResident}, "active": {Lifecycle: statecharts.ActorLifecycleActive},
	} {
		filtered, err := sys.QueryActors(ctx, query)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(filtered.Actors) == 0 {
			t.Fatalf("%s filter returned no actors", name)
		}
		if name == "durable" && (len(filtered.Actors) != 1 || filtered.Actors[0].ID != "b-durable") {
			t.Fatalf("durable = %#v", filtered.Actors)
		}
	}
}

func TestQueryActorsMixedStorageAndLocalCursorBoundaries(t *testing.T) {
	ctx := context.Background()
	storage := openTestLog(t)
	chart := inspectionTestChart(t)
	seed := NewSystem(WithStorage(storage), WithIdleTimeout(0))
	if err := seed.Register(chart); err != nil {
		t.Fatal(err)
	}
	for _, id := range []ActorID{"a-stored", "c-stored", "e-overlay"} {
		if err := seed.Spawn(ctx, id, chart.ID(), Durable()); err != nil {
			t.Fatal(err)
		}
	}
	if err := seed.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	sys := NewSystem(WithStorage(storage), WithIdleTimeout(0))
	t.Cleanup(func() { _ = sys.Stop(context.Background()) })
	if err := sys.Register(chart); err != nil {
		t.Fatal(err)
	}
	if err := sys.Spawn(ctx, "b-local", chart.ID()); err != nil {
		t.Fatal(err)
	}
	if err := sys.Spawn(ctx, "d-local", chart.ID()); err != nil {
		t.Fatal(err)
	}
	if err := sys.Spawn(ctx, "e-overlay", chart.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	var got []ActorID
	q := ActorQuery{Limit: 2}
	for {
		page, err := sys.QueryActors(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		for _, actor := range page.Actors {
			got = append(got, actor.ID)
			if actor.ID == "e-overlay" && (!actor.Adopted || actor.Residency != ResidencyResident) {
				t.Fatalf("overlay = %#v", actor)
			}
		}
		if page.Next == "" {
			break
		}
		q.After = page.Next
	}
	want := []ActorID{"a-stored", "b-local", "c-stored", "d-local", "e-overlay"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mixed traversal = %v, want %v", got, want)
	}
}

func TestQueryActorsContinuesAcrossShortStoragePagesAndDroppedOverlays(t *testing.T) {
	ctx := context.Background()
	base := openTestLog(t)
	chart := inspectionTestChart(t)
	seed := NewSystem(WithStorage(base), WithIdleTimeout(0))
	if err := seed.Register(chart); err != nil {
		t.Fatal(err)
	}
	for _, id := range []ActorID{"a-live", "b-live", "c-stored", "d-stored"} {
		if err := seed.Spawn(ctx, id, chart.ID(), Durable()); err != nil {
			t.Fatal(err)
		}
	}
	if err := seed.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	storage := &cappedActorQueryStorage{Storage: base, limit: 2}
	sys := NewSystem(WithStorage(storage), WithIdleTimeout(0))
	t.Cleanup(func() { _ = sys.Stop(context.Background()) })
	if err := sys.Register(chart); err != nil {
		t.Fatal(err)
	}
	for _, id := range []ActorID{"a-live", "b-live"} {
		if err := sys.Spawn(ctx, id, chart.ID(), Durable()); err != nil {
			t.Fatal(err)
		}
	}

	first, err := sys.QueryActors(ctx, ActorQuery{Limit: 1, Residency: ResidencyPagedOut})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Actors) != 1 || first.Actors[0].ID != "c-stored" || first.Next == "" {
		t.Fatalf("first paged-out page = %#v, next %q", first.Actors, first.Next)
	}
	second, err := sys.QueryActors(ctx, ActorQuery{After: first.Next, Limit: 1, Residency: ResidencyPagedOut})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Actors) != 1 || second.Actors[0].ID != "d-stored" || second.Next != "" {
		t.Fatalf("second paged-out page = %#v, next %q", second.Actors, second.Next)
	}
	if storage.calls.Load() < 3 {
		t.Fatalf("storage query calls = %d, want multiple short pages", storage.calls.Load())
	}
}

func TestInspectActorResidentPagedOutAndStorageOnlyNeverHydrates(t *testing.T) {
	ctx := context.Background()
	storage := openTestLog(t)
	var created atomic.Int64
	chart := inspectionCountingChart(t, &created)
	sys := NewSystem(WithStorage(storage), WithNodeName("node-a"), WithMaxResident(1), WithIdleTimeout(0))
	t.Cleanup(func() { _ = sys.Stop(context.Background()) })
	if err := sys.Register(chart); err != nil {
		t.Fatal(err)
	}
	if err := sys.Spawn(ctx, "a-paged", chart.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	if err := sys.Spawn(ctx, "b-live", chart.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	before := created.Load()
	info, live, err := sys.InspectActor(ctx, "b-live")
	if err != nil || live == nil || info.Address != "b-live@node-a" {
		t.Fatalf("resident inspection = %#v, %#v, %v", info, live, err)
	}
	info, live, err = sys.InspectActor(ctx, "a-paged")
	if err != nil || live != nil || info.Residency != ResidencyPagedOut {
		t.Fatalf("paged inspection = %#v, %#v, %v", info, live, err)
	}
	if created.Load() != before {
		t.Fatalf("paged inspection hydrated: sessions %d -> %d", before, created.Load())
	}

	fresh := NewSystem(WithStorage(storage), WithNodeName("node-c"), WithIdleTimeout(0))
	t.Cleanup(func() { _ = fresh.Stop(context.Background()) })
	info, live, err = fresh.InspectActor(ctx, "a-paged")
	if err != nil || live != nil || info.Adopted || info.Address != "a-paged@node-c" {
		t.Fatalf("storage inspection = %#v, %#v, %v", info, live, err)
	}
	if len(fresh.table) != 0 {
		t.Fatal("storage-only inspection adopted actor")
	}
}

func TestDefinitionAndHistoryInspectionAreMetadataOnly(t *testing.T) {
	ctx := context.Background()
	storage := openTestLog(t)
	model := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	v1 := publicationChart(t, model, "finished-v1", "inspection-v1")
	sys := NewSystem(WithStorage(storage), WithMaxResident(1), WithIdleTimeout(0))
	t.Cleanup(func() { _ = sys.Stop(context.Background()) })
	if err := sys.Register(v1); err != nil {
		t.Fatal(err)
	}
	if err := sys.Spawn(ctx, "history", v1.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	if err := sys.Tell(ctx, "history", statecharts.Event{Name: "finish", Type: statecharts.EventExternal}); err != nil {
		t.Fatal(err)
	}
	v2, err := sys.Publish(ctx, publicationDefinition("finished-v2", "inspection-v2"))
	if err != nil {
		t.Fatal(err)
	}
	definition, err := sys.InspectActorDefinition(ctx, "history")
	if err != nil {
		t.Fatal(err)
	}
	if definition.PinnedRevision != v1.Revision() || definition.CurrentRevision != v2 || !definition.CurrentAvailable {
		t.Fatalf("definition = %#v", definition)
	}
	page, err := sys.QueryActorHistory(ctx, "history", ActorHistoryQuery{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || page.Entries[0].SessionID != "history" || page.Next != page.Entries[0].Seq {
		t.Fatalf("history page = %#v", page)
	}
	next, err := sys.QueryActorHistory(ctx, "history", ActorHistoryQuery{After: page.Next, Limit: 10})
	if err != nil || len(next.Entries) == 0 {
		t.Fatalf("history continuation = %#v, %v", next, err)
	}

	plain := NewSystem()
	t.Cleanup(func() { _ = plain.Stop(context.Background()) })
	if err := plain.Register(v1); err != nil {
		t.Fatal(err)
	}
	if err := plain.Spawn(ctx, "ephemeral", v1.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err := plain.QueryActorHistory(ctx, "ephemeral", ActorHistoryQuery{}); !errors.Is(err, ErrHistoryUnavailable) {
		t.Fatalf("history error = %v", err)
	}

	fresh := NewSystem(WithStorage(storage), WithIdleTimeout(0))
	t.Cleanup(func() { _ = fresh.Stop(context.Background()) })
	storedDefinition, err := fresh.InspectActorDefinition(ctx, "history")
	if err != nil || storedDefinition.PinnedRevision != v1.Revision() {
		t.Fatalf("storage definition = %#v, %v", storedDefinition, err)
	}
	if len(fresh.table) != 0 {
		t.Fatal("metadata/history read adopted actor")
	}
}

func TestQueryActorHistoryTailAndExactBoundary(t *testing.T) {
	ctx := context.Background()
	storage := openTestLog(t)
	chart := inspectionTestChart(t)
	sys := NewSystem(WithStorage(storage), WithIdleTimeout(0))
	t.Cleanup(func() { _ = sys.Stop(context.Background()) })
	if err := sys.Register(chart); err != nil {
		t.Fatal(err)
	}
	if err := sys.Spawn(ctx, "history-tail", chart.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := sys.Tell(ctx, "history-tail", statecharts.Event{Name: "inspect", Type: statecharts.EventExternal}); err != nil {
			t.Fatal(err)
		}
	}

	tail, err := sys.QueryActorHistory(ctx, "history-tail", ActorHistoryQuery{Limit: 2, Tail: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(tail.Entries) != 2 || tail.Entries[0].Seq != 4 || tail.Entries[1].Seq != 5 || tail.Next != 0 {
		t.Fatalf("tail page = %#v", tail)
	}
	exact, err := sys.QueryActorHistory(ctx, "history-tail", ActorHistoryQuery{After: 3, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(exact.Entries) != 2 || exact.Entries[0].Seq != 4 || exact.Entries[1].Seq != 5 || exact.Next != 0 {
		t.Fatalf("exact boundary page = %#v", exact)
	}
}

func TestQueryActorsReportsTerminalDurableLifecycle(t *testing.T) {
	ctx := context.Background()
	storage := openTestLog(t)
	chart, err := statecharts.Compile(
		terminalPublicationDefinition("finished-v1", "inspection-terminal"),
		statecharts.NewGoModel(func() *struct{} { return &struct{}{} }),
	)
	if err != nil {
		t.Fatal(err)
	}
	sys := NewSystem(WithStorage(storage), WithIdleTimeout(0))
	t.Cleanup(func() { _ = sys.Stop(context.Background()) })
	if err := sys.Register(chart); err != nil {
		t.Fatal(err)
	}
	if err := sys.Spawn(ctx, "terminal", chart.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	if err := sys.Tell(ctx, "terminal", statecharts.Event{Name: "finish", Type: statecharts.EventExternal}); err != nil {
		t.Fatal(err)
	}

	page, err := sys.QueryActors(ctx, ActorQuery{Lifecycle: statecharts.ActorLifecycleTerminal})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Actors) != 1 || page.Actors[0].ID != "terminal" || page.Actors[0].TerminalAt.IsZero() {
		t.Fatalf("terminal page = %#v", page.Actors)
	}
}
