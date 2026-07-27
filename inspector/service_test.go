package inspector

import (
	"context"
	"errors"
	"reflect"
	"testing"

	statecharts "github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
)

func testSystem(t *testing.T, chartID, actorID string) *actors.System {
	t.Helper()
	chart, err := statecharts.Build(statecharts.Atomic(statecharts.Identifier(chartID)), statecharts.NewGoModel(func() *struct{ Chart string } {
		return &struct{ Chart string }{Chart: chartID}
	}), statecharts.WithRevisionSalt(chartID))
	if err != nil {
		t.Fatal(err)
	}
	system := actors.NewSystem(actors.WithNodeName("same-node"))
	t.Cleanup(func() { _ = system.Stop(context.Background()) })
	if err := system.Register(chart); err != nil {
		t.Fatal(err)
	}
	if err := system.Spawn(context.Background(), actors.ActorID(actorID), chart.ID()); err != nil {
		t.Fatal(err)
	}
	return system
}

func TestRegistryIsolationSortingAndCloseDoesNotOwnSystems(t *testing.T) {
	a := testSystem(t, "chart-a", "shared")
	b := testSystem(t, "chart-b", "shared")
	s := New(WithAuthorizer(AllowAll()))
	if err := s.RegisterSystem("zeta", b); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterSystem("alpha", a); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Systems(context.Background()); err != nil || !reflect.DeepEqual(got, []string{"alpha", "zeta"}) {
		t.Fatalf("Systems = %v, %v", got, err)
	}
	for _, tc := range []struct {
		name string
		sys  *actors.System
		want error
	}{{"", a, nil}, {"nil", nil, nil}, {"alpha", a, ErrDuplicateSystem}} {
		if err := s.RegisterSystem(tc.name, tc.sys); err == nil || tc.want != nil && !errors.Is(err, tc.want) {
			t.Fatalf("RegisterSystem(%q) = %v", tc.name, err)
		}
	}
	for name, wantKind := range map[string]statecharts.Identifier{"alpha": "chart-a", "zeta": "chart-b"} {
		page, err := s.QueryActors(context.Background(), name, actors.ActorQuery{})
		if err != nil || len(page.Actors) != 1 || page.Actors[0].ID != "shared" || page.Actors[0].Kind != wantKind {
			t.Fatalf("QueryActors(%s) = %#v, %v", name, page, err)
		}
		info, live, err := s.InspectActor(context.Background(), name, "shared")
		if err != nil || live == nil || info.Kind != wantKind {
			t.Fatalf("InspectActor(%s) = %#v, %#v, %v", name, info, live, err)
		}
	}
	alphaStream, err := s.Subscribe(t.Context(), "alpha", 4)
	if err != nil {
		t.Fatal(err)
	}
	zetaStream, err := s.Subscribe(t.Context(), "zeta", 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Tell(t.Context(), "shared", statecharts.Event{Name: "trace", Type: statecharts.EventExternal}); err != nil {
		t.Fatal(err)
	}
	if err := b.Tell(t.Context(), "shared", statecharts.Event{Name: "trace", Type: statecharts.EventExternal}); err != nil {
		t.Fatal(err)
	}
	if record := <-alphaStream.C; record.System != "alpha" || record.Observation == nil || record.Observation.Actor.ID != "shared" {
		t.Fatalf("alpha stream = %#v", record)
	}
	if record := <-zetaStream.C; record.System != "zeta" || record.Observation == nil || record.Observation.Actor.ID != "shared" {
		t.Fatalf("zeta stream = %#v", record)
	}
	alphaStream.Close()
	zetaStream.Close()
	s.Close()
	if len(s.systems) != 0 {
		t.Fatal("registry was not emptied")
	}
	for _, system := range []*actors.System{a, b} {
		if err := system.Tell(context.Background(), "shared", statecharts.Event{Name: "still-alive", Type: statecharts.EventExternal}); err != nil {
			t.Fatalf("Tell after inspector close: %v", err)
		}
	}
}

func TestDefinitionAuthorizationIsIndependent(t *testing.T) {
	system := testSystem(t, "definition-auth", "actor")
	var requests []AuthorizationRequest
	service := New(WithAuthorizer(AuthorizerFunc(func(_ context.Context, request AuthorizationRequest) error {
		requests = append(requests, request)
		if request.Operation == OperationReadDefinition {
			return errors.New("definition denied detail")
		}
		return nil
	})))
	t.Cleanup(service.Close)
	if err := service.RegisterSystem("system", system); err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.InspectActor(t.Context(), "system", "actor"); err != nil {
		t.Fatalf("actor read = %v", err)
	}
	if _, err := service.InspectActorDefinition(t.Context(), "system", "actor"); !errors.Is(err, ErrUnauthorized) || err.Error() != ErrUnauthorized.Error() {
		t.Fatalf("definition read = %v", err)
	}
	if len(requests) != 2 || requests[0] != (AuthorizationRequest{Operation: OperationReadActor, System: "system", ActorID: "actor"}) || requests[1] != (AuthorizationRequest{Operation: OperationReadDefinition, System: "system", ActorID: "actor"}) {
		t.Fatalf("authorization requests = %#v", requests)
	}
}

func TestNilAuthorizerDeniesEveryExportedOperation(t *testing.T) {
	s := New()
	checks := map[string]func() error{
		"systems":    func() error { _, e := s.Systems(context.Background()); return e },
		"actors":     func() error { _, e := s.QueryActors(context.Background(), "x", actors.ActorQuery{}); return e },
		"actor":      func() error { _, _, e := s.InspectActor(context.Background(), "x", "a"); return e },
		"definition": func() error { _, e := s.InspectActorDefinition(context.Background(), "x", "a"); return e },
		"history": func() error {
			_, e := s.QueryActorHistory(context.Background(), "x", "a", actors.ActorHistoryQuery{})
			return e
		},
		"recent":    func() error { _, e := s.Recent(context.Background(), "x", 0, 1); return e },
		"subscribe": func() error { _, e := s.Subscribe(context.Background(), "x", 1); return e },
		"send":      func() error { return s.SendEvent(context.Background(), "x", "a", "event", statecharts.Value{}) },
	}
	for name, check := range checks {
		if err := check(); !errors.Is(err, ErrUnauthorized) {
			t.Errorf("%s = %v", name, err)
		}
	}
}
