package actors

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/dhamidi/statecharts"
)

func nextObservation(t *testing.T, sub *ObservationSubscription) Observation {
	t.Helper()
	select {
	case observation, ok := <-sub.C:
		if !ok {
			t.Fatal("observation subscription closed")
		}
		return observation
	default:
		t.Fatal("synchronous observation was not delivered")
	}
	return Observation{}
}

func drainObservations(sub *ObservationSubscription) []Observation {
	var observations []Observation
	for {
		select {
		case observation, ok := <-sub.C:
			if !ok {
				return observations
			}
			observations = append(observations, observation)
		default:
			return observations
		}
	}
}

func TestObservationLifecycleAndMacrosteps(t *testing.T) {
	clock := statecharts.NewManualClock(time.Date(2026, 7, 27, 1, 2, 3, 0, time.FixedZone("test", 3600)))
	model := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	definition := publicationDefinition("finished-v1", "observations")
	definition.Root.Children[1].Kind = statecharts.KindFinal
	chart, err := statecharts.Compile(definition, model)
	if err != nil {
		t.Fatal(err)
	}
	system := NewSystem(WithClock(clock))
	t.Cleanup(func() { _ = system.Stop(context.Background()) })
	if err := system.Register(chart); err != nil {
		t.Fatal(err)
	}
	sub, err := system.SubscribeObservations(16)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := system.Spawn(t.Context(), "observed", chart.ID()); err != nil {
		t.Fatal(err)
	}
	wantKinds := []ObservationKind{ObservationActorDiscovered, ObservationResidencyChanged, ObservationMacrostep, ObservationResidencyChanged}
	for i, kind := range wantKinds {
		observation := nextObservation(t, sub)
		if observation.Kind != kind || observation.Sequence != uint64(i+1) || observation.Timestamp.Location() != time.UTC {
			t.Fatalf("observation %d = %#v, want kind %q sequence %d UTC", i, observation, kind, i+1)
		}
		if observation.Actor == nil || observation.Actor.ID != "observed" {
			t.Fatalf("actor observation = %#v", observation.Actor)
		}
		if kind == ObservationMacrostep && (observation.Macrostep == nil || observation.Macrostep.Trigger != nil) {
			t.Fatalf("initial macrostep = %#v", observation.Macrostep)
		}
	}
	if err := system.Tell(t.Context(), "observed", statecharts.Event{Name: "finish", Type: statecharts.EventExternal}); err != nil {
		t.Fatal(err)
	}
	terminal := nextObservation(t, sub)
	trace := nextObservation(t, sub)
	if terminal.Kind != ObservationActorTerminal || terminal.Actor.Lifecycle != statecharts.ActorLifecycleTerminal {
		t.Fatalf("terminal = %#v", terminal)
	}
	if trace.Kind != ObservationMacrostep || trace.Macrostep == nil || len(trace.Macrostep.Microsteps) != 1 || len(trace.Macrostep.Microsteps[0].Transitions) != 1 {
		t.Fatalf("live macrostep = %#v", trace)
	}
	if ref := trace.Macrostep.Microsteps[0].Transitions[0]; ref.Source != "waiting" || ref.Index != 0 {
		t.Fatalf("transition ref = %#v", ref)
	}
}

func TestObservationPublicationDropsIsolationAndStop(t *testing.T) {
	model := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	v1 := publicationChart(t, model, "finished-v1", "observation-v1")
	system := NewSystem()
	if err := system.Register(v1); err != nil {
		t.Fatal(err)
	}
	slow, err := system.SubscribeObservations(1)
	if err != nil {
		t.Fatal(err)
	}
	fast, err := system.SubscribeObservations(16)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := system.Publish(t.Context(), publicationDefinition("finished-v2", "observation-v2"))
	if err != nil {
		t.Fatal(err)
	}
	firstFast := nextObservation(t, fast)
	if firstFast.Definition == nil || firstFast.Definition.PreviousRevision != v1.Revision() || firstFast.Definition.Revision != v2 {
		t.Fatalf("publication = %#v", firstFast)
	}
	firstFast.Definition.ChartID = "mutated"
	firstSlow := nextObservation(t, slow)
	if firstSlow.Definition == nil || firstSlow.Definition.ChartID != v1.ID() {
		t.Fatalf("publication payloads alias: %#v", firstSlow)
	}
	if _, current, ok := system.CurrentDefinition(v1.ID()); !ok || current != v2 {
		t.Fatalf("mutated publication changed current definition to %q, %v", current, ok)
	}
	if _, err := system.Publish(t.Context(), publicationDefinition("finished-v2", "observation-v2")); err != nil {
		t.Fatal(err)
	}
	if got := drainObservations(fast); len(got) != 0 {
		t.Fatalf("idempotent publication emitted to fast subscriber: %#v", got)
	}
	if got := drainObservations(slow); len(got) != 0 {
		t.Fatalf("idempotent publication emitted to slow subscriber: %#v", got)
	}
	if err := system.Spawn(t.Context(), "drop-test", v1.ID()); err != nil {
		t.Fatal(err)
	}
	// Slow contains discovery; the remaining spawn observations were dropped.
	_ = nextObservation(t, slow)
	if err := system.Tell(t.Context(), "drop-test", statecharts.Event{Name: "finish", Type: statecharts.EventExternal}); err != nil {
		t.Fatal(err)
	}
	recovered := nextObservation(t, slow)
	if recovered.Dropped < 3 {
		t.Fatalf("Dropped = %d, want at least three stalled observations", recovered.Dropped)
	}
	if first := nextObservation(t, fast); first.Dropped != 0 || first.Actor == nil || first.Actor.ID != "drop-test" {
		t.Fatalf("fast subscriber = %#v", first)
	}
	slow.Close()
	slow.Close()
	if _, ok := <-slow.C; ok {
		t.Fatal("closed subscription channel remained open")
	}
	if err := system.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	for range fast.C {
	}
	if _, err := system.SubscribeObservations(1); !errors.Is(err, ErrSystemStopped) {
		t.Fatalf("Subscribe after Stop = %v, want ErrSystemStopped", err)
	}
}

func TestObservationExactBoundedLoss(t *testing.T) {
	system := NewSystem()
	slow, err := system.SubscribeObservations(1)
	if err != nil {
		t.Fatal(err)
	}
	fast, err := system.SubscribeObservations(32)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(slow.Close)
	t.Cleanup(fast.Close)

	system.publishObservation(Observation{Kind: ObservationResidencyChanged})
	const dropped = 7
	for range dropped {
		system.publishObservation(Observation{Kind: ObservationResidencyChanged})
	}
	first := nextObservation(t, slow)
	system.publishObservation(Observation{Kind: ObservationResidencyChanged})
	recovered := nextObservation(t, slow)
	if first.Sequence != 1 || first.Dropped != 0 || recovered.Sequence != dropped+2 || recovered.Dropped != dropped {
		t.Fatalf("slow deliveries = %#v, %#v; want sequences 1/%d and exact dropped %d", first, recovered, dropped+2, dropped)
	}
	all := drainObservations(fast)
	if len(all) != dropped+2 {
		t.Fatalf("fast received %d, want %d", len(all), dropped+2)
	}
	for i, observation := range all {
		if observation.Sequence != uint64(i+1) || observation.Dropped != 0 {
			t.Fatalf("fast[%d] = %#v", i, observation)
		}
	}
}

func TestDurableReplayObservationsAreLiveOnly(t *testing.T) {
	ctx := t.Context()
	storage := openTestLog(t)
	model := statecharts.NewGoModel(func() *struct{} { return &struct{}{} })
	chart := publicationChart(t, model, "finished-v1", "observation-replay")
	seed := NewSystem(WithStorage(storage))
	if err := seed.Register(chart); err != nil {
		t.Fatal(err)
	}
	if err := seed.Spawn(ctx, "replayed", chart.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	if err := seed.Tell(ctx, "replayed", statecharts.Event{Name: "finish", Type: statecharts.EventExternal}); err != nil {
		t.Fatal(err)
	}
	if err := seed.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	fresh := NewSystem(WithStorage(storage))
	t.Cleanup(func() { _ = fresh.Stop(context.Background()) })
	if err := fresh.Register(chart); err != nil {
		t.Fatal(err)
	}
	sub, err := fresh.SubscribeObservations(16)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := fresh.Spawn(ctx, "replayed", chart.ID(), Durable()); err != nil {
		t.Fatal(err)
	}
	bootstrap := drainObservations(sub)
	var hydrating, resident bool
	for _, observation := range bootstrap {
		if observation.Kind == ObservationMacrostep {
			t.Fatalf("replay emitted macrostep: %#v", observation)
		}
		if observation.Actor != nil && observation.Actor.Residency == ResidencyHydrating {
			hydrating = true
		}
		if observation.Actor != nil && observation.Actor.Residency == ResidencyResident {
			resident = true
		}
	}
	if !hydrating || !resident {
		t.Fatalf("bootstrap observations = %#v", bootstrap)
	}
	if err := fresh.Tell(ctx, "replayed", statecharts.Event{Name: "ignored", Type: statecharts.EventExternal}); err != nil {
		t.Fatal(err)
	}
	live := drainObservations(sub)
	macrosteps := 0
	for _, observation := range live {
		if observation.Kind == ObservationMacrostep {
			macrosteps++
		}
	}
	if macrosteps != 1 {
		t.Fatalf("live observations = %#v; want one macrostep", live)
	}
}

func TestObservationCloneDeepIsolation(t *testing.T) {
	stringValue, err := statecharts.StringValue("original")
	if err != nil {
		t.Fatal(err)
	}
	trace := statecharts.MacrostepTrace{
		Trigger: &statecharts.Event{Name: "trigger", Data: stringValue}, Before: []statecharts.Identifier{"before"}, After: []statecharts.Identifier{"after"},
		Microsteps: []statecharts.MicrostepTrace{{Trigger: &statecharts.Event{Name: "micro", Data: stringValue}, Transitions: []statecharts.TransitionRef{{Source: "source", Index: 2}}, Exited: []statecharts.Identifier{"exit"}, Entered: []statecharts.Identifier{"enter"}}},
	}
	original := Observation{Actor: &ActorInfo{ID: "actor", Kind: "kind"}, Macrostep: &trace}
	a, b := original.Clone(), original.Clone()
	a.Actor.ID = "mutated"
	a.Macrostep.Trigger.Name = "mutated"
	a.Macrostep.Before[0], a.Macrostep.After[0] = "mutated", "mutated"
	a.Macrostep.Microsteps[0].Trigger.Name = "mutated"
	a.Macrostep.Microsteps[0].Transitions[0].Source = "mutated"
	a.Macrostep.Microsteps[0].Exited[0], a.Macrostep.Microsteps[0].Entered[0] = "mutated", "mutated"
	if b.Actor.ID != "actor" || b.Macrostep.Trigger.Name != "trigger" || b.Macrostep.Before[0] != "before" || b.Macrostep.After[0] != "after" || b.Macrostep.Microsteps[0].Trigger.Name != "micro" || b.Macrostep.Microsteps[0].Transitions[0].Source != "source" || b.Macrostep.Microsteps[0].Exited[0] != "exit" || b.Macrostep.Microsteps[0].Entered[0] != "enter" {
		t.Fatalf("clone aliased: %#v", b)
	}
}

func TestObservationSubscribersCannotMutateEachOtherOrSystem(t *testing.T) {
	chart, err := statecharts.Build(
		statecharts.Compound("isolation", "a", statecharts.Children(
			statecharts.Atomic("a", statecharts.On("go", statecharts.Target("b"))),
			statecharts.Atomic("b", statecharts.On("back", statecharts.Target("a"))),
		)),
		statecharts.NewGoModel(func() *struct{} { return &struct{}{} }),
	)
	if err != nil {
		t.Fatal(err)
	}
	system := NewSystem()
	t.Cleanup(func() { _ = system.Stop(context.Background()) })
	if err := system.Register(chart); err != nil {
		t.Fatal(err)
	}
	a, err := system.SubscribeObservations(16)
	if err != nil {
		t.Fatal(err)
	}
	b, err := system.SubscribeObservations(16)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	defer b.Close()
	if err := system.Spawn(t.Context(), "isolated", chart.ID()); err != nil {
		t.Fatal(err)
	}
	drainObservations(a)
	drainObservations(b)
	payload, err := statecharts.MapValue(map[string]statecharts.Value{"count": statecharts.Int64Value(7)})
	if err != nil {
		t.Fatal(err)
	}
	if err := system.Tell(t.Context(), "isolated", statecharts.Event{Name: "go", Type: statecharts.EventExternal, Data: payload}); err != nil {
		t.Fatal(err)
	}
	left := nextObservation(t, a)
	right := nextObservation(t, b)
	if left.Kind != ObservationMacrostep || right.Kind != ObservationMacrostep {
		t.Fatalf("macrosteps = %#v / %#v", left, right)
	}
	left.Actor.ID = "mutated"
	left.Macrostep.Trigger.Name = "mutated"
	left.Macrostep.Trigger.Data = statecharts.NullValue()
	left.Macrostep.Before[0] = "mutated"
	left.Macrostep.After[0] = "mutated"
	left.Macrostep.Microsteps[0].Trigger.Name = "mutated"
	left.Macrostep.Microsteps[0].Trigger.Data = statecharts.NullValue()
	left.Macrostep.Microsteps[0].Transitions[0] = statecharts.TransitionRef{Source: "mutated", Index: 99}
	left.Macrostep.Microsteps[0].Exited[0] = "mutated"
	left.Macrostep.Microsteps[0].Entered[0] = "mutated"

	data, ok := right.Macrostep.Trigger.Data.AsMap()
	count, countOK := data["count"].AsInt64()
	if !ok || !countOK || count != 7 || right.Actor.ID != "isolated" || right.Macrostep.Trigger.Name != "go" ||
		!reflect.DeepEqual(right.Macrostep.Before, []statecharts.Identifier{"a"}) ||
		!reflect.DeepEqual(right.Macrostep.After, []statecharts.Identifier{"b"}) ||
		!reflect.DeepEqual(right.Macrostep.Microsteps[0].Transitions, []statecharts.TransitionRef{{Source: "a", Index: 0}}) {
		t.Fatalf("subscriber payloads alias: %#v", right)
	}
	directory, err := system.QueryActors(t.Context(), ActorQuery{})
	if err != nil || len(directory.Actors) != 1 || directory.Actors[0].ID != "isolated" {
		t.Fatalf("directory = %#v, %v", directory, err)
	}
	definition, err := system.InspectActorDefinition(t.Context(), "isolated")
	if err != nil || definition.Pinned.Root.Children[0].Transitions[0].Targets[0] != "b" {
		t.Fatalf("definition = %#v, %v", definition, err)
	}
	if err := system.Tell(t.Context(), "isolated", statecharts.Event{Name: "back", Type: statecharts.EventExternal}); err != nil {
		t.Fatal(err)
	}
	if next := nextObservation(t, b); next.Macrostep == nil || !reflect.DeepEqual(next.Macrostep.Microsteps[0].Transitions, []statecharts.TransitionRef{{Source: "b", Index: 0}}) {
		t.Fatalf("future observation was mutated: %#v", next)
	}
}

func TestObservationClosePublishAndStopRaces(t *testing.T) {
	for iteration := 0; iteration < 20; iteration++ {
		system := NewSystem()
		sub, err := system.SubscribeObservations(1)
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; sub.Close(); sub.Close() }()
		go func() {
			defer wg.Done()
			<-start
			for range 100 {
				system.publishObservation(Observation{Kind: ObservationResidencyChanged})
			}
		}()
		close(start)
		wg.Wait()
		if got := system.observerCount.Load(); got != 0 {
			t.Fatalf("observer count = %d", got)
		}
		if len(system.observations) != 0 {
			t.Fatalf("registry retained %d subscriptions", len(system.observations))
		}
	}

	for iteration := 0; iteration < 20; iteration++ {
		system := NewSystem()
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var sub *ObservationSubscription
		var subscribeErr error
		go func() { defer wg.Done(); <-start; sub, subscribeErr = system.SubscribeObservations(1) }()
		go func() { defer wg.Done(); <-start; _ = system.Stop(context.Background()) }()
		close(start)
		wg.Wait()
		if subscribeErr != nil && !errors.Is(subscribeErr, ErrSystemStopped) {
			t.Fatal(subscribeErr)
		}
		if subscribeErr == nil {
			if _, ok := <-sub.C; ok {
				t.Fatal("Stop did not close racing subscription")
			}
		}
		if system.observerCount.Load() != 0 || len(system.observations) != 0 {
			t.Fatal("Stop retained subscription")
		}
	}
}

func TestObservationBufferValidationAndLifecycleFlagWithoutSubscribers(t *testing.T) {
	system := NewSystem()
	for _, size := range []int{0, 1, MaxObservationBuffer} {
		sub, err := system.SubscribeObservations(size)
		if err != nil {
			t.Fatalf("buffer %d: %v", size, err)
		}
		sub.Close()
	}
	for _, size := range []int{-1, MaxObservationBuffer + 1, int(^uint(0) >> 1)} {
		if sub, err := system.SubscribeObservations(size); err == nil || sub != nil {
			t.Fatalf("buffer %d accepted", size)
		}
	}
	entry := &actorEntry{name: "once"}
	system.notifyDiscovered(entry)
	system.notifyTerminal(entry)
	if !entry.discovered.Load() || !entry.observedEnd.Load() {
		t.Fatal("one-time lifecycle flags were not set")
	}
	sub, err := system.SubscribeObservations(2)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	system.notifyDiscovered(entry)
	system.notifyTerminal(entry)
	if got := drainObservations(sub); len(got) != 0 {
		t.Fatalf("duplicate lifecycle published: %#v", got)
	}
}

func TestSystemMacrostepObserverDisabledWithoutSubscribers(t *testing.T) {
	system := NewSystem()
	entry := &actorEntry{}
	if (systemMacrostepObserver{s: system, entry: entry}).Enabled() {
		t.Fatal("observer enabled without subscribers")
	}
	if _, err := system.SubscribeObservations(-1); err == nil {
		t.Fatal("negative observation buffer accepted")
	}
	if _, err := system.SubscribeObservations(MaxObservationBuffer + 1); err == nil {
		t.Fatal("oversized observation buffer accepted")
	}
}
