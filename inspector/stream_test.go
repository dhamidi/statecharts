package inspector

import (
	"context"
	"errors"
	"sync"
	"testing"

	statecharts "github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
)

func streamTestService(ringSize int) (*Service, *registered) {
	s := New(WithAuthorizer(AllowAll()), WithRingSize(ringSize))
	r := &registered{subscribers: make(map[uint64]*subscriber)}
	s.systems["test"] = r
	return s, r
}

func TestRecentZeroCursorAfterRolloverAndBoundaries(t *testing.T) {
	s, r := streamTestService(2)
	for range 3 {
		r.add(s, "test", StreamObservation, &actors.Observation{}, 0, "")
	}
	p, err := s.Recent(context.Background(), "test", 0, 1)
	if err != nil || p.Expired || p.Oldest != 2 || p.Latest != 3 || p.Next != 2 || len(p.Records) != 1 {
		t.Fatalf("first page = %#v, %v", p, err)
	}
	p, err = s.Recent(context.Background(), "test", p.Next, 1)
	if err != nil || p.Expired || p.Next != 3 || len(p.Records) != 1 || p.Records[0].Sequence != 3 {
		t.Fatalf("second page = %#v, %v", p, err)
	}
	p, err = s.Recent(context.Background(), "test", 1, 10)
	if err != nil || p.Expired {
		t.Fatalf("oldest-1 is an exact boundary: %#v, %v", p, err)
	}
}

func TestRecentCursorAheadOfCurrentStreamExpiresAndRestartsFromRetainedBoundary(t *testing.T) {
	s, r := streamTestService(2)
	for range 3 {
		r.add(s, "test", StreamObservation, &actors.Observation{}, 0, "")
	}
	p, err := s.Recent(context.Background(), "test", 500, 10)
	if err != nil || !p.Expired || p.Oldest != 2 || p.Latest != 3 || p.Next != 3 || len(p.Records) != 3 {
		t.Fatalf("page = %#v, %v", p, err)
	}
	if gap := p.Records[0]; gap.Kind != StreamGap || gap.Sequence != 1 || gap.Reason != "cursor ahead of stream" {
		t.Fatalf("gap = %#v", gap)
	}
	if p.Records[1].Sequence != 2 || p.Records[2].Sequence != 3 {
		t.Fatalf("retained records = %#v", p.Records)
	}
}

func TestSlowSubscriberGetsCurrentRecordWithDroppedCount(t *testing.T) {
	s, r := streamTestService(8)
	ch := make(chan StreamRecord, 1)
	r.subscribers[1] = &subscriber{ch: ch}
	r.add(s, "test", StreamObservation, &actors.Observation{}, 0, "")
	r.add(s, "test", StreamObservation, &actors.Observation{}, 0, "")
	first := <-ch
	if first.Sequence != 1 {
		t.Fatal(first)
	}
	r.add(s, "test", StreamObservation, &actors.Observation{}, 0, "")
	delivery := <-ch
	if delivery.Sequence != 3 || delivery.Kind != StreamObservation || delivery.Observation == nil || delivery.Dropped != 1 {
		t.Fatalf("delivery skipped current observation: %#v", delivery)
	}
}

func TestSourceEndClosesSubscribersAndRejectsLaterSubscribe(t *testing.T) {
	s, r := streamTestService(8)
	source := make(chan actors.Observation)
	r.source = &actors.ObservationSubscription{C: source}
	s.wg.Add(1)
	go s.drain("test", r)
	sub, err := s.Subscribe(context.Background(), "test", 1)
	if err != nil {
		t.Fatal(err)
	}
	close(source)
	if _, ok := <-sub.C; ok {
		t.Fatal("subscriber remained open")
	}
	if _, err := s.Subscribe(context.Background(), "test", 1); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("later subscribe error = %v", err)
	}
}

func TestSourceDroppedBecomesSingleGapAndIsCleared(t *testing.T) {
	s, r := streamTestService(8)
	source := make(chan actors.Observation, 1)
	r.source = &actors.ObservationSubscription{C: source}
	s.wg.Add(1)
	go s.drain("test", r)
	source <- actors.Observation{Dropped: 3}
	close(source)
	s.wg.Wait()
	page, err := s.Recent(context.Background(), "test", 0, 10)
	if err != nil || len(page.Records) != 2 || page.Records[0].Kind != StreamGap || page.Records[0].Dropped != 3 || page.Records[1].Observation.Dropped != 0 {
		t.Fatalf("records = %#v, err = %v", page.Records, err)
	}
}

func TestSubscriberLossCombinesWithSourceGap(t *testing.T) {
	s, r := streamTestService(8)
	ch := make(chan StreamRecord, 1)
	r.subscribers[1] = &subscriber{ch: ch, dropped: 2}
	r.add(s, "test", StreamGap, nil, 4, "source observations dropped")
	x := <-ch
	if x.Kind != StreamGap || x.Dropped != 6 || x.Reason != "source observations dropped; subscriber observations dropped" {
		t.Fatalf("combined gap = %#v", x)
	}
}

func TestSubscriptionAndServiceCloseRaceWithPublishing(t *testing.T) {
	system := testSystem(t, "stream-close", "actor")
	service := New(WithAuthorizer(AllowAll()))
	if err := service.RegisterSystem("system", system); err != nil {
		t.Fatal(err)
	}
	sub, err := service.Subscribe(t.Context(), "system", 1)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		defer wg.Done()
		<-start
		for range 100 {
			_ = system.Tell(context.Background(), "actor", statecharts.Event{Name: "trace", Type: statecharts.EventExternal})
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		sub.Close()
		sub.Close()
	}()
	go func() {
		defer wg.Done()
		<-start
		service.Close()
	}()
	go func() {
		defer wg.Done()
		<-start
		for range 100 {
			_, _ = service.Recent(context.Background(), "system", 0, 1)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for range 100 {
			x, _ := service.Subscribe(context.Background(), "system", 1)
			if x != nil {
				x.Close()
			}
		}
	}()
	close(start)
	wg.Wait()
	for range sub.C {
	}
	if err := system.Tell(t.Context(), "actor", statecharts.Event{Name: "still-live", Type: statecharts.EventExternal}); err != nil {
		t.Fatalf("Service.Close stopped actor system: %v", err)
	}
}
