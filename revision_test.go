package statecharts

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"
)

type revisionState struct{ Count int }

func buildGoRevisionChart(t *testing.T, event, functionVersion, salt string, reverseRegistration, reverseMap bool, delta int) (*Chart, *GoModel[revisionState]) {
	t.Helper()
	model := NewGoModel(func() *revisionState { return &revisionState{} })
	registerUsed := func() GoActionRef {
		ref, err := model.Action("revision.record", functionVersion, func(state *revisionState, _ ExecContext, _ []Value) error {
			state.Count += delta
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	registerUnused := func() {
		if _, err := model.Action("revision.unused", "v9", func(*revisionState, ExecContext, []Value) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	var action GoActionRef
	if reverseRegistration {
		registerUnused()
		action = registerUsed()
	} else {
		action = registerUsed()
		registerUnused()
	}
	fields := make(map[string]Value, 2)
	if reverseMap {
		fields["z"] = Int64Value(2)
		fields["a"], _ = StringValue("first")
	} else {
		fields["a"], _ = StringValue("first")
		fields["z"] = Int64Value(2)
	}
	payload, err := MapValue(fields)
	if err != nil {
		t.Fatal(err)
	}
	chart, err := Build(
		Atomic("revision-test", On(event, Then(action.Do(), Raise("recorded", GoLiteral(payload))))),
		model,
		WithRevisionSalt(salt),
	)
	if err != nil {
		t.Fatal(err)
	}
	return chart, model
}

func TestChartRevisionIsDeterministicAndRoundTrips(t *testing.T) {
	left, model := buildGoRevisionChart(t, "apply", "v1", "salt", false, false, 1)
	right, _ := buildGoRevisionChart(t, "apply", "v1", "salt", true, true, 999)
	if left.Revision() != right.Revision() {
		t.Fatalf("equivalent independently built revisions differ: %q != %q", left.Revision(), right.Revision())
	}
	if !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(string(left.Revision())) {
		t.Fatalf("Revision = %q, want sha256:<64 lowercase hex digits>", left.Revision())
	}
	wire, err := json.Marshal(left.Definition())
	if err != nil {
		t.Fatal(err)
	}
	var decoded Definition
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	recompiled, err := Compile(decoded, model)
	if err != nil {
		t.Fatal(err)
	}
	if recompiled.Revision() != left.Revision() {
		t.Fatalf("round-tripped revision = %q, want %q", recompiled.Revision(), left.Revision())
	}
}

func TestChartRevisionChangesWithSemanticInputs(t *testing.T) {
	base, _ := buildGoRevisionChart(t, "apply", "v1", "salt", false, false, 1)
	tests := []struct {
		name  string
		chart *Chart
	}{
		{"definition", func() *Chart {
			chart, _ := buildGoRevisionChart(t, "apply.changed", "v1", "salt", false, false, 1)
			return chart
		}()},
		{"function version", func() *Chart {
			chart, _ := buildGoRevisionChart(t, "apply", "v2", "salt", false, false, 1)
			return chart
		}()},
		{"revision salt", func() *Chart {
			chart, _ := buildGoRevisionChart(t, "apply", "v1", "other", false, false, 1)
			return chart
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.chart.Revision() == base.Revision() {
				t.Fatalf("revision did not change: %q", base.Revision())
			}
		})
	}
}

type revisionDatamodel struct {
	name        Identifier
	fingerprint []byte
}

func (m revisionDatamodel) Name() Identifier { return m.name }
func (m revisionDatamodel) Compile(*Definition) (DatamodelProgram, error) {
	return &revisionProgram{recordingProgram: &recordingProgram{}, fingerprint: append([]byte(nil), m.fingerprint...)}, nil
}

type revisionProgram struct {
	*recordingProgram
	fingerprint []byte
}

func (p *revisionProgram) Fingerprint() []byte { return append([]byte(nil), p.fingerprint...) }

func TestChartRevisionIncludesDatamodelNameAndFingerprint(t *testing.T) {
	build := func(model revisionDatamodel) *Chart {
		chart, err := Build(Atomic("revision-model"), model)
		if err != nil {
			t.Fatal(err)
		}
		return chart
	}
	left := build(revisionDatamodel{name: "model-a", fingerprint: []byte("bc")})
	right := build(revisionDatamodel{name: "model-ab", fingerprint: []byte("c")})
	changedFingerprint := build(revisionDatamodel{name: "model-a", fingerprint: []byte("bd")})
	if left.Revision() == right.Revision() {
		t.Fatal("datamodel name/fingerprint field boundaries collide")
	}
	if left.Revision() == changedFingerprint.Revision() {
		t.Fatal("datamodel fingerprint did not affect revision")
	}
	if _, err := Build(Atomic("empty-fingerprint"), revisionDatamodel{name: "empty"}); err == nil || !strings.Contains(err.Error(), "empty program fingerprint") {
		t.Fatalf("empty fingerprint error = %v", err)
	}
}

func TestRehydrateIgnoresSnapshotFromOtherRevision(t *testing.T) {
	type replayState struct{ Count int }
	build := func(salt string, created *[]*replayState) *Chart {
		model := NewGoModel(func() *replayState {
			state := &replayState{}
			*created = append(*created, state)
			return state
		})
		apply, err := model.Action("revision.replay.apply", "v1", func(state *replayState, _ ExecContext, _ []Value) error {
			state.Count++
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		chart, err := Build(Atomic("revision-replay", On("apply", Then(apply.Do()))), model, WithRevisionSalt(salt))
		if err != nil {
			t.Fatal(err)
		}
		return chart
	}

	ctx := context.Background()
	log := newMemLog()
	store := newMemSnapshotStore()
	sessionID := SessionID("revision-cache-miss")
	event := Event{Name: "apply", Type: EventExternal}
	seq, err := log.Append(ctx, LogEntry{SessionID: sessionID, Kind: KindExternalEvent, Timestamp: time.Unix(1, 0), Event: event})
	if err != nil {
		t.Fatal(err)
	}
	var firstCreated []*replayState
	first := build("first", &firstCreated)
	instance, err := first.NewInstance()
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := instance.Send(ctx, event); err != nil {
		t.Fatal(err)
	}
	snapshot, err := instance.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(wire), `"revision"`) || strings.Contains(string(wire), "chart_version") {
		t.Fatalf("snapshot wire does not use revision identity: %s", wire)
	}
	if err := instance.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, sessionID, Checkpoint{Snapshot: snapshot, Seq: seq}); err != nil {
		t.Fatal(err)
	}

	var secondCreated []*replayState
	second := build("second", &secondCreated)
	rehydrated, err := second.Rehydrate(ctx, log, store, sessionID, NoopIOProcessor)
	if err != nil {
		t.Fatal(err)
	}
	defer rehydrated.Stop(ctx)
	if len(secondCreated) != 1 || secondCreated[0].Count != 1 {
		t.Fatalf("replayed models = %#v, want one fresh model with Count=1", secondCreated)
	}
	refreshed, ok, err := store.Load(ctx, sessionID)
	if err != nil || !ok {
		t.Fatalf("load refreshed checkpoint = ok %v, err %v", ok, err)
	}
	if refreshed.Snapshot.Revision != second.Revision() {
		t.Fatalf("refreshed snapshot revision = %q, want %q", refreshed.Snapshot.Revision, second.Revision())
	}
}
