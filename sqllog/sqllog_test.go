package sqllog_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/sqllog"
)

func openTestLog(t *testing.T) *sqllog.Log {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	log, err := sqllog.New(db, sqllog.SQLite)
	if err != nil {
		t.Fatalf("sqllog.New: %v", err)
	}
	return log
}

func TestAppendAssignsSequentialSeq(t *testing.T) {
	log := openTestLog(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		seq, err := log.Append(ctx, statecharts.LogEntry{
			SessionID: "s1", Kind: statecharts.KindExternalEvent,
			Timestamp: time.Now().UTC(),
			Event:     statecharts.Event{Name: "go", Type: statecharts.EventExternal},
		})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if seq != uint64(i+1) {
			t.Fatalf("Append seq = %d, want %d", seq, i+1)
		}
	}

	// A different session must get its own independent sequence.
	seq, err := log.Append(ctx, statecharts.LogEntry{
		SessionID: "s2", Kind: statecharts.KindExternalEvent,
		Timestamp: time.Now().UTC(),
		Event:     statecharts.Event{Name: "go", Type: statecharts.EventExternal},
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if seq != 1 {
		t.Fatalf("Append seq for new session = %d, want 1", seq)
	}
}

func TestReadStreamsInOrderFromOffset(t *testing.T) {
	log := openTestLog(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if _, err := log.Append(ctx, statecharts.LogEntry{
			SessionID: "s1", Kind: statecharts.KindExternalEvent,
			Timestamp: time.Now().UTC(),
			Event:     statecharts.Event{Name: statecharts.Identifier("evt"), Type: statecharts.EventExternal},
		}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	var seqs []uint64
	for entry, err := range log.Read(ctx, "s1", 3) {
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		seqs = append(seqs, entry.Seq)
	}
	want := []uint64{3, 4, 5}
	if len(seqs) != len(want) {
		t.Fatalf("Read from 3 = %v, want %v", seqs, want)
	}
	for i := range want {
		if seqs[i] != want[i] {
			t.Fatalf("Read from 3 = %v, want %v", seqs, want)
		}
	}

	last, err := log.LastSeq(ctx, "s1")
	if err != nil {
		t.Fatalf("LastSeq: %v", err)
	}
	if last != 5 {
		t.Fatalf("LastSeq = %d, want 5", last)
	}

	last, err = log.LastSeq(ctx, "unknown-session")
	if err != nil {
		t.Fatalf("LastSeq: %v", err)
	}
	if last != 0 {
		t.Fatalf("LastSeq(unknown) = %d, want 0", last)
	}
}

type payload struct {
	Amount int `json:"amount"`
}

func TestAppendReadRoundTripsEventDataPayload(t *testing.T) {
	statecharts.RegisterDataType("test.payload", func() statecharts.DataUnmarshaler {
		return &statecharts.JSONData[payload]{TypeName: "test.payload"}
	})

	log := openTestLog(t)
	ctx := context.Background()

	data := statecharts.NewJSONData("test.payload", payload{Amount: 42})
	_, err := log.Append(ctx, statecharts.LogEntry{
		SessionID: "s1", Kind: statecharts.KindExternalEvent,
		Timestamp: time.Now().UTC(),
		Event:     statecharts.Event{Name: "paid", Type: statecharts.EventExternal, Data: data},
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	var got statecharts.Event
	for entry, err := range log.Read(ctx, "s1", 1) {
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		got = entry.Event
	}

	decoded, ok := got.Data.(*statecharts.JSONData[payload])
	if !ok {
		t.Fatalf("decoded Data type = %T, want *JSONData[payload]", got.Data)
	}
	if decoded.Value.Amount != 42 {
		t.Fatalf("decoded amount = %d, want 42", decoded.Value.Amount)
	}
}

func TestSnapshotStoreSaveLoad(t *testing.T) {
	log := openTestLog(t)
	ctx := context.Background()

	if _, ok, err := log.Load(ctx, "s1"); err != nil || ok {
		t.Fatalf("Load before Save: ok=%v err=%v, want ok=false err=nil", ok, err)
	}

	snap := statecharts.Snapshot{
		Version:       1,
		Configuration: []statecharts.Identifier{"open"},
		Running:       true,
	}
	if err := log.Save(ctx, "s1", statecharts.Checkpoint{Snapshot: snap, Seq: 7}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	cp, ok, err := log.Load(ctx, "s1")
	if err != nil || !ok {
		t.Fatalf("Load: ok=%v err=%v, want ok=true err=nil", ok, err)
	}
	if cp.Seq != 7 {
		t.Fatalf("Load seq = %d, want 7", cp.Seq)
	}
	if len(cp.Snapshot.Configuration) != 1 || cp.Snapshot.Configuration[0] != "open" {
		t.Fatalf("Load configuration = %v, want ['open']", cp.Snapshot.Configuration)
	}

	// Save again for the same session must upsert (replace), not duplicate.
	snap2 := snap
	snap2.Configuration = []statecharts.Identifier{"closed"}
	if err := log.Save(ctx, "s1", statecharts.Checkpoint{Snapshot: snap2, Seq: 9}); err != nil {
		t.Fatalf("Save (again): %v", err)
	}
	cp, ok, err = log.Load(ctx, "s1")
	if err != nil || !ok {
		t.Fatalf("Load after re-save: ok=%v err=%v", ok, err)
	}
	if cp.Seq != 9 || cp.Snapshot.Configuration[0] != "closed" {
		t.Fatalf("Load after re-save = %+v, want seq=9 configuration=['closed']", cp)
	}
}

func TestRehydrateAgainstRealDatabase(t *testing.T) {
	log := openTestLog(t)
	ctx := context.Background()
	sessionID := "door-1"

	notLocked := statecharts.Cond(func(d *doorModel, ec statecharts.ExecContext) bool { return !d.Locked })
	recordOpen := statecharts.Action(func(d *doorModel, ec statecharts.ExecContext) error { d.OpenCount++; return nil })
	chart, err := statecharts.Build(
		statecharts.Compound("door", "closed",
			statecharts.Children(
				statecharts.Atomic("closed", statecharts.On("open.request",
					statecharts.Target("open"), statecharts.If(notLocked), statecharts.Then(recordOpen))),
				statecharts.Atomic("open", statecharts.On("close.request", statecharts.Target("closed"))),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	d := &doorModel{}
	in := statecharts.New(chart, d)
	if err := in.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ev := statecharts.Event{Name: "open.request", Type: statecharts.EventExternal}
	if _, err := log.Append(ctx, statecharts.LogEntry{
		SessionID: sessionID, Kind: statecharts.KindExternalEvent, Timestamp: time.Now().UTC(), Event: ev,
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := in.Send(ctx, ev); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := in.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	d2 := &doorModel{}
	in2, err := statecharts.Rehydrate(ctx, chart, d2, log, log, sessionID, statecharts.NoopIOProcessor)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}
	cfg := in2.Configuration()
	found := false
	for _, id := range cfg {
		if id == "open" {
			found = true
		}
	}
	if !found {
		t.Fatalf("rehydrated configuration = %v, want to contain 'open'", cfg)
	}
	if d2.OpenCount != 1 {
		t.Fatalf("d2.OpenCount = %d, want 1", d2.OpenCount)
	}
	if err := in2.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

type doorModel struct {
	OpenCount int
	Locked    bool
}
