package sqllog_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/sqllog"
)

var (
	_ statecharts.Log           = (*sqllog.Storage)(nil)
	_ statecharts.SnapshotStore = (*sqllog.Storage)(nil)
)

func TestLoadMalformedSnapshotIsInvalidSnapshot(t *testing.T) {
	store := openTestLog(t)
	if _, err := store.DB().Exec(`INSERT INTO statechart_snapshot(session_id, seq, snapshot_json) VALUES ('bad', 1, '{')`); err != nil {
		t.Fatal(err)
	}
	_, _, err := store.Load(context.Background(), "bad")
	if !errors.Is(err, statecharts.ErrInvalidSnapshot) {
		t.Fatalf("Load error = %v, want ErrInvalidSnapshot", err)
	}
}

func openTestLog(t *testing.T) *sqllog.Log {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
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

func TestAppendReadRoundTripsEventDataPayload(t *testing.T) {
	log := openTestLog(t)
	ctx := context.Background()

	payload, err := statecharts.MapValue(map[string]statecharts.Value{"amount": statecharts.Int64Value(42)})
	if err != nil {
		t.Fatalf("MapValue: %v", err)
	}
	data, err := statecharts.TaggedValue("test.payload/v1", payload)
	if err != nil {
		t.Fatalf("TaggedValue: %v", err)
	}
	_, err = log.Append(ctx, statecharts.LogEntry{
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

	if !got.Data.Equal(data) {
		t.Fatalf("decoded Data = %#v, want %#v", got.Data, data)
	}
}

func canonicalValues(t *testing.T) []statecharts.Value {
	t.Helper()
	text, err := statecharts.StringValue("hello")
	if err != nil {
		t.Fatalf("StringValue: %v", err)
	}
	number, err := statecharts.NumberValue("123.4500")
	if err != nil {
		t.Fatalf("NumberValue: %v", err)
	}
	object, err := statecharts.MapValue(map[string]statecharts.Value{"key": text})
	if err != nil {
		t.Fatalf("MapValue: %v", err)
	}
	tagged, err := statecharts.TaggedValue("test.value/v1", object)
	if err != nil {
		t.Fatalf("TaggedValue: %v", err)
	}
	return []statecharts.Value{
		statecharts.NullValue(),
		statecharts.BoolValue(true),
		text,
		number,
		statecharts.ListValue([]statecharts.Value{text, number}),
		object,
		tagged,
	}
}

func TestLogAndOutboxRoundTripEveryCanonicalValueKind(t *testing.T) {
	store := openTestLog(t)
	ctx := context.Background()
	values := canonicalValues(t)

	for i, value := range values {
		if _, err := store.Append(ctx, statecharts.LogEntry{
			SessionID: "value-log", Kind: statecharts.KindExternalEvent,
			Timestamp: time.Unix(int64(i+1), 0).UTC(),
			Event:     statecharts.Event{Name: statecharts.Identifier(fmt.Sprintf("kind-%d", i)), Data: value},
		}); err != nil {
			t.Fatalf("Append value %d (%s): %v", i, value.Kind(), err)
		}

		deliveryID := statecharts.DeliveryID(fmt.Sprintf("value-outbox:%d", i))
		if err := store.StoreOutbound(ctx, statecharts.OutboundMessage{
			SessionID: "value-outbox", DeliveryID: deliveryID,
			Request: statecharts.SendRequest{
				DeliveryID: deliveryID, SendID: statecharts.Identifier(fmt.Sprintf("send-%d", i)),
				Target: "worker", Type: statecharts.SCXMLEventProcessor,
				Event: statecharts.Identifier(fmt.Sprintf("kind-%d", i)), Data: value,
			},
		}); err != nil {
			t.Fatalf("StoreOutbound value %d (%s): %v", i, value.Kind(), err)
		}
	}

	var logValues []statecharts.Value
	for entry, err := range store.Read(ctx, "value-log", 1) {
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		logValues = append(logValues, entry.Event.Data)
	}
	if len(logValues) != len(values) {
		t.Fatalf("log values = %d, want %d", len(logValues), len(values))
	}
	for i := range values {
		if !logValues[i].Equal(values[i]) {
			t.Fatalf("log value %d = %#v, want %#v", i, logValues[i], values[i])
		}
	}

	outbounds, err := store.Outbounds(ctx, "value-outbox")
	if err != nil {
		t.Fatalf("Outbounds: %v", err)
	}
	if len(outbounds) != len(values) {
		t.Fatalf("outbox values = %d, want %d", len(outbounds), len(values))
	}
	for i := range values {
		if !outbounds[i].Request.Data.Equal(values[i]) {
			t.Fatalf("outbox value %d = %#v, want %#v", i, outbounds[i].Request.Data, values[i])
		}
	}
}

func TestAppendReadRoundTripsTimerDispatchMetadata(t *testing.T) {
	log := openTestLog(t)
	ctx := context.Background()
	_, err := log.Append(ctx, statecharts.LogEntry{
		SessionID: "timer-metadata", Kind: statecharts.KindTimerFired,
		Timestamp: time.Unix(10, 0),
		SendID:    "send-1", Target: "worker-1", Type: "custom-io",
		Event: statecharts.Event{Name: "work", Type: statecharts.EventExternal},
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	var got statecharts.LogEntry
	for entry, err := range log.Read(ctx, "timer-metadata", 1) {
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		got = entry
	}
	if got.SendID != "send-1" || got.Target != "worker-1" || got.Type != "custom-io" {
		t.Fatalf("timer metadata = {SendID:%q Target:%q Type:%q}, want send-1/worker-1/custom-io", got.SendID, got.Target, got.Type)
	}
}

func TestNewRejectsNonCurrentSchemaBeforeMutation(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	_, err = db.Exec(`CREATE TABLE statechart_log (
		session_id TEXT NOT NULL, seq INTEGER NOT NULL, kind TEXT NOT NULL, ts TIMESTAMP NOT NULL,
		entry_send_id TEXT NOT NULL DEFAULT '', entry_target TEXT NOT NULL DEFAULT '',
		event_name TEXT NOT NULL, event_type INTEGER NOT NULL, event_send_id TEXT NOT NULL DEFAULT '',
		event_origin TEXT NOT NULL DEFAULT '', event_origin_type TEXT NOT NULL DEFAULT '',
		event_invoke_id TEXT NOT NULL DEFAULT '', data_type TEXT NOT NULL DEFAULT '', data_payload BLOB,
		PRIMARY KEY (session_id, seq)
	); PRAGMA user_version=5`)
	if err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
	if _, err := sqllog.New(db, sqllog.SQLite); err == nil {
		t.Fatal("New accepted a legacy schema")
	}
	rows, err := db.Query(`SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE 'statechart_%' ORDER BY name`)
	if err != nil {
		t.Fatalf("inspect rejected schema: %v", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Fatalf("scan rejected schema: %v", err)
		}
		tables = append(tables, table)
	}
	if len(tables) != 1 || tables[0] != "statechart_log" {
		t.Fatalf("New mutated rejected schema tables: %v", tables)
	}
	var hasValueData int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('statechart_log') WHERE name='value_data'`).Scan(&hasValueData); err != nil {
		t.Fatalf("inspect rejected columns: %v", err)
	}
	if hasValueData != 0 {
		t.Fatal("New mutated the rejected legacy table")
	}
}

func TestFreshSchemaUsesOnlyCanonicalPayloadColumns(t *testing.T) {
	store := openTestLog(t)
	for _, table := range []string{"statechart_log", "statechart_outbound"} {
		rows, err := store.DB().Query(fmt.Sprintf("SELECT name FROM pragma_table_info('%s')", table))
		if err != nil {
			t.Fatalf("inspect %s: %v", table, err)
		}
		columns := map[string]bool{}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				t.Fatalf("scan %s: %v", table, err)
			}
			columns[name] = true
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close %s columns: %v", table, err)
		}
		if !columns["value_data"] {
			t.Fatalf("%s has no canonical value_data column: %v", table, columns)
		}
		if columns["data_type"] || columns["data_payload"] {
			t.Fatalf("%s retains legacy payload columns: %v", table, columns)
		}
	}
}

func TestStoreOutboundRejectsDeliveryIDCollision(t *testing.T) {
	store := openTestLog(t)
	ctx := context.Background()
	first := statecharts.OutboundMessage{
		SessionID:  "sender",
		DeliveryID: "sender:v1:1",
		Request: statecharts.SendRequest{
			DeliveryID: "sender:v1:1",
			SendID:     "send.1",
			Target:     "worker-a",
			Type:       statecharts.SCXMLEventProcessor,
			Event:      "work",
		},
		Status: statecharts.OutboundPending,
	}
	if err := store.StoreOutbound(ctx, first); err != nil {
		t.Fatalf("StoreOutbound first: %v", err)
	}
	if err := store.StoreOutbound(ctx, first); err != nil {
		t.Fatalf("StoreOutbound exact retry: %v", err)
	}

	collision := first
	collision.Request.Target = "worker-b"
	if err := store.StoreOutbound(ctx, collision); !errors.Is(err, statecharts.ErrOutboundCollision) {
		t.Fatalf("StoreOutbound collision error = %v, want ErrOutboundCollision", err)
	}

	messages, err := store.Outbounds(ctx, first.SessionID)
	if err != nil {
		t.Fatalf("Outbounds: %v", err)
	}
	if len(messages) != 1 || messages[0].Request.Target != "worker-a" {
		t.Fatalf("stored outbounds = %#v, want only original worker-a request", messages)
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
	sessionID := statecharts.SessionID("door-1")

	var models []*doorModel
	model := statecharts.NewGoModel(func() *doorModel {
		d := &doorModel{}
		models = append(models, d)
		return d
	})
	notLocked, err := model.Condition("door.not-locked", "v1", func(d *doorModel, _ statecharts.ExecContext, _ []statecharts.Value) (bool, error) {
		return !d.Locked, nil
	})
	if err != nil {
		t.Fatalf("register condition: %v", err)
	}
	recordOpen, err := model.Action("door.record-open", "v1", func(d *doorModel, _ statecharts.ExecContext, _ []statecharts.Value) error {
		d.OpenCount++
		return nil
	})
	if err != nil {
		t.Fatalf("register action: %v", err)
	}
	chart, err := statecharts.Build(
		statecharts.Compound("door", "closed",
			statecharts.Children(
				statecharts.Atomic("closed", statecharts.On("open.request",
					statecharts.Target("open"), statecharts.If(notLocked.If()), statecharts.Then(recordOpen.Do()))),
				statecharts.Atomic("open", statecharts.On("close.request", statecharts.Target("closed"))),
			),
		),
		model,
		statecharts.WithRevisionSalt("sqllog-door-test-v1"),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	in, err := chart.NewInstance()
	if err != nil {
		t.Fatalf("NewInstance: %v", err)
	}
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

	in2, err := chart.Rehydrate(ctx, log, log, sessionID, statecharts.NoopIOProcessor)
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
	if got := models[len(models)-1].OpenCount; got != 1 {
		t.Fatalf("rehydrated OpenCount = %d, want 1", got)
	}
	if err := in2.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

type doorModel struct {
	OpenCount int
	Locked    bool
}
