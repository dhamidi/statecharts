package sqlite3

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/sqllog"
)

var (
	_ statecharts.Log             = (*Storage)(nil)
	_ statecharts.SnapshotStore   = (*Storage)(nil)
	_ statecharts.DefinitionStore = (*Storage)(nil)
	_ statecharts.ActorStore      = (*Storage)(nil)
)

type sqliteArtifactModel struct{}

func sqliteTestArtifact(t *testing.T, id, salt string) statecharts.DefinitionArtifact {
	t.Helper()
	chart, err := statecharts.Build(
		statecharts.Atomic(statecharts.Identifier(id)),
		statecharts.NewGoModel(func() *sqliteArtifactModel { return &sqliteArtifactModel{} }),
		statecharts.WithRevisionSalt(salt),
	)
	if err != nil {
		t.Fatal(err)
	}
	return chart.DefinitionArtifact()
}

func sqliteTestActor(artifact statecharts.DefinitionArtifact, id string) statecharts.ActorMetadata {
	return statecharts.ActorMetadata{
		ActorID: statecharts.Identifier(id), ChartID: artifact.ChartID, Revision: artifact.Revision,
		SessionID: statecharts.SessionID(id), Durable: true,
		Lifecycle: statecharts.ActorLifecycleActive,
		StartedAt: time.Date(2026, 7, 15, 13, 0, 0, 0, time.UTC),
	}
}

func TestOpenConfiguresEveryConnection(t *testing.T) {
	store, err := Open(t.TempDir() + "/nested/system.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	c1, err := store.DB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := store.DB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	for i, conn := range []*sql.Conn{c1, c2} {
		for pragma, want := range map[string]string{"journal_mode": "wal", "busy_timeout": "5000", "foreign_keys": "1", "synchronous": "1", "wal_autocheckpoint": "1000"} {
			var got string
			if err := conn.QueryRowContext(ctx, "PRAGMA "+pragma).Scan(&got); err != nil {
				t.Fatalf("connection %d %s: %v", i, pragma, err)
			}
			if got != want {
				t.Errorf("connection %d PRAGMA %s = %q, want %q", i, pragma, got, want)
			}
		}
	}
}

func TestOpenFilesAreIsolated(t *testing.T) {
	dir := t.TempDir()
	a, err := Open(dir + "/a.db")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(dir + "/b.db")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	entry := statecharts.LogEntry{SessionID: "same", Kind: statecharts.KindExternalEvent, Timestamp: time.Now(), Event: statecharts.Event{Name: "x"}}
	if _, err := a.Append(context.Background(), entry); err != nil {
		t.Fatal(err)
	}
	if got, err := b.LastSeq(context.Background(), "same"); err != nil || got != 0 {
		t.Fatalf("second database LastSeq = %d, %v", got, err)
	}
	artifact := sqliteTestArtifact(t, "counter", "v1")
	if _, err := a.PutDefinition(context.Background(), artifact); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := b.GetDefinition(context.Background(), artifact.Revision); err != nil || ok {
		t.Fatalf("second database GetDefinition = ok %v, err %v", ok, err)
	}
	actor := sqliteTestActor(artifact, "red")
	if _, _, err := a.BeginActor(context.Background(), actor); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := b.GetActor(context.Background(), actor.ActorID); err != nil || ok {
		t.Fatalf("second database GetActor = ok %v, err %v", ok, err)
	}
}

func TestOpenTreatsPlainPathQueryCharactersLiterally(t *testing.T) {
	path := filepath.Join(t.TempDir(), "counter?mode=memory#blue.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("plain database path was interpreted as a URI: %v", err)
	}
}

func TestIndependentHandlesSerializeConcurrentActorStarts(t *testing.T) {
	path := t.TempDir() + "/shared.db"
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	artifact := sqliteTestArtifact(t, "counter", "v1")
	if _, err := first.PutDefinition(context.Background(), artifact); err != nil {
		t.Fatal(err)
	}
	actor := sqliteTestActor(artifact, "red")
	start := make(chan struct{})
	type outcome struct {
		result statecharts.ActorBeginResult
		err    error
	}
	outcomes := make(chan outcome, 2)
	var wg sync.WaitGroup
	for _, store := range []*Storage{first, second} {
		store := store
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, result, err := store.BeginActor(context.Background(), actor)
			outcomes <- outcome{result: result, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(outcomes)
	started, already := 0, 0
	for outcome := range outcomes {
		if outcome.err != nil {
			t.Fatalf("BeginActor: %v", outcome.err)
		}
		switch outcome.result {
		case statecharts.ActorStarted:
			started++
		case statecharts.ActorAlreadyActive:
			already++
		}
	}
	if started != 1 || already != 1 {
		t.Fatalf("results = started %d, already %d; want 1, 1", started, already)
	}
	if seq, err := first.LastSeq(context.Background(), actor.SessionID); err != nil || seq != 1 {
		t.Fatalf("LastSeq = %d, %v", seq, err)
	}
}

func TestIndependentHandlesLinearizeActorBeginAndDefinitionDelete(t *testing.T) {
	path := t.TempDir() + "/shared.db"
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	for i := 0; i < 32; i++ {
		id := fmt.Sprintf("counter-%d", i)
		artifact := sqliteTestArtifact(t, id, "v1")
		if _, err := first.PutDefinition(context.Background(), artifact); err != nil {
			t.Fatal(err)
		}
		actor := sqliteTestActor(artifact, fmt.Sprintf("actor-%d", i))
		start := make(chan struct{})
		var beginResult statecharts.ActorBeginResult
		var beginErr error
		var deleteResult statecharts.DefinitionDeleteResult
		var deleteErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, beginResult, beginErr = first.BeginActor(context.Background(), actor)
		}()
		go func() {
			defer wg.Done()
			<-start
			deleteResult, deleteErr = second.DeleteDefinitionIfUnreferenced(context.Background(), artifact.Revision)
		}()
		close(start)
		wg.Wait()
		if deleteErr != nil {
			t.Fatalf("iteration %d delete: %v", i, deleteErr)
		}
		switch deleteResult {
		case statecharts.DefinitionReferenced:
			if beginErr != nil || beginResult != statecharts.ActorStarted {
				t.Fatalf("iteration %d referenced begin = %v, %v", i, beginResult, beginErr)
			}
		case statecharts.DefinitionDeleted:
			if !errors.Is(beginErr, statecharts.ErrDefinitionNotFound) {
				t.Fatalf("iteration %d deleted begin error = %v", i, beginErr)
			}
		default:
			t.Fatalf("iteration %d delete result = %v", i, deleteResult)
		}
	}
}

func TestOpenConcurrentAppendsAreGapless(t *testing.T) {
	store, err := Open(t.TempDir() + "/log.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	const n = 80
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := store.Append(context.Background(), statecharts.LogEntry{SessionID: "shared", Kind: statecharts.KindExternalEvent, Timestamp: time.Now(), Event: statecharts.Event{Name: statecharts.Identifier(fmt.Sprint(i))}})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("Append: %v", err)
		}
	}
	var want uint64 = 1
	for entry, err := range store.Read(context.Background(), "shared", 1) {
		if err != nil {
			t.Fatal(err)
		}
		if entry.Seq != want {
			t.Fatalf("sequence = %d, want %d", entry.Seq, want)
		}
		want++
	}
	if want != n+1 {
		t.Fatalf("read %d entries, want %d", want-1, n)
	}
}

func TestOpenReturnsDatabaseSQLBackedStorage(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if store.Storage == nil || store.DB() == nil {
		t.Fatal("Open returned storage without its database/sql implementation")
	}
	var _ *sqllog.Storage = store.Storage
}
