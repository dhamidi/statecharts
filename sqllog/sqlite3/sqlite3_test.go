package sqlite3

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/sqllog"
)

var (
	_ statecharts.Log           = (*Storage)(nil)
	_ statecharts.SnapshotStore = (*Storage)(nil)
)

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
