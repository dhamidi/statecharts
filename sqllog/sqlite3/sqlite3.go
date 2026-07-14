// Package sqlite3 opens SQLite-backed sqllog storage using the pure-Go
// modernc.org/sqlite driver. Importing this package opts into that driver;
// the parent sqllog package itself only depends on database/sql.
package sqlite3

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/dhamidi/statecharts/sqllog"
	_ "modernc.org/sqlite"
)

// Storage is SQLite-backed sqllog storage. Close releases the database and
// its pooled connections; DB exposes that database/sql pool for diagnostics
// and advanced use.
type Storage struct {
	*sqllog.Storage
}

// Open opens an isolated SQLite database, configures every pooled connection,
// enables WAL for file-backed databases, and applies sqllog's schema.
func Open(path string) (*Storage, error) {
	if path == "" {
		return nil, fmt.Errorf("sqllog/sqlite3: empty path")
	}
	fileBacked := path != ":memory:" && !strings.Contains(path, "mode=memory")
	if fileBacked && !strings.HasPrefix(path, "file:") {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("sqllog/sqlite3: create database directory: %w", err)
		}
	}
	dsn := path
	if !strings.HasPrefix(dsn, "file:") && dsn != ":memory:" {
		dsn = "file:" + filepath.ToSlash(dsn)
	}
	pragmas := []string{"busy_timeout(5000)", "foreign_keys(ON)", "synchronous(NORMAL)", "wal_autocheckpoint(1000)"}
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	for _, pragma := range pragmas {
		dsn += sep + "_pragma=" + url.QueryEscape(pragma)
		sep = "&"
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqllog/sqlite3: open: %w", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	if !fileBacked {
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqllog/sqlite3: ping: %w", err)
	}
	if fileBacked {
		var mode string
		if err := db.QueryRow(`PRAGMA journal_mode=WAL`).Scan(&mode); err != nil || !strings.EqualFold(mode, "wal") {
			_ = db.Close()
			if err != nil {
				return nil, fmt.Errorf("sqllog/sqlite3: enable WAL: %w", err)
			}
			return nil, fmt.Errorf("sqllog/sqlite3: enable WAL: got journal_mode %q", mode)
		}
	}

	storage, err := sqllog.New(db, sqllog.SQLite)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Storage{Storage: storage}, nil
}
