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
// uses immediate transactions for cross-handle writer linearization, enables
// WAL for file-backed databases, and applies sqllog's schema.
func Open(path string) (*Storage, error) {
	if path == "" {
		return nil, fmt.Errorf("sqllog/sqlite3: empty path")
	}
	dsn := path
	fileBacked := true
	switch {
	case path == ":memory:":
		fileBacked = false
	case strings.HasPrefix(path, "file:"):
		uri, err := url.Parse(path)
		if err != nil {
			return nil, fmt.Errorf("sqllog/sqlite3: parse file URI: %w", err)
		}
		fileBacked = uri.Query().Get("mode") != "memory" && uri.Opaque != ":memory:"
	default:
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("sqllog/sqlite3: resolve database path: %w", err)
		}
		if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
			return nil, fmt.Errorf("sqllog/sqlite3: create database directory: %w", err)
		}
		dsn = (&url.URL{Scheme: "file", Path: filepath.ToSlash(absolute)}).String()
	}
	pragmas := []string{"busy_timeout(5000)", "foreign_keys(ON)", "synchronous(NORMAL)", "wal_autocheckpoint(1000)"}
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	dsn += sep + "_txlock=immediate"
	sep = "&"
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
