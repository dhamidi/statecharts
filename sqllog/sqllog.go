// Package sqllog provides a database/sql-backed implementation of
// statecharts.Log and statecharts.SnapshotStore. It lives in its own
// package (rather than the statecharts root) so database/sql and whichever
// driver a caller registers are not forced dependencies of core library
// users.
package sqllog

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"iter"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dhamidi/statecharts"
	_ "modernc.org/sqlite"
)

// Storage implements statecharts.Log and statecharts.SnapshotStore against a
// single *sql.DB.
type Storage struct {
	db      *sql.DB
	dialect Dialect
}

// Log is retained as a source-compatible name for Storage.
type Log = Storage

// OpenSQLite opens an isolated SQLite database and applies sqllog's schema.
func OpenSQLite(path string) (*Storage, error) {
	if path == "" {
		return nil, fmt.Errorf("sqllog: empty SQLite path")
	}
	fileBacked := path != ":memory:" && !strings.Contains(path, "mode=memory")
	if fileBacked && !strings.HasPrefix(path, "file:") {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("sqllog: create database directory: %w", err)
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
		return nil, fmt.Errorf("sqllog: open SQLite: %w", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	if !fileBacked {
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqllog: ping SQLite: %w", err)
	}
	if fileBacked {
		var mode string
		if err := db.QueryRow(`PRAGMA journal_mode=WAL`).Scan(&mode); err != nil || !strings.EqualFold(mode, "wal") {
			db.Close()
			if err != nil {
				return nil, fmt.Errorf("sqllog: enable WAL: %w", err)
			}
			return nil, fmt.Errorf("sqllog: enable WAL: got journal_mode %q", mode)
		}
	}
	s, err := New(db, SQLite)
	if err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the database and its pooled connections.
func (l *Storage) Close() error { return l.db.Close() }

// DB returns the underlying pool for diagnostics and advanced use.
func (l *Storage) DB() *sql.DB { return l.db }

// New opens (and lazily creates) the schema for dialect against db. No
// external migration tool is used or required.
func New(db *sql.DB, dialect Dialect) (*Storage, error) {
	if dialect != SQLite {
		return nil, fmt.Errorf("sqllog: dialect %q is not supported", dialect)
	}
	if err := migrateSchema(db); err != nil {
		return nil, err
	}
	return &Storage{db: db, dialect: dialect}, nil
}

func migrateSchema(db *sql.DB) (err error) {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("sqllog: migration connection: %w", err)
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return fmt.Errorf("sqllog: begin migration: %w", err)
	}
	defer func() {
		if err != nil {
			_, _ = conn.ExecContext(ctx, `ROLLBACK`)
		}
	}()
	var version int
	if err = conn.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("sqllog: read schema version: %w", err)
	}
	if version < 1 {
		stmts, _ := ddlFor(SQLite)
		for _, stmt := range stmts {
			if _, err = conn.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("sqllog: create schema: %w", err)
			}
		}
	}
	if version < 2 {
		rows, qerr := conn.QueryContext(ctx, `PRAGMA table_info(statechart_log)`)
		if qerr != nil {
			return fmt.Errorf("sqllog: inspect schema: %w", qerr)
		}
		has := false
		for rows.Next() {
			var cid int
			var name, typ string
			var notnull, pk int
			var def any
			if qerr = rows.Scan(&cid, &name, &typ, &notnull, &def, &pk); qerr != nil {
				rows.Close()
				return qerr
			}
			has = has || name == "entry_type"
		}
		rows.Close()
		if !has {
			if _, err = conn.ExecContext(ctx, `ALTER TABLE statechart_log ADD COLUMN entry_type TEXT NOT NULL DEFAULT ''`); err != nil {
				return fmt.Errorf("sqllog: migrate entry_type: %w", err)
			}
		}
		if _, err = conn.ExecContext(ctx, `PRAGMA user_version=2`); err != nil {
			return fmt.Errorf("sqllog: set schema version: %w", err)
		}
	}
	if version < 3 {
		// Version 3 adds durable delivery identity, inbox dedup, and outbox.
		var hasDelivery bool
		rows, qerr := conn.QueryContext(ctx, `PRAGMA table_info(statechart_log)`)
		if qerr != nil {
			return fmt.Errorf("sqllog: inspect delivery schema: %w", qerr)
		}
		for rows.Next() {
			var cid, nn, pk int
			var name, typ string
			var def any
			if qerr = rows.Scan(&cid, &name, &typ, &nn, &def, &pk); qerr != nil {
				rows.Close()
				return qerr
			}
			hasDelivery = hasDelivery || name == "delivery_id"
		}
		rows.Close()
		if !hasDelivery {
			if _, err = conn.ExecContext(ctx, `ALTER TABLE statechart_log ADD COLUMN delivery_id TEXT NOT NULL DEFAULT ''`); err != nil {
				return fmt.Errorf("sqllog: migrate delivery_id: %w", err)
			}
		}
		stmts, _ := ddlFor(SQLite)
		for _, stmt := range stmts[2:] {
			if _, err = conn.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("sqllog: create durability schema: %w", err)
			}
		}
		if _, err = conn.ExecContext(ctx, `PRAGMA user_version=3`); err != nil {
			return fmt.Errorf("sqllog: set schema version: %w", err)
		}
	}
	if version < 4 {
		// Version 4 records processor outcomes so deterministic replay can
		// reproduce synchronous send failures without performing I/O.
		columns := map[string]bool{}
		rows, qerr := conn.QueryContext(ctx, `PRAGMA table_info(statechart_outbound)`)
		if qerr != nil {
			return fmt.Errorf("sqllog: inspect outbound result schema: %w", qerr)
		}
		for rows.Next() {
			var cid, nn, pk int
			var name, typ string
			var def any
			if qerr = rows.Scan(&cid, &name, &typ, &nn, &def, &pk); qerr != nil {
				rows.Close()
				return qerr
			}
			columns[name] = true
		}
		rows.Close()
		alter := []struct {
			name string
			sql  string
		}{
			{"result_error", `ALTER TABLE statechart_outbound ADD COLUMN result_error TEXT NOT NULL DEFAULT ''`},
			{"result_execution", `ALTER TABLE statechart_outbound ADD COLUMN result_execution INTEGER NOT NULL DEFAULT 0`},
			{"result_synchronous", `ALTER TABLE statechart_outbound ADD COLUMN result_synchronous INTEGER NOT NULL DEFAULT 0`},
		}
		for _, column := range alter {
			if columns[column.name] {
				continue
			}
			if _, err = conn.ExecContext(ctx, column.sql); err != nil {
				return fmt.Errorf("sqllog: migrate %s: %w", column.name, err)
			}
		}
		if _, err = conn.ExecContext(ctx, `PRAGMA user_version=4`); err != nil {
			return fmt.Errorf("sqllog: set schema version: %w", err)
		}
	}
	if version < 5 {
		// Version 5 makes outbox recovery order explicit instead of relying
		// on SQLite's mutable, implementation-defined rowid order.
		var hasSeq bool
		rows, qerr := conn.QueryContext(ctx, `PRAGMA table_info(statechart_outbound)`)
		if qerr != nil {
			return fmt.Errorf("sqllog: inspect outbound sequence schema: %w", qerr)
		}
		for rows.Next() {
			var cid, nn, pk int
			var name, typ string
			var def any
			if qerr = rows.Scan(&cid, &name, &typ, &nn, &def, &pk); qerr != nil {
				rows.Close()
				return qerr
			}
			hasSeq = hasSeq || name == "seq"
		}
		rows.Close()
		if !hasSeq {
			if _, err = conn.ExecContext(ctx, `ALTER TABLE statechart_outbound ADD COLUMN seq INTEGER NOT NULL DEFAULT 0`); err != nil {
				return fmt.Errorf("sqllog: migrate outbound sequence: %w", err)
			}
			if _, err = conn.ExecContext(ctx, `UPDATE statechart_outbound AS current SET seq = (SELECT COUNT(*) FROM statechart_outbound AS preceding WHERE preceding.session_id = current.session_id AND preceding.rowid <= current.rowid)`); err != nil {
				return fmt.Errorf("sqllog: backfill outbound sequence: %w", err)
			}
		}
		if _, err = conn.ExecContext(ctx, `PRAGMA user_version=5`); err != nil {
			return fmt.Errorf("sqllog: set schema version: %w", err)
		}
	}
	if _, err = conn.ExecContext(ctx, `COMMIT`); err != nil {
		return fmt.Errorf("sqllog: commit migration: %w", err)
	}
	return nil
}

// Append implements statecharts.Log.
func (l *Storage) Append(ctx context.Context, entry statecharts.LogEntry) (uint64, error) {
	enc, err := statecharts.EncodeEvent(entry.Event)
	if err != nil {
		return 0, fmt.Errorf("sqllog: encode event: %w", err)
	}

	var seq uint64
	err = l.db.QueryRowContext(ctx, insertLogSQL[l.dialect],
		entry.SessionID, string(entry.Kind), entry.Timestamp.UTC(),
		string(entry.SendID), string(entry.Target), string(entry.Type),
		string(enc.Name), int(enc.Type), string(enc.SendID), string(enc.Origin), string(enc.OriginType), string(enc.InvokeID),
		string(enc.DeliveryID), enc.DataType, enc.DataPayload, entry.SessionID,
	).Scan(&seq)
	if err != nil {
		return 0, fmt.Errorf("sqllog: insert: %w", err)
	}
	return seq, nil
}

// Read implements statecharts.Log.
func (l *Log) Read(ctx context.Context, sessionID statecharts.SessionID, from uint64) iter.Seq2[statecharts.LogEntry, error] {
	return func(yield func(statecharts.LogEntry, error) bool) {
		rows, err := l.db.QueryContext(ctx, selectLogSQL[l.dialect], sessionID, from)
		if err != nil {
			yield(statecharts.LogEntry{}, fmt.Errorf("sqllog: query: %w", err))
			return
		}
		defer rows.Close()

		for rows.Next() {
			var (
				seq                                                      uint64
				kind                                                     string
				ts                                                       time.Time
				entrySendID, entryTarget, entryType                      string
				eventName                                                string
				eventType                                                int
				eventSendID, eventOrigin, eventOriginType, eventInvokeID string
				dataType, deliveryID                                     string
				dataPayload                                              []byte
			)
			if err := rows.Scan(&seq, &kind, &ts, &entrySendID, &entryTarget, &entryType,
				&eventName, &eventType, &eventSendID, &eventOrigin, &eventOriginType, &eventInvokeID,
				&deliveryID, &dataType, &dataPayload); err != nil {
				yield(statecharts.LogEntry{}, fmt.Errorf("sqllog: scan: %w", err))
				return
			}

			ev, err := statecharts.DecodeEvent(statecharts.EncodedEvent{
				Name:        statecharts.Identifier(eventName),
				Type:        statecharts.EventType(eventType),
				SendID:      statecharts.Identifier(eventSendID),
				Origin:      statecharts.Identifier(eventOrigin),
				OriginType:  statecharts.Identifier(eventOriginType),
				InvokeID:    statecharts.Identifier(eventInvokeID),
				DeliveryID:  statecharts.DeliveryID(deliveryID),
				DataType:    dataType,
				DataPayload: dataPayload,
			})
			if err != nil {
				yield(statecharts.LogEntry{}, fmt.Errorf("sqllog: decode event: %w", err))
				return
			}

			entry := statecharts.LogEntry{
				SessionID: sessionID,
				Seq:       seq,
				Kind:      statecharts.EntryKind(kind),
				Timestamp: ts,
				Event:     ev,
				SendID:    statecharts.Identifier(entrySendID),
				Target:    statecharts.Identifier(entryTarget),
				Type:      statecharts.Identifier(entryType),
			}
			if !yield(entry, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(statecharts.LogEntry{}, fmt.Errorf("sqllog: rows: %w", err))
		}
	}
}

// AppendIngress atomically deduplicates nonempty delivery IDs and appends WAL.
func (l *Storage) AppendIngress(ctx context.Context, entry statecharts.LogEntry, id statecharts.DeliveryID) (uint64, bool, error) {
	if id == "" {
		seq, err := l.Append(ctx, entry)
		return seq, err == nil, err
	}
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback()
	r, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO statechart_inbound(session_id,delivery_id) VALUES(?,?)`, entry.SessionID, id)
	if err != nil {
		return 0, false, err
	}
	n, _ := r.RowsAffected()
	if n == 0 {
		return 0, false, tx.Commit()
	}
	enc, err := statecharts.EncodeEvent(entry.Event)
	if err != nil {
		return 0, false, err
	}
	var seq uint64
	err = tx.QueryRowContext(ctx, insertLogSQL[l.dialect], entry.SessionID, string(entry.Kind), entry.Timestamp.UTC(), string(entry.SendID), string(entry.Target), string(entry.Type), string(enc.Name), int(enc.Type), string(enc.SendID), string(enc.Origin), string(enc.OriginType), string(enc.InvokeID), string(id), enc.DataType, enc.DataPayload, entry.SessionID).Scan(&seq)
	if err != nil {
		return 0, false, err
	}
	if err = tx.Commit(); err != nil {
		return 0, false, err
	}
	return seq, true, nil
}

func (l *Storage) StoreOutbound(ctx context.Context, m statecharts.OutboundMessage) error {
	if m.Request.DeliveryID != "" && m.Request.DeliveryID != m.DeliveryID {
		return fmt.Errorf("sqllog: outbound message/request delivery IDs %q and %q differ: %w", m.DeliveryID, m.Request.DeliveryID, statecharts.ErrOutboundCollision)
	}
	enc, err := statecharts.EncodeEvent(statecharts.Event{Name: m.Request.Event, Data: m.Request.Data})
	if err != nil {
		return fmt.Errorf("sqllog: encode outbound: %w", err)
	}
	status := m.Status
	if status == "" {
		status = statecharts.OutboundPending
	}
	result, err := l.db.ExecContext(ctx, `INSERT OR IGNORE INTO statechart_outbound(session_id,delivery_id,seq,send_id,event_send_id,target,processor_type,event_name,data_type,data_payload,status,result_error,result_execution,result_synchronous) SELECT ?,?,COALESCE(MAX(seq),0)+1,?,?,?,?,?,?,?,?,?,?,? FROM statechart_outbound WHERE session_id=?`, m.SessionID, m.DeliveryID, m.Request.SendID, m.Request.EventSendID, m.Request.Target, m.Request.Type, m.Request.Event, enc.DataType, enc.DataPayload, status, m.Result.Error, m.Result.Execution, m.Result.Synchronous, m.SessionID)
	if err != nil {
		return err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if inserted == 1 {
		return nil
	}

	var sendID, eventSendID, target, typ, eventName, dataType string
	var payload []byte
	err = l.db.QueryRowContext(ctx, `SELECT send_id,event_send_id,target,processor_type,event_name,data_type,data_payload FROM statechart_outbound WHERE session_id=? AND delivery_id=?`, m.SessionID, m.DeliveryID).
		Scan(&sendID, &eventSendID, &target, &typ, &eventName, &dataType, &payload)
	if err != nil {
		return fmt.Errorf("sqllog: inspect existing outbound: %w", err)
	}
	if sendID != string(m.Request.SendID) || eventSendID != string(m.Request.EventSendID) || target != string(m.Request.Target) || typ != string(m.Request.Type) || eventName != string(m.Request.Event) || dataType != enc.DataType || !bytes.Equal(payload, enc.DataPayload) {
		return fmt.Errorf("sqllog: session %q delivery %q identifies a different request: %w", m.SessionID, m.DeliveryID, statecharts.ErrOutboundCollision)
	}
	return nil
}

func (l *Storage) ResolveOutbound(ctx context.Context, sid statecharts.SessionID, id statecharts.DeliveryID, r statecharts.OutboundResult) error {
	_, err := l.db.ExecContext(ctx, `UPDATE statechart_outbound SET status=?,result_error=?,result_execution=?,result_synchronous=? WHERE session_id=? AND delivery_id=?`, statecharts.OutboundResolved, r.Error, r.Execution, r.Synchronous, sid, id)
	return err
}

func (l *Storage) Outbounds(ctx context.Context, sid statecharts.SessionID) ([]statecharts.OutboundMessage, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT delivery_id,send_id,event_send_id,target,processor_type,event_name,data_type,data_payload,status,result_error,result_execution,result_synchronous FROM statechart_outbound WHERE session_id=? ORDER BY seq`, sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []statecharts.OutboundMessage
	for rows.Next() {
		var id, sendID, eventSendID, target, typ, name, dataType, status, resultErr string
		var payload []byte
		var execution, synchronous bool
		if err := rows.Scan(&id, &sendID, &eventSendID, &target, &typ, &name, &dataType, &payload, &status, &resultErr, &execution, &synchronous); err != nil {
			return nil, err
		}
		ev, err := statecharts.DecodeEvent(statecharts.EncodedEvent{Name: statecharts.Identifier(name), DataType: dataType, DataPayload: payload})
		if err != nil {
			return nil, err
		}
		out = append(out, statecharts.OutboundMessage{SessionID: sid, DeliveryID: statecharts.DeliveryID(id), Request: statecharts.SendRequest{DeliveryID: statecharts.DeliveryID(id), SendID: statecharts.Identifier(sendID), EventSendID: statecharts.Identifier(eventSendID), Target: statecharts.Identifier(target), Type: statecharts.Identifier(typ), Event: statecharts.Identifier(name), Data: ev.Data}, Status: statecharts.OutboundStatus(status), Result: statecharts.OutboundResult{Error: resultErr, Execution: execution, Synchronous: synchronous}})
	}
	return out, rows.Err()
}

// LastSeq implements statecharts.Log.
func (l *Log) LastSeq(ctx context.Context, sessionID statecharts.SessionID) (uint64, error) {
	var last sql.NullInt64
	if err := l.db.QueryRowContext(ctx, maxSeqSQL[l.dialect], sessionID).Scan(&last); err != nil {
		return 0, fmt.Errorf("sqllog: query max seq: %w", err)
	}
	if !last.Valid {
		return 0, nil
	}
	return uint64(last.Int64), nil
}

// Save implements statecharts.SnapshotStore.
func (l *Log) Save(ctx context.Context, sessionID statecharts.SessionID, cp statecharts.Checkpoint) error {
	b, err := json.Marshal(cp.Snapshot)
	if err != nil {
		return fmt.Errorf("sqllog: marshal snapshot: %w", err)
	}
	if _, err := l.db.ExecContext(ctx, upsertSnapshotSQL[l.dialect], sessionID, cp.Seq, b); err != nil {
		return fmt.Errorf("sqllog: save snapshot: %w", err)
	}
	return nil
}

// Load implements statecharts.SnapshotStore.
func (l *Log) Load(ctx context.Context, sessionID statecharts.SessionID) (statecharts.Checkpoint, bool, error) {
	var seq uint64
	var b []byte
	err := l.db.QueryRowContext(ctx, selectSnapshotSQL[l.dialect], sessionID).Scan(&seq, &b)
	if err == sql.ErrNoRows {
		return statecharts.Checkpoint{}, false, nil
	}
	if err != nil {
		return statecharts.Checkpoint{}, false, fmt.Errorf("sqllog: load snapshot: %w", err)
	}
	var snap statecharts.Snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return statecharts.Checkpoint{}, false, fmt.Errorf("sqllog: unmarshal snapshot: %w: %v", statecharts.ErrInvalidSnapshot, err)
	}
	return statecharts.Checkpoint{Snapshot: snap, Seq: seq}, true, nil
}
