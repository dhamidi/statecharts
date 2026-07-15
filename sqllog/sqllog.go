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
	"time"

	"github.com/dhamidi/statecharts"
)

// Storage implements statecharts.Log and statecharts.SnapshotStore against a
// single *sql.DB.
type Storage struct {
	db      *sql.DB
	dialect Dialect
}

// Log is retained as a source-compatible name for Storage.
type Log = Storage

// Close releases the database and its pooled connections.
func (l *Storage) Close() error { return l.db.Close() }

// DB returns the underlying pool for diagnostics and advanced use.
func (l *Storage) DB() *sql.DB { return l.db }

// New applies sqllog's schema to a caller-provided database/sql pool. The
// caller is responsible for importing, opening, and configuring its driver.
// No external migration tool is used or required.
func New(db *sql.DB, dialect Dialect) (*Storage, error) {
	if err := initializeSchema(db, dialect); err != nil {
		return nil, err
	}
	return &Storage{db: db, dialect: dialect}, nil
}

const schemaVersion = 2

func initializeSchema(db *sql.DB, dialect Dialect) (err error) {
	statements, err := ddlFor(dialect)
	if err != nil {
		return err
	}
	tableQuery, err := tableQueryFor(dialect)
	if err != nil {
		return err
	}
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqllog: begin schema transaction: %w", err)
	}
	defer tx.Rollback()
	tables, err := schemaTables(ctx, tx, tableQuery)
	if err != nil {
		return fmt.Errorf("sqllog: inspect schema: %w", err)
	}
	if len(tables) > 0 {
		if !tables["statechart_schema"] {
			return fmt.Errorf("sqllog: non-current schema has no version marker")
		}
		var count, version int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MAX(version), 0) FROM statechart_schema`).Scan(&count, &version); err != nil {
			return fmt.Errorf("sqllog: read schema version: %w", err)
		}
		if count != 1 || version != schemaVersion {
			return fmt.Errorf("sqllog: schema version %d is not current (want %d)", version, schemaVersion)
		}
		if err := validateSchema(ctx, tx); err != nil {
			return fmt.Errorf("sqllog: non-current schema: %w", err)
		}
		return nil
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("sqllog: create schema: %w", err)
		}
	}
	if err := validateSchema(ctx, tx); err != nil {
		return fmt.Errorf("sqllog: validate fresh schema: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO statechart_schema(version) VALUES (?)`, schemaVersion); err != nil {
		return fmt.Errorf("sqllog: record schema version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqllog: commit schema: %w", err)
	}
	return nil
}

func schemaTables(ctx context.Context, tx *sql.Tx, query string) (map[string]bool, error) {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tables := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables[name] = true
	}
	return tables, rows.Err()
}

func validateSchema(ctx context.Context, tx *sql.Tx) error {
	queries := []string{
		`SELECT session_id,seq,kind,ts,entry_send_id,entry_target,entry_type,event_name,event_type,event_send_id,event_origin,event_origin_type,event_invoke_id,delivery_id,value_data FROM statechart_log WHERE 1=0`,
		`SELECT session_id,seq,snapshot_json FROM statechart_snapshot WHERE 1=0`,
		`SELECT session_id,delivery_id FROM statechart_inbound WHERE 1=0`,
		`SELECT session_id,delivery_id,seq,send_id,event_send_id,target,processor_type,event_name,value_data,status,result_error,result_execution,result_synchronous FROM statechart_outbound WHERE 1=0`,
	}
	for _, query := range queries {
		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			return err
		}
		rows.Close()
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
		string(enc.DeliveryID), enc.Data, entry.SessionID,
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
				deliveryID                                               string
				valueData                                                []byte
			)
			if err := rows.Scan(&seq, &kind, &ts, &entrySendID, &entryTarget, &entryType,
				&eventName, &eventType, &eventSendID, &eventOrigin, &eventOriginType, &eventInvokeID,
				&deliveryID, &valueData); err != nil {
				yield(statecharts.LogEntry{}, fmt.Errorf("sqllog: scan: %w", err))
				return
			}

			ev, err := statecharts.DecodeEvent(statecharts.EncodedEvent{
				Name:       statecharts.Identifier(eventName),
				Type:       statecharts.EventType(eventType),
				SendID:     statecharts.Identifier(eventSendID),
				Origin:     statecharts.Identifier(eventOrigin),
				OriginType: statecharts.Identifier(eventOriginType),
				InvokeID:   statecharts.Identifier(eventInvokeID),
				DeliveryID: statecharts.DeliveryID(deliveryID),
				Data:       valueData,
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
	err = tx.QueryRowContext(ctx, insertLogSQL[l.dialect], entry.SessionID, string(entry.Kind), entry.Timestamp.UTC(), string(entry.SendID), string(entry.Target), string(entry.Type), string(enc.Name), int(enc.Type), string(enc.SendID), string(enc.Origin), string(enc.OriginType), string(enc.InvokeID), string(id), enc.Data, entry.SessionID).Scan(&seq)
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
	result, err := l.db.ExecContext(ctx, `INSERT OR IGNORE INTO statechart_outbound(session_id,delivery_id,seq,send_id,event_send_id,target,processor_type,event_name,value_data,status,result_error,result_execution,result_synchronous) SELECT ?,?,COALESCE(MAX(seq),0)+1,?,?,?,?,?,?,?,?,?,? FROM statechart_outbound WHERE session_id=?`, m.SessionID, m.DeliveryID, m.Request.SendID, m.Request.EventSendID, m.Request.Target, m.Request.Type, m.Request.Event, enc.Data, status, m.Result.Error, m.Result.Execution, m.Result.Synchronous, m.SessionID)
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

	var sendID, eventSendID, target, typ, eventName string
	var payload []byte
	err = l.db.QueryRowContext(ctx, `SELECT send_id,event_send_id,target,processor_type,event_name,value_data FROM statechart_outbound WHERE session_id=? AND delivery_id=?`, m.SessionID, m.DeliveryID).
		Scan(&sendID, &eventSendID, &target, &typ, &eventName, &payload)
	if err != nil {
		return fmt.Errorf("sqllog: inspect existing outbound: %w", err)
	}
	if sendID != string(m.Request.SendID) || eventSendID != string(m.Request.EventSendID) || target != string(m.Request.Target) || typ != string(m.Request.Type) || eventName != string(m.Request.Event) || !bytes.Equal(payload, enc.Data) {
		return fmt.Errorf("sqllog: session %q delivery %q identifies a different request: %w", m.SessionID, m.DeliveryID, statecharts.ErrOutboundCollision)
	}
	return nil
}

func (l *Storage) ResolveOutbound(ctx context.Context, sid statecharts.SessionID, id statecharts.DeliveryID, r statecharts.OutboundResult) error {
	_, err := l.db.ExecContext(ctx, `UPDATE statechart_outbound SET status=?,result_error=?,result_execution=?,result_synchronous=? WHERE session_id=? AND delivery_id=?`, statecharts.OutboundResolved, r.Error, r.Execution, r.Synchronous, sid, id)
	return err
}

func (l *Storage) Outbounds(ctx context.Context, sid statecharts.SessionID) ([]statecharts.OutboundMessage, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT delivery_id,send_id,event_send_id,target,processor_type,event_name,value_data,status,result_error,result_execution,result_synchronous FROM statechart_outbound WHERE session_id=? ORDER BY seq`, sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []statecharts.OutboundMessage
	for rows.Next() {
		var id, sendID, eventSendID, target, typ, name, status, resultErr string
		var payload []byte
		var execution, synchronous bool
		if err := rows.Scan(&id, &sendID, &eventSendID, &target, &typ, &name, &payload, &status, &resultErr, &execution, &synchronous); err != nil {
			return nil, err
		}
		ev, err := statecharts.DecodeEvent(statecharts.EncodedEvent{Name: statecharts.Identifier(name), Data: payload})
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
