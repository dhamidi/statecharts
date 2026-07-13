// Package sqllog provides a database/sql-backed implementation of
// statecharts.Log and statecharts.SnapshotStore. It lives in its own
// package (rather than the statecharts root) so database/sql and whichever
// driver a caller registers are not forced dependencies of core library
// users.
package sqllog

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"iter"
	"time"

	"github.com/dhamidi/statecharts"
)

// Log implements statecharts.Log and statecharts.SnapshotStore against a
// single *sql.DB.
type Log struct {
	db      *sql.DB
	dialect Dialect
}

// New opens (and lazily creates) the schema for dialect against db. No
// external migration tool is used or required.
func New(db *sql.DB, dialect Dialect) (*Log, error) {
	stmts, err := ddlFor(dialect)
	if err != nil {
		return nil, err
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return nil, fmt.Errorf("sqllog: create schema: %w", err)
		}
	}
	return &Log{db: db, dialect: dialect}, nil
}

// Append implements statecharts.Log.
func (l *Log) Append(ctx context.Context, entry statecharts.LogEntry) (uint64, error) {
	enc, err := statecharts.EncodeEvent(entry.Event)
	if err != nil {
		return 0, fmt.Errorf("sqllog: encode event: %w", err)
	}

	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("sqllog: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	var last sql.NullInt64
	if err := tx.QueryRowContext(ctx, maxSeqSQL[l.dialect], entry.SessionID).Scan(&last); err != nil {
		return 0, fmt.Errorf("sqllog: query max seq: %w", err)
	}
	seq := uint64(last.Int64) + 1

	_, err = tx.ExecContext(ctx, insertLogSQL[l.dialect],
		entry.SessionID, seq, string(entry.Kind), entry.Timestamp.UTC(),
		string(entry.SendID), string(entry.Target),
		string(enc.Name), int(enc.Type), string(enc.SendID), string(enc.Origin), string(enc.OriginType), string(enc.InvokeID),
		enc.DataType, enc.DataPayload,
	)
	if err != nil {
		return 0, fmt.Errorf("sqllog: insert: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("sqllog: commit: %w", err)
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
				entrySendID, entryTarget                                 string
				eventName                                                string
				eventType                                                int
				eventSendID, eventOrigin, eventOriginType, eventInvokeID string
				dataType                                                 string
				dataPayload                                              []byte
			)
			if err := rows.Scan(&seq, &kind, &ts, &entrySendID, &entryTarget,
				&eventName, &eventType, &eventSendID, &eventOrigin, &eventOriginType, &eventInvokeID,
				&dataType, &dataPayload); err != nil {
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
		return statecharts.Checkpoint{}, false, fmt.Errorf("sqllog: unmarshal snapshot: %w", err)
	}
	return statecharts.Checkpoint{Snapshot: snap, Seq: seq}, true, nil
}
