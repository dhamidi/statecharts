package sqllog

import "fmt"

// Dialect selects the DDL variant used by New. Only SQLite is implemented
// so far (a pure-Go driver keeps this package's tests dependency-light and
// CI-friendly); Postgres and MySQL are placeholders for when they're
// actually needed.
type Dialect string

const (
	SQLite   Dialect = "sqlite"
	Postgres Dialect = "postgres"
	MySQL    Dialect = "mysql"
)

var createTableDDL = map[Dialect][]string{
	SQLite: {
		`CREATE TABLE IF NOT EXISTS statechart_log (
			session_id        TEXT    NOT NULL,
			seq               INTEGER NOT NULL,
			kind              TEXT    NOT NULL,
			ts                TIMESTAMP NOT NULL,
			entry_send_id     TEXT    NOT NULL DEFAULT '',
			entry_target      TEXT    NOT NULL DEFAULT '',
			entry_type        TEXT    NOT NULL DEFAULT '',
			event_name        TEXT    NOT NULL,
			event_type        INTEGER NOT NULL,
			event_send_id     TEXT    NOT NULL DEFAULT '',
			event_origin      TEXT    NOT NULL DEFAULT '',
			event_origin_type TEXT    NOT NULL DEFAULT '',
			event_invoke_id   TEXT    NOT NULL DEFAULT '',
			data_type         TEXT    NOT NULL DEFAULT '',
			data_payload      BLOB,
			PRIMARY KEY (session_id, seq)
		)`,
		`CREATE TABLE IF NOT EXISTS statechart_snapshot (
			session_id    TEXT PRIMARY KEY,
			seq           INTEGER NOT NULL,
			snapshot_json BLOB NOT NULL
		)`,
	},
}

var insertLogSQL = map[Dialect]string{
	SQLite: `INSERT INTO statechart_log (
		session_id, seq, kind, ts, entry_send_id, entry_target, entry_type,
		event_name, event_type, event_send_id, event_origin, event_origin_type, event_invoke_id,
		data_type, data_payload
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
}

var selectLogSQL = map[Dialect]string{
	SQLite: `SELECT
		seq, kind, ts, entry_send_id, entry_target, entry_type,
		event_name, event_type, event_send_id, event_origin, event_origin_type, event_invoke_id,
		data_type, data_payload
	FROM statechart_log WHERE session_id = ? AND seq >= ? ORDER BY seq`,
}

var maxSeqSQL = map[Dialect]string{
	SQLite: `SELECT MAX(seq) FROM statechart_log WHERE session_id = ?`,
}

var upsertSnapshotSQL = map[Dialect]string{
	SQLite: `INSERT INTO statechart_snapshot (session_id, seq, snapshot_json) VALUES (?, ?, ?)
		ON CONFLICT(session_id) DO UPDATE SET seq = excluded.seq, snapshot_json = excluded.snapshot_json`,
}

var selectSnapshotSQL = map[Dialect]string{
	SQLite: `SELECT seq, snapshot_json FROM statechart_snapshot WHERE session_id = ?`,
}

func ddlFor(d Dialect) ([]string, error) {
	stmts, ok := createTableDDL[d]
	if !ok {
		return nil, fmt.Errorf("sqllog: dialect %q is not yet implemented", d)
	}
	return stmts, nil
}
