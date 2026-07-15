package sqllog

import "fmt"

// Dialect selects the DDL variant used by New.
type Dialect string

const (
	SQLite Dialect = "sqlite"
)

var createTableDDL = map[Dialect][]string{
	SQLite: {
		`CREATE TABLE IF NOT EXISTS statechart_schema (version INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS statechart_definition (
			revision                  TEXT    PRIMARY KEY,
			revision_envelope_version INTEGER NOT NULL,
			chart_id                  TEXT    NOT NULL,
			datamodel                 TEXT    NOT NULL,
			canonical_definition      BLOB    NOT NULL,
			program_fingerprint       BLOB    NOT NULL,
			CHECK (revision <> ''),
			CHECK (revision_envelope_version > 0),
			CHECK (chart_id <> ''),
			CHECK (datamodel <> ''),
			CHECK (length(canonical_definition) > 0),
			CHECK (length(program_fingerprint) > 0)
		)`,
		`CREATE TABLE IF NOT EXISTS statechart_actor (
			actor_id    TEXT      PRIMARY KEY,
			chart_id    TEXT      NOT NULL,
			revision    TEXT      NOT NULL,
			session_id  TEXT      NOT NULL UNIQUE,
			durable     INTEGER   NOT NULL CHECK (durable = 1),
			lifecycle   TEXT      NOT NULL CHECK (lifecycle IN ('active', 'terminal')),
			started_at  TIMESTAMP NOT NULL,
			terminal_at TIMESTAMP,
			CHECK ((lifecycle = 'active' AND terminal_at IS NULL) OR (lifecycle = 'terminal' AND terminal_at IS NOT NULL))
		)`,
		`CREATE INDEX IF NOT EXISTS statechart_actor_active_revision ON statechart_actor(revision) WHERE lifecycle = 'active'`,
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
			delivery_id       TEXT    NOT NULL DEFAULT '',
			value_data        BLOB    NOT NULL,
			PRIMARY KEY (session_id, seq)
		)`,
		`CREATE TABLE IF NOT EXISTS statechart_snapshot (
			session_id    TEXT PRIMARY KEY,
			seq           INTEGER NOT NULL,
			snapshot_json BLOB NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS statechart_inbound (session_id TEXT NOT NULL, delivery_id TEXT NOT NULL, PRIMARY KEY(session_id, delivery_id))`,
		`CREATE TABLE IF NOT EXISTS statechart_outbound (session_id TEXT NOT NULL, delivery_id TEXT NOT NULL, seq INTEGER NOT NULL, send_id TEXT NOT NULL, event_send_id TEXT NOT NULL, target TEXT NOT NULL, processor_type TEXT NOT NULL, event_name TEXT NOT NULL, value_data BLOB NOT NULL, status TEXT NOT NULL, result_error TEXT NOT NULL DEFAULT '', result_execution INTEGER NOT NULL DEFAULT 0, result_synchronous INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(session_id, delivery_id))`,
	},
}

var insertLogSQL = map[Dialect]string{
	SQLite: `INSERT INTO statechart_log (
		session_id, seq, kind, ts, entry_send_id, entry_target, entry_type,
		event_name, event_type, event_send_id, event_origin, event_origin_type, event_invoke_id,
		delivery_id, value_data
	) SELECT ?, COALESCE(MAX(seq), 0) + 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
	  FROM statechart_log WHERE session_id = ?
	RETURNING seq`,
}

var selectLogSQL = map[Dialect]string{
	SQLite: `SELECT
		seq, kind, ts, entry_send_id, entry_target, entry_type,
		event_name, event_type, event_send_id, event_origin, event_origin_type, event_invoke_id,
		delivery_id, value_data
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

var existingTablesSQL = map[Dialect]string{
	SQLite: `SELECT name FROM sqlite_schema WHERE type = 'table' AND name LIKE 'statechart_%' ORDER BY name`,
}

func ddlFor(d Dialect) ([]string, error) {
	stmts, ok := createTableDDL[d]
	if !ok {
		return nil, fmt.Errorf("sqllog: dialect %q is not yet implemented", d)
	}
	return stmts, nil
}

func tableQueryFor(d Dialect) (string, error) {
	query, ok := existingTablesSQL[d]
	if !ok {
		return "", fmt.Errorf("sqllog: dialect %q is not yet implemented", d)
	}
	return query, nil
}
