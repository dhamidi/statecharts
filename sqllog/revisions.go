package sqllog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/dhamidi/statecharts"
)

type rowScanner interface {
	Scan(dest ...any) error
}

type rowQueryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type queryTransaction interface {
	rowQueryer
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type writeTransaction struct {
	conn *sql.Conn
	ctx  context.Context
	done bool
}

func (l *Storage) beginWrite(ctx context.Context) (*writeTransaction, error) {
	conn, err := l.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	statement := ""
	switch l.dialect {
	case SQLite:
		if _, err := conn.ExecContext(ctx, "PRAGMA busy_timeout=5000"); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("sqllog: configure SQLite write transaction: %w", err)
		}
		statement = "BEGIN IMMEDIATE"
	default:
		_ = conn.Close()
		return nil, fmt.Errorf("sqllog: write transaction for dialect %q is not implemented", l.dialect)
	}
	if _, err := conn.ExecContext(ctx, statement); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &writeTransaction{conn: conn, ctx: ctx}, nil
}

func (tx *writeTransaction) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return tx.conn.ExecContext(ctx, query, args...)
}

func (tx *writeTransaction) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return tx.conn.QueryContext(ctx, query, args...)
}

func (tx *writeTransaction) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return tx.conn.QueryRowContext(ctx, query, args...)
}

func (tx *writeTransaction) Commit() error {
	if tx.done {
		return sql.ErrTxDone
	}
	if _, err := tx.conn.ExecContext(tx.ctx, "COMMIT"); err != nil {
		return err
	}
	tx.done = true
	return tx.conn.Close()
}

func (tx *writeTransaction) Rollback() error {
	if tx.done {
		return sql.ErrTxDone
	}
	_, rollbackErr := tx.conn.ExecContext(context.Background(), "ROLLBACK")
	tx.done = true
	return errors.Join(rollbackErr, tx.conn.Close())
}

func scanDefinitionArtifact(row rowScanner) (statecharts.DefinitionArtifact, error) {
	var revision, chartID, datamodel string
	var envelopeVersion int64
	var canonical, fingerprint []byte
	if err := row.Scan(&revision, &envelopeVersion, &chartID, &datamodel, &canonical, &fingerprint); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return statecharts.DefinitionArtifact{}, err
		}
		return statecharts.DefinitionArtifact{}, fmt.Errorf("%w: scan persisted definition: %v", statecharts.ErrInvalidDefinitionArtifact, err)
	}
	if envelopeVersion < 0 || envelopeVersion > math.MaxUint32 {
		return statecharts.DefinitionArtifact{}, fmt.Errorf("%w: revision envelope version %d is out of range", statecharts.ErrInvalidDefinitionArtifact, envelopeVersion)
	}
	artifact := statecharts.DefinitionArtifact{
		Revision:                statecharts.RevisionID(revision),
		RevisionEnvelopeVersion: uint32(envelopeVersion),
		ChartID:                 statecharts.Identifier(chartID),
		Datamodel:               statecharts.Identifier(datamodel),
		CanonicalDefinition:     append([]byte(nil), canonical...),
		ProgramFingerprint:      append([]byte(nil), fingerprint...),
	}
	if err := artifact.Validate(); err != nil {
		return statecharts.DefinitionArtifact{}, err
	}
	return artifact, nil
}

func loadDefinition(ctx context.Context, queryer rowQueryer, revision statecharts.RevisionID) (statecharts.DefinitionArtifact, bool, error) {
	artifact, err := scanDefinitionArtifact(queryer.QueryRowContext(ctx, `SELECT revision,revision_envelope_version,chart_id,datamodel,canonical_definition,program_fingerprint FROM statechart_definition WHERE revision=?`, revision))
	if errors.Is(err, sql.ErrNoRows) {
		return statecharts.DefinitionArtifact{}, false, nil
	}
	if err != nil {
		return statecharts.DefinitionArtifact{}, false, err
	}
	return artifact, true, nil
}

// PutDefinition implements statecharts.DefinitionStore.
func (l *Storage) PutDefinition(ctx context.Context, artifact statecharts.DefinitionArtifact) (statecharts.DefinitionPutResult, error) {
	tx, err := l.beginWrite(ctx)
	if err != nil {
		return 0, fmt.Errorf("sqllog: begin definition write: %w", err)
	}
	defer tx.Rollback()
	stored, found, err := loadDefinition(ctx, tx, artifact.Revision)
	if err != nil {
		return 0, fmt.Errorf("sqllog: inspect definition %q: %w", artifact.Revision, err)
	}
	if found {
		if !stored.Equal(artifact) {
			return 0, statecharts.ErrDefinitionCollision
		}
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("sqllog: commit unchanged definition: %w", err)
		}
		return statecharts.DefinitionUnchanged, nil
	}
	if err := artifact.Validate(); err != nil {
		return 0, err
	}
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO statechart_definition(revision,revision_envelope_version,chart_id,datamodel,canonical_definition,program_fingerprint) VALUES(?,?,?,?,?,?)`,
		artifact.Revision, artifact.RevisionEnvelopeVersion, artifact.ChartID, artifact.Datamodel, artifact.CanonicalDefinition, artifact.ProgramFingerprint)
	if err != nil {
		return 0, fmt.Errorf("sqllog: insert definition %q: %w", artifact.Revision, err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("sqllog: inspect definition insert: %w", err)
	}
	if inserted == 0 {
		stored, found, err = loadDefinition(ctx, tx, artifact.Revision)
		if err != nil {
			return 0, fmt.Errorf("sqllog: inspect concurrently stored definition: %w", err)
		}
		if !found || !stored.Equal(artifact) {
			return 0, statecharts.ErrDefinitionCollision
		}
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("sqllog: commit concurrent definition retry: %w", err)
		}
		return statecharts.DefinitionUnchanged, nil
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("sqllog: commit definition: %w", err)
	}
	return statecharts.DefinitionStored, nil
}

// GetDefinition implements statecharts.DefinitionStore.
func (l *Storage) GetDefinition(ctx context.Context, revision statecharts.RevisionID) (statecharts.DefinitionArtifact, bool, error) {
	artifact, found, err := loadDefinition(ctx, l.db, revision)
	if err != nil {
		return statecharts.DefinitionArtifact{}, false, fmt.Errorf("sqllog: get definition %q: %w", revision, err)
	}
	return artifact, found, nil
}

func scanActor(row rowScanner) (statecharts.ActorMetadata, error) {
	var actorID, chartID, revision, sessionID, lifecycle string
	var durable int64
	var startedAt time.Time
	var terminalAt sql.NullTime
	if err := row.Scan(&actorID, &chartID, &revision, &sessionID, &durable, &lifecycle, &startedAt, &terminalAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return statecharts.ActorMetadata{}, err
		}
		return statecharts.ActorMetadata{}, fmt.Errorf("%w: scan persisted actor: %v", statecharts.ErrInvalidActorMetadata, err)
	}
	if durable != 0 && durable != 1 {
		return statecharts.ActorMetadata{}, fmt.Errorf("%w: actor %q has invalid durable value %d", statecharts.ErrInvalidActorMetadata, actorID, durable)
	}
	metadata := statecharts.ActorMetadata{
		ActorID:    statecharts.Identifier(actorID),
		ChartID:    statecharts.Identifier(chartID),
		Revision:   statecharts.RevisionID(revision),
		SessionID:  statecharts.SessionID(sessionID),
		Durable:    durable == 1,
		Lifecycle:  statecharts.ActorLifecycle(lifecycle),
		StartedAt:  startedAt.UTC(),
		TerminalAt: time.Time{},
	}
	if terminalAt.Valid {
		metadata.TerminalAt = terminalAt.Time.UTC()
	}
	return metadata, nil
}

func loadActor(ctx context.Context, queryer rowQueryer, actorID statecharts.Identifier) (statecharts.ActorMetadata, bool, error) {
	metadata, err := scanActor(queryer.QueryRowContext(ctx, `SELECT actor_id,chart_id,revision,session_id,durable,lifecycle,started_at,terminal_at FROM statechart_actor WHERE actor_id=?`, actorID))
	if errors.Is(err, sql.ErrNoRows) {
		return statecharts.ActorMetadata{}, false, nil
	}
	if err != nil {
		return statecharts.ActorMetadata{}, false, err
	}
	return metadata, true, nil
}

func validateStoredActor(ctx context.Context, tx queryTransaction, metadata statecharts.ActorMetadata) error {
	if err := metadata.Validate(); err != nil {
		return err
	}
	var (
		seq                                                      int64
		kind                                                     string
		timestamp                                                time.Time
		entrySendID, entryTarget, entryType                      string
		eventName                                                string
		eventType                                                int64
		eventSendID, eventOrigin, eventOriginType, eventInvokeID string
		deliveryID                                               string
		valueData                                                []byte
	)
	err := tx.QueryRowContext(ctx, `SELECT seq,kind,ts,entry_send_id,entry_target,entry_type,event_name,event_type,event_send_id,event_origin,event_origin_type,event_invoke_id,delivery_id,value_data FROM statechart_log WHERE session_id=? ORDER BY seq LIMIT 1`, metadata.SessionID).
		Scan(&seq, &kind, &timestamp, &entrySendID, &entryTarget, &entryType, &eventName, &eventType, &eventSendID, &eventOrigin, &eventOriginType, &eventInvokeID, &deliveryID, &valueData)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: actor %q has no session-start record", statecharts.ErrInvalidActorMetadata, metadata.ActorID)
	}
	if err != nil {
		return fmt.Errorf("%w: scan actor %q session start: %v", statecharts.ErrInvalidActorMetadata, metadata.ActorID, err)
	}
	if eventType < 0 || eventType > math.MaxUint8 {
		return fmt.Errorf("%w: actor %q session start has invalid event type", statecharts.ErrInvalidActorMetadata, metadata.ActorID)
	}
	event, err := statecharts.DecodeEvent(statecharts.EncodedEvent{
		Name: statecharts.Identifier(eventName), Type: statecharts.EventType(eventType),
		SendID: statecharts.Identifier(eventSendID), Origin: statecharts.Identifier(eventOrigin),
		OriginType: statecharts.Identifier(eventOriginType), InvokeID: statecharts.Identifier(eventInvokeID),
		DeliveryID: statecharts.DeliveryID(deliveryID), Data: append([]byte(nil), valueData...),
	})
	if err != nil {
		return fmt.Errorf("%w: actor %q session start event: %v", statecharts.ErrInvalidActorMetadata, metadata.ActorID, err)
	}
	var startCount int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM statechart_log WHERE session_id=? AND kind=?`, metadata.SessionID, statecharts.KindSessionStarted).Scan(&startCount); err != nil {
		return fmt.Errorf("sqllog: count actor %q session starts: %w", metadata.ActorID, err)
	}
	if seq != 1 || startCount != 1 || statecharts.EntryKind(kind) != statecharts.KindSessionStarted || !timestamp.Equal(metadata.StartedAt) ||
		entrySendID != "" || entryTarget != "" || entryType != "" || event.Name != "" || event.Type != statecharts.EventExternal || !event.Data.Equal(statecharts.NullValue()) ||
		event.SendID != "" || event.Origin != "" || event.OriginType != "" || event.InvokeID != "" || event.DeliveryID != "" {
		return fmt.Errorf("%w: actor %q has an inconsistent session-start record", statecharts.ErrInvalidActorMetadata, metadata.ActorID)
	}
	if metadata.Lifecycle == statecharts.ActorLifecycleActive {
		artifact, found, err := loadDefinition(ctx, tx, metadata.Revision)
		if err != nil {
			return err
		}
		if !found {
			return statecharts.ErrDefinitionNotFound
		}
		if artifact.ChartID != metadata.ChartID {
			return fmt.Errorf("%w: actor %q chart identity does not match revision", statecharts.ErrInvalidActorMetadata, metadata.ActorID)
		}
	}
	return nil
}

func listActors(ctx context.Context, tx queryTransaction, where string, args ...any) ([]statecharts.ActorMetadata, error) {
	query := `SELECT actor_id,chart_id,revision,session_id,durable,lifecycle,started_at,terminal_at FROM statechart_actor` + where + ` ORDER BY actor_id`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var actors []statecharts.ActorMetadata
	for rows.Next() {
		metadata, err := scanActor(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		actors = append(actors, metadata)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, metadata := range actors {
		if err := validateStoredActor(ctx, tx, metadata); err != nil {
			return nil, err
		}
	}
	return actors, nil
}

// BeginActor implements statecharts.ActorStore.
func (l *Storage) BeginActor(ctx context.Context, metadata statecharts.ActorMetadata) (statecharts.ActorMetadata, statecharts.ActorBeginResult, error) {
	if err := metadata.Validate(); err != nil {
		return statecharts.ActorMetadata{}, 0, err
	}
	if metadata.Lifecycle != statecharts.ActorLifecycleActive {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("%w: begin lifecycle is %q, want %q", statecharts.ErrInvalidActorMetadata, metadata.Lifecycle, statecharts.ActorLifecycleActive)
	}
	metadata.StartedAt = metadata.StartedAt.UTC()
	tx, err := l.beginWrite(ctx)
	if err != nil {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("sqllog: begin actor transaction: %w", err)
	}
	defer tx.Rollback()
	stored, found, err := loadActor(ctx, tx, metadata.ActorID)
	if err != nil {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("sqllog: inspect actor %q: %w", metadata.ActorID, err)
	}
	if found {
		if err := validateStoredActor(ctx, tx, stored); err != nil {
			return statecharts.ActorMetadata{}, 0, err
		}
		if stored.Lifecycle == statecharts.ActorLifecycleTerminal {
			return stored, 0, statecharts.ErrActorTerminal
		}
		if stored.ChartID != metadata.ChartID || stored.Revision != metadata.Revision || stored.SessionID != metadata.SessionID || stored.Durable != metadata.Durable {
			return stored, 0, statecharts.ErrActorCollision
		}
		if err := tx.Commit(); err != nil {
			return statecharts.ActorMetadata{}, 0, fmt.Errorf("sqllog: commit actor retry: %w", err)
		}
		return stored, statecharts.ActorAlreadyActive, nil
	}
	artifact, found, err := loadDefinition(ctx, tx, metadata.Revision)
	if err != nil {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("sqllog: load actor definition: %w", err)
	}
	if !found {
		return statecharts.ActorMetadata{}, 0, statecharts.ErrDefinitionNotFound
	}
	if artifact.ChartID != metadata.ChartID {
		return statecharts.ActorMetadata{}, 0, statecharts.ErrActorCollision
	}
	var occupied int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM statechart_actor WHERE session_id=?) OR EXISTS(SELECT 1 FROM statechart_log WHERE session_id=?)`, metadata.SessionID, metadata.SessionID).Scan(&occupied); err != nil {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("sqllog: inspect actor session %q: %w", metadata.SessionID, err)
	}
	if occupied != 0 {
		return statecharts.ActorMetadata{}, 0, statecharts.ErrActorCollision
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO statechart_actor(actor_id,chart_id,revision,session_id,durable,lifecycle,started_at,terminal_at) VALUES(?,?,?,?,?,?,?,NULL)`,
		metadata.ActorID, metadata.ChartID, metadata.Revision, metadata.SessionID, 1, metadata.Lifecycle, metadata.StartedAt); err != nil {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("sqllog: insert actor %q: %w", metadata.ActorID, err)
	}
	encoded, err := statecharts.EncodeEvent(statecharts.Event{})
	if err != nil {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("sqllog: encode actor session start: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO statechart_log(session_id,seq,kind,ts,entry_send_id,entry_target,entry_type,event_name,event_type,event_send_id,event_origin,event_origin_type,event_invoke_id,delivery_id,value_data) VALUES(?,1,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		metadata.SessionID, statecharts.KindSessionStarted, metadata.StartedAt, "", "", "", "", statecharts.EventExternal, "", "", "", "", "", encoded.Data); err != nil {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("sqllog: insert actor %q session start: %w", metadata.ActorID, err)
	}
	if err := tx.Commit(); err != nil {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("sqllog: commit actor %q: %w", metadata.ActorID, err)
	}
	return metadata, statecharts.ActorStarted, nil
}

// GetActor implements statecharts.ActorStore.
func (l *Storage) GetActor(ctx context.Context, actorID statecharts.Identifier) (statecharts.ActorMetadata, bool, error) {
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return statecharts.ActorMetadata{}, false, fmt.Errorf("sqllog: begin actor read: %w", err)
	}
	defer tx.Rollback()
	metadata, found, err := loadActor(ctx, tx, actorID)
	if err != nil {
		return statecharts.ActorMetadata{}, false, fmt.Errorf("sqllog: get actor %q: %w", actorID, err)
	}
	if !found {
		if err := tx.Commit(); err != nil {
			return statecharts.ActorMetadata{}, false, err
		}
		return statecharts.ActorMetadata{}, false, nil
	}
	if err := validateStoredActor(ctx, tx, metadata); err != nil {
		return statecharts.ActorMetadata{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return statecharts.ActorMetadata{}, false, fmt.Errorf("sqllog: commit actor read: %w", err)
	}
	return metadata, true, nil
}

// ListNonTerminalActors implements statecharts.ActorStore.
func (l *Storage) ListNonTerminalActors(ctx context.Context) ([]statecharts.ActorMetadata, error) {
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("sqllog: begin actor list: %w", err)
	}
	defer tx.Rollback()
	stored, err := listActors(ctx, tx, ``)
	if err != nil {
		return nil, fmt.Errorf("sqllog: list active actors: %w", err)
	}
	actors := make([]statecharts.ActorMetadata, 0, len(stored))
	for _, actor := range stored {
		if actor.Lifecycle == statecharts.ActorLifecycleActive {
			actors = append(actors, actor)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("sqllog: commit actor list: %w", err)
	}
	return actors, nil
}

// MarkActorTerminal implements statecharts.ActorStore.
func (l *Storage) MarkActorTerminal(ctx context.Context, actorID statecharts.Identifier, terminalAt time.Time) (statecharts.ActorMetadata, statecharts.ActorTerminalResult, error) {
	if _, err := statecharts.NewIdentifier(string(actorID)); err != nil {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("%w: actor ID: %v", statecharts.ErrInvalidActorMetadata, err)
	}
	if terminalAt.IsZero() {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("%w: terminal time is zero", statecharts.ErrInvalidActorMetadata)
	}
	terminalAt = terminalAt.UTC()
	tx, err := l.beginWrite(ctx)
	if err != nil {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("sqllog: begin terminal transaction: %w", err)
	}
	defer tx.Rollback()
	metadata, found, err := loadActor(ctx, tx, actorID)
	if err != nil {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("sqllog: get actor %q: %w", actorID, err)
	}
	if !found {
		if err := tx.Commit(); err != nil {
			return statecharts.ActorMetadata{}, 0, err
		}
		return statecharts.ActorMetadata{}, statecharts.ActorNotFound, nil
	}
	if err := validateStoredActor(ctx, tx, metadata); err != nil {
		return statecharts.ActorMetadata{}, 0, err
	}
	if metadata.Lifecycle == statecharts.ActorLifecycleTerminal {
		if err := tx.Commit(); err != nil {
			return statecharts.ActorMetadata{}, 0, err
		}
		return metadata, statecharts.ActorAlreadyTerminal, nil
	}
	if terminalAt.Before(metadata.StartedAt) {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("%w: terminal time precedes start time", statecharts.ErrInvalidActorMetadata)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE statechart_actor SET lifecycle=?,terminal_at=? WHERE actor_id=? AND lifecycle=?`, statecharts.ActorLifecycleTerminal, terminalAt, actorID, statecharts.ActorLifecycleActive); err != nil {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("sqllog: mark actor %q terminal: %w", actorID, err)
	}
	metadata.Lifecycle = statecharts.ActorLifecycleTerminal
	metadata.TerminalAt = terminalAt
	if err := tx.Commit(); err != nil {
		return statecharts.ActorMetadata{}, 0, fmt.Errorf("sqllog: commit actor terminal: %w", err)
	}
	return metadata, statecharts.ActorMarkedTerminal, nil
}

// ReferencedRevisions implements statecharts.ActorStore.
func (l *Storage) ReferencedRevisions(ctx context.Context) ([]statecharts.RevisionID, error) {
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("sqllog: begin revision reference read: %w", err)
	}
	defer tx.Rollback()
	actors, err := listActors(ctx, tx, ``)
	if err != nil {
		return nil, fmt.Errorf("sqllog: list revision references: %w", err)
	}
	set := make(map[statecharts.RevisionID]struct{})
	for _, actor := range actors {
		if actor.Lifecycle == statecharts.ActorLifecycleActive {
			set[actor.Revision] = struct{}{}
		}
	}
	revisions := make([]statecharts.RevisionID, 0, len(set))
	for revision := range set {
		revisions = append(revisions, revision)
	}
	sort.Slice(revisions, func(i, j int) bool { return revisions[i] < revisions[j] })
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("sqllog: commit revision reference read: %w", err)
	}
	return revisions, nil
}

// DeleteDefinitionIfUnreferenced implements statecharts.DefinitionStore.
func (l *Storage) DeleteDefinitionIfUnreferenced(ctx context.Context, revision statecharts.RevisionID) (statecharts.DefinitionDeleteResult, error) {
	tx, err := l.beginWrite(ctx)
	if err != nil {
		return 0, fmt.Errorf("sqllog: begin definition deletion: %w", err)
	}
	defer tx.Rollback()
	_, found, err := loadDefinition(ctx, tx, revision)
	if err != nil {
		return 0, fmt.Errorf("sqllog: inspect definition for deletion: %w", err)
	}
	if !found {
		if err := tx.Commit(); err != nil {
			return 0, err
		}
		return statecharts.DefinitionNotFound, nil
	}
	actors, err := listActors(ctx, tx, ` WHERE revision=?`, revision)
	if err != nil {
		return 0, fmt.Errorf("sqllog: inspect definition references: %w", err)
	}
	for _, actor := range actors {
		if actor.Lifecycle == statecharts.ActorLifecycleActive {
			if err := tx.Commit(); err != nil {
				return 0, err
			}
			return statecharts.DefinitionReferenced, nil
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM statechart_definition WHERE revision=?`, revision); err != nil {
		return 0, fmt.Errorf("sqllog: delete definition %q: %w", revision, err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("sqllog: commit definition deletion: %w", err)
	}
	return statecharts.DefinitionDeleted, nil
}
