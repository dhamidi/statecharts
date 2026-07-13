package statecharts

import (
	"cmp"
	"database/sql/driver"
	"fmt"
)

// SessionID identifies one running session -- SCXML 5.10's _sessionid: an
// Instance's identity for logging, snapshotting, and Rehydrate. Unlike
// Identifier, it is an opaque token: it has no dot-segment structure, is
// never matched as an event descriptor, and is never split into segments.
type SessionID string

// String implements fmt.Stringer.
func (id SessionID) String() string {
	return string(id)
}

// Compare gives a total, deterministic lexicographic order over SessionID
// values, for diagnostics, sorted dumps, and deterministic map-key
// iteration.
func (id SessionID) Compare(other SessionID) int {
	return cmp.Compare(string(id), string(other))
}

// MarshalText implements encoding.TextMarshaler.
func (id SessionID) MarshalText() ([]byte, error) {
	return []byte(id), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (id *SessionID) UnmarshalText(text []byte) error {
	*id = SessionID(text)
	return nil
}

// Value implements database/sql/driver.Valuer.
func (id SessionID) Value() (driver.Value, error) {
	return string(id), nil
}

// Scan implements database/sql.Scanner.
func (id *SessionID) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*id = ""
	case string:
		*id = SessionID(v)
	case []byte:
		*id = SessionID(v)
	default:
		return fmt.Errorf("statecharts: cannot scan %T into SessionID", src)
	}
	return nil
}
