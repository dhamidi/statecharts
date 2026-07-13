package statecharts

import (
	"cmp"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
)

// Identifier is a dot-segmented, case-sensitive name. It is used uniformly
// for state IDs, event names, event descriptors (the "event" attribute of a
// transition), and SCXML send targets (including the special "#_..." forms).
type Identifier string

var segmentPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// NewIdentifier validates s and returns it as an Identifier. Use this for
// untrusted input (e.g. values read back from a database or external
// message); identifiers built from Go source literals in chart definitions
// can be used directly via the defined string type without calling this.
func NewIdentifier(s string) (Identifier, error) {
	if s == "" {
		return "", fmt.Errorf("statecharts: empty identifier")
	}
	check := strings.TrimPrefix(s, "#")
	if check == "" {
		return "", fmt.Errorf("statecharts: invalid identifier %q", s)
	}
	if check == "*" {
		return Identifier(s), nil
	}
	segments := strings.Split(check, ".")
	for i, seg := range segments {
		last := i == len(segments)-1
		if seg == "*" && last {
			continue // wildcard suffix sugar, e.g. "error.*"
		}
		if seg == "" && last {
			continue // trailing-dot sugar, e.g. "error."
		}
		if !segmentPattern.MatchString(seg) {
			return "", fmt.Errorf("statecharts: invalid identifier %q: invalid segment %q", s, seg)
		}
	}
	return Identifier(s), nil
}

// String implements fmt.Stringer.
func (id Identifier) String() string {
	return string(id)
}

// Segments splits id on ".".
func (id Identifier) Segments() []string {
	return strings.Split(string(id), ".")
}

// Matches reports whether id, used as an event descriptor, matches the event
// name eventName, per SCXML 3.12.1: a descriptor matches if it is an exact
// match or a token-prefix match of the event name. A bare "*" matches any
// event name; a trailing ".*" or "." is sugar with the same effect as the
// bare token-prefix it leaves behind once trimmed.
func (id Identifier) Matches(eventName Identifier) bool {
	d := string(id)
	if d == "*" {
		return true
	}
	d = strings.TrimSuffix(d, ".*")
	d = strings.TrimSuffix(d, ".")
	if d == "" {
		return true
	}
	dTokens := strings.Split(d, ".")
	nTokens := strings.Split(string(eventName), ".")
	if len(dTokens) > len(nTokens) {
		return false
	}
	for i, t := range dTokens {
		if t != nTokens[i] {
			return false
		}
	}
	return true
}

// Compare gives a total, deterministic lexicographic order over Identifier
// values, for diagnostics, sorted dumps, and deterministic map-key
// iteration. It is NOT SCXML "document order" -- document order is an
// explicit sequence number assigned by Build, tracking declaration order in
// the Go builder calls, and must never be derived by sorting Identifiers.
func (id Identifier) Compare(other Identifier) int {
	return cmp.Compare(string(id), string(other))
}

// MarshalText implements encoding.TextMarshaler.
func (id Identifier) MarshalText() ([]byte, error) {
	return []byte(id), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (id *Identifier) UnmarshalText(text []byte) error {
	*id = Identifier(text)
	return nil
}

// Value implements database/sql/driver.Valuer.
func (id Identifier) Value() (driver.Value, error) {
	return string(id), nil
}

// Scan implements database/sql.Scanner.
func (id *Identifier) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*id = ""
	case string:
		*id = Identifier(v)
	case []byte:
		*id = Identifier(v)
	default:
		return fmt.Errorf("statecharts: cannot scan %T into Identifier", src)
	}
	return nil
}
