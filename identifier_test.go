package statecharts

import (
	"database/sql/driver"
	"encoding/json"
	"testing"
)

func TestIdentifierMatches(t *testing.T) {
	cases := []struct {
		descriptor Identifier
		event      Identifier
		want       bool
	}{
		{"foo.bar", "foo.bar", true},        // exact match
		{"foo", "foo.bar", true},            // token prefix
		{"foo.bar", "foo", false},           // descriptor longer than event
		{"foo", "foobar", false},            // must be a token match, not string prefix
		{"*", "anything.at.all", true},      // bare wildcard
		{"foo.*", "foo.bar.baz", true},      // wildcard suffix sugar
		{"foo.*", "foo", true},              // wildcard suffix, exact token match remaining
		{"error.", "error.execution", true}, // trailing-dot sugar
		{"Foo", "foo", false},               // case-sensitive
		{"foo.bar", "foo.barbaz", false},
	}
	for _, c := range cases {
		if got := c.descriptor.Matches(c.event); got != c.want {
			t.Errorf("Identifier(%q).Matches(%q) = %v, want %v", c.descriptor, c.event, got, c.want)
		}
	}
}

func TestNewIdentifierValidation(t *testing.T) {
	valid := []string{"foo", "foo.bar", "foo.bar.baz", "*", "#_internal", "#_parent", "error."}
	for _, s := range valid {
		if _, err := NewIdentifier(s); err != nil {
			t.Errorf("NewIdentifier(%q) unexpected error: %v", s, err)
		}
	}

	invalid := []string{"", ".", "..", "a..b", ".a", "#"}
	for _, s := range invalid {
		if _, err := NewIdentifier(s); err == nil {
			t.Errorf("NewIdentifier(%q) expected error, got nil", s)
		}
	}
}

func TestIdentifierTextMarshaling(t *testing.T) {
	type wrapper struct {
		ID Identifier `json:"id"`
	}
	w := wrapper{ID: "foo.bar"}
	b, err := json.Marshal(w)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(b) != `{"id":"foo.bar"}` {
		t.Fatalf("unexpected JSON: %s", b)
	}
	var got wrapper
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.ID != w.ID {
		t.Fatalf("round-trip mismatch: got %q want %q", got.ID, w.ID)
	}
}

func TestIdentifierSQLValueScan(t *testing.T) {
	id := Identifier("foo.bar")
	v, err := id.Value()
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	if v.(string) != "foo.bar" {
		t.Fatalf("Value() = %v, want foo.bar", v)
	}

	var scanned Identifier
	if err := scanned.Scan(v); err != nil {
		t.Fatalf("Scan(string): %v", err)
	}
	if scanned != id {
		t.Fatalf("Scan round-trip mismatch: got %q want %q", scanned, id)
	}

	if err := scanned.Scan([]byte("baz.qux")); err != nil {
		t.Fatalf("Scan([]byte): %v", err)
	}
	if scanned != "baz.qux" {
		t.Fatalf("Scan([]byte) = %q, want baz.qux", scanned)
	}

	if err := scanned.Scan(nil); err != nil {
		t.Fatalf("Scan(nil): %v", err)
	}
	if scanned != "" {
		t.Fatalf("Scan(nil) = %q, want empty", scanned)
	}

	if err := scanned.Scan(42); err == nil {
		t.Fatalf("Scan(int) expected error, got nil")
	}

	var _ driver.Valuer = id
}

func TestIdentifierCompare(t *testing.T) {
	if Identifier("a").Compare("b") >= 0 {
		t.Fatalf(`"a".Compare("b") should be negative`)
	}
	if Identifier("b").Compare("a") <= 0 {
		t.Fatalf(`"b".Compare("a") should be positive`)
	}
	if Identifier("a").Compare("a") != 0 {
		t.Fatalf(`"a".Compare("a") should be zero`)
	}
}

func TestIdentifierSegments(t *testing.T) {
	got := Identifier("foo.bar.baz").Segments()
	want := []string{"foo", "bar", "baz"}
	if len(got) != len(want) {
		t.Fatalf("Segments() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Segments()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
