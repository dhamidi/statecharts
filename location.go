package statecharts

import (
	"fmt"
	"net/url"
)

// Location is the address an IOProcessor advertises for the current
// session (IOProcessorInfo.Location) -- what a different session sets as
// its own SendOptions.Target to reach this one, per SCXML 5.10's
// _ioprocessors. It wraps a parsed net/url.URL, so a real network address
// (scheme, host, path) round-trips with genuine structure a real transport
// can inspect, while an opaque in-process token -- an actor's own dotted
// name, say -- is exactly as easy to construct as it always was: net/url
// does not require a scheme or host, so "locator-1" parses just as validly
// as "https://example.com/svc", and both round-trip through String()
// unchanged.
type Location struct {
	u *url.URL
}

// NewLocation parses s as a URL and returns the result. The only strings
// it rejects are genuinely malformed ones (bad percent-encoding, a stray
// control character) -- the absence of a scheme or host is not malformed,
// it is simply an opaque, schemeless Location.
func NewLocation(s string) (Location, error) {
	u, err := url.Parse(s)
	if err != nil {
		return Location{}, fmt.Errorf("statecharts: invalid location %q: %w", s, err)
	}
	return Location{u: u}, nil
}

// LocationFromIdentifier converts id directly to a Location. It never
// fails: id's own construction (NewIdentifier) already restricts it to
// characters that are always a valid URL path segment with no escaping
// needed, so parsing id's text can never produce an error here.
func LocationFromIdentifier(id Identifier) Location {
	u, _ := url.Parse(string(id))
	return Location{u: u}
}

// URL returns the *url.URL backing loc, for a caller that wants to inspect
// scheme/host/path directly -- e.g. a real network IOProcessor deciding how
// to dial the address a peer advertised.
func (loc Location) URL() *url.URL {
	return loc.u
}

// String implements fmt.Stringer, rendering loc back to the same text form
// a SendOptions.Target expects.
func (loc Location) String() string {
	if loc.u == nil {
		return ""
	}
	return loc.u.String()
}
