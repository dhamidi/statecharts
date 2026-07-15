package statecharts

import (
	"encoding/binary"
	"encoding/json"
)

const (
	// DefinitionCanonicalVersion identifies the internal canonical definition
	// encoding. It must change when that encoding changes.
	DefinitionCanonicalVersion uint32 = 1

	// DefinitionCanonicalMagic separates canonical definitions from other
	// versioned byte formats when they are used as revision material.
	DefinitionCanonicalMagic = "statecharts-definition\x00"
)

// CanonicalBytes returns deterministic, versioned bytes suitable for chart
// revision material. The encoding is internal: authoring surfaces should
// translate to and from Definition rather than expose this format.
//
// The canonical form resolves generated state IDs and default transition
// types on a clone. It does not mutate d.
func (d Definition) CanonicalBytes() ([]byte, error) {
	normalized, err := normalizeDefinition(d)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(normalized)
	if err != nil {
		return nil, err
	}
	result := make([]byte, len(DefinitionCanonicalMagic)+4+len(body))
	copy(result, DefinitionCanonicalMagic)
	binary.BigEndian.PutUint32(result[len(DefinitionCanonicalMagic):], DefinitionCanonicalVersion)
	copy(result[len(DefinitionCanonicalMagic)+4:], body)
	return result, nil
}
