package statecharts

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

const (
	// DefinitionCanonicalVersion identifies the internal canonical definition
	// encoding. It must change when that encoding changes.
	DefinitionCanonicalVersion uint32 = 2

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

func decodeCanonicalDefinition(data []byte) (Definition, error) {
	headerSize := len(DefinitionCanonicalMagic) + 4
	if len(data) < headerSize {
		return Definition{}, fmt.Errorf("canonical definition is truncated")
	}
	if !bytes.Equal(data[:len(DefinitionCanonicalMagic)], []byte(DefinitionCanonicalMagic)) {
		return Definition{}, fmt.Errorf("canonical definition has invalid magic")
	}
	version := binary.BigEndian.Uint32(data[len(DefinitionCanonicalMagic):headerSize])
	if version != DefinitionCanonicalVersion {
		return Definition{}, fmt.Errorf("canonical definition version %d is unsupported (want %d)", version, DefinitionCanonicalVersion)
	}
	decoder := json.NewDecoder(bytes.NewReader(data[headerSize:]))
	decoder.DisallowUnknownFields()
	var definition Definition
	if err := decoder.Decode(&definition); err != nil {
		return Definition{}, fmt.Errorf("decode canonical definition: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return Definition{}, fmt.Errorf("decode canonical definition: trailing JSON value")
		}
		return Definition{}, fmt.Errorf("decode canonical definition: trailing data: %w", err)
	}
	normalized, err := normalizeDefinition(definition)
	if err != nil {
		return Definition{}, err
	}
	canonical, err := normalized.CanonicalBytes()
	if err != nil {
		return Definition{}, err
	}
	if !bytes.Equal(canonical, data) {
		return Definition{}, fmt.Errorf("definition bytes are not canonical")
	}
	return normalized, nil
}
