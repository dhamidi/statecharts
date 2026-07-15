package statecharts

import (
	"bytes"
	"errors"
	"fmt"
)

var (
	// ErrInvalidDefinitionArtifact marks corrupt, unsupported, or internally
	// inconsistent persisted definition data.
	ErrInvalidDefinitionArtifact = errors.New("statecharts: invalid definition artifact")
	// ErrDefinitionCollision marks an attempt to associate one RevisionID
	// with artifact bytes other than the bytes already stored for that ID.
	ErrDefinitionCollision = errors.New("statecharts: definition revision collision")
	// ErrDefinitionNotFound marks an operation that requires an immutable
	// definition artifact that is not present.
	ErrDefinitionNotFound = errors.New("statecharts: definition artifact not found")
)

// DefinitionArtifact is the deterministic data required to recompile one
// Chart revision after a process restart. Go functions are represented only
// by the named/versioned references in CanonicalDefinition; the process must
// supply their implementations through the selected Datamodel.
type DefinitionArtifact struct {
	Revision                RevisionID
	RevisionEnvelopeVersion uint32
	ChartID                 Identifier // stable Definition.ID, independent of the root state's structural ID
	Datamodel               Identifier
	CanonicalDefinition     []byte
	ProgramFingerprint      []byte
}

// Clone returns a deep copy whose byte slices are independently owned.
func (a DefinitionArtifact) Clone() DefinitionArtifact {
	a.CanonicalDefinition = append([]byte(nil), a.CanonicalDefinition...)
	a.ProgramFingerprint = append([]byte(nil), a.ProgramFingerprint...)
	return a
}

// Equal reports whether every persisted field is byte-for-byte equal.
func (a DefinitionArtifact) Equal(other DefinitionArtifact) bool {
	return a.Revision == other.Revision &&
		a.RevisionEnvelopeVersion == other.RevisionEnvelopeVersion &&
		a.ChartID == other.ChartID &&
		a.Datamodel == other.Datamodel &&
		bytes.Equal(a.CanonicalDefinition, other.CanonicalDefinition) &&
		bytes.Equal(a.ProgramFingerprint, other.ProgramFingerprint)
}

// Validate checks canonical encoding, identity fields, envelope version, and
// revision integrity.
func (a DefinitionArtifact) Validate() error {
	_, err := a.validatedDefinition()
	return err
}

// Definition decodes and validates an independently editable definition.
func (a DefinitionArtifact) Definition() (Definition, error) {
	return a.validatedDefinition()
}

func (a DefinitionArtifact) validatedDefinition() (Definition, error) {
	invalid := func(format string, args ...any) (Definition, error) {
		return Definition{}, fmt.Errorf("%w: %s", ErrInvalidDefinitionArtifact, fmt.Sprintf(format, args...))
	}
	if a.RevisionEnvelopeVersion != RevisionEnvelopeVersion {
		return invalid("revision envelope version %d is unsupported (want %d)", a.RevisionEnvelopeVersion, RevisionEnvelopeVersion)
	}
	if len(a.ProgramFingerprint) == 0 {
		return invalid("program fingerprint is empty")
	}
	definition, err := decodeCanonicalDefinition(a.CanonicalDefinition)
	if err != nil {
		return invalid("%v", err)
	}
	if definition.ID != a.ChartID {
		return invalid("chart identity %q does not match definition identity %q", a.ChartID, definition.ID)
	}
	if definition.Datamodel != a.Datamodel {
		return invalid("datamodel %q does not match definition datamodel %q", a.Datamodel, definition.Datamodel)
	}
	want := deriveRevisionFromCanonical(a.CanonicalDefinition, a.Datamodel, a.ProgramFingerprint)
	if a.Revision != want {
		return invalid("revision %q does not match artifact content (want %q)", a.Revision, want)
	}
	return definition, nil
}

// Compile validates a, resolves its references through model, and verifies
// that recompilation produces the exact persisted program fingerprint and
// revision. Applications must do this before replaying a pinned actor.
func (a DefinitionArtifact) Compile(model Datamodel) (*Chart, error) {
	definition, err := a.validatedDefinition()
	if err != nil {
		return nil, err
	}
	if model == nil {
		return nil, fmt.Errorf("statecharts: compile definition artifact: nil datamodel")
	}
	if model.Name() != a.Datamodel {
		return nil, fmt.Errorf("statecharts: compile definition artifact: datamodel %q does not match artifact datamodel %q", model.Name(), a.Datamodel)
	}
	chart, err := Compile(definition, model)
	if err != nil {
		return nil, fmt.Errorf("statecharts: compile definition artifact: %w", err)
	}
	if !bytes.Equal(chart.programFingerprint, a.ProgramFingerprint) || chart.Revision() != a.Revision {
		return nil, fmt.Errorf("%w: recompilation produced revision %q with a different program fingerprint", ErrInvalidDefinitionArtifact, chart.Revision())
	}
	return chart, nil
}
