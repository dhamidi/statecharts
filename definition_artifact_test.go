package statecharts

import (
	"bytes"
	"errors"
	"testing"
)

type artifactState struct{ Count int }

func buildArtifactChart(t *testing.T) (*Chart, *GoModel[artifactState]) {
	t.Helper()
	model := NewGoModel(func() *artifactState { return &artifactState{} })
	record, err := model.Action("artifact.record", "v1", func(state *artifactState, _ ExecContext, _ []Value) error {
		state.Count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	chart, err := Build(Atomic("artifact-chart", On("record", Then(record.Do()))), model, WithRevisionSalt("artifact-v1"))
	if err != nil {
		t.Fatal(err)
	}
	return chart, model
}

func TestDefinitionArtifactRoundTripsAndRecompiles(t *testing.T) {
	chart, model := buildArtifactChart(t)
	artifact := chart.DefinitionArtifact()
	if err := artifact.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if artifact.Revision != chart.Revision() || artifact.ChartID != chart.Definition().ID || artifact.Datamodel != model.Name() {
		t.Fatalf("artifact identity = %#v", artifact)
	}
	if artifact.RevisionEnvelopeVersion != RevisionEnvelopeVersion {
		t.Fatalf("envelope version = %d, want %d", artifact.RevisionEnvelopeVersion, RevisionEnvelopeVersion)
	}

	definition, err := artifact.Definition()
	if err != nil {
		t.Fatalf("Definition: %v", err)
	}
	wantCanonical, err := chart.Definition().CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	gotCanonical, err := definition.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotCanonical, wantCanonical) {
		t.Fatal("decoded definition differs from compiled definition")
	}

	recompiled, err := artifact.Compile(model)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if recompiled.Revision() != chart.Revision() {
		t.Fatalf("recompiled revision = %q, want %q", recompiled.Revision(), chart.Revision())
	}
}

func TestDefinitionArtifactOwnsCanonicalAndFingerprintBytes(t *testing.T) {
	chart, _ := buildArtifactChart(t)
	first := chart.DefinitionArtifact()
	want := first.Clone()
	first.CanonicalDefinition[0] ^= 0xff
	first.ProgramFingerprint[0] ^= 0xff
	second := chart.DefinitionArtifact()
	if !second.Equal(want) {
		t.Fatal("mutating returned artifact changed the chart's artifact")
	}
	clone := second.Clone()
	clone.CanonicalDefinition[0] ^= 0xff
	clone.ProgramFingerprint[0] ^= 0xff
	if !second.Equal(want) {
		t.Fatal("mutating artifact clone changed its source")
	}
}

func TestDefinitionArtifactRejectsCorruptionAndWrongCompiler(t *testing.T) {
	chart, _ := buildArtifactChart(t)
	valid := chart.DefinitionArtifact()
	tests := []struct {
		name   string
		mutate func(*DefinitionArtifact)
	}{
		{"envelope version", func(a *DefinitionArtifact) { a.RevisionEnvelopeVersion++ }},
		{"revision", func(a *DefinitionArtifact) { a.Revision = RevisionID("sha256:wrong") }},
		{"chart identity", func(a *DefinitionArtifact) { a.ChartID = "other-chart" }},
		{"datamodel", func(a *DefinitionArtifact) { a.Datamodel = "other-model" }},
		{"fingerprint", func(a *DefinitionArtifact) { a.ProgramFingerprint[0] ^= 0xff }},
		{"canonical magic", func(a *DefinitionArtifact) { a.CanonicalDefinition[0] ^= 0xff }},
		{"canonical body", func(a *DefinitionArtifact) { a.CanonicalDefinition[len(a.CanonicalDefinition)-1] ^= 0x01 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			artifact := valid.Clone()
			test.mutate(&artifact)
			if err := artifact.Validate(); !errors.Is(err, ErrInvalidDefinitionArtifact) {
				t.Fatalf("Validate error = %v, want ErrInvalidDefinitionArtifact", err)
			}
		})
	}
	noncanonical := valid.Clone()
	noncanonical.CanonicalDefinition = append(noncanonical.CanonicalDefinition, ' ')
	noncanonical.Revision = deriveRevisionFromCanonical(noncanonical.CanonicalDefinition, noncanonical.Datamodel, noncanonical.ProgramFingerprint)
	if err := noncanonical.Validate(); !errors.Is(err, ErrInvalidDefinitionArtifact) {
		t.Fatalf("non-canonical definition error = %v, want ErrInvalidDefinitionArtifact", err)
	}

	missingRegistry := NewGoModel(func() *artifactState { return &artifactState{} })
	if _, err := valid.Compile(missingRegistry); err == nil {
		t.Fatal("Compile accepted a model missing the artifact's named function")
	}
}
