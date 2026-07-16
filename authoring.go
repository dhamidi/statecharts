package statecharts

import (
	"errors"
	"fmt"
)

// AuthoringOption configures the default Go authoring path created by New.
type AuthoringOption func(*authoringConfig)

type authoringConfig struct {
	version string
}

// Version sets the default version for registered behavior and supplies the
// chart revision salt. Bump it when Go behavior changes without changing the
// chart structure. The default is "v1".
func Version(version string) AuthoringOption {
	return func(config *authoringConfig) { config.version = version }
}

// Builder is the concise, default authoring surface for Go applications. It
// owns one model-local behavior registry, qualifies short behavior names with
// the chart ID, and reports registration errors when Build is called.
//
// Use the package-level Build with an explicit Datamodel instead when
// authoring for another datamodel or when direct registry control is required.
type Builder[D any] struct {
	id      Identifier
	version string
	model   *GoModel[D]
	errors  []error
}

// New starts a Go-authored chart definition. id is the stable chart identity;
// behavior registered as "publish" is stored as "<id>.publish" in the
// canonical Definition. The returned Builder is intended for configuration by
// one goroutine before Build.
func New[D any](id Identifier, factory func() *D, options ...AuthoringOption) *Builder[D] {
	config := authoringConfig{version: "v1"}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	builder := &Builder[D]{id: id, version: config.version, model: NewGoModel(factory)}
	if err := validatePlainIdentifier(id); err != nil {
		builder.errors = append(builder.errors, fmt.Errorf("statecharts: chart ID %q: %w", id, err))
	}
	if config.version == "" {
		builder.errors = append(builder.errors, fmt.Errorf("statecharts: version must not be empty"))
	} else if err := validateUTF8(config.version); err != nil {
		builder.errors = append(builder.errors, fmt.Errorf("statecharts: version: %w", err))
	}
	if factory == nil {
		builder.errors = append(builder.errors, fmt.Errorf("statecharts: model factory must not be nil"))
	}
	return builder
}

// Action registers a named action at the Builder's default Version.
func (b *Builder[D]) Action(name string, action GoAction[D]) GoActionRef {
	return b.ActionVersion(name, b.version, action)
}

// ActionVersion registers a retained implementation at an explicit version.
// This is useful when old, revision-pinned actors may still resolve it.
func (b *Builder[D]) ActionVersion(name string, version string, action GoAction[D]) GoActionRef {
	qualified := b.qualify(name)
	reference, err := b.model.Action(qualified, version, action)
	b.record("action", name, err)
	return reference
}

// Condition registers a named condition at the Builder's default Version.
func (b *Builder[D]) Condition(name string, condition GoCondition[D]) GoConditionRef {
	return b.ConditionVersion(name, b.version, condition)
}

// ConditionVersion registers a retained condition implementation at an
// explicit version.
func (b *Builder[D]) ConditionVersion(name string, version string, condition GoCondition[D]) GoConditionRef {
	qualified := b.qualify(name)
	reference, err := b.model.Condition(qualified, version, condition)
	b.record("condition", name, err)
	return reference
}

// Value registers a named value producer at the Builder's default Version.
func (b *Builder[D]) Value(name string, value GoValue[D]) GoValueRef {
	return b.ValueVersion(name, b.version, value)
}

// ValueVersion registers a retained value implementation at an explicit
// version.
func (b *Builder[D]) ValueVersion(name string, version string, value GoValue[D]) GoValueRef {
	qualified := b.qualify(name)
	reference, err := b.model.Value(qualified, version, value)
	b.record("value", name, err)
	return reference
}

// Location registers a named readable/writable location at the Builder's
// default Version.
func (b *Builder[D]) Location(name string, get GoValue[D], set GoLocation[D]) GoLocationRef {
	return b.LocationVersion(name, b.version, get, set)
}

// LocationVersion registers a retained location implementation at an
// explicit version.
func (b *Builder[D]) LocationVersion(name string, version string, get GoValue[D], set GoLocation[D]) GoLocationRef {
	qualified := b.qualify(name)
	reference, err := b.model.Location(qualified, version, get, set)
	b.record("location", name, err)
	return reference
}

// Build compiles root through the ordinary Definition compiler. The Builder's
// ID becomes the chart ID and its Version becomes the default revision salt;
// an explicit WithRevisionSalt option may override that salt.
func (b *Builder[D]) Build(root StateDefinition, options ...BuildOption) (*Chart, error) {
	if b == nil {
		return nil, fmt.Errorf("statecharts: nil Builder")
	}
	if err := errors.Join(b.errors...); err != nil {
		return nil, err
	}
	defaults := []BuildOption{
		func(definition *Definition) {
			definition.ID = b.id
			definition.RevisionSalt = b.version
		},
	}
	defaults = append(defaults, options...)
	return Build(root, b.model, defaults...)
}

func (b *Builder[D]) qualify(name string) Identifier {
	return b.id + "." + Identifier(name)
}

func (b *Builder[D]) record(kind string, name string, err error) {
	if err != nil {
		b.errors = append(b.errors, fmt.Errorf("statecharts: %s %q: %w", kind, name, err))
	}
}
