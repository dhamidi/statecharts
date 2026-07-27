package inspector

import (
	"context"
	"errors"

	statecharts "github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
)

// ErrRedaction is a deliberately generic redaction failure. It never embeds
// source data or a recovered panic value.
var ErrRedaction = errors.New("inspector: redaction failed")

// RedactionCategory identifies the semantic use of sensitive data.
type RedactionCategory string

const (
	CategoryDatamodel   RedactionCategory = "datamodel"
	CategoryEvent       RedactionCategory = "event"
	CategoryPendingSend RedactionCategory = "pending-send"
	CategoryInvocation  RedactionCategory = "invocation"
	CategoryDiagnostic  RedactionCategory = "diagnostic"
)

// RedactionContext identifies data being redacted.
type RedactionContext struct {
	System   string
	ActorID  actors.ActorID
	Category RedactionCategory
}

// Redactor replaces canonical values and diagnostic text.
type Redactor interface {
	RedactValue(context.Context, RedactionContext, statecharts.Value) (statecharts.Value, error)
	RedactText(context.Context, RedactionContext, string) (string, error)
}

// RedactorFuncs adapts optional functions to Redactor; nil functions clone or
// preserve their input.
type RedactorFuncs struct {
	Value func(context.Context, RedactionContext, statecharts.Value) (statecharts.Value, error)
	Text  func(context.Context, RedactionContext, string) (string, error)
}

func (r RedactorFuncs) RedactValue(ctx context.Context, rc RedactionContext, value statecharts.Value) (statecharts.Value, error) {
	if r.Value == nil {
		return value.Clone(), nil
	}
	v, err := r.Value(ctx, rc, value.Clone())
	if err != nil {
		return statecharts.Value{}, err
	}
	return v.Clone(), nil
}
func (r RedactorFuncs) RedactText(ctx context.Context, rc RedactionContext, text string) (string, error) {
	if r.Text == nil {
		return text, nil
	}
	return r.Text(ctx, rc, text)
}
