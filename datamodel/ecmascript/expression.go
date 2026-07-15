package ecmascript

import (
	"fmt"
	"unicode/utf8"

	"github.com/dhamidi/statecharts"
)

const expressionKind statecharts.Identifier = "ecmascript.source"

// ExpressionKind identifies a textual ECMAScript expression form.
type ExpressionKind string

const (
	// SourceExpression is ordinary ECMAScript source text. The definition
	// location determines whether it is evaluated as a value, condition,
	// assignable location, or script.
	SourceExpression ExpressionKind = "source"
)

// Source returns a serializable ECMAScript source expression.
func Source(text string) (statecharts.Expression, error) {
	value, err := statecharts.StringValue(text)
	if err != nil {
		return statecharts.Expression{}, fmt.Errorf("ecmascript: source: %w", err)
	}
	return statecharts.Expression{Kind: expressionKind, Data: value}, nil
}

// TextExpressionCodec converts package-owned source expressions to and from
// plain text. It is deliberately separate from Definition codecs.
type TextExpressionCodec struct{}

// ParseExpression wraps source text in a serializable expression.
func (TextExpressionCodec) ParseExpression(kind ExpressionKind, text string) (statecharts.Expression, error) {
	if kind != SourceExpression {
		return statecharts.Expression{}, fmt.Errorf("ecmascript: unsupported text expression kind %q", kind)
	}
	return Source(text)
}

// FormatExpression extracts source text without normalizing whitespace.
func (TextExpressionCodec) FormatExpression(kind ExpressionKind, expression statecharts.Expression) (string, error) {
	if kind != SourceExpression {
		return "", fmt.Errorf("ecmascript: unsupported text expression kind %q", kind)
	}
	text, err := expressionSource(expression)
	if err != nil {
		return "", err
	}
	return text, nil
}

func expressionSource(expression statecharts.Expression) (string, error) {
	if expression.Kind != expressionKind {
		return "", fmt.Errorf("ecmascript: expression kind %q is not %q", expression.Kind, expressionKind)
	}
	text, ok := expression.Data.AsString()
	if !ok {
		return "", fmt.Errorf("ecmascript: source expression data is %s, want string", expression.Data.Kind())
	}
	if !utf8.ValidString(text) {
		return "", fmt.Errorf("ecmascript: source is not valid UTF-8")
	}
	return text, nil
}
