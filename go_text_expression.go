package statecharts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// GoTextExpressionCodec represents GoModel expressions as deterministic JSON
// text. The text contains stable registered function names and versions, never
// Go function pointers. It can therefore be inspected, edited, and resolved
// again against the same model-local registry during compilation.
type GoTextExpressionCodec struct{}

// ParseExpression decodes one GoModel expression and rejects trailing input.
func (GoTextExpressionCodec) ParseExpression(_ TextExpressionRole, text string) (Expression, error) {
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.DisallowUnknownFields()
	var expression Expression
	if err := decoder.Decode(&expression); err != nil {
		return Expression{}, fmt.Errorf("statecharts: Go expression: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("trailing JSON value")
		}
		return Expression{}, fmt.Errorf("statecharts: Go expression: %w", err)
	}
	if !isGoExpressionKind(expression.Kind) {
		return Expression{}, fmt.Errorf("statecharts: expression kind %q is not owned by GoModel", expression.Kind)
	}
	return expression.Clone(), nil
}

// FormatExpression encodes one GoModel expression without changing its
// canonical Value representation.
func (GoTextExpressionCodec) FormatExpression(_ TextExpressionRole, expression Expression) (string, error) {
	if !isGoExpressionKind(expression.Kind) {
		return "", fmt.Errorf("statecharts: expression kind %q is not owned by GoModel", expression.Kind)
	}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(expression.Clone()); err != nil {
		return "", fmt.Errorf("statecharts: Go expression: %w", err)
	}
	return strings.TrimSuffix(buffer.String(), "\n"), nil
}

func isGoExpressionKind(kind Identifier) bool {
	switch kind {
	case goConditionKind, goScriptKind, goValueKind, goLocationKind, goLiteralKind, goDataKind:
		return true
	default:
		return false
	}
}

// TextExpressionCodec returns the default text codec for this GoModel.
func (*GoModel[D]) TextExpressionCodec() TextExpressionCodec { return GoTextExpressionCodec{} }
