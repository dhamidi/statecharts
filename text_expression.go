package statecharts

// TextExpressionRole describes how a textual surface syntax will use an
// expression. Datamodel codecs may use the role to parse context-sensitive
// expression forms; models with one source language may ignore it.
type TextExpressionRole string

const (
	TextExpressionValue     TextExpressionRole = "value"
	TextExpressionCondition TextExpressionRole = "condition"
	TextExpressionLocation  TextExpressionRole = "location"
	TextExpressionScript    TextExpressionRole = "script"
	TextExpressionEvent     TextExpressionRole = "event"
	TextExpressionTarget    TextExpressionRole = "target"
	TextExpressionType      TextExpressionRole = "type"
	TextExpressionSendID    TextExpressionRole = "send-id"
	TextExpressionDelay     TextExpressionRole = "delay"
	TextExpressionLabel     TextExpressionRole = "label"
	TextExpressionArray     TextExpressionRole = "array"
)

// TextExpressionCodec is the optional bridge between a datamodel's canonical
// Expression values and expression text in a surface syntax. It is owned by
// the datamodel rather than by Definition or any particular syntax package.
type TextExpressionCodec interface {
	ParseExpression(role TextExpressionRole, text string) (Expression, error)
	FormatExpression(role TextExpressionRole, expression Expression) (string, error)
}
